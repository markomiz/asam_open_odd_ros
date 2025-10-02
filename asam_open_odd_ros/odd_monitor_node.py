from __future__ import annotations
from typing import Dict, List, Tuple, Optional
import threading

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy

from asam_open_odd_core.openodd_yaml_parser import load_openodd_yaml
from asam_open_odd_core.openodd_types import TaxonomyValue as CoreTaxVal
from asam_open_odd_core.openodd_evaluator import evaluate_odd
from asam_open_odd_core.openodd_evaluator import DictValueResolver

from asam_open_odd_msgs.msg import Cod as CodMsg, OddReport as OddReportMsg
from .converters import cod_to_msg, report_to_msg
from .mappings import load_mappings, get_ros_message_type, extract_field, transform_value


class OddMonitorNode(Node):
    def __init__(self):
        super().__init__("odd_monitor")

        # params
        odd_yaml_path = self.declare_parameter(
            "odd_yaml", "").get_parameter_value().string_value
        taxonomy_yaml_path = self.declare_parameter(
            "taxonomy_yaml", "").get_parameter_value().string_value
        mappings_yaml_path = self.declare_parameter(
            "mappings_yaml", "").get_parameter_value().string_value
        publish_hz = float(self.declare_parameter(
            "publish_hz", 5.0).get_parameter_value().double_value)

        if not odd_yaml_path or not taxonomy_yaml_path or not mappings_yaml_path:
            raise RuntimeError(
                "Parameters 'odd_yaml', 'taxonomy_yaml', and 'mappings_yaml' are required.")

        # load YAMLs
        tax, _ = load_openodd_yaml(open(taxonomy_yaml_path, "r").read())
        _, odd = load_openodd_yaml(open(odd_yaml_path, "r").read())
        if tax is None or odd is None:
            raise RuntimeError("Failed to parse taxonomy or ODD YAML.")

        self._taxonomy = tax
        self._odd = odd
        self._mappings = load_mappings(mappings_yaml_path)

        # Subscriptions (group by topic)
        self._topic_to_mappings: Dict[str, List] = {}
        for m in self._mappings:
            self._topic_to_mappings.setdefault(m.topic, []).append(m)

        qos = QoSProfile(depth=10)
        qos.reliability = ReliabilityPolicy.BEST_EFFORT
        qos.history = HistoryPolicy.KEEP_LAST

        self._subs = []
        for topic, maps in self._topic_to_mappings.items():
            msg_type = get_ros_message_type(
                maps[0].msg_type)  # assume same type per topic
            self._subs.append(
                self.create_subscription(
                    msg_type, topic, lambda msg, t=topic: self._on_msg(
                        t, msg), qos
                )
            )
            self.get_logger().info(
                f"Subscribed: {topic} [{maps[0].msg_type}] x{len(maps)} mappings")

        # COD store
        self._lock = threading.Lock()
        # path tuple -> (value, unit_id)
        self._values: Dict[Tuple[str, ...], Tuple[object, Optional[str]]] = {}

        # Publishers
        self._pub_cod = self.create_publisher(CodMsg, "~/cod", 10)
        self._pub_report = self.create_publisher(OddReportMsg, "~/report", 10)

        # Timer
        self._timer = self.create_timer(
            1.0 / max(publish_hz, 0.1), self._on_tick)

    def _on_msg(self, topic: str, msg):
        maps = self._topic_to_mappings.get(topic, [])
        with self._lock:
            for m in maps:
                try:
                    raw = extract_field(msg, m.field)
                except Exception as e:
                    self.get_logger().warn(
                        f"extract_field failed for {topic}.{m.field}: {e}")
                    continue
                val = transform_value(raw, scale=m.scale,
                                      offset=m.offset, enum_map=m.enum_map)
                self._values[m.taxonomy] = (val, m.unit_id)

    def _build_cod_values(self) -> List[CoreTaxVal]:
        vals: List[CoreTaxVal] = []
        with self._lock:
            for path, (val, unit_id) in self._values.items():
                vals.append(CoreTaxVal(taxonomy_path=path,
                            value=val, unit_id=unit_id))
        return vals

    def _on_tick(self):
        cod_values = self._build_cod_values()
        # publish COD
        cod_msg = cod_to_msg(cod_values, frame_id="map",
                             now=self.get_clock().now())
        self._pub_cod.publish(cod_msg)

        # evaluate ODD
        resolver = DictValueResolver(
            {".".join(k): v for k, v in self._values.items()})
        report = evaluate_odd(self._odd, resolver)
        rep_msg = report_to_msg(report, frame_id="map",
                                now=self.get_clock().now())
        self._pub_report.publish(rep_msg)
        print(report)


def main(args=None):
    rclpy.init(args=args)
    node = OddMonitorNode()
    try:
        rclpy.spin(node)
    finally:
        node.destroy_node()
        rclpy.shutdown()
