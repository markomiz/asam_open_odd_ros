from __future__ import annotations
from typing import List, Optional, Union, Tuple, Dict

from rclpy.time import Time
from std_msgs.msg import Header

from asam_open_odd_core.openodd_types import TaxonomyValue as CoreTaxVal
from asam_open_odd_core.openodd_evaluator import (
    ODDReport as CoreReport,
    ModuleReport as CoreModuleReport,
    SectionCheck, GroupCheck, LeafCheck,
)

from asam_open_odd_msgs.msg import (
    TaxonomyValue, Cod, OddReport, ModuleReport, CheckNode, LabelTruth
)

# ---- COD conversion ----


def _fill_value(tv_msg: TaxonomyValue, value) -> None:
    if isinstance(value, bool):
        tv_msg.value_type = TaxonomyValue.VALUE_BOOL
        tv_msg.bool_value = bool(value)
    elif isinstance(value, int):
        tv_msg.value_type = TaxonomyValue.VALUE_INT
        tv_msg.int_value = int(value)
    elif isinstance(value, float):
        tv_msg.value_type = TaxonomyValue.VALUE_FLOAT
        tv_msg.float_value = float(value)
    elif isinstance(value, str):
        tv_msg.value_type = TaxonomyValue.VALUE_STRING
        tv_msg.string_value = value
    else:
        tv_msg.value_type = TaxonomyValue.VALUE_NONE


def to_msg_taxonomy_value(tv: CoreTaxVal) -> TaxonomyValue:
    m = TaxonomyValue()
    m.id = "/".join(tv.taxonomy_path)  # keeping id as slash path
    m.path = ".".join(tv.taxonomy_path)
    _fill_value(m, tv.value)
    m.unit_id = tv.unit_id or ""
    m.status = TaxonomyValue.STATUS_OK
    return m


def cod_to_msg(values: List[CoreTaxVal], frame_id: str = "", now: Optional[Time] = None) -> Cod:
    msg = Cod()
    msg.header = Header()
    if now is not None:
        msg.header.stamp = now.to_msg()
    msg.header.frame_id = frame_id
    msg.values = [to_msg_taxonomy_value(v) for v in values]
    # Spatial left empty for now
    return msg

# ---- ODDReport conversion ----


def _next_id(counter: List[int]) -> int:
    counter[0] += 1
    return counter[0]


def _leaf_to_node(id_: int, parent_id: int, leaf: LeafCheck) -> CheckNode:
    n = CheckNode()
    n.id = id_
    n.parent_id = parent_id
    n.node_type = CheckNode.NODE_LEAF
    n.passed = leaf.passed
    n.target = leaf.target
    n.predicate = leaf.predicate
    # observed value
    if isinstance(leaf.value, bool):
        n.value_type = CheckNode.VALUE_BOOL
        n.bool_value = bool(leaf.value)
    elif isinstance(leaf.value, int):
        n.value_type = CheckNode.VALUE_INT
        n.int_value = int(leaf.value)
    elif isinstance(leaf.value, float):
        n.value_type = CheckNode.VALUE_FLOAT
        n.float_value = float(leaf.value)
    elif isinstance(leaf.value, str):
        n.value_type = CheckNode.VALUE_STRING
        n.string_value = leaf.value
    else:
        n.value_type = CheckNode.VALUE_NONE
    n.unit_id = leaf.unit_id or ""
    return n


def _dump_section(section: SectionCheck) -> Tuple[int, List[CheckNode]]:
    nodes: List[CheckNode] = []
    cid = [0]  # simple counter in list

    def walk(node, parent_id: int) -> int:
        if isinstance(node, LeafCheck):
            nid = _next_id(cid)
            nodes.append(_leaf_to_node(nid, parent_id, node))
            return nid
        # Group or Section share fields: op + results + passed
        nid = _next_id(cid)
        n = CheckNode()
        n.id = nid
        n.parent_id = parent_id
        n.node_type = CheckNode.NODE_SECTION
        n.section_op = CheckNode.SECTION_AND if node.op.name == "AND" else CheckNode.SECTION_OR
        n.passed = node.passed
        nodes.append(n)
        for ch in node.results:
            walk(ch, nid)
        return nid

    root_id = walk(section, -1)
    return root_id, nodes


def report_to_msg(report: CoreReport, frame_id: str = "", now: Optional[Time] = None) -> OddReport:
    msg = OddReport()
    msg.header = Header()
    if now is not None:
        msg.header.stamp = now.to_msg()
    msg.header.frame_id = frame_id

    # modules
    for mid, rep in report.module_reports.items():
        m = ModuleReport()
        m.module_id = mid
        m.truth = rep.truth
        m.reasons = list(rep.reasons)
        if rep.include:
            rid, nodes = _dump_section(rep.include)
            m.include_root_id = rid
            m.include_nodes = nodes
        if rep.exclude:
            rid, nodes = _dump_section(rep.exclude)
            m.exclude_root_id = rid
            m.exclude_nodes = nodes
        msg.modules.append(m)

    # labels
    for lab, val in sorted(report.label_truth.items()):
        lt = LabelTruth()
        lt.label = lab
        lt.value = bool(val)
        msg.labels.append(lt)

    return msg
