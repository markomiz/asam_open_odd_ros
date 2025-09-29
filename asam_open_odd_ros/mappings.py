from __future__ import annotations
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple, Any
import yaml
from rosidl_runtime_py.utilities import get_message


@dataclass
class Mapping:
    topic: str
    msg_type: str
    field: str                  # dot path in the message
    taxonomy: Tuple[str, ...]   # path segments
    unit_id: Optional[str]
    scale: float
    offset: float
    enum_map: Dict[str, Any]    # map incoming to literals or values


def load_mappings(path: str) -> List[Mapping]:
    data = yaml.safe_load(open(path, "r")) or {}
    mappings = []
    for i, m in enumerate(data.get("mappings", [])):
        topic = str(m["topic"])
        msg_type = str(m["type"])
        field = str(m.get("field", "data"))
        taxonomy = tuple(str(s) for s in str(
            m["taxonomy"]).replace("/", ".").split(".") if s)
        unit_id = m.get("unit_id")
        scale = float(m.get("scale", 1.0))
        offset = float(m.get("offset", 0.0))
        enum_map = {str(k): v for (k, v) in (m.get("enum_map") or {}).items()}
        mappings.append(Mapping(topic, msg_type, field,
                        taxonomy, unit_id, scale, offset, enum_map))
    return mappings


def get_ros_message_type(type_str: str):
    # e.g. "std_msgs/msg/Float32"
    return get_message(type_str)


def extract_field(msg: Any, field_path: str) -> Any:
    cur = msg
    for name in (s for s in field_path.split(".") if s):
        cur = getattr(cur, name)
    return cur


def transform_value(raw: Any, *, scale: float, offset: float, enum_map: Dict[str, Any]) -> Any:
    if enum_map:
        # normalize key to string for mapping
        key = str(raw)
        return enum_map.get(key, key)
    if isinstance(raw, (int, float)):
        return raw * scale + offset
    return raw
