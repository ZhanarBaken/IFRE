from __future__ import annotations

from typing import Dict, Iterable, Set, Tuple

from app.models.schemas import Compatibility
from app.utils.normalize import normalize_task_type, normalize_unit_type


def build_compat_index(
    rules: Iterable[Compatibility],
) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
    compat: Dict[str, Set[str]] = {}
    compat_norm: Dict[str, Set[str]] = {}
    for rule in rules:
        compat.setdefault(rule.task_type, set()).add(rule.unit_type)
        t_norm = normalize_task_type(rule.task_type)
        u_norm = normalize_unit_type(rule.unit_type)
        if t_norm and u_norm:
            compat_norm.setdefault(t_norm, set()).add(u_norm)
    return compat, compat_norm


def compatibility_status(
    task_type: str | None,
    unit_type: str | None,
    compat: Dict[str, Set[str]],
    compat_norm: Dict[str, Set[str]],
) -> bool | None:
    if not task_type or task_type == "unknown":
        return True
    if not unit_type or unit_type == "unknown":
        return None
    if task_type in compat and unit_type in compat[task_type]:
        return True
    t_norm = normalize_task_type(task_type)
    u_norm = normalize_unit_type(unit_type)
    if t_norm and u_norm and t_norm in compat_norm and u_norm in compat_norm[t_norm]:
        return True
    return False
