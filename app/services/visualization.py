from __future__ import annotations

from html import escape
import json
import math
from pathlib import Path
from string import Template
from typing import Dict, List, Sequence

import folium
from folium import Element

from app.models.schemas import AssignmentItem, UnassignedItem


_TEMPLATE_CACHE: Dict[str, str] = {}


def _load_template(name: str) -> str:
    cached = _TEMPLATE_CACHE.get(name)
    if cached is not None:
        return cached
    base_dir = Path(__file__).resolve().parent.parent
    path = base_dir / "templates" / name
    content = path.read_text(encoding="utf-8")
    _TEMPLATE_CACHE[name] = content
    return content


def route_map_html(coords: List[List[float]]) -> str:
    if not coords:
        raise ValueError("coords are empty")

    coords = _downsample_coords(coords)
    start = coords[0]
    end = coords[-1]

    fmap = folium.Map(
        location=[start[1], start[0]],
        zoom_start=12,
        control_scale=True,
        height="70vh",
        width="100%",
    )
    folium.Marker([start[1], start[0]], tooltip="Старт", icon=folium.Icon(color="green")).add_to(fmap)
    folium.Marker([end[1], end[0]], tooltip="Финиш", icon=folium.Icon(color="red")).add_to(fmap)
    folium.PolyLine([(lat, lon) for lon, lat in coords], color="blue", weight=4, opacity=0.8).add_to(fmap)

    return fmap.get_root().render()


def batch_plan_html(
    assignments: Sequence[AssignmentItem],
    unassigned: Sequence[UnassignedItem],
    summary: str | None = None,
) -> str:
    coords = [coord for item in assignments for coord in item.route_coords if len(coord) >= 2]
    if coords:
        avg_lon = sum(c[0] for c in coords) / len(coords)
        avg_lat = sum(c[1] for c in coords) / len(coords)
        zoom = 12
    else:
        avg_lon, avg_lat, zoom = 0.0, 0.0, 2

    fmap = folium.Map(
        location=[avg_lat, avg_lon],
        zoom_start=zoom,
        control_scale=True,
        height="70vh",
        width="100%",
    )

    palette = [
        "#1f77b4",
        "#ff7f0e",
        "#2ca02c",
        "#d62728",
        "#9467bd",
        "#8c564b",
        "#e377c2",
        "#7f7f7f",
        "#bcbd22",
        "#17becf",
    ]
    color_by_unit: dict[int, str] = {}

    grouped: dict[int, list[AssignmentItem]] = {}
    for item in assignments:
        grouped.setdefault(item.wialon_id, []).append(item)

    polyline_vars: Dict[str, str] = {}
    polyline_colors: Dict[str, str] = {}

    for unit_id, items in grouped.items():
        items_sorted = sorted(items, key=lambda x: x.start_time)
        color = color_by_unit.setdefault(unit_id, palette[len(color_by_unit) % len(palette)])

        first_coords = items_sorted[0].route_coords if items_sorted else []
        if first_coords and len(first_coords[0]) >= 2:
            start_lon, start_lat = first_coords[0][0], first_coords[0][1]
            folium.Marker(
                [start_lat, start_lon],
                tooltip=f"старт техники {unit_id}",
                icon=folium.Icon(color="green", icon="play"),
            ).add_to(fmap)

        for idx, item in enumerate(items_sorted, start=1):
            if not item.route_coords:
                continue
            sampled = _downsample_coords(item.route_coords)
            latlon = [(coord[1], coord[0]) for coord in sampled if len(coord) >= 2]
            if len(latlon) < 2:
                continue
            line = folium.PolyLine(
                latlon,
                color=color,
                weight=4,
                opacity=0.85,
                tooltip=f"заявка {item.task_id} / техника {unit_id}",
            ).add_to(fmap)
            polyline_vars[item.task_id] = line.get_name()
            polyline_colors[item.task_id] = color
            end = latlon[-1]
            folium.Marker(
                location=end,
                icon=folium.DivIcon(
                    html=(
                        f'<div style="font-size: 12px; color: white; background: {color}; '
                        f'border-radius: 12px; width: 22px; height: 22px; '
                        f'line-height: 22px; text-align: center;">{idx}</div>'
                    )
                ),
                tooltip=f"{item.task_id} (точка {idx})",
            ).add_to(fmap)

    polyline_js = _build_polyline_js(polyline_vars, polyline_colors, fmap.get_name())
    if polyline_js:
        fmap.get_root().script.add_child(Element(polyline_js))
    table_html = _batch_table_html(assignments, unassigned, summary)
    fmap.get_root().html.add_child(Element(table_html))

    return fmap.get_root().render()


def _downsample_coords(coords: List[List[float]], max_points: int = 250) -> List[List[float]]:
    if len(coords) <= max_points:
        return coords
    step = int(math.ceil(len(coords) / max_points))
    sampled = coords[::step]
    if sampled[-1] != coords[-1]:
        sampled.append(coords[-1])
    return sampled


def _build_polyline_js(
    polyline_vars: Dict[str, str],
    polyline_colors: Dict[str, str],
    map_name: str,
) -> str:
    if not polyline_vars:
        return ""

    lines: List[str] = []
    lines.append("(function(){")
    lines.append("  window.addEventListener('load', function(){")
    lines.append(f"    const mapName = {json.dumps(map_name)};")
    lines.append("    const linesByTask = {};")
    for task_id, var_name in polyline_vars.items():
        lines.append(f"    linesByTask[{json.dumps(task_id)}] = {var_name};")
    lines.append(f"    const colors = {json.dumps(polyline_colors)};")
    lines.append("    let pinned = null;")
    lines.append("    const waitForMap = () => {")
    lines.append("      const map = window[mapName];")
    lines.append("      if(!map){ setTimeout(waitForMap, 120); return; }")
    lines.append("      const setActive = (taskId, on) => {")
    lines.append("        const row = document.querySelector(`tr.assign-row[data-task=\"${taskId}\"]`);")
    lines.append("        if(!row) return;")
    lines.append("        if(on){ row.classList.add('active'); } else { row.classList.remove('active'); }")
    lines.append("      };")
    lines.append("      const highlight = (taskId) => {")
    lines.append("        const line = linesByTask[taskId];")
    lines.append("        if(!line) return;")
    lines.append("        line.setStyle({color:'#ffd400', weight:7, opacity:1});")
    lines.append("        setActive(taskId, true);")
    lines.append("      };")
    lines.append("      const reset = (taskId, force=false) => {")
    lines.append("        if(!force && pinned === taskId) return;")
    lines.append("        const line = linesByTask[taskId];")
    lines.append("        if(!line) return;")
    lines.append("        const color = colors[taskId] || '#1f77b4';")
    lines.append("        line.setStyle({color:color, weight:4, opacity:0.85});")
    lines.append("        setActive(taskId, false);")
    lines.append("      };")
    lines.append("      const pin = (taskId) => {")
    lines.append("        if(pinned && pinned !== taskId){ reset(pinned, true); }")
    lines.append("        pinned = taskId;")
    lines.append("        highlight(taskId);")
    lines.append("        const line = linesByTask[taskId];")
    lines.append("        if(line && line.getBounds){ map.fitBounds(line.getBounds(), {padding:[20,20]}); }")
    lines.append("      };")
    lines.append("      const rows = document.querySelectorAll('tr.assign-row[data-task]');")
    lines.append("      rows.forEach(row => {")
    lines.append("        const taskId = row.getAttribute('data-task');")
    lines.append("        if(!linesByTask[taskId]) return;")
    lines.append("        row.addEventListener('mouseenter', () => highlight(taskId));")
    lines.append("        row.addEventListener('mouseleave', () => reset(taskId));")
    lines.append("        row.addEventListener('click', () => pin(taskId));")
    lines.append("      });")
    lines.append("      Object.keys(linesByTask).forEach(taskId => {")
    lines.append("        const line = linesByTask[taskId];")
    lines.append("        line.on('mouseover', () => highlight(taskId));")
    lines.append("        line.on('mouseout', () => reset(taskId));")
    lines.append("        line.on('click', () => pin(taskId));")
    lines.append("      });")
    lines.append("    };")
    lines.append("    waitForMap();")
    lines.append("  });")
    lines.append("})();")
    return "\n".join(lines)


def _batch_table_html(
    assignments: Sequence[AssignmentItem],
    unassigned: Sequence[UnassignedItem],
    summary: str | None,
) -> str:
    assigned_rows = []
    for item in assignments:
        assigned_rows.append(
            f'<tr class="assign-row" data-task="{escape(item.task_id)}">'
            f"<td>{escape(item.task_id)}</td>"
            f"<td>{item.wialon_id}</td>"
            f"<td>{item.eta_minutes}</td>"
            f"<td>{item.distance_km:.2f}</td>"
            f"<td>{item.score:.3f}</td>"
            f"<td>{escape(item.reason)}</td>"
            f"<td>{escape(item.planned_start.isoformat())}</td>"
            f"<td>{escape(item.start_time.isoformat())}</td>"
            f"<td>{escape(item.end_time.isoformat())}</td>"
            "</tr>"
        )
    if not assigned_rows:
        assigned_rows.append('<tr><td colspan="9">Нет назначений</td></tr>')

    unassigned_rows = []
    for item in unassigned:
        unassigned_rows.append(
            "<tr>"
            f"<td>{escape(item.task_id)}</td>"
            f"<td>{escape(item.reason)}</td>"
            "</tr>"
        )
    if not unassigned_rows:
        unassigned_rows.append('<tr><td colspan="2">Нет неназначенных задач</td></tr>')

    summary_text = escape(summary) if summary else "н/д"
    template = Template(_load_template("batch_plan.html"))
    return template.safe_substitute(
        summary_text=summary_text,
        assigned_count=str(len(assignments)),
        unassigned_count=str(len(unassigned)),
        assigned_rows="".join(assigned_rows),
        unassigned_rows="".join(unassigned_rows),
    )
