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
    points = []
    for item in assignments:
        points.append((item.start_lon, item.start_lat))
        points.append((item.end_lon, item.end_lat))
    if points:
        avg_lon = sum(p[0] for p in points) / len(points)
        avg_lat = sum(p[1] for p in points) / len(points)
        zoom = 10
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

    row_colors: Dict[str, str] = {}
    for item in assignments:
        color = color_by_unit.setdefault(item.wialon_id, palette[len(color_by_unit) % len(palette)])
        row_colors[item.task_id] = color

    lazy_js = _build_lazy_map_js(fmap.get_name())
    if lazy_js:
        fmap.get_root().script.add_child(Element(lazy_js))
    table_html = _batch_table_html(assignments, unassigned, summary, row_colors)
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


def _build_lazy_map_js(map_name: str) -> str:
    lines: List[str] = []
    lines.append("(function(){")
    lines.append("  window.addEventListener('load', function(){")
    lines.append(f"    const mapName = {json.dumps(map_name)};")
    lines.append("    const linesByTask = {};")
    lines.append("    let pinned = null;")
    lines.append("    const downsample = (coords, maxPoints=400) => {")
    lines.append("      if(!coords || coords.length <= maxPoints) return coords || [];")
    lines.append("      const step = Math.ceil(coords.length / maxPoints);")
    lines.append("      const sampled = [];")
    lines.append("      for(let i=0;i<coords.length;i+=step){ sampled.push(coords[i]); }")
    lines.append("      if(sampled[sampled.length-1] !== coords[coords.length-1]){ sampled.push(coords[coords.length-1]); }")
    lines.append("      return sampled;")
    lines.append("    };")
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
    lines.append("        const row = document.querySelector(`tr.assign-row[data-task=\"${taskId}\"]`);")
    lines.append("        const color = row ? (row.getAttribute('data-color') || '#1f77b4') : '#1f77b4';")
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
    lines.append("      const buildLine = (taskId, row, coords) => {")
    lines.append("        if(!coords || coords.length < 2) return;")
    lines.append("        const color = row.getAttribute('data-color') || '#1f77b4';")
    lines.append("        const sampled = downsample(coords);")
    lines.append("        const latlon = sampled.map(c => [c[1], c[0]]);")
    lines.append("        const line = L.polyline(latlon, {color: color, weight:4, opacity:0.85}).addTo(map);")
    lines.append("        linesByTask[taskId] = line;")
    lines.append("        line.on('mouseover', () => highlight(taskId));")
    lines.append("        line.on('mouseout', () => reset(taskId));")
    lines.append("        line.on('click', () => pin(taskId));")
    lines.append("      };")
    lines.append("      const rows = document.querySelectorAll('tr.assign-row[data-task]');")
    lines.append("      rows.forEach(row => {")
    lines.append("        const taskId = row.getAttribute('data-task');")
    lines.append("        if(!taskId) return;")
    lines.append("        row.addEventListener('mouseenter', () => { if(linesByTask[taskId]) highlight(taskId); });")
    lines.append("        row.addEventListener('mouseleave', () => { if(linesByTask[taskId]) reset(taskId); });")
    lines.append("        row.addEventListener('click', async () => {")
    lines.append("          if(linesByTask[taskId]){ pin(taskId); return; }")
    lines.append("          const startLon = row.getAttribute('data-start-lon');")
    lines.append("          const startLat = row.getAttribute('data-start-lat');")
    lines.append("          const endLon = row.getAttribute('data-end-lon');")
    lines.append("          const endLat = row.getAttribute('data-end-lat');")
    lines.append("          if(!startLon || !startLat || !endLon || !endLat) return;")
    lines.append("          row.classList.add('loading');")
    lines.append("          try {")
    lines.append("            const res = await fetch('/api/route', {")
    lines.append("              method: 'POST',")
    lines.append("              headers: { 'Content-Type': 'application/json' },")
    lines.append("              body: JSON.stringify({ from: { lon: parseFloat(startLon), lat: parseFloat(startLat) }, to: { lon: parseFloat(endLon), lat: parseFloat(endLat) } })")
    lines.append("            });")
    lines.append("            const data = await res.json();")
    lines.append("            if(!res.ok){ throw new Error(data.detail || 'route error'); }")
    lines.append("            buildLine(taskId, row, data.coords || []);")
    lines.append("            pin(taskId);")
    lines.append("          } catch (err) {")
    lines.append("            console.error(err);")
    lines.append("          } finally {")
    lines.append("            row.classList.remove('loading');")
    lines.append("          }")
    lines.append("        });")
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
    row_colors: Dict[str, str],
) -> str:
    assigned_rows = []
    for item in assignments:
        color = row_colors.get(item.task_id, "#1f77b4")
        assigned_rows.append(
            f'<tr class="assign-row" data-task="{escape(item.task_id)}" '
            f'data-start-lon="{item.start_lon:.6f}" data-start-lat="{item.start_lat:.6f}" '
            f'data-end-lon="{item.end_lon:.6f}" data-end-lat="{item.end_lat:.6f}" '
            f'data-color="{color}">'
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
