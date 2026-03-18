from __future__ import annotations

from html import escape
from typing import List, Sequence

import folium
from folium import Element

from app.models.schemas import AssignmentItem, UnassignedItem


def route_map_html(coords: List[List[float]]) -> str:
    if not coords:
        raise ValueError("coords are empty")

    start = coords[0]
    end = coords[-1]

    fmap = folium.Map(location=[start[1], start[0]], zoom_start=12, control_scale=True)
    folium.Marker([start[1], start[0]], tooltip="Start", icon=folium.Icon(color="green")).add_to(fmap)
    folium.Marker([end[1], end[0]], tooltip="End", icon=folium.Icon(color="red")).add_to(fmap)
    folium.PolyLine([(lat, lon) for lon, lat in coords], color="blue", weight=4, opacity=0.8).add_to(fmap)

    return fmap.get_root().render()


def batch_plan_html(
    assignments: Sequence[AssignmentItem],
    unassigned: Sequence[UnassignedItem],
    summary: str | None = None,
    ai_summary: str | None = None,
) -> str:
    coords = [coord for item in assignments for coord in item.route_coords if len(coord) >= 2]
    if coords:
        avg_lon = sum(c[0] for c in coords) / len(coords)
        avg_lat = sum(c[1] for c in coords) / len(coords)
        zoom = 12
    else:
        avg_lon, avg_lat, zoom = 0.0, 0.0, 2

    fmap = folium.Map(location=[avg_lat, avg_lon], zoom_start=zoom, control_scale=True, height=520)

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

    for unit_id, items in grouped.items():
        items_sorted = sorted(items, key=lambda x: x.start_time)
        color = color_by_unit.setdefault(unit_id, palette[len(color_by_unit) % len(palette)])

        first_coords = items_sorted[0].route_coords if items_sorted else []
        if first_coords and len(first_coords[0]) >= 2:
            start_lon, start_lat = first_coords[0][0], first_coords[0][1]
            folium.Marker(
                [start_lat, start_lon],
                tooltip=f"unit {unit_id} start",
                icon=folium.Icon(color="green", icon="play"),
            ).add_to(fmap)

        for idx, item in enumerate(items_sorted, start=1):
            if not item.route_coords:
                continue
            latlon = [(coord[1], coord[0]) for coord in item.route_coords if len(coord) >= 2]
            if len(latlon) < 2:
                continue
            folium.PolyLine(
                latlon,
                color=color,
                weight=4,
                opacity=0.85,
                tooltip=f"task {item.task_id} / unit {unit_id}",
            ).add_to(fmap)
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
                tooltip=f"{item.task_id} (stop {idx})",
            ).add_to(fmap)

    table_html = _batch_table_html(assignments, unassigned, summary, ai_summary)
    fmap.get_root().html.add_child(Element(table_html))

    return fmap.get_root().render()


def _batch_table_html(
    assignments: Sequence[AssignmentItem],
    unassigned: Sequence[UnassignedItem],
    summary: str | None,
    ai_summary: str | None,
) -> str:
    assigned_rows = []
    for item in assignments:
        assigned_rows.append(
            "<tr>"
            f"<td>{escape(item.task_id)}</td>"
            f"<td>{item.wialon_id}</td>"
            f"<td>{item.eta_minutes}</td>"
            f"<td>{item.distance_km:.2f}</td>"
            f"<td>{item.score:.3f}</td>"
            f"<td>{escape(item.reason)}</td>"
            f"<td>{escape(item.start_time.isoformat())}</td>"
            f"<td>{escape(item.end_time.isoformat())}</td>"
            "</tr>"
        )
    if not assigned_rows:
        assigned_rows.append('<tr><td colspan="8">No assignments</td></tr>')

    unassigned_rows = []
    for item in unassigned:
        unassigned_rows.append(
            "<tr>"
            f"<td>{escape(item.task_id)}</td>"
            f"<td>{escape(item.reason)}</td>"
            "</tr>"
        )
    if not unassigned_rows:
        unassigned_rows.append('<tr><td colspan="2">No unassigned tasks</td></tr>')

    summary_text = escape(summary) if summary else "n/a"
    ai_text = escape(ai_summary) if ai_summary else ""
    return f"""
<style>
  body {{ margin: 0; font-family: "Segoe UI", Arial, sans-serif; color: #1f2933; }}
  .batch-wrap {{ padding: 16px 20px 28px; }}
  .batch-summary {{ margin: 0 0 12px; font-size: 14px; color: #52606d; }}
  .batch-ai {{ margin: 0 0 16px; padding: 10px 12px; background: #f1f5f9; border-left: 3px solid #334e68; }}
  table {{ width: 100%; border-collapse: collapse; margin: 8px 0 18px; font-size: 13px; }}
  th, td {{ border: 1px solid #d9e2ec; padding: 6px 8px; text-align: left; }}
  th {{ background: #f5f7fa; }}
  h2 {{ margin: 14px 0 8px; font-size: 16px; }}
</style>
<div class="batch-wrap">
  <div class="batch-summary">
    Summary: {summary_text}. Assigned: {len(assignments)}. Unassigned: {len(unassigned)}.
  </div>
  {f'<div class="batch-ai">AI: {ai_text}</div>' if ai_text else ''}
  <h2>Assigned tasks</h2>
  <table>
    <thead>
      <tr>
        <th>task_id</th>
        <th>wialon_id</th>
        <th>eta_min</th>
        <th>distance_km</th>
        <th>score</th>
        <th>reason</th>
        <th>start_time</th>
        <th>end_time</th>
      </tr>
    </thead>
    <tbody>
      {''.join(assigned_rows)}
    </tbody>
  </table>
  <h2>Unassigned tasks</h2>
  <table>
    <thead>
      <tr>
        <th>task_id</th>
        <th>reason</th>
      </tr>
    </thead>
    <tbody>
      {''.join(unassigned_rows)}
    </tbody>
  </table>
</div>
"""
