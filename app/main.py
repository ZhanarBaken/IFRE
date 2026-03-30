from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from urllib.parse import urlencode
import json


class UTF8JSONResponse(JSONResponse):
    def render(self, content) -> bytes:
        return json.dumps(content, ensure_ascii=False, allow_nan=False).encode("utf-8")

from app.core.config import settings
from app.data.repositories import get_repository
from app.models.schemas import (
    AssignmentRequest,
    AssignmentResponse,
    CompareResponse,
    CompareSummary,
    MultitaskRequest,
    MultitaskResponse,
    MatrixRequest,
    MatrixResponse,
    RecommendationRequest,
    RecommendationResponse,
    RouteRequest,
    RouteResponse,
    TaskFilters,
    Task,
    UnitStateResponse,
    WellResponse,
)
from app.services.assignments import AssignmentService
from app.services.fleet_state import FleetStateService
from app.services.multitask import MultitaskService
from app.services.recommendations import RecommendationService
from app.services.routing import RoutingService
from app.services.visualization import batch_plan_html, route_map_html
from app.utils.graph import Graph, NodeIndex, should_make_bidirectional


app = FastAPI(title="IFRE Routing Service", version="0.1.0", default_response_class=UTF8JSONResponse)

PLANNING_MODE_MINUTES: dict[str, int] = {
    "shift_8": 480,
    "shift_12": 720,
    "day": 1440,
    "unlimited": 10_000_000,
    "custom": 480,  # fallback if field is empty
}
PLANNING_MODE_DEFAULT = "day"


def resolve_max_total(
    planning_mode: str | None,
    explicit_minutes: int | None,
) -> int:
    """Return max_total_time_minutes: explicit value wins over mode; mode wins over default."""
    if explicit_minutes is not None:
        return explicit_minutes
    mode = planning_mode or PLANNING_MODE_DEFAULT
    if mode not in PLANNING_MODE_MINUTES:
        raise ValueError(f"Unknown planning_mode '{mode}'. Valid: {list(PLANNING_MODE_MINUTES)}")
    return PLANNING_MODE_MINUTES[mode]


@app.on_event("startup")
def startup() -> None:
    repo = get_repository()
    nodes = repo.road_nodes()
    edges = repo.road_edges()
    bidirectional = settings.graph_bidirectional
    if bidirectional is None:
        bidirectional = should_make_bidirectional(edges, settings.graph_bidirectional_threshold)
    graph = Graph(nodes, edges, bidirectional=bidirectional)
    node_index = NodeIndex(nodes)
    routing = RoutingService(repo, graph, node_index)
    recommendations = RecommendationService(repo, routing)
    multitask = MultitaskService(repo, routing)
    assignments = AssignmentService(repo, routing)

    fleet_state = FleetStateService(repo, routing)

    app.state.repo = repo
    app.state.graph = graph
    app.state.node_index = node_index
    app.state.routing = routing
    app.state.recommendations = recommendations
    app.state.multitask = multitask
    app.state.assignments = assignments
    app.state.fleet_state = fleet_state


@app.post("/api/route", response_model=RouteResponse)
async def route(req: RouteRequest):
    try:
        if req.from_.wialon_id is not None:
            unit = app.state.repo.unit_by_id(req.from_.wialon_id)
            if unit is None:
                raise ValueError("unit not found")
            start_lon, start_lat = unit.pos_x, unit.pos_y
        elif req.from_.lon is not None and req.from_.lat is not None:
            start_lon, start_lat = req.from_.lon, req.from_.lat
        else:
            raise ValueError("from: provide wialon_id or lon/lat")

        if req.to.uwi is not None:
            well = app.state.repo.well_by_uwi(req.to.uwi)
            if well is None:
                raise ValueError("well not found")
            end_lon, end_lat = well.lon, well.lat
        elif req.to.lon is not None and req.to.lat is not None:
            end_lon, end_lat = req.to.lon, req.to.lat
        else:
            raise ValueError("to: provide uwi or lon/lat")

        payload = app.state.routing.route_between_points(start_lon, start_lat, end_lon, end_lat)
        return payload
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/matrix", response_model=MatrixResponse)
async def matrix(req: MatrixRequest):
    try:
        start_nodes = list(req.start_nodes or [])
        end_nodes = list(req.end_nodes or [])

        if req.from_wialon_ids:
            for wid in req.from_wialon_ids:
                unit = app.state.repo.unit_by_id(wid)
                if unit is None:
                    raise ValueError(f"unit not found: {wid}")
                node = app.state.node_index.nearest(unit.pos_x, unit.pos_y)
                if node not in start_nodes:
                    start_nodes.append(node)

        if req.to_uwis:
            for uwi in req.to_uwis:
                well = app.state.repo.well_by_uwi(uwi)
                if well is None:
                    raise ValueError(f"well not found: {uwi}")
                node = app.state.node_index.nearest(well.lon, well.lat)
                if node not in end_nodes:
                    end_nodes.append(node)

        if not start_nodes or not end_nodes:
            raise ValueError("provide start_nodes/end_nodes or from_wialon_ids/to_uwis")

        result = app.state.routing.distance_time_matrix(start_nodes, end_nodes)
        items = []
        for (start_node, end_node), (distance_km, time_minutes) in result.items():
            items.append(
                {
                    "start_node": start_node,
                    "end_node": end_node,
                    "distance_km": distance_km,
                    "time_minutes": time_minutes,
                }
            )
        return {"items": items}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/recommendations", response_model=RecommendationResponse)
async def recommendations(req: RecommendationRequest, top_units: int = 3):
    try:
        units = await asyncio.to_thread(
            app.state.recommendations.recommend,
            task_id=req.task_id,
            priority=req.priority,
            destination_uwi=req.destination_uwi,
            task_type=req.task_type,
            planned_start=req.planned_start,
            duration_hours=req.duration_hours,
            mode=req.mode,
            exclude_busy=req.exclude_busy,
            top_n=max(1, top_units),
        )
        return {"units": units}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/multitask", response_model=MultitaskResponse)
async def multitask(req: MultitaskRequest):
    try:
        constraints = req.constraints
        max_total = resolve_max_total(
            req.planning_mode,
            constraints.max_total_time_minutes if constraints else None,
        )
        max_detour = (
            constraints.max_detour_ratio
            if constraints and constraints.max_detour_ratio is not None
            else 1.3
        )
        payload = await asyncio.to_thread(
            app.state.multitask.evaluate, req.task_ids, req.filters, max_total, max_detour
        )
        return payload
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/assignments", response_model=AssignmentResponse)
async def assignments(req: AssignmentRequest):
    try:
        constraints = req.constraints
        max_total = resolve_max_total(
            req.planning_mode,
            constraints.max_total_time_minutes if constraints else None,
        )
        max_detour = (
            constraints.max_detour_ratio
            if constraints and constraints.max_detour_ratio is not None
            else 1.3
        )
        payload = await asyncio.to_thread(
            app.state.assignments.plan,
            req.task_ids, req.filters, max_total, max_detour, req.grouping,
        )
        return payload
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/assignments/compare", response_model=CompareResponse)
async def assignments_compare(req: AssignmentRequest):
    """Run both greedy (baseline) and OR-Tools (optimized) and return side-by-side comparison."""
    try:
        constraints = req.constraints
        max_total = resolve_max_total(
            req.planning_mode,
            constraints.max_total_time_minutes if constraints else None,
        )
        max_detour = (
            constraints.max_detour_ratio
            if constraints and constraints.max_detour_ratio is not None
            else 1.3
        )

        baseline_resp, optimized_resp = await asyncio.gather(
            asyncio.to_thread(
                app.state.assignments.plan,
                req.task_ids, req.filters, max_total, max_detour, False, False,
            ),
            asyncio.to_thread(
                app.state.assignments.plan,
                req.task_ids, req.filters, max_total, max_detour, False, True,
            ),
        )

        def summarize(resp, algorithm: str) -> CompareSummary:
            scores = [a.score for a in resp.assignments]
            return CompareSummary(
                algorithm=algorithm,
                assigned=len(resp.assignments),
                unassigned=len(resp.unassigned),
                avg_score=round(sum(scores) / len(scores), 1) if scores else 0.0,
                total_distance_km=round(sum(a.distance_km for a in resp.assignments), 2),
                vehicles_used=len({a.wialon_id for a in resp.assignments}),
            )

        base = summarize(baseline_resp, "greedy")
        opt = summarize(optimized_resp, "ortools")

        score_imp = (
            round((opt.avg_score - base.avg_score) / base.avg_score * 100, 1)
            if base.avg_score > 0 else 0.0
        )
        dist_imp = (
            round((base.total_distance_km - opt.total_distance_km) / base.total_distance_km * 100, 1)
            if base.total_distance_km > 0 else 0.0
        )

        return CompareResponse(
            baseline=base,
            optimized=opt,
            score_improvement_pct=score_imp,
            distance_improvement_pct=dist_imp,
            vehicles_saved=base.vehicles_used - opt.vehicles_used,
            baseline_assignments=baseline_resp.assignments,
            optimized_assignments=optimized_resp.assignments,
            baseline_unassigned=baseline_resp.unassigned,
            optimized_unassigned=optimized_resp.unassigned,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/tasks", response_model=list[Task])
async def tasks(
    task_ids: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    shift: str | None = None,
    limit: int = 50,
):
    try:
        parsed_ids = None
        if task_ids:
            parsed_ids = [item.strip() for item in task_ids.split(",") if item.strip()]
        if parsed_ids:
            return app.state.repo.tasks_by_ids(parsed_ids)

        if start_date is None and end_date is None and shift is None:
            # Fallback: return first N tasks
            return app.state.repo.tasks()[:limit]

        if shift:
            if not start_date:
                raise RuntimeError("start_date is required when shift is set")
            if end_date and end_date != start_date:
                raise RuntimeError("shift filter supports only a single date")
            if shift == "day":
                start_dt = datetime.combine(start_date, datetime.min.time()).replace(hour=8)
                end_dt = datetime.combine(start_date, datetime.min.time()).replace(hour=20)
            elif shift == "night":
                start_dt = datetime.combine(start_date, datetime.min.time()).replace(hour=20)
                end_dt = datetime.combine(start_date, datetime.min.time()).replace(hour=8) + timedelta(days=1)
            else:
                raise RuntimeError("shift must be 'day' or 'night'")
            return app.state.repo.tasks_by_window(start_dt, end_dt, limit=limit)

        if not start_date:
            raise RuntimeError("start_date is required")
        end = end_date or start_date
        start_dt = datetime.combine(start_date, datetime.min.time())
        end_dt = datetime.combine(end + timedelta(days=1), datetime.min.time())
        return app.state.repo.tasks_by_window(start_dt, end_dt, limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/tasks/debug")
async def tasks_debug(limit: int = 50):
    try:
        # Ensure tasks are loaded to populate debug cache for EAV sources
        _ = app.state.repo.tasks()
        all_items = app.state.repo.tasks_debug(limit=None)
        total = len(all_items)
        items = all_items[:limit] if limit is not None else all_items
        return {"total": total, "items": items}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/units", response_model=list[UnitStateResponse])
async def units_list(planning_time: datetime | None = None):
    try:
        state = app.state.fleet_state.build_state(anchor_time=planning_time)
        return [
            UnitStateResponse(
                wialon_id=u.wialon_id,
                name=u.name,
                unit_type=u.unit_type,
                lon=u.lon,
                lat=u.lat,
                available_at=u.available_at,
                speed_kmph=u.speed_kmph,
            )
            for u in state.values()
        ]
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/wells", response_model=list[WellResponse])
async def wells_list(uwi: str | None = None):
    try:
        if uwi:
            uwis = [item.strip() for item in uwi.split(",") if item.strip()]
            wells = app.state.repo.wells_by_uwi(uwis)
        else:
            wells = app.state.repo.wells()
        return [WellResponse(uwi=w.uwi, lon=w.lon, lat=w.lat, well_name=w.well_name) for w in wells]
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/health")
async def health():
    return {
        "status": "ok",
    }


@app.get("/demo/route-map", response_class=HTMLResponse)
async def demo_route_map(wialon_id: int, uwi: str):
    try:
        unit = app.state.repo.unit_by_id(wialon_id)
        if unit is None:
            raise ValueError("unit not found")
        well = app.state.repo.well_by_uwi(uwi)
        if well is None:
            raise ValueError("well not found")
        route = app.state.routing.route_between_points(unit.pos_x, unit.pos_y, well.lon, well.lat)
        return route_map_html(route["coords"])
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/demo/batch-plan", response_class=HTMLResponse)
async def demo_batch_plan(
    request: Request,
    task_ids: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    shift: str | None = None,
    limit: int = 20,
    planning_mode: str | None = None,
    max_total_time_minutes: int | None = None,
    max_detour_ratio: float | None = None,
    max_detour_pct: int | None = None,
    grouping: bool | None = None,
    embed: bool = False,
):
    try:
        if not embed:
            sd = start_date.isoformat() if start_date else ""
            ed = end_date.isoformat() if end_date else ""
            pm = planning_mode or "day"
            lim = limit
            grp = "true" if grouping is not False else "false"
            tid = task_ids or ""
            if max_detour_pct is not None:
                dr = 1.0 + max_detour_pct / 100.0
            else:
                dr = max_detour_ratio if max_detour_ratio is not None else 1.3
            html = f"""
<!doctype html>
<html lang="ru">
  <head>
    <meta charset="utf-8" />
    <title>IFRE — Планирование выездов</title>
    <style>
      *, *::before, *::after {{ box-sizing: border-box; }}
      html, body {{ height: 100%; margin: 0; }}
      body {{ font-family: "Segoe UI", Arial, sans-serif; background: #f1f5f9; display: flex; flex-direction: column; }}
      .panel {{
        background: #fff; border-bottom: 1px solid #d9e2ec;
        padding: 16px 24px; display: flex; align-items: center; gap: 20px; flex-wrap: wrap;
      }}
      .brand {{
        font-size: 15px; font-weight: 700; color: #1e3a5f; white-space: nowrap;
        background: #e8f0fe; border-radius: 6px; padding: 6px 12px;
        border: 1px solid #c7d7f9; letter-spacing: .3px; height: 34px;
        display: flex; align-items: center;
      }}
      .sep-v {{
        width: 1px; height: 28px; background: #d9e2ec; flex-shrink: 0;
      }}
      .field {{ display: flex; flex-direction: column; gap: 3px; }}
      .field label {{ font-size: 10px; color: #52606d; font-weight: 700; text-transform: uppercase; letter-spacing: .5px; }}
      .field input, .field select {{
        height: 34px; border: 1px solid #c8d5e3; border-radius: 6px; padding: 0 10px;
        font-size: 13px; color: #1f2933; background: #f8fafc; outline: none;
      }}
      .field input:focus, .field select:focus {{ border-color: #2563eb; background: #fff; }}
      .or-badge {{
        font-size: 11px; font-weight: 700; color: #94a3b8; background: #f1f5f9;
        border: 1px solid #d9e2ec; border-radius: 20px; padding: 2px 8px;
        white-space: nowrap; align-self: center; margin-top: 16px;
      }}
      .run-btn {{
        background: #2563eb; color: #fff; border: none; border-radius: 6px;
        height: 34px; padding: 0 22px; font-size: 14px; font-weight: 600;
        cursor: pointer; white-space: nowrap; margin-top: 16px;
      }}
      .run-btn:hover {{ background: #1d4ed8; }}
      #loading {{
        position: fixed; inset: 0; top: 60px; display: none; align-items: center; justify-content: center;
        background: rgba(241,245,249,.85); color: #1f2933; z-index: 9999; flex-direction: column; gap: 10px;
      }}
      .spinner {{
        width: 42px; height: 42px; border: 4px solid #cbd5e1; border-top-color: #2563eb;
        border-radius: 50%; animation: spin 1s linear infinite;
      }}
      @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
      iframe {{ flex: 1; border: none; width: 100%; }}
    </style>
  </head>
  <body>
    <form class="panel" method="get" action="/demo/batch-plan" id="planForm">
      <span class="brand">IFRE</span>
      <div class="sep-v"></div>
      <div class="field">
        <label>№ заявок</label>
        <input type="text" name="task_ids" value="{tid}" placeholder="10038, 10042, 12635"
               oninput="toggleDates(this.value)" style="width:200px" />
      </div>
      <span class="or-badge" id="orBadge">или</span>
      <div id="dateFields" style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end;">
        <div class="field">
          <label>Дата с</label>
          <input type="date" name="start_date" value="{sd}" id="startDate" style="width:140px" />
        </div>
        <div class="field">
          <label>Дата по</label>
          <input type="date" name="end_date" value="{ed}" style="width:140px" />
        </div>
        <div class="field">
          <label>Лимит заявок</label>
          <input type="number" name="limit" value="{lim}" min="1" max="200" style="width:80px" />
        </div>
      </div>
      <div class="sep-v"></div>
      <div class="field">
        <label>Горизонт</label>
        <div style="display:flex;gap:4px;align-items:center">
          <select name="planning_mode" style="width:120px" onchange="toggleCustomHorizon(this.value)">
            <option value="shift_8" {"selected" if pm=="shift_8" else ""}>Смена 8 ч</option>
            <option value="shift_12" {"selected" if pm=="shift_12" else ""}>Смена 12 ч</option>
            <option value="day" {"selected" if pm=="day" else ""}>Сутки</option>
            <option value="unlimited" {"selected" if pm=="unlimited" else ""}>Без ограничений</option>
            <option value="custom" {"selected" if pm=="custom" else ""}>Своё...</option>
          </select>
          <input type="number" name="max_total_time_minutes" id="customHorizon" placeholder="мин"
            style="width:90px;height:34px;display:{"flex" if pm=="custom" else "none"};box-sizing:border-box;border:1px solid #cbd5e1;border-radius:6px;padding:0 8px;font-size:13px;color:#1f2933;background:#f8fafc;outline:none"
            min="1" value="">
        </div>
      </div>
      <div class="field">
        <label>Группировка</label>
        <select name="grouping" style="width:160px">
          <option value="true" {"selected" if grp=="true" else ""}>Да (multi-stop)</option>
          <option value="false" {"selected" if grp=="false" else ""}>Нет (1 заявка = 1 машина)</option>
        </select>
      </div>
      <div class="sep-v"></div>
      <div class="field">
        <label>Крюк, %</label>
        <input type="number" name="max_detour_pct" style="width:70px" min="0" max="200" step="5"
          value="{int(round((dr - 1.0) * 100))}">
      </div>
      <button class="run-btn" type="submit">Рассчитать</button>
    </form>
    <div id="loading">
      <div class="spinner"></div>
      <div>Идёт расчёт маршрутов, подождите...</div>
    </div>
    <iframe id="frame" src="" style="display:none"></iframe>
    <script>
      function toggleCustomHorizon(val) {{
        const el = document.getElementById('customHorizon');
        el.style.display = val === 'custom' ? 'block' : 'none';
        el.required = val === 'custom';
      }}
      toggleCustomHorizon(document.querySelector('[name=planning_mode]').value);

      function toggleDates(val) {{
        const show = val.trim() === '';
        document.getElementById('dateFields').style.display = show ? 'flex' : 'none';
        document.getElementById('orBadge').style.display = show ? '' : 'none';
        document.getElementById('startDate').required = show;
      }}
      toggleDates(document.querySelector('[name=task_ids]').value);

      const form = document.getElementById('planForm');
      const frame = document.getElementById('frame');
      const loading = document.getElementById('loading');
      form.addEventListener('submit', function(e) {{
        e.preventDefault();
        const data = new FormData(form);
        const params = new URLSearchParams();
        for (const [k, v] of data.entries()) {{
          if (v !== '') params.append(k, v);
        }}
        params.set('embed', 'true');
        frame.src = '/demo/batch-plan?' + params.toString();
        frame.style.display = 'block';
        loading.style.display = 'flex';
        frame.onload = () => {{ loading.style.display = 'none'; }};
      }});
    </script>
  </body>
</html>
"""
            return HTMLResponse(content=html)

        parsed_ids = None
        if task_ids:
            parsed_ids = [item.strip() for item in task_ids.split(",") if item.strip()]

        filters = None
        if parsed_ids is None:
            if start_date is not None or end_date is not None or shift is not None:
                filters = TaskFilters(start_date=start_date, end_date=end_date, shift=shift, limit=limit)

        if grouping is None:
            grouping = True

        effective_max_total = resolve_max_total(planning_mode, max_total_time_minutes)
        if max_detour_pct is not None:
            effective_max_detour = 1.0 + max_detour_pct / 100.0
        else:
            effective_max_detour = max_detour_ratio if max_detour_ratio is not None else 1.3

        payload = app.state.assignments.plan(
            parsed_ids,
            filters,
            effective_max_total,
            effective_max_detour,
            grouping=grouping,
        )
        multitask_reason = None
        if grouping:
            try:
                mt = app.state.multitask.evaluate(
                    task_ids=parsed_ids,
                    filters=filters,
                    max_total_time_minutes=effective_max_total,
                    max_detour_ratio=effective_max_detour,
                )
                multitask_reason = mt.get("reason")
            except Exception as exc:
                multitask_reason = f"не удалось получить причину группировки: {exc}"

        # Build display groups from actual assignments (by vehicle)
        vehicle_tasks: dict[int, list[str]] = {}
        for item in payload.assignments:
            vehicle_tasks.setdefault(item.wialon_id, []).append(item.task_id)
        raw_groups = list(vehicle_tasks.values())

        return batch_plan_html(
            payload.assignments,
            payload.unassigned,
            summary=payload.summary,
            multitask_reason=multitask_reason,
            raw_groups=raw_groups,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
