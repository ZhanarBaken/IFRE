from __future__ import annotations

from datetime import date, datetime, timedelta

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

from app.core.config import settings
from app.data.repositories import get_repository
from app.models.schemas import (
    AssignmentRequest,
    AssignmentResponse,
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
)
from app.services.assignments import AssignmentService
from app.services.multitask import MultitaskService
from app.services.recommendations import RecommendationService
from app.services.routing import RoutingService
from app.services.visualization import batch_plan_html, route_map_html
from app.utils.graph import Graph, NodeIndex, should_make_bidirectional


app = FastAPI(title="IFRE Routing Service", version="0.1.0")


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

    app.state.repo = repo
    app.state.graph = graph
    app.state.node_index = node_index
    app.state.routing = routing
    app.state.recommendations = recommendations
    app.state.multitask = multitask
    app.state.assignments = assignments


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
        matrix = app.state.routing.distance_time_matrix(req.start_nodes, req.end_nodes)
        items = []
        for (start_node, end_node), (distance_km, time_minutes) in matrix.items():
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
async def recommendations(req: RecommendationRequest):
    try:
        units = app.state.recommendations.recommend(
            task_id=req.task_id,
            priority=req.priority,
            destination_uwi=req.destination_uwi,
            task_type=req.task_type,
            planned_start=req.planned_start,
            duration_hours=req.duration_hours,
            mode=req.mode,
            exclude_busy=req.exclude_busy,
        )
        return {"units": units}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/multitask", response_model=MultitaskResponse)
async def multitask(req: MultitaskRequest):
    try:
        constraints = req.constraints
        max_total = constraints.max_total_time_minutes if constraints else 480
        max_detour = constraints.max_detour_ratio if constraints else 1.3
        payload = app.state.multitask.evaluate(req.task_ids, req.filters, max_total, max_detour)
        return payload
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/assignments", response_model=AssignmentResponse)
async def assignments(req: AssignmentRequest):
    try:
        constraints = req.constraints
        max_total = constraints.max_total_time_minutes if constraints else 480
        max_detour = constraints.max_detour_ratio if constraints else 1.3
        payload = app.state.assignments.plan(req.task_ids, req.filters, max_total, max_detour, grouping=req.grouping)
        return payload
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
    task_ids: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    shift: str | None = None,
    limit: int = 20,
    max_total_time_minutes: int = 480,
    max_detour_ratio: float = 1.3,
    grouping: bool | None = None,
):
    try:
        parsed_ids = None
        if task_ids:
            parsed_ids = [item.strip() for item in task_ids.split(",") if item.strip()]

        filters = None
        if parsed_ids is None:
            if start_date is not None or end_date is not None or shift is not None:
                filters = TaskFilters(start_date=start_date, end_date=end_date, shift=shift, limit=limit)

        if grouping is None:
            grouping = True

        payload = app.state.assignments.plan(
            parsed_ids,
            filters,
            max_total_time_minutes,
            max_detour_ratio,
            grouping=grouping,
        )
        return batch_plan_html(payload.assignments, payload.unassigned, summary=payload.summary)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
