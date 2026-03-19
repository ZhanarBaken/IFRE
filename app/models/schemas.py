from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel, Field
from pydantic.config import ConfigDict


class RoadNode(BaseModel):
    node_id: int
    lon: float
    lat: float


class RoadEdge(BaseModel):
    source: int
    target: int
    weight: float


class Well(BaseModel):
    uwi: str
    lon: float
    lat: float


class WialonUnitSnapshot(BaseModel):
    wialon_id: int
    name: str
    unit_type: str
    pos_x: float
    pos_y: float
    pos_t: datetime


class Task(BaseModel):
    task_id: str
    priority: str
    destination_uwi: str
    planned_start: datetime
    duration_hours: float
    task_type: str
    start_day: date | None = None
    shift: str | None = None

    model_config = ConfigDict(extra="ignore")


class Compatibility(BaseModel):
    task_type: str
    unit_type: str


class RecommendationRequest(BaseModel):
    task_id: str
    priority: str
    destination_uwi: str
    planned_start: datetime
    duration_hours: float | None = None
    task_type: Optional[str] = None
    mode: Optional[str] = None  # "optimized" or "baseline"
    exclude_busy: bool = False


class RecommendationUnit(BaseModel):
    wialon_id: int
    name: str
    eta_minutes: int
    distance_km: float
    score: float = Field(description="Итоговый балл кандидата от 0 до 100 (чем выше, тем лучше)")
    reason: Optional[str] = None


class RecommendationResponse(BaseModel):
    units: List[RecommendationUnit]


class RouteFrom(BaseModel):
    wialon_id: Optional[int] = None
    lon: Optional[float] = None
    lat: Optional[float] = None


class RouteTo(BaseModel):
    uwi: Optional[str] = None
    lon: Optional[float] = None
    lat: Optional[float] = None


class RouteRequest(BaseModel):
    from_: RouteFrom = Field(alias="from")
    to: RouteTo
    model_config = ConfigDict(populate_by_name=True)


class RouteResponse(BaseModel):
    distance_km: float
    time_minutes: int
    nodes: List[int]
    coords: List[List[float]]


class MatrixRequest(BaseModel):
    start_nodes: List[int]
    end_nodes: List[int]


class MatrixItem(BaseModel):
    start_node: int
    end_node: int
    distance_km: float
    time_minutes: int


class MatrixResponse(BaseModel):
    items: List[MatrixItem]


class MultitaskConstraints(BaseModel):
    max_total_time_minutes: int | None = None
    max_detour_ratio: float | None = None


class TaskFilters(BaseModel):
    start_date: date | None = None
    end_date: date | None = None
    shift: str | None = None  # "day" or "night"
    limit: int | None = None


class MultitaskRequest(BaseModel):
    task_ids: List[str] | None = None
    filters: Optional[TaskFilters] = None
    constraints: Optional[MultitaskConstraints] = None


class AssignmentRequest(BaseModel):
    task_ids: List[str] | None = None
    filters: Optional[TaskFilters] = None
    constraints: Optional[MultitaskConstraints] = None
    grouping: bool | None = None


class AssignmentItem(BaseModel):
    task_id: str
    wialon_id: int
    eta_minutes: int
    distance_km: float
    score: float = Field(description="Итоговый балл назначения от 0 до 100 (чем выше, тем лучше)")
    reason: str
    planned_duration_hours: float
    planned_start: datetime
    start_time: datetime
    end_time: datetime
    route_nodes: List[int]
    route_coords: List[List[float]]


class UnassignedItem(BaseModel):
    task_id: str
    reason: str


class AssignmentResponse(BaseModel):
    assignments: List[AssignmentItem]
    unassigned: List[UnassignedItem]
    summary: str


class MultitaskResponse(BaseModel):
    groups: List[List[str]]
    strategy_summary: str
    total_distance_km: float
    total_time_minutes: int
    baseline_distance_km: float
    baseline_time_minutes: int
    savings_percent: float
    reason: str
