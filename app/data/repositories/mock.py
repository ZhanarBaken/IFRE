from __future__ import annotations

from datetime import datetime
from typing import List

from app.data import mock_data
from app.data.repositories.base import BaseRepository
from app.models.schemas import (
    Compatibility,
    RoadEdge,
    RoadNode,
    Task,
    TaskAssignment,
    Well,
    WialonUnitSnapshot,
)


class MockRepository(BaseRepository):
    def road_nodes(self) -> List[RoadNode]:
        return [RoadNode(**row) for row in mock_data.ROAD_NODES]

    def road_edges(self) -> List[RoadEdge]:
        return [RoadEdge(**row) for row in mock_data.ROAD_EDGES]

    def wells(self) -> List[Well]:
        return [Well(**row) for row in mock_data.WELLS]

    def units_snapshot(self) -> List[WialonUnitSnapshot]:
        return [WialonUnitSnapshot(**row) for row in mock_data.WIALON_SNAPSHOTS]

    def units_snapshots_history(self) -> List[WialonUnitSnapshot]:
        return [WialonUnitSnapshot(**row) for row in mock_data.WIALON_SNAPSHOTS]

    def tasks(self) -> List[Task]:
        return [Task(**row) for row in mock_data.TASKS]

    def compatibility(self) -> List[Compatibility]:
        return [Compatibility(**row) for row in mock_data.COMPATIBILITY]

    def tasks_by_ids(self, task_ids: List[str]) -> List[Task]:
        if not task_ids:
            return []
        wanted = set(task_ids)
        return [Task(**row) for row in mock_data.TASKS if row["task_id"] in wanted]

    def tasks_by_window(self, start_dt, end_dt, limit: int | None = None) -> List[Task]:
        items = []
        for row in mock_data.TASKS:
            planned_start = row.get("planned_start")
            if planned_start is None:
                start_day = row.get("start_day")
                shift = str(row.get("shift", "")).lower()
                if start_day is None:
                    continue
                if isinstance(start_day, str):
                    start_day = datetime.fromisoformat(start_day).date()
                if shift in {"day", "daytime", "d"}:
                    planned_start = datetime.combine(start_day, datetime.min.time()).replace(hour=8)
                elif shift in {"night", "n"}:
                    planned_start = datetime.combine(start_day, datetime.min.time()).replace(hour=20)
                else:
                    continue
            if isinstance(planned_start, str):
                planned_start = datetime.fromisoformat(planned_start)
            if start_dt <= planned_start < end_dt:
                payload = dict(row)
                if "planned_start" not in payload or payload["planned_start"] is None:
                    payload["planned_start"] = planned_start
                items.append(Task(**payload))
        if limit is not None:
            return items[:limit]
        return items

    def wells_by_uwi(self, uwis: List[str]) -> List[Well]:
        if not uwis:
            return []
        wanted = set(uwis)
        return [Well(**row) for row in mock_data.WELLS if row["uwi"] in wanted]

    def assignments(self) -> List[TaskAssignment]:
        return [TaskAssignment(**row) for row in mock_data.TASK_ASSIGNMENTS]
