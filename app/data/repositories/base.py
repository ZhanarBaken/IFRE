from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List

from app.models.schemas import (
    Compatibility,
    RoadEdge,
    RoadNode,
    Task,
    Well,
    WialonUnitSnapshot,
)


class BaseRepository(ABC):
    @abstractmethod
    def road_nodes(self) -> List[RoadNode]:
        raise NotImplementedError

    @abstractmethod
    def road_edges(self) -> List[RoadEdge]:
        raise NotImplementedError

    @abstractmethod
    def wells(self) -> List[Well]:
        raise NotImplementedError

    @abstractmethod
    def units_snapshot(self) -> List[WialonUnitSnapshot]:
        raise NotImplementedError

    @abstractmethod
    def units_snapshots_history(self) -> List[WialonUnitSnapshot]:
        raise NotImplementedError

    @abstractmethod
    def tasks(self) -> List[Task]:
        raise NotImplementedError

    @abstractmethod
    def compatibility(self) -> List[Compatibility]:
        raise NotImplementedError

    @abstractmethod
    def tasks_by_ids(self, task_ids: List[str]) -> List[Task]:
        raise NotImplementedError

    @abstractmethod
    def tasks_by_window(self, start_dt, end_dt, limit: int | None = None) -> List[Task]:
        raise NotImplementedError

    @abstractmethod
    def wells_by_uwi(self, uwis: List[str]) -> List[Well]:
        raise NotImplementedError

    def tasks_debug(self, limit: int | None = None) -> List[dict]:
        return []

    def task_by_id(self, task_id: str) -> Task | None:
        if not task_id:
            return None
        items = self.tasks_by_ids([task_id])
        return items[0] if items else None

    def well_by_uwi(self, uwi: str) -> Well | None:
        if not uwi:
            return None
        items = self.wells_by_uwi([uwi])
        return items[0] if items else None

    def unit_by_id(self, wialon_id: int) -> WialonUnitSnapshot | None:
        for unit in self.units_snapshot():
            if unit.wialon_id == wialon_id:
                return unit
        return None
