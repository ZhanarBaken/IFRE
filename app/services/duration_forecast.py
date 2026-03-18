from __future__ import annotations

from collections import defaultdict
from statistics import median
from typing import Iterable

from app.data.repositories.base import BaseRepository
from app.models.schemas import Task


DEFAULT_DURATION_HOURS = 2.0


class StatsDurationModel:
    def __init__(self, medians: dict[str, float], global_median: float) -> None:
        self.medians = medians
        self.global_median = global_median

    @classmethod
    def from_tasks(cls, tasks: Iterable[Task]) -> "StatsDurationModel":
        by_type: dict[str, list[float]] = defaultdict(list)
        all_values: list[float] = []
        for task in tasks:
            duration = task.duration_hours
            if duration is None:
                continue
            try:
                value = float(duration)
            except (TypeError, ValueError):
                continue
            if value <= 0:
                continue
            by_type[task.task_type].append(value)
            all_values.append(value)

        medians = {task_type: median(values) for task_type, values in by_type.items() if values}
        global_median = median(all_values) if all_values else DEFAULT_DURATION_HOURS
        return cls(medians=medians, global_median=global_median)

    def predict(self, task_type: str | None) -> float:
        if task_type and task_type in self.medians:
            return self.medians[task_type]
        return self.global_median


class MLDurationModelStub:
    """
    Placeholder for a future ML model trained on historical actuals.

    Expected interface:
    - load(path): load trained model
    - predict(task_type, features): return duration_hours or None
    - is_ready(): whether model is loaded/trained
    """

    def load(self, _path: str) -> None:
        return None

    def is_ready(self) -> bool:
        return False

    def predict(self, _task_type: str | None, _features: dict | None = None) -> float | None:
        return None


class DurationForecaster:
    def __init__(self, repo: BaseRepository) -> None:
        self.repo = repo
        self._stats: StatsDurationModel | None = None
        self._ml_model = MLDurationModelStub()

    def _ensure_stats(self) -> StatsDurationModel:
        if self._stats is None:
            self._stats = StatsDurationModel.from_tasks(self.repo.tasks())
        return self._stats

    def predict(self, task_type: str | None, features: dict | None = None) -> float:
        if self._ml_model.is_ready():
            ml_value = self._ml_model.predict(task_type, features)
            if ml_value is not None and ml_value > 0:
                return float(ml_value)
        stats = self._ensure_stats()
        return stats.predict(task_type)

    def ensure_duration(self, task_type: str | None, duration_hours: float | None) -> float:
        if duration_hours is None:
            return self.predict(task_type)
        try:
            value = float(duration_hours)
        except (TypeError, ValueError):
            return self.predict(task_type)
        if value <= 0:
            return self.predict(task_type)
        return value

    def fill_missing(self, tasks: Iterable[Task]) -> None:
        for task in tasks:
            task.duration_hours = self.ensure_duration(task.task_type, task.duration_hours)
