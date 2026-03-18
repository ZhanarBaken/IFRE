from __future__ import annotations

from datetime import date, datetime, time
import logging
from typing import Dict, Iterable, List, Optional

from sqlalchemy import MetaData, Table, create_engine, inspect, select

from app.core.config import settings
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


logger = logging.getLogger(__name__)


class PostgresRepository(BaseRepository):
    def __init__(self, db_url: str) -> None:
        self.db_url = db_url
        self.engine = create_engine(db_url, pool_pre_ping=True)
        self.meta = MetaData()
        self._tables: Dict[tuple[str | None, str], Table] = {}
        self._cache: Dict[str, list] = {}

    def road_nodes(self) -> List[RoadNode]:
        return self._get_cached("road_nodes", self._load_road_nodes)

    def road_edges(self) -> List[RoadEdge]:
        return self._get_cached("road_edges", self._load_road_edges)

    def wells(self) -> List[Well]:
        return self._load_wells()

    def units_snapshot(self) -> List[WialonUnitSnapshot]:
        return self._load_units_snapshot()

    def tasks(self) -> List[Task]:
        return self._load_tasks()

    def compatibility(self) -> List[Compatibility]:
        return self._load_compatibility()

    def units_snapshots_history(self) -> List[WialonUnitSnapshot]:
        return self._load_units_snapshot_history()

    def tasks_by_ids(self, task_ids: List[str]) -> List[Task]:
        if not task_ids:
            return []
        table = self._tasks_table()
        id_col = self._find_column(table, ["task_id", "id", "code", "uid", "task_code"], "task_id")
        rows = self._fetch_where(table, id_col.in_(task_ids))
        return self._parse_tasks(rows)

    def tasks_by_window(self, start_dt, end_dt, limit: int | None = None) -> List[Task]:
        table = self._tasks_table()
        try:
            start_col = self._find_column(
                table,
                ["planned_start", "start_time", "planned_start_at", "start_at"],
                "planned_start",
            )
            where_clause = (start_col >= start_dt) & (start_col < end_dt)
            rows = self._fetch_where(table, where_clause, limit=limit)
            return self._parse_tasks(rows)
        except RuntimeError:
            # Fallback: load all tasks and filter in memory (for schemas without planned_start).
            rows = self._fetch_all(table)
            tasks = self._parse_tasks(rows)
            filtered = [task for task in tasks if start_dt <= task.planned_start < end_dt]
            if limit is not None:
                return filtered[:limit]
            return filtered

    def wells_by_uwi(self, uwis: List[str]) -> List[Well]:
        if not uwis:
            return []
        table = self._wells_table()
        uwi_col = self._find_column(table, ["uwi", "well_uwi", "code", "id"], "uwi")
        rows = self._fetch_where(table, uwi_col.in_(uwis))
        return self._parse_wells(rows)

    def assignments(self) -> List[TaskAssignment]:
        table = self._assignments_table()
        if table is None:
            raise RuntimeError("Table task_assignments not found")
        rows = self._fetch_all(table)
        if not rows:
            return []
        return self._parse_assignments(rows)

    def _get_cached(self, key: str, loader):
        if key not in self._cache:
            self._cache[key] = loader()
        return self._cache[key]

    def _table(self, name: str) -> Optional[Table]:
        candidates = [settings.db_schema, "public", None]
        insp = inspect(self.engine)
        for schema in candidates:
            table_names = insp.get_table_names(schema=schema)
            if name in table_names:
                cache_key = (schema, name)
                if cache_key in self._tables:
                    return self._tables[cache_key]
                table = Table(name, self.meta, autoload_with=self.engine, schema=schema)
                self._tables[cache_key] = table
                return table
        return None

    def _assignments_table(self) -> Optional[Table]:
        for name in ["task_assignments", "assignments", "plan_assignments", "task_plan"]:
            table = self._table(name)
            if table is not None:
                self._require_columns(
                    table,
                    {
                        "task_id": ["task_id", "task", "goal_id"],
                        "wialon_id": ["wialon_id", "unit_id", "assigned_unit_id"],
                    },
                )
                return table
        return None

    def _fetch_all(self, table: Table) -> List[dict]:
        with self.engine.connect() as conn:
            result = conn.execute(select(table))
            return [dict(row._mapping) for row in result]

    def _fetch_where(self, table: Table, where_clause=None, limit: int | None = None) -> List[dict]:
        stmt = select(table)
        if where_clause is not None:
            stmt = stmt.where(where_clause)
        if limit is not None:
            stmt = stmt.limit(limit)
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            return [dict(row._mapping) for row in result]

    def _row_value(self, row: dict, names: Iterable[str], default=None):
        for name in names:
            if name in row and row[name] is not None:
                return row[name]
        return default

    def _require_columns(self, table: Table, required: Dict[str, Iterable[str]]) -> None:
        cols = set(table.columns.keys())
        missing = {
            label: list(names)
            for label, names in required.items()
            if not any(name in cols for name in names)
        }
        if missing:
            raise RuntimeError(
                f"{table.fullname}: missing required columns mapping: {missing}. "
                f"Available columns: {sorted(cols)}"
            )

    def _find_column(self, table: Table, names: Iterable[str], label: str):
        for name in names:
            if name in table.c:
                return table.c[name]
        raise RuntimeError(
            f"{table.fullname}: missing column for {label}. "
            f"Expected one of {list(names)}. Available columns: {list(table.c.keys())}"
        )

    def _require_value(self, row: dict, names: Iterable[str], table_name: str, field: str, idx: int):
        value = self._row_value(row, names)
        if value is None:
            raise RuntimeError(
                f"{table_name}: row {idx} missing {field}. "
                f"Expected one of {list(names)}. Available keys: {sorted(row.keys())}"
            )
        return value

    def _to_datetime(self, value, context: str) -> datetime:
        if value is None:
            raise RuntimeError(f"{context}: datetime is NULL")
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, time(8, 0))
        if isinstance(value, (int, float)):
            ts = float(value)
            if ts > 1_000_000_000_000:  # milliseconds
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                raise RuntimeError(f"{context}: invalid datetime string {value!r}") from None
        raise RuntimeError(f"{context}: unsupported datetime type {type(value)}")

    def _to_date(self, value, context: str) -> date:
        if value is None:
            raise RuntimeError(f"{context}: date is NULL")
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            try:
                return date.fromisoformat(value)
            except ValueError:
                raise RuntimeError(f"{context}: invalid date string {value!r}") from None
        raise RuntimeError(f"{context}: unsupported date type {type(value)}")

    def _to_float(self, value, context: str) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            raise RuntimeError(f"{context}: invalid float {value!r}") from None

    def _to_int(self, value, context: str) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            raise RuntimeError(f"{context}: invalid int {value!r}") from None

    def _load_road_nodes(self) -> List[RoadNode]:
        table = self._table("road_nodes")
        if table is None:
            raise RuntimeError("Table road_nodes not found")
        self._require_columns(
            table,
            {
                "node_id": ["node_id", "id"],
                "lon": ["lon", "longitude", "x", "pos_x"],
                "lat": ["lat", "latitude", "y", "pos_y"],
            },
        )
        rows = self._fetch_all(table)
        if not rows:
            raise RuntimeError("road_nodes: table is empty")
        items = []
        for idx, row in enumerate(rows):
            node_id = self._require_value(row, ["node_id", "id"], "road_nodes", "node_id", idx)
            lon = self._require_value(row, ["lon", "longitude", "x", "pos_x"], "road_nodes", "lon", idx)
            lat = self._require_value(row, ["lat", "latitude", "y", "pos_y"], "road_nodes", "lat", idx)
            items.append(
                RoadNode(
                    node_id=self._to_int(node_id, f"road_nodes row {idx} node_id"),
                    lon=self._to_float(lon, f"road_nodes row {idx} lon"),
                    lat=self._to_float(lat, f"road_nodes row {idx} lat"),
                )
            )
        if not items:
            raise RuntimeError("road_nodes: no valid rows after parsing")
        return items

    def _load_road_edges(self) -> List[RoadEdge]:
        table = self._table("road_edges")
        if table is None:
            raise RuntimeError("Table road_edges not found")
        self._require_columns(
            table,
            {
                "source": ["source", "from_id", "from_node", "start_node"],
                "target": ["target", "to_id", "to_node", "end_node"],
                "weight": ["weight", "cost", "distance", "len", "length"],
            },
        )
        rows = self._fetch_all(table)
        if not rows:
            raise RuntimeError("road_edges: table is empty")
        items = []
        for idx, row in enumerate(rows):
            source = self._require_value(row, ["source", "from_id", "from_node", "start_node"], "road_edges", "source", idx)
            target = self._require_value(row, ["target", "to_id", "to_node", "end_node"], "road_edges", "target", idx)
            weight = self._require_value(row, ["weight", "cost", "distance", "len", "length"], "road_edges", "weight", idx)
            items.append(
                RoadEdge(
                    source=self._to_int(source, f"road_edges row {idx} source"),
                    target=self._to_int(target, f"road_edges row {idx} target"),
                    weight=self._to_float(weight, f"road_edges row {idx} weight"),
                )
            )
        if not items:
            raise RuntimeError("road_edges: no valid rows after parsing")
        return items

    def _load_wells(self) -> List[Well]:
        table = self._wells_table()
        rows = self._fetch_all(table)
        if not rows:
            raise RuntimeError("wells: table is empty")
        return self._parse_wells(rows)

    def _wells_table(self) -> Table:
        table = self._table("wells")
        if table is None:
            raise RuntimeError("Table wells not found")
        self._require_columns(
            table,
            {
                "uwi": ["uwi", "well_uwi", "code", "id"],
                "lon": ["lon", "longitude", "x", "pos_x"],
                "lat": ["lat", "latitude", "y", "pos_y"],
            },
        )
        return table

    def _parse_wells(self, rows: List[dict]) -> List[Well]:
        items = []
        for idx, row in enumerate(rows):
            uwi = self._require_value(row, ["uwi", "well_uwi", "code", "id"], "wells", "uwi", idx)
            lon = self._row_value(row, ["lon", "longitude", "x", "pos_x"])
            lat = self._row_value(row, ["lat", "latitude", "y", "pos_y"])
            if lon is None or lat is None:
                # Allowed by spec: coordinates can be NULL. Skip such wells.
                continue
            items.append(
                Well(
                    uwi=str(uwi),
                    lon=self._to_float(lon, f"wells row {idx} lon"),
                    lat=self._to_float(lat, f"wells row {idx} lat"),
                )
            )
        if not items:
            raise RuntimeError("wells: no valid rows after parsing")
        return items

    def _load_units_snapshot(self) -> List[WialonUnitSnapshot]:
        rows = self._fetch_all_snapshots()
        if not rows:
            raise RuntimeError("No wialon_units_snapshot tables found")

        latest: Dict[int, dict] = {}
        for idx, row in enumerate(rows):
            wialon_id = self._require_value(
                row, ["wialon_id", "id", "unit_id"], "wialon_units_snapshot", "wialon_id", idx
            )
            pos_t_raw = self._require_value(
                row, ["pos_t", "timestamp", "time", "created_at"], "wialon_units_snapshot", "pos_t", idx
            )
            pos_t = self._to_datetime(pos_t_raw, f"wialon_units_snapshot row {idx} pos_t")
            existing = latest.get(int(wialon_id))
            if existing is None or pos_t > existing["pos_t"]:
                latest[int(wialon_id)] = {
                    "row": row,
                    "pos_t": pos_t,
                }

        items = []
        for idx, (unit_id, payload) in enumerate(latest.items()):
            row = payload["row"]
            name = self._row_value(row, ["name", "unit_name"]) or f"unit-{unit_id}"
            unit_type = self._row_value(row, ["unit_type", "type", "work_type"]) or "unknown"
            lon = self._require_value(
                row, ["pos_x", "lon", "longitude", "x"], "wialon_units_snapshot", "pos_x", idx
            )
            lat = self._require_value(
                row, ["pos_y", "lat", "latitude", "y"], "wialon_units_snapshot", "pos_y", idx
            )
            items.append(
                WialonUnitSnapshot(
                    wialon_id=int(unit_id),
                    name=str(name),
                    unit_type=str(unit_type),
                    pos_x=self._to_float(lon, f"wialon_units_snapshot row {idx} pos_x"),
                    pos_y=self._to_float(lat, f"wialon_units_snapshot row {idx} pos_y"),
                    pos_t=payload["pos_t"],
                )
            )
        if not items:
            raise RuntimeError("wialon_units_snapshot: no valid rows after parsing")
        return items

    def _load_units_snapshot_history(self) -> List[WialonUnitSnapshot]:
        rows = self._fetch_all_snapshots()
        if not rows:
            raise RuntimeError("No wialon_units_snapshot tables found")
        items = self._parse_snapshot_rows(rows, require_latest=False)
        if not items:
            raise RuntimeError("wialon_units_snapshot: no valid history rows after parsing")
        return items

    def _fetch_all_snapshots(self) -> List[dict]:
        tables = [
            self._table("wialon_units_snapshot_1"),
            self._table("wialon_units_snapshot_2"),
            self._table("wialon_units_snapshot_3"),
            self._table("wialon_units_snapshot"),
        ]
        rows: List[dict] = []
        for table in tables:
            if table is None:
                continue
            rows.extend(self._fetch_all(table))
        return rows

    def _parse_snapshot_rows(self, rows: List[dict], require_latest: bool) -> List[WialonUnitSnapshot]:
        items = []
        for idx, row in enumerate(rows):
            wialon_id = self._require_value(
                row, ["wialon_id", "id", "unit_id"], "wialon_units_snapshot", "wialon_id", idx
            )
            pos_t_raw = self._require_value(
                row, ["pos_t", "timestamp", "time", "created_at"], "wialon_units_snapshot", "pos_t", idx
            )
            pos_t = self._to_datetime(pos_t_raw, f"wialon_units_snapshot row {idx} pos_t")
            name = self._row_value(row, ["name", "unit_name"]) or f"unit-{wialon_id}"
            unit_type = self._row_value(row, ["unit_type", "type", "work_type"]) or "unknown"
            lon = self._require_value(
                row, ["pos_x", "lon", "longitude", "x"], "wialon_units_snapshot", "pos_x", idx
            )
            lat = self._require_value(
                row, ["pos_y", "lat", "latitude", "y"], "wialon_units_snapshot", "pos_y", idx
            )
            items.append(
                WialonUnitSnapshot(
                    wialon_id=int(wialon_id),
                    name=str(name),
                    unit_type=str(unit_type),
                    pos_x=self._to_float(lon, f"wialon_units_snapshot row {idx} pos_x"),
                    pos_y=self._to_float(lat, f"wialon_units_snapshot row {idx} pos_y"),
                    pos_t=pos_t,
                )
            )
        return items

    def _load_tasks(self) -> List[Task]:
        table = self._tasks_table()
        rows = self._fetch_all(table)
        if not rows:
            raise RuntimeError("tasks: table is empty")
        return self._parse_tasks(rows)

    def _tasks_table(self) -> Table:
        table = self._table("tasks")
        if table is None:
            raise RuntimeError("Table tasks not found")
        self._require_columns(
            table,
            {
                "task_id": ["task_id", "id", "code", "uid", "task_code"],
                "destination_uwi": ["destination_uwi", "uwi", "well_uwi", "destination", "target_uwi"],
            },
        )
        return table

    def _parse_tasks(self, rows: List[dict]) -> List[Task]:
        items = []
        for idx, row in enumerate(rows):
            task_id = self._require_value(
                row, ["task_id", "id", "code", "uid", "task_code"], "tasks", "task_id", idx
            )
            priority = self._row_value(row, ["priority", "priority_level"]) or "medium"
            destination_uwi = self._require_value(
                row, ["destination_uwi", "uwi", "well_uwi", "destination", "target_uwi"], "tasks", "destination_uwi", idx
            )
            planned_start_raw = self._row_value(
                row, ["planned_start", "start_time", "planned_start_at", "start_at"]
            )
            start_day_raw = self._row_value(row, ["start_day", "start_date", "day"])
            shift_raw = self._row_value(row, ["shift", "work_shift", "shift_code"])
            start_day = None
            shift = None
            if start_day_raw is not None:
                start_day = self._to_date(start_day_raw, f"tasks row {idx} start_day")
            if shift_raw is not None:
                shift = str(shift_raw).lower()

            if planned_start_raw is None:
                if start_day is None or shift is None:
                    raise RuntimeError(
                        f"tasks: row {idx} missing planned_start; provide planned_start or (start_day + shift)"
                    )
                if shift in {"day", "daytime", "d"}:
                    planned_start = datetime.combine(start_day, time(8, 0))
                    shift = "day"
                elif shift in {"night", "n"}:
                    planned_start = datetime.combine(start_day, time(20, 0))
                    shift = "night"
                else:
                    raise RuntimeError(f"tasks: row {idx} unknown shift value {shift_raw!r}")
            else:
                planned_start = self._to_datetime(planned_start_raw, f"tasks row {idx} planned_start")

            duration_hours = self._row_value(
                row,
                [
                    "duration_hours",
                    "planned_duration_hours",
                    "duration",
                    "planned_duration",
                    "duration_h",
                ],
            )
            duration_minutes = self._row_value(
                row,
                [
                    "duration_minutes",
                    "planned_duration_minutes",
                    "duration_min",
                ],
            )
            if duration_hours is None and duration_minutes is None:
                raise RuntimeError(f"tasks: row {idx} missing duration")
            if duration_hours is None and duration_minutes is not None:
                duration_hours = self._to_float(duration_minutes, f"tasks row {idx} duration_minutes") / 60.0
            duration_hours = self._to_float(duration_hours, f"tasks row {idx} duration_hours")

            task_type = self._row_value(row, ["task_type", "work_type", "type"]) or "unknown"
            items.append(
                Task(
                    task_id=str(task_id),
                    priority=str(priority),
                    destination_uwi=str(destination_uwi),
                    planned_start=planned_start,
                    duration_hours=duration_hours,
                    task_type=str(task_type),
                    start_day=start_day,
                    shift=shift,
                )
            )
        return items

    def _parse_assignments(self, rows: List[dict]) -> List[TaskAssignment]:
        items = []
        for idx, row in enumerate(rows):
            task_id = self._require_value(row, ["task_id", "task", "goal_id"], "task_assignments", "task_id", idx)
            wialon_id = self._require_value(
                row, ["wialon_id", "unit_id", "assigned_unit_id"], "task_assignments", "wialon_id", idx
            )
            status = self._row_value(row, ["status", "task_status", "state"])
            actual_start_raw = self._row_value(row, ["actual_start", "started_at", "start_actual"])
            actual_start = None
            if actual_start_raw is not None:
                actual_start = self._to_datetime(actual_start_raw, f"task_assignments row {idx} actual_start")
            items.append(
                TaskAssignment(
                    task_id=str(task_id),
                    wialon_id=self._to_int(wialon_id, f"task_assignments row {idx} wialon_id"),
                    status=str(status) if status is not None else None,
                    actual_start=actual_start,
                )
            )
        return items

    def _load_compatibility(self) -> List[Compatibility]:
        table = self._table("compatibility")
        if table is None:
            raise RuntimeError("Table compatibility not found")
        self._require_columns(
            table,
            {
                "task_type": ["task_type", "work_type", "task", "type"],
                "unit_type": ["unit_type", "vehicle_type", "unit", "type"],
            },
        )
        rows = self._fetch_all(table)
        if not rows:
            logger.warning("compatibility: table is empty; all units will be treated as compatible")
            return []
        items = []
        for idx, row in enumerate(rows):
            task_type = self._require_value(
                row, ["task_type", "work_type", "task", "type"], "compatibility", "task_type", idx
            )
            unit_type = self._require_value(
                row, ["unit_type", "vehicle_type", "unit", "type"], "compatibility", "unit_type", idx
            )
            items.append(
                Compatibility(
                    task_type=str(task_type),
                    unit_type=str(unit_type),
                )
            )
        return items
