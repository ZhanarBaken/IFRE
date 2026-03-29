from __future__ import annotations

from datetime import date, datetime, time, timezone
import json
import logging
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from sqlalchemy import MetaData, Table, create_engine, inspect, select, text

from app.core.config import settings
from app.data.repositories.base import BaseRepository
from app.models.schemas import (
    Compatibility,
    RoadEdge,
    RoadNode,
    Task,
    Well,
    WialonUnitSnapshot,
)
from app.utils.normalize import normalize_plate


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
        return self._get_cached("wells", self._load_wells)

    def units_snapshot(self) -> List[WialonUnitSnapshot]:
        return self._get_cached("units_snapshot", self._load_units_snapshot)

    def tasks(self) -> List[Task]:
        table = self._tasks_table()
        if table is None:
            return self._get_cached("tasks_eav", self._load_tasks_eav)
        return self._load_tasks()

    def compatibility(self) -> List[Compatibility]:
        return self._get_cached("compatibility", self._load_compatibility)

    def units_snapshots_history(self) -> List[WialonUnitSnapshot]:
        return self._get_cached("units_snapshots_history", self._load_units_snapshot_history)

    def tasks_by_ids(self, task_ids: List[str]) -> List[Task]:
        if not task_ids:
            return []
        table = self._tasks_table()
        if table is None:
            return self._tasks_by_ids_eav(task_ids)
        id_col = self._find_column(table, ["task_id", "id", "code", "uid", "task_code"], "task_id")
        rows = self._fetch_where(table, id_col.in_(task_ids))
        return self._parse_tasks(rows)

    def tasks_by_window(self, start_dt, end_dt, limit: int | None = None) -> List[Task]:
        table = self._tasks_table()
        if table is None:
            return self._tasks_by_window_eav(start_dt, end_dt, limit=limit)
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

    def tasks_debug(self, limit: int | None = None) -> List[dict]:
        items = self._cache.get("tasks_eav_skipped", [])
        if limit is not None:
            return items[:limit]
        return items

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

    @staticmethod
    def _fix_encoding(s: str) -> str:
        """Fix text stored as Win-1251 but read as latin-1 (common PG misconfiguration)."""
        try:
            return s.encode("latin-1").decode("utf-8")
        except (UnicodeDecodeError, UnicodeEncodeError):
            return s

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
            if value.tzinfo is not None:
                return value.astimezone(timezone.utc).replace(tzinfo=None)
            return value
        if isinstance(value, date):
            return datetime.combine(value, time(8, 0))
        if isinstance(value, (int, float)):
            ts = float(value)
            if ts > 1_000_000_000_000:  # milliseconds
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
        if isinstance(value, str):
            try:
                cleaned = value.replace("Z", "+00:00")
                parsed = datetime.fromisoformat(cleaned)
                if parsed.tzinfo is not None:
                    return parsed.astimezone(timezone.utc).replace(tzinfo=None)
                return parsed
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
        vehkind_by_plate = self._get_cached("vehkind_by_plate", self._load_vehkind_by_plate)

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
            name = self._row_value(row, ["name", "unit_name", "nm"]) or f"unit-{unit_id}"
            unit_type = self._row_value(row, ["unit_type", "type", "work_type", "cls"]) or "unknown"
            reg_plate = self._row_value(
                row,
                ["reg_plate", "registration_plate", "plate", "number", "reg_number", "num", "snum"],
            )
            if reg_plate and vehkind_by_plate:
                vehkind = vehkind_by_plate.get(normalize_plate(reg_plate))
                if vehkind:
                    unit_type = vehkind
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
        vehkind_by_plate = self._get_cached("vehkind_by_plate", self._load_vehkind_by_plate)
        items = []
        for idx, row in enumerate(rows):
            wialon_id = self._require_value(
                row, ["wialon_id", "id", "unit_id"], "wialon_units_snapshot", "wialon_id", idx
            )
            pos_t_raw = self._require_value(
                row, ["pos_t", "timestamp", "time", "created_at"], "wialon_units_snapshot", "pos_t", idx
            )
            pos_t = self._to_datetime(pos_t_raw, f"wialon_units_snapshot row {idx} pos_t")
            name = self._row_value(row, ["name", "unit_name", "nm"]) or f"unit-{wialon_id}"
            unit_type = self._row_value(row, ["unit_type", "type", "work_type", "cls"]) or "unknown"
            reg_plate = self._row_value(
                row,
                ["reg_plate", "registration_plate", "plate", "number", "reg_number", "num", "snum"],
            )
            if reg_plate and vehkind_by_plate:
                vehkind = vehkind_by_plate.get(normalize_plate(reg_plate))
                if vehkind:
                    unit_type = vehkind
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
        if table is None:
            return self._load_tasks_eav()
        rows = self._fetch_all(table)
        if not rows:
            raise RuntimeError("tasks: table is empty")
        return self._parse_tasks(rows)

    def _tasks_table(self) -> Table | None:
        table = self._table("tasks")
        if table is None:
            return None
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
                    task_type=self._fix_encoding(str(task_type)),
                    start_day=start_day,
                    shift=shift,
                )
            )
        return items

    # -------- EAV-based tasks (dcm.*) --------

    def _task_document_codes(self) -> List[str]:
        raw = settings.task_document_codes or ""
        codes = [item.strip() for item in raw.split(",") if item.strip()]
        return codes or ["TRS_ORDER"]

    def _task_document_ids(self) -> Dict[int, str]:
        codes = self._task_document_codes()
        query = text("SELECT id, code FROM dcm.documents WHERE code = ANY(:codes)")
        with self.engine.connect() as conn:
            rows = conn.execute(query, {"codes": codes}).fetchall()
        return {int(row[0]): str(row[1]) for row in rows}

    def _load_eav_mapping(self) -> Dict[str, Dict[str, List[str]]]:
        if "eav_mapping" in self._cache:
            return self._cache["eav_mapping"]
        path = settings.eav_mapping_file
        if not path:
            self._cache["eav_mapping"] = {}
            return {}
        file_path = Path(path)
        if not file_path.is_absolute():
            # Resolve relative to project root (…/IFRE)
            file_path = Path(__file__).resolve().parents[3] / file_path
        if not file_path.exists():
            logger.warning("eav_mapping_file not found: %s", file_path)
            self._cache["eav_mapping"] = {}
            return {}
        try:
            data = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("failed to read eav_mapping_file %s: %s", file_path, exc)
            self._cache["eav_mapping"] = {}
            return {}
        if not isinstance(data, dict):
            logger.warning("eav_mapping_file has invalid format: expected object")
            self._cache["eav_mapping"] = {}
            return {}
        self._cache["eav_mapping"] = data
        return data

    def _load_tasks_eav(self) -> List[Task]:
        doc_ids = self._task_document_ids()
        if not doc_ids:
            logger.warning("tasks_eav: no documents found for codes %s", self._task_document_codes())
            return []

        # Load records for selected documents.
        query = text(
            "SELECT id, number, date, document_id FROM dcm.records WHERE document_id = ANY(:doc_ids)"
        )
        with self.engine.connect() as conn:
            records = conn.execute(query, {"doc_ids": list(doc_ids.keys())}).fetchall()
        if not records:
            return []

        record_ids = [int(r[0]) for r in records]
        record_meta = {
            int(r[0]): {"number": r[1], "date": r[2], "doc_code": doc_ids[int(r[3])]} for r in records
        }

        # Build indicator code map.
        per_doc_map, code_to_id = self._load_task_indicators(doc_ids.values())
        if not code_to_id:
            logger.warning("tasks_eav: no matching indicators found for documents %s", list(doc_ids.values()))
            return []

        indicator_ids = list(code_to_id.values())
        query = text(
            """
            SELECT riv.record_id, i.code, riv.value_int, riv.value_float, riv.value_str, riv.value_text,
                   riv.value_datetime, riv.value_reference, riv.value_json
            FROM dcm.record_indicator_values riv
            JOIN dcm.indicators i ON i.id = riv.indicator_id
            WHERE riv.record_id = ANY(:record_ids)
              AND riv.indicator_id = ANY(:indicator_ids)
            """
        )
        with self.engine.connect() as conn:
            rows = conn.execute(
                query, {"record_ids": record_ids, "indicator_ids": indicator_ids}
            ).fetchall()

        values_by_record: Dict[int, Dict[str, dict]] = {}
        for row in rows:
            record_id = int(row[0])
            code = row[1]
            values_by_record.setdefault(record_id, {})[code] = {
                "value_int": row[2],
                "value_float": row[3],
                "value_str": row[4],
                "value_text": row[5],
                "value_datetime": row[6],
                "value_reference": row[7],
                "value_json": row[8],
            }

        # Lookup tables for list values and wells.
        list_values = self._load_list_values()
        elements_map = self._load_elements_map()
        wells_map = self._load_well_lookup()

        tasks: List[Task] = []
        skipped: List[dict] = []
        number_map: Dict[str, List[str]] = {}
        compat_pairs: set[tuple[str, str]] = set()

        for record_id, meta in record_meta.items():
            doc_code = meta["doc_code"]
            field_codes = per_doc_map.get(doc_code, {})
            values = values_by_record.get(record_id, {})

            shift = self._resolve_shift(values, field_codes, list_values.get("shift", {}))
            planned_start = self._resolve_planned_start(values, field_codes, meta.get("date"), shift)
            start_day = planned_start.date() if planned_start else None
            if planned_start is None:
                skipped.append(
                    {
                        "record_id": record_id,
                        "doc_code": doc_code,
                        "reason": "missing_date",
                    }
                )
                continue

            priority = self._resolve_priority(values, field_codes, list_values.get("priority", {}))
            duration_hours = self._resolve_duration(values, field_codes)
            task_type = self._resolve_task_type(values, field_codes, elements_map)
            vehkind = self._resolve_vehkind(values, field_codes, elements_map)
            if task_type and task_type != "unknown" and vehkind and vehkind != "unknown":
                compat_pairs.add((task_type, vehkind))

            well_candidates: List[str] = []
            destination_uwi = self._resolve_well_uwi(
                values, field_codes, elements_map, wells_map, debug_candidates=well_candidates
            )
            if not destination_uwi:
                skipped.append(
                    {
                        "record_id": record_id,
                        "doc_code": doc_code,
                        "reason": "missing_well",
                        "well_candidates": well_candidates,
                    }
                )
                continue

            task_id = str(record_id)
            if meta.get("number"):
                number_map.setdefault(str(meta["number"]), []).append(task_id)

            tasks.append(
                Task(
                    task_id=task_id,
                    priority=priority,
                    destination_uwi=destination_uwi,
                    planned_start=planned_start,
                    duration_hours=duration_hours,
                    task_type=task_type,
                    start_day=start_day,
                    shift=shift,
                )
            )

        self._cache["tasks_eav_number_map"] = number_map
        self._cache["tasks_eav_skipped"] = skipped
        self._cache["compatibility_eav"] = [
            Compatibility(task_type=pair[0], unit_type=pair[1]) for pair in sorted(compat_pairs)
        ]
        if skipped:
            logger.warning("tasks_eav: skipped %s records", len(skipped))
        return tasks

    def _tasks_by_ids_eav(self, task_ids: List[str]) -> List[Task]:
        tasks = self.tasks()
        if not tasks:
            return []
        id_set = {str(tid) for tid in task_ids}
        number_map: Dict[str, List[str]] = self._cache.get("tasks_eav_number_map", {})
        expanded = set(id_set)
        for tid in list(id_set):
            if tid in number_map:
                expanded.update(number_map[tid])
        return [task for task in tasks if task.task_id in expanded]

    def _tasks_by_window_eav(self, start_dt, end_dt, limit: int | None = None) -> List[Task]:
        start_norm = self._normalize_dt(start_dt)
        end_norm = self._normalize_dt(end_dt)
        tasks = [
            t
            for t in self.tasks()
            if start_norm <= self._normalize_dt(t.planned_start) < end_norm
        ]
        if limit is not None:
            return tasks[:limit]
        return tasks

    def _load_task_indicators(self, doc_codes: Iterable[str]) -> tuple[Dict[str, Dict[str, List[str]]], Dict[str, int]]:
        # Preferred indicator suffixes by field.
        suffixes = {
            "well": ["WELL1C", "WELL", "OBJECT1C", "OBJECT", "WAY1C", "WAY", "LOC", "PLC", "SIT"],
            "task_type": ["WKIND1C", "WKIND", "VEHKIND1C", "VEHKIND", "TYPE", "PLAN"],
            "vehkind": ["VEHKIND1C", "VEHKIND"],
            "priority": ["PRY"],
            "shift": ["SHIFT"],
            "date": ["SDATE", "DATE", "DTM", "EDATE"],
            "duration": ["HOURS", "DURATION", "TIME"],
            "note": ["NOTE"],
        }

        mapping = self._load_eav_mapping()
        candidate_codes = set()
        for doc_code in doc_codes:
            if mapping.get(doc_code):
                for codes in mapping[doc_code].values():
                    for code in codes:
                        candidate_codes.add(code)
            for suff_list in suffixes.values():
                for suffix in suff_list:
                    candidate_codes.add(f"{doc_code}_{suffix}")

        if not candidate_codes:
            return {}, {}

        query = text(
            "SELECT id, code, document_id FROM dcm.indicators WHERE code = ANY(:codes)"
        )
        with self.engine.connect() as conn:
            rows = conn.execute(query, {"codes": list(candidate_codes)}).fetchall()

        code_to_id = {str(row[1]): int(row[0]) for row in rows}

        per_doc: Dict[str, Dict[str, List[str]]] = {}
        for doc_code in doc_codes:
            field_codes: Dict[str, List[str]] = {}
            if mapping.get(doc_code):
                for field, codes in mapping[doc_code].items():
                    for code in codes:
                        if code in code_to_id:
                            field_codes.setdefault(field, [])
                            if code not in field_codes[field]:
                                field_codes[field].append(code)
            for field, suff_list in suffixes.items():
                for suffix in suff_list:
                    code = f"{doc_code}_{suffix}"
                    if code in code_to_id:
                        field_codes.setdefault(field, [])
                        if code not in field_codes[field]:
                            field_codes[field].append(code)
            if field_codes:
                per_doc[doc_code] = field_codes
        return per_doc, code_to_id

    def _load_list_values(self) -> Dict[str, Dict[int, str]]:
        query = text(
            "SELECT id, list, code FROM stm.list_values WHERE list IN ('priority', 'shift')"
        )
        with self.engine.connect() as conn:
            rows = conn.execute(query).fetchall()
        result: Dict[str, Dict[int, str]] = {"priority": {}, "shift": {}}
        for row in rows:
            result[str(row[1])][int(row[0])] = str(row[2])
        return result

    def _field_codes(self, field_codes: Dict[str, List[str]], field: str) -> List[str]:
        codes = field_codes.get(field)
        if not codes:
            return []
        if isinstance(codes, str):
            return [codes]
        return list(codes)

    def _load_elements_map(self) -> Dict[int, str]:
        query = text("SELECT id, short_name, full_name, code, dictionary_id FROM dct.elements")
        with self.engine.connect() as conn:
            rows = conn.execute(query).fetchall()
        elements: Dict[int, str] = {}
        for row in rows:
            element_id = int(row[0])
            short_name = self._extract_localized_text(row[1])
            full_name = self._extract_localized_text(row[2])
            code = row[3]
            value = short_name or full_name or (str(code) if code is not None else None)
            if value:
                elements[element_id] = value
        return elements

    def _load_vehkind_by_plate(self) -> Dict[str, str]:
        try:
            query = text(
                "SELECT id, code FROM dct.indicators WHERE code IN ('TRS_VEHCARD_SNUM', 'TRS_VEHCARD_CLASS')"
            )
            with self.engine.connect() as conn:
                rows = conn.execute(query).fetchall()
        except Exception as exc:
            logger.warning("vehkind mapping: failed to read indicators: %s", exc)
            return {}

        code_to_id = {str(row[1]): int(row[0]) for row in rows}
        plate_id = code_to_id.get("TRS_VEHCARD_SNUM")
        class_id = code_to_id.get("TRS_VEHCARD_CLASS")
        if plate_id is None or class_id is None:
            return {}

        try:
            query = text(
                """
                SELECT element_id, indicator_id, value_str, value_reference
                FROM dct.element_indicator_values
                WHERE indicator_id = ANY(:ids)
                """
            )
            with self.engine.connect() as conn:
                rows = conn.execute(query, {"ids": [plate_id, class_id]}).fetchall()
        except Exception as exc:
            logger.warning("vehkind mapping: failed to read element_indicator_values: %s", exc)
            return {}

        plate_by_element: Dict[int, str] = {}
        vehkind_ref_by_element: Dict[int, int] = {}
        vehkind_str_by_element: Dict[int, str] = {}
        for row in rows:
            element_id = int(row[0])
            indicator_id = int(row[1])
            value_str = row[2]
            value_ref = row[3]
            if indicator_id == plate_id:
                if value_str:
                    plate_by_element[element_id] = str(value_str)
            elif indicator_id == class_id:
                if value_ref is not None:
                    vehkind_ref_by_element[element_id] = int(value_ref)
                elif value_str:
                    vehkind_str_by_element[element_id] = str(value_str)

        elements_map = self._load_elements_map()
        result: Dict[str, str] = {}
        for element_id, plate in plate_by_element.items():
            plate_norm = normalize_plate(plate)
            if not plate_norm:
                continue
            vehkind = None
            ref_id = vehkind_ref_by_element.get(element_id)
            if ref_id is not None:
                vehkind = elements_map.get(int(ref_id))
            if not vehkind:
                vehkind = vehkind_str_by_element.get(element_id)
            if vehkind:
                result[plate_norm] = vehkind
        return result

    def _load_well_lookup(self) -> Dict[str, Dict[str, str]]:
        table = self._wells_table()
        rows = self._fetch_all(table)
        by_name: Dict[str, str] = {}
        by_uwi: Dict[str, str] = {}
        by_core: Dict[str, str] = {}
        by_compact: Dict[str, str] = {}
        by_core_prefix: Dict[str, str] = {}
        for row in rows:
            uwi = row.get("uwi")
            if not uwi:
                continue
            uwi_str = str(uwi).strip()
            by_uwi[self._normalize_text(uwi_str)] = uwi_str
            core = self._normalize_core(uwi_str)
            if core:
                by_core.setdefault(core, uwi_str)
                prefix = self._normalize_core_prefix(core)
                if prefix:
                    by_core_prefix.setdefault(prefix, uwi_str)
            compact = self._normalize_compact(uwi_str)
            if compact:
                by_compact.setdefault(compact, uwi_str)
            name = row.get("well_name")
            if name:
                name_str = str(name)
                by_name[self._normalize_text(name_str)] = uwi_str
                core = self._normalize_core(name_str)
                if core:
                    by_core.setdefault(core, uwi_str)
                    prefix = self._normalize_core_prefix(core)
                    if prefix:
                        by_core_prefix.setdefault(prefix, uwi_str)
                compact = self._normalize_compact(name_str)
                if compact:
                    by_compact.setdefault(compact, uwi_str)
        return {
            "by_name": by_name,
            "by_uwi": by_uwi,
            "by_core": by_core,
            "by_compact": by_compact,
            "by_core_prefix": by_core_prefix,
        }

    def _resolve_priority(self, values: Dict[str, dict], field_codes: Dict[str, List[str]], priority_map: Dict[int, str]) -> str:
        for code in self._field_codes(field_codes, "priority"):
            ref_id = values.get(code, {}).get("value_reference")
            if ref_id is None:
                continue
            list_code = priority_map.get(int(ref_id))
            if list_code in {"high"}:
                return "high"
            if list_code in {"average", "medium"}:
                return "medium"
            if list_code in {"low"}:
                return "low"
        return "medium"

    def _resolve_shift(self, values: Dict[str, dict], field_codes: Dict[str, List[str]], shift_map: Dict[int, str]) -> str | None:
        for code in self._field_codes(field_codes, "shift"):
            ref_id = values.get(code, {}).get("value_reference")
            if ref_id is None:
                continue
            list_code = shift_map.get(int(ref_id))
            if list_code == "change_2":
                return "day"
            if list_code == "change_1":
                return "night"
        return None

    def _resolve_planned_start(self, values: Dict[str, dict], field_codes: Dict[str, List[str]], fallback_date, shift: str | None) -> datetime | None:
        planned = None
        for code in self._field_codes(field_codes, "date"):
            item = values.get(code, {})
            raw = item.get("value_datetime") or item.get("value_str")
            if raw is None:
                continue
            try:
                planned = self._to_datetime(raw, "tasks_eav planned_start")
                break
            except Exception:
                planned = None
        if planned is None and fallback_date is not None:
            try:
                planned = self._to_datetime(fallback_date, "tasks_eav planned_start fallback")
            except Exception:
                planned = None
        if planned is None:
            return None
        if shift == "day":
            return datetime.combine(planned.date(), time(8, 0))
        if shift == "night":
            return datetime.combine(planned.date(), time(20, 0))
        return planned

    def _resolve_duration(self, values: Dict[str, dict], field_codes: Dict[str, List[str]]) -> float:
        for code in self._field_codes(field_codes, "duration"):
            item = values.get(code, {})
            raw = item.get("value_int")
            if raw is None:
                raw = item.get("value_float")
            if raw is None:
                raw = item.get("value_str")
            if raw is None:
                continue
            try:
                return float(raw)
            except (TypeError, ValueError):
                continue
        return 0.0

    def _resolve_task_type(self, values: Dict[str, dict], field_codes: Dict[str, List[str]], elements_map: Dict[int, str]) -> str:
        for code in self._field_codes(field_codes, "task_type"):
            item = values.get(code, {})
            if code.endswith("1C"):
                desc = self._extract_from_value_json(item.get("value_json"), ["Description", "Name"])
                if desc:
                    return self._fix_encoding(desc)
            if item.get("value_text"):
                return self._fix_encoding(str(item.get("value_text")))
            if item.get("value_str"):
                return self._fix_encoding(str(item.get("value_str")))
            ref_id = item.get("value_reference")
            if ref_id is not None:
                return self._fix_encoding(elements_map.get(int(ref_id), "unknown"))
        return "unknown"

    def _resolve_vehkind(self, values: Dict[str, dict], field_codes: Dict[str, List[str]], elements_map: Dict[int, str]) -> str:
        for code in self._field_codes(field_codes, "vehkind"):
            item = values.get(code, {})
            if code.endswith("1C"):
                desc = self._extract_from_value_json(item.get("value_json"), ["Description", "Name"])
                if desc:
                    return desc
            if item.get("value_text"):
                return str(item.get("value_text"))
            if item.get("value_str"):
                return str(item.get("value_str"))
            ref_id = item.get("value_reference")
            if ref_id is not None:
                return elements_map.get(int(ref_id), "unknown")
        return "unknown"

    def _resolve_well_uwi(
        self,
        values: Dict[str, dict],
        field_codes: Dict[str, List[str]],
        elements_map: Dict[int, str],
        wells_map: Dict[str, Dict[str, str]],
        debug_candidates: List[str] | None = None,
    ) -> str | None:
        by_name = wells_map.get("by_name", {})
        by_uwi = wells_map.get("by_uwi", {})
        by_core = wells_map.get("by_core", {})
        by_compact = wells_map.get("by_compact", {})
        by_core_prefix = wells_map.get("by_core_prefix", {})
        for code in self._field_codes(field_codes, "well"):
            item = values.get(code, {})
            candidates: List[str] = []
            if code.endswith("1C"):
                desc = self._extract_from_value_json(item.get("value_json"), ["Description", "Name"])
                if desc:
                    candidates.append(desc)
            if item.get("value_text"):
                candidates.append(str(item.get("value_text")))
            if item.get("value_str"):
                candidates.append(str(item.get("value_str")))
            ref_id = item.get("value_reference")
            if ref_id is not None:
                name = elements_map.get(int(ref_id))
                if name:
                    candidates.append(name)
            if debug_candidates is not None:
                debug_candidates.extend(candidates)
            for raw in candidates:
                stripped = self._strip_well_suffixes(raw)
                if debug_candidates is not None and stripped and stripped not in debug_candidates:
                    debug_candidates.append(stripped)
                key = self._normalize_text(raw)
                key_stripped = self._normalize_text(stripped) if stripped else key
                if key in by_name:
                    return by_name[key]
                if key in by_uwi:
                    return by_uwi[key]
                if key_stripped in by_name:
                    return by_name[key_stripped]
                if key_stripped in by_uwi:
                    return by_uwi[key_stripped]
                core_key = self._normalize_core(raw)
                if core_key and core_key in by_core:
                    return by_core[core_key]
                core_prefix = self._normalize_core_prefix(core_key)
                if core_prefix and core_prefix in by_core_prefix:
                    return by_core_prefix[core_prefix]
                compact_key = self._normalize_compact(raw)
                if compact_key and compact_key in by_compact:
                    return by_compact[compact_key]
                token = self._normalize_text(raw.split()[0]) if raw.split() else key
                if token in by_name:
                    return by_name[token]
                if token in by_uwi:
                    return by_uwi[token]
                # fallback: partial match
                for name_key, uwi in by_name.items():
                    if key and (key in name_key or name_key in key):
                        return uwi
        return None

    def _extract_from_value_json(self, raw: str | None, keys: List[str]) -> str | None:
        if not raw:
            return None
        text_value = raw.strip()
        if text_value.startswith('"') and text_value.endswith('"'):
            text_value = text_value[1:-1]
        text_value = text_value.replace('""', '"')
        lower = text_value.lower()
        for key in keys:
            key_lower = key.lower()
            idx = lower.find(key_lower)
            if idx == -1:
                continue
            colon = lower.find(":", idx)
            if colon == -1:
                continue
            start = None
            quote_char = None
            for j in range(colon + 1, len(text_value)):
                if text_value[j] in ('"', "'"):
                    start = j + 1
                    quote_char = text_value[j]
                    break
            if start is None or quote_char is None:
                continue
            end = text_value.find(quote_char, start)
            if end == -1:
                continue
            value = text_value[start:end].strip().strip('"')
            if value:
                return value
        return None

    def _extract_localized_text(self, value) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            return str(value)
        if value.startswith("{") and "ru" in value:
            match = re.search(r'"ru"\\s*:\\s*"([^"]+)"', value)
            if match:
                return match.group(1)
        return value

    def _normalize_text(self, value: str) -> str:
        return re.sub(r"\\s+", " ", value.strip().lower())

    def _normalize_dt(self, value: datetime) -> datetime:
        if isinstance(value, datetime) and value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value

    def _normalize_core(self, value: str) -> str:
        # Keep digits and slash to match patterns like 4416/28
        if not value:
            return ""
        cleaned = re.sub(r"[^0-9/]+", "", value)
        return cleaned.strip("/")

    def _normalize_compact(self, value: str) -> str:
        # Compact alnum key for loose matching (e.g., G_4416/28 -> g441628)
        if not value:
            return ""
        cleaned = re.sub(r"[^a-zA-Z0-9]+", "", value).lower()
        return cleaned

    def _normalize_core_prefix(self, core: str) -> str:
        if not core:
            return ""
        return core.split("/")[0]

    def _strip_well_suffixes(self, value: str) -> str:
        if not value:
            return ""
        text = value
        text = re.sub(r"\\([^)]*\\)", " ", text)
        # common suffix tokens in well descriptions
        text = re.sub(r"\\b(доб|доб\\.|нагн|нагн\\.|нагнет|гориз|гориз\\.|гор\\.|газ|неф|нагнет\\.|инж|инж\\.)\\b", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"\\s+", " ", text)
        return text.strip()

    def _load_compatibility(self) -> List[Compatibility]:
        if "compatibility_eav" not in self._cache:
            try:
                self._get_cached("tasks_eav", self._load_tasks_eav)
            except Exception as exc:
                logger.warning("compatibility: failed to derive from EAV: %s", exc)
                return []
        derived = self._cache.get("compatibility_eav", [])
        if not derived:
            logger.warning("compatibility: no derived rules; all units treated as compatible")
        return derived
