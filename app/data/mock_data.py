ROAD_NODES = [
    {"node_id": 1, "lon": 68.10, "lat": 51.70},
    {"node_id": 2, "lon": 68.11, "lat": 51.70},
    {"node_id": 3, "lon": 68.12, "lat": 51.70},
    {"node_id": 4, "lon": 68.10, "lat": 51.71},
    {"node_id": 5, "lon": 68.11, "lat": 51.71},
    {"node_id": 6, "lon": 68.12, "lat": 51.71},
    {"node_id": 7, "lon": 68.10, "lat": 51.72},
    {"node_id": 8, "lon": 68.11, "lat": 51.72},
]

ROAD_EDGES = [
    {"source": 1, "target": 2, "weight": 1000.0},
    {"source": 2, "target": 1, "weight": 1000.0},
    {"source": 2, "target": 3, "weight": 1000.0},
    {"source": 3, "target": 2, "weight": 1000.0},
    {"source": 4, "target": 5, "weight": 1000.0},
    {"source": 5, "target": 4, "weight": 1000.0},
    {"source": 5, "target": 6, "weight": 1000.0},
    {"source": 6, "target": 5, "weight": 1000.0},
    {"source": 7, "target": 8, "weight": 1000.0},
    {"source": 8, "target": 7, "weight": 1000.0},
    {"source": 1, "target": 4, "weight": 1000.0},
    {"source": 4, "target": 1, "weight": 1000.0},
    {"source": 2, "target": 5, "weight": 1000.0},
    {"source": 5, "target": 2, "weight": 1000.0},
    {"source": 3, "target": 6, "weight": 1000.0},
    {"source": 6, "target": 3, "weight": 1000.0},
    {"source": 4, "target": 7, "weight": 1000.0},
    {"source": 7, "target": 4, "weight": 1000.0},
    {"source": 5, "target": 8, "weight": 1000.0},
    {"source": 8, "target": 5, "weight": 1000.0},
]

WELLS = [
    {"uwi": "W-001", "lon": 68.105, "lat": 51.705},
    {"uwi": "W-002", "lon": 68.115, "lat": 51.705},
    {"uwi": "W-003", "lon": 68.125, "lat": 51.705},
    {"uwi": "W-004", "lon": 68.105, "lat": 51.715},
    {"uwi": "W-005", "lon": 68.115, "lat": 51.715},
]

WIALON_SNAPSHOTS = [
    {
        "wialon_id": 1001,
        "name": "ACN-12 A045KM",
        "unit_type": "acid",
        "pos_x": 68.101,
        "pos_y": 51.701,
        "pos_t": "2025-02-20T07:30:00",
    },
    {
        "wialon_id": 1002,
        "name": "CA-320 B112OR",
        "unit_type": "cement",
        "pos_x": 68.112,
        "pos_y": 51.701,
        "pos_t": "2025-02-20T07:35:00",
    },
    {
        "wialon_id": 1003,
        "name": "ACN-12 K330MN",
        "unit_type": "acid",
        "pos_x": 68.121,
        "pos_y": 51.709,
        "pos_t": "2025-02-20T07:40:00",
    },
    {
        "wialon_id": 1004,
        "name": "TR-4X4 X919TT",
        "unit_type": "transport",
        "pos_x": 68.106,
        "pos_y": 51.719,
        "pos_t": "2025-02-20T07:20:00",
    },
    {
        "wialon_id": 1005,
        "name": "ACN-12 Z991AA",
        "unit_type": "acid",
        "pos_x": 68.130,
        "pos_y": 51.725,
        "pos_t": "2025-02-20T07:10:00",
    },
    {
        "wialon_id": 1006,
        "name": "CA-320 X551BB",
        "unit_type": "cement",
        "pos_x": 68.090,
        "pos_y": 51.695,
        "pos_t": "2025-02-20T07:15:00",
    },
]

TASKS = [
    {
        "task_id": "T-2025-0042",
        "priority": "high",
        "destination_uwi": "W-001",
        "planned_start": "2025-02-20T08:00:00",
        "duration_hours": 4.5,
        "task_type": "acid",
    },
    {
        "task_id": "T-2025-0043",
        "priority": "medium",
        "destination_uwi": "W-002",
        "planned_start": "2025-02-20T09:00:00",
        "duration_hours": 3.0,
        "task_type": "cement",
    },
    {
        "task_id": "T-2025-0044",
        "priority": "low",
        "destination_uwi": "W-003",
        "planned_start": "2025-02-20T10:00:00",
        "duration_hours": 2.0,
        "task_type": "acid",
    },
    {
        "task_id": "T-2025-0045",
        "priority": "high",
        "destination_uwi": "W-004",
        "planned_start": "2025-02-20T11:00:00",
        "duration_hours": 1.5,
        "task_type": "transport",
    },
    {
        "task_id": "T-2025-0046",
        "priority": "medium",
        "destination_uwi": "W-005",
        "planned_start": "2025-02-20T12:00:00",
        "duration_hours": 2.5,
        "task_type": "transport",
    },
    {
        "task_id": "T-2025-0047",
        "priority": "high",
        "destination_uwi": "W-002",
        "planned_start": "2025-02-20T08:30:00",
        "duration_hours": 2.0,
        "task_type": "cement",
    },
]

TASK_ASSIGNMENTS = [
    {
        "task_id": "T-2025-0042",
        "wialon_id": 1001,
        "status": "in_progress",
        "actual_start": "2025-02-20T07:30:00",
    },
    {
        "task_id": "T-2025-0047",
        "wialon_id": 1006,
        "status": "assigned",
        "actual_start": "2025-02-20T08:00:00",
    },
]

COMPATIBILITY = [
    {"task_type": "acid", "unit_type": "acid"},
    {"task_type": "cement", "unit_type": "cement"},
    {"task_type": "transport", "unit_type": "transport"},
]
