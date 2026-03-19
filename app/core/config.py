from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    db_url: str = "postgresql+psycopg://user:pass@host:5432/dbname"
    db_schema: str = "references"
    avg_speed_kmph: float = 30.0
    min_speed_kmph: float = 5.0
    max_speed_kmph: float = 120.0
    edge_weight_in_meters: bool = True
    score_w_distance: float = 0.30
    score_w_eta: float = 0.30
    score_w_wait: float = 0.15
    score_w_late: float = 0.25
    score_points_scale: float = 10.0
    graph_bidirectional: bool | None = None
    graph_bidirectional_threshold: float = 0.05
    task_document_codes: str = "TRS_ORDER,TRS_BBJORDER,TRS_RTAORDER,TRS_RTAPLNORDER"
    eav_mapping_file: str = "app/data/eav_mapping.json"
    compatibility_strict: bool = False
    compatibility_penalty: float = 10.0
    use_snapshot_by_planning_date: bool = True
    anchor_units_at_plan_start: bool = True
    assignments_grouping: bool = True
    max_total_time_minutes_default: int = 480
    reason_ai_enabled: bool = False
    reason_ai_api_url: str = "https://llm.alem.ai/v1/chat/completions"
    reason_ai_api_key: str | None = None
    reason_ai_model: str = "qwen3"
    reason_ai_timeout_sec: float = 15.0

    # Env is provided by docker env_file (.envs) or shell; we don't read .env here.
    model_config = SettingsConfigDict(env_prefix="IFRE_", extra="ignore")


settings = Settings()
