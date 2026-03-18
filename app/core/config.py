from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    data_source: str = "mock"
    db_url: str = "postgresql+psycopg://user:pass@host:5432/dbname"
    db_schema: str = "references"
    avg_speed_kmph: float = 30.0
    edge_weight_in_meters: bool = True
    score_w_distance: float = 0.7
    score_w_eta: float = 0.3
    score_w_wait: float = 0.2
    score_w_late: float = 1.0
    ai_api_key: str | None = None
    ai_base_url: str = "https://api.openai.com/v1"
    ai_model: str = "gpt-4o-mini"
    ai_timeout_s: float = 4.0
    use_task_assignments: bool = False

    model_config = SettingsConfigDict(env_prefix="IFRE_", env_file=".env", extra="ignore")


settings = Settings()
