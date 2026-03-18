from app.core.config import settings
from app.data.repositories.base import BaseRepository
from app.data.repositories.mock import MockRepository
from app.data.repositories.postgres import PostgresRepository


def get_repository() -> BaseRepository:
    if settings.data_source.lower() == "postgres":
        return PostgresRepository(settings.db_url)
    return MockRepository()
