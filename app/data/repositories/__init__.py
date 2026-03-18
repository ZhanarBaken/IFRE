from app.core.config import settings
from app.data.repositories.base import BaseRepository
from app.data.repositories.postgres import PostgresRepository


def get_repository() -> BaseRepository:
    return PostgresRepository(settings.db_url)
