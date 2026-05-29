from pymongo import AsyncMongoClient

from src.core.config import settings

_client: AsyncMongoClient | None = None


def get_client() -> AsyncMongoClient:
    global _client
    if _client is None:
        _client = AsyncMongoClient(settings.mongo_url)
    return _client


def get_db():
    return get_client()[settings.mongo_db]


async def close_client() -> None:
    global _client
    if _client is not None:
        _client.close()
        _client = None
