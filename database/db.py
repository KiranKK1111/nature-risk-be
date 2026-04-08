"""
Database connection module for NatureRisk backend.
All configuration loaded from .env via config.settings.
"""

import asyncpg
from config import settings

_pool: asyncpg.Pool = None


async def _init_connection(conn):
    """Called for every new connection — sets the search_path."""
    await conn.execute(f"SET search_path TO {settings.DB_SCHEMA}, public")


async def init_db():
    """Initialize the database connection pool."""
    global _pool
    _pool = await asyncpg.create_pool(
        settings.DATABASE_URL,
        min_size=settings.DB_POOL_MIN,
        max_size=settings.DB_POOL_MAX,
        command_timeout=settings.DB_COMMAND_TIMEOUT,
        init=_init_connection,
        statement_cache_size=0,
    )
    print(f"Database pool created: {settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME} (schema: {settings.DB_SCHEMA})")


async def close_db():
    """Close the database connection pool."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        print("Database pool closed.")


async def get_db() -> asyncpg.Connection:
    """FastAPI dependency that provides a database connection."""
    async with _pool.acquire() as conn:
        await conn.execute(f"SET search_path TO {settings.DB_SCHEMA}, public")
        yield conn
