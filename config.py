"""
Centralized configuration loaded from .env file.

Usage:
    from config import settings
    print(settings.DB_HOST)
    print(settings.DATABASE_URL)
"""

import os
from dotenv import load_dotenv

# Load .env from the same directory as this file
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))


def _get(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _get_int(key: str, default: int = 0) -> int:
    return int(os.environ.get(key, str(default)))


class Settings:
    # Server
    PORT: int = _get_int("PORT", 8000)
    HOST: str = _get("HOST", "0.0.0.0")
    WORKERS: int = _get_int("WORKERS", 1)
    ROOT_PATH: str = _get("ROOT_PATH", "/esg/nature-risk")
    LOG_LEVEL: str = _get("LOG_LEVEL", "INFO")

    # Database
    DB_HOST: str = _get("DB_HOST", "localhost")
    DB_PORT: int = _get_int("DB_PORT", 5432)
    DB_NAME: str = _get("DB_NAME", "postgres")
    DB_USER: str = _get("DB_USER", "postgres")
    DB_PASSWORD: str = _get("DB_PASSWORD", "postgres")
    DB_SCHEMA: str = _get("DB_SCHEMA", "nature_risk")
    DB_POOL_MIN: int = _get_int("DB_POOL_MIN", 2)
    DB_POOL_MAX: int = _get_int("DB_POOL_MAX", 10)
    DB_COMMAND_TIMEOUT: int = _get_int("DB_COMMAND_TIMEOUT", 60)

    # CORS
    CORS_ORIGINS: list[str] = [o.strip() for o in _get("CORS_ORIGINS", "*").split(",")]

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


settings = Settings()
