import os
from pathlib import Path
from dotenv import load_dotenv
import logging
from typing import Optional
from urllib.parse import quote

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
ENV_LOCAL = BASE_DIR / ".env_local"

if ENV_LOCAL.exists():
    load_dotenv(ENV_LOCAL)
    ENVIRONMENT = "local"
else:
    ENVIRONMENT = "production"

#logger.info("Environment: %s", ENVIRONMENT)

_LOCAL_DATABASE_URL: Optional[str] = None
_RENDER_DATABASE_URL: Optional[str] = None


def _build_database_url(
    server: str,
    port: str,
    database_name: str,
    user_name: str,
    password: str,
    sslmode: Optional[str] = None,
) -> str:
    query_string = f"?sslmode={sslmode}" if sslmode else ""
    encoded_user = quote(user_name, safe="")
    encoded_password = quote(password, safe="")

    return (
        "postgresql+psycopg2://"
        f"{encoded_user}:{encoded_password}@"
        f"{server}:{port}/"
        f"{database_name}{query_string}"
    )


def build_database_url() -> str:
    required = {
        "DATABASE_SERVER": os.getenv("DATABASE_SERVER"),
        "DATABASE_PORT": os.getenv("DATABASE_PORT"),
        "DATABASE_NAME": os.getenv("DATABASE_NAME"),
        "DATABASE_USER_NAME": os.getenv("DATABASE_USER_NAME"),
        "DATABASE_PASSWORD": os.getenv("DATABASE_PASSWORD"),
    }

    missing_vars = [var_name for var_name, value in required.items() if not value]
    if missing_vars:
        raise ValueError(
            f"Missing database env vars ({ENVIRONMENT}): {', '.join(missing_vars)}"
        )

    return _build_database_url(
        server=required["DATABASE_SERVER"],
        port=required["DATABASE_PORT"],
        database_name=required["DATABASE_NAME"],
        user_name=required["DATABASE_USER_NAME"],
        password=required["DATABASE_PASSWORD"],
    )


def build_database_render_url() -> str:
    required = {
        "DATABASE_SERVER": os.getenv("RENDER_SERVER"),
        "DATABASE_PORT": os.getenv("DATABASE_PORT"),
        "DATABASE_NAME": os.getenv("RENDER_DATABASE_NAME"),
        "DATABASE_USER_NAME": os.getenv("RENDER_DATABASE_USER_NAME"),
        "DATABASE_PASSWORD": os.getenv("RENDER_DATABASE_PASSWORD"),
    }

    missing_vars = [var_name for var_name, value in required.items() if not value]
    if missing_vars:
        raise ValueError(
            f"Missing database env vars ({ENVIRONMENT}): {', '.join(missing_vars)}"
        )

    return _build_database_url(
        server=required["DATABASE_SERVER"],
        port=required["DATABASE_PORT"],
        database_name=required["DATABASE_NAME"],
        user_name=required["DATABASE_USER_NAME"],
        password=required["DATABASE_PASSWORD"],
        sslmode="require",
    )


def get_database_url() -> str:
    global _LOCAL_DATABASE_URL
    if _LOCAL_DATABASE_URL is None:
        _LOCAL_DATABASE_URL = build_database_url()
        logger.info("Local database URL initialized")
    return _LOCAL_DATABASE_URL


def get_database_render_url() -> str:
    global _RENDER_DATABASE_URL
    if _RENDER_DATABASE_URL is None:
        _RENDER_DATABASE_URL = build_database_render_url()
        logger.info("Render database URL initialized")
    return _RENDER_DATABASE_URL