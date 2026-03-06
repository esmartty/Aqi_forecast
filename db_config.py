import os
from pathlib import Path
from dotenv import load_dotenv
import logging
from typing import Optional

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
ENV_LOCAL = BASE_DIR / ".env_local"

if ENV_LOCAL.exists():
    load_dotenv(ENV_LOCAL)
    ENVIRONMENT = "local"
else:
    ENVIRONMENT = "production"

#logger.info("Environment: %s", ENVIRONMENT)

_DATABASE_URL: Optional[str] = None

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

    return (
        "postgresql+psycopg2://"
        f"{required['DATABASE_USER_NAME']}:" 
        f"{required['DATABASE_PASSWORD']}@"
        f"{required['DATABASE_SERVER']}:" 
        f"{required['DATABASE_PORT']}/"
        f"{required['DATABASE_NAME']}"
    )

def get_database_url() -> str:
    global _DATABASE_URL
    if _DATABASE_URL is None:
        _DATABASE_URL = build_database_url()
        logger.info("Database URL initialized")
    return _DATABASE_URL