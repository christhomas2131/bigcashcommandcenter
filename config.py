"""
config.py — Central configuration loaded from environment variables.

All secrets and environment-specific settings live here.
Copy .env.example to .env for local development.
On Render, set these in the service's Environment tab.
"""

import os

from dotenv import load_dotenv

# Load .env file in local dev; no-op in production (env vars already set)
load_dotenv()


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set.\n"
            "Local dev: copy .env.example → .env and fill in your Postgres URL.\n"
            "Render: add DATABASE_URL from your Postgres service."
        )
    # psycopg2 requires postgresql:// not postgres:// (Render sometimes uses the latter)
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url


# ---------------------------------------------------------------------------
# App behaviour
# ---------------------------------------------------------------------------

APP_ENV: str = os.environ.get("APP_ENV", "development")
IS_PRODUCTION: bool = APP_ENV == "production"

# Set SEED_ON_STARTUP=true to seed sample data on first run (local dev only)
SEED_ON_STARTUP: bool = os.environ.get("SEED_ON_STARTUP", "false").lower() == "true"


# ---------------------------------------------------------------------------
# Ingestion / crawler
# ---------------------------------------------------------------------------

JSEARCH_API_KEY: str = os.environ.get("JSEARCH_API_KEY", "")
ADZUNA_APP_ID: str = os.environ.get("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY: str = os.environ.get("ADZUNA_APP_KEY", "")
USAJOBS_EMAIL: str = os.environ.get("USAJOBS_EMAIL", "")
USAJOBS_API_KEY: str = os.environ.get("USAJOBS_API_KEY", "")

# Path to the search_config.json that controls queries, companies, and filters
INGESTION_SEARCH_CONFIG: str = os.environ.get(
    "INGESTION_SEARCH_CONFIG",
    "job-scraper/search_config.json",
)
