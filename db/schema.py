"""
db/schema.py — Table definitions and idempotent migration.

Call migrate() once at app startup and once at the start of each ingestion run.
All statements use IF NOT EXISTS / ADD COLUMN IF NOT EXISTS so re-running is safe.
"""

import psycopg2
from db.connection import cursor, close_pool

# ---------------------------------------------------------------------------
# DDL — existing tables (ported from SQLite)
# ---------------------------------------------------------------------------

_JOBS = """
CREATE TABLE IF NOT EXISTS jobs (
    id                   SERIAL PRIMARY KEY,
    company_name         TEXT    NOT NULL,
    role_title           TEXT    NOT NULL,
    status               TEXT    NOT NULL DEFAULT 'Researching',
    date_added           DATE    NOT NULL DEFAULT CURRENT_DATE,
    date_applied         DATE,
    salary_min           INTEGER,
    salary_max           INTEGER,
    location             TEXT,
    work_type            TEXT             DEFAULT 'Remote',
    source               TEXT             DEFAULT 'Other',
    job_url              TEXT,
    notes                TEXT,
    priority             TEXT             DEFAULT 'Medium',
    first_response_date  DATE,
    -- production additions
    dedupe_fingerprint   TEXT,
    external_job_id      TEXT,
    description_raw      TEXT,
    created_at           TIMESTAMPTZ      DEFAULT NOW(),
    updated_at           TIMESTAMPTZ      DEFAULT NOW()
)
"""

_CONTACTS = """
CREATE TABLE IF NOT EXISTS contacts (
    id               SERIAL PRIMARY KEY,
    job_id           INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    contact_name     TEXT    NOT NULL,
    contact_title    TEXT,
    contact_email    TEXT,
    contact_phone    TEXT,
    contact_linkedin TEXT,
    notes            TEXT
)
"""

_FOLLOW_UPS = """
CREATE TABLE IF NOT EXISTS follow_ups (
    id               SERIAL PRIMARY KEY,
    job_id           INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    follow_up_date   DATE    NOT NULL,
    follow_up_type   TEXT    NOT NULL DEFAULT 'Email',
    completed        BOOLEAN NOT NULL DEFAULT FALSE,
    notes            TEXT
)
"""

_INTERVIEW_STAGES = """
CREATE TABLE IF NOT EXISTS interview_stages (
    id               SERIAL PRIMARY KEY,
    job_id           INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    stage_name       TEXT    NOT NULL,
    stage_date       DATE    NOT NULL,
    interviewer_name TEXT,
    format           TEXT             DEFAULT 'Video',
    status           TEXT             DEFAULT 'Scheduled',
    notes            TEXT
)
"""

_SETTINGS = """
CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT
)
"""

# ---------------------------------------------------------------------------
# DDL — new production tables
# ---------------------------------------------------------------------------

_INGESTION_RUNS = """
CREATE TABLE IF NOT EXISTS ingestion_runs (
    id           SERIAL PRIMARY KEY,
    source       TEXT    NOT NULL DEFAULT 'all',
    started_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status       TEXT    NOT NULL DEFAULT 'running',  -- running | completed | error
    jobs_found   INTEGER NOT NULL DEFAULT 0,
    jobs_created INTEGER NOT NULL DEFAULT 0,
    jobs_updated INTEGER NOT NULL DEFAULT 0,
    jobs_skipped INTEGER NOT NULL DEFAULT 0,
    error_count  INTEGER NOT NULL DEFAULT 0,
    run_notes    TEXT
)
"""

_JOB_ANALYSIS = """
CREATE TABLE IF NOT EXISTS job_analysis (
    id                        SERIAL PRIMARY KEY,
    job_id                    INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    fit_score                 NUMERIC(4,1),
    target_role_bucket        TEXT,
    reasons_for_fit           TEXT,
    risks_or_gaps             TEXT,
    keyword_matches           TEXT,   -- comma-separated or JSON text
    recommended_resume_focus  TEXT,
    recruiter_note            TEXT,
    generated_resume_reference TEXT,
    analyzed_at               TIMESTAMPTZ DEFAULT NOW(),
    created_at                TIMESTAMPTZ DEFAULT NOW(),
    updated_at                TIMESTAMPTZ DEFAULT NOW()
)
"""

_RESUME_PROFILES = """
CREATE TABLE IF NOT EXISTS resume_profiles (
    id                 SERIAL PRIMARY KEY,
    profile_name       TEXT    NOT NULL,
    is_canonical       BOOLEAN NOT NULL DEFAULT FALSE,
    raw_content        TEXT,
    structured_content TEXT,    -- JSON stored as TEXT for portability
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ DEFAULT NOW()
)
"""

_RESUME_VERSIONS = """
CREATE TABLE IF NOT EXISTS resume_versions (
    id                 SERIAL PRIMARY KEY,
    job_id             INTEGER REFERENCES jobs(id) ON DELETE SET NULL,
    profile_id         INTEGER REFERENCES resume_profiles(id) ON DELETE SET NULL,
    version_name       TEXT,
    raw_content        TEXT,
    structured_content TEXT,
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ DEFAULT NOW()
)
"""

_ACTIVITY_LOG = """
CREATE TABLE IF NOT EXISTS activity_log (
    id          SERIAL PRIMARY KEY,
    entity_type TEXT,   -- 'job' | 'contact' | 'follow_up' | 'ingestion_run'
    entity_id   INTEGER,
    action      TEXT,   -- 'created' | 'updated' | 'status_changed' | 'deleted'
    details     TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
)
"""

_COMPANIES = """
CREATE TABLE IF NOT EXISTS companies (
    id         SERIAL PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    website    TEXT,
    notes      TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
)
"""

# ---------------------------------------------------------------------------
# Safe column additions for existing deployments
# ---------------------------------------------------------------------------

_SAFE_MIGRATIONS = [
    "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS first_response_date  DATE",
    "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS dedupe_fingerprint   TEXT",
    "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS external_job_id      TEXT",
    "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS description_raw      TEXT",
    "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS created_at           TIMESTAMPTZ DEFAULT NOW()",
    "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS updated_at           TIMESTAMPTZ DEFAULT NOW()",
]


# ---------------------------------------------------------------------------
# migrate()
# ---------------------------------------------------------------------------

def _run_migrate() -> None:
    ddl_statements = [
        _JOBS, _CONTACTS, _FOLLOW_UPS, _INTERVIEW_STAGES, _SETTINGS,
        _INGESTION_RUNS, _JOB_ANALYSIS, _RESUME_PROFILES,
        _RESUME_VERSIONS, _ACTIVITY_LOG, _COMPANIES,
    ]
    with cursor(row_dict=False) as cur:
        for stmt in ddl_statements:
            cur.execute(stmt)
        for stmt in _SAFE_MIGRATIONS:
            # Use a savepoint so a failed ALTER (column already exists) doesn't
            # abort the entire transaction.
            cur.execute("SAVEPOINT safe_migration")
            try:
                cur.execute(stmt)
                cur.execute("RELEASE SAVEPOINT safe_migration")
            except Exception:
                cur.execute("ROLLBACK TO SAVEPOINT safe_migration")


def migrate() -> None:
    """
    Create all tables and apply column additions idempotently.
    Retries once on stale/dropped connections before raising.
    """
    try:
        _run_migrate()
    except (psycopg2.InterfaceError, psycopg2.OperationalError):
        close_pool()
        _run_migrate()  # second attempt with a fresh pool
