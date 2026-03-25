"""
db/repository.py — Postgres CRUD layer.

Drop-in replacement for database.py: identical function signatures so app.py
only needs its import path changed.  All SQL translated from SQLite:
  ?         → %s
  lastrowid → RETURNING id
  julianday → date subtraction / EXTRACT(EPOCH …)
  strftime  → to_char
  booleans  → Python True/False (not 0/1)
  toggle    → NOT completed
"""

from __future__ import annotations

from datetime import date, timedelta

from db.connection import cursor
from db.schema import migrate


# ---------------------------------------------------------------------------
# Init (replaces init_db)
# ---------------------------------------------------------------------------

def init_db() -> None:
    """Create / migrate all tables.  Safe to call every startup."""
    migrate()


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

def get_all_jobs() -> list[dict]:
    with cursor() as cur:
        cur.execute("SELECT * FROM jobs ORDER BY date_added DESC")
        return [dict(r) for r in cur.fetchall()]


def get_job(job_id: int) -> dict | None:
    with cursor() as cur:
        cur.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def add_job(data: dict) -> int:
    with cursor() as cur:
        cur.execute("""
            INSERT INTO jobs (
                company_name, role_title, status, date_added, date_applied,
                salary_min, salary_max, location, work_type, source,
                job_url, notes, priority, first_response_date,
                dedupe_fingerprint, external_job_id, description_raw
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s
            )
            RETURNING id
        """, (
            data["company_name"],
            data["role_title"],
            data.get("status", "Researching"),
            data.get("date_added", date.today()),
            data.get("date_applied"),
            data.get("salary_min"),
            data.get("salary_max"),
            data.get("location"),
            data.get("work_type", "Remote"),
            data.get("source", "Other"),
            data.get("job_url"),
            data.get("notes"),
            data.get("priority", "Medium"),
            data.get("first_response_date"),
            data.get("dedupe_fingerprint"),
            data.get("external_job_id"),
            data.get("description_raw"),
        ))
        return cur.fetchone()["id"]


def update_job(job_id: int, data: dict) -> None:
    with cursor() as cur:
        cur.execute("""
            UPDATE jobs SET
                company_name = %s, role_title = %s, status = %s,
                date_applied = %s, salary_min = %s, salary_max = %s,
                location = %s, work_type = %s, source = %s,
                job_url = %s, notes = %s, priority = %s,
                first_response_date = %s, updated_at = NOW()
            WHERE id = %s
        """, (
            data["company_name"],
            data["role_title"],
            data["status"],
            data.get("date_applied"),
            data.get("salary_min"),
            data.get("salary_max"),
            data.get("location"),
            data.get("work_type"),
            data.get("source"),
            data.get("job_url"),
            data.get("notes"),
            data.get("priority"),
            data.get("first_response_date"),
            job_id,
        ))


def update_job_status(job_id: int, status: str) -> None:
    with cursor() as cur:
        cur.execute(
            "UPDATE jobs SET status = %s, updated_at = NOW() WHERE id = %s",
            (status, job_id),
        )


def delete_job(job_id: int) -> None:
    with cursor() as cur:
        cur.execute("DELETE FROM jobs WHERE id = %s", (job_id,))


# ---------------------------------------------------------------------------
# Contacts
# ---------------------------------------------------------------------------

def get_contacts(job_id: int) -> list[dict]:
    with cursor() as cur:
        cur.execute("SELECT * FROM contacts WHERE job_id = %s", (job_id,))
        return [dict(r) for r in cur.fetchall()]


def add_contact(data: dict) -> None:
    with cursor() as cur:
        cur.execute("""
            INSERT INTO contacts
                (job_id, contact_name, contact_title, contact_email,
                 contact_phone, contact_linkedin, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            data["job_id"],
            data["contact_name"],
            data.get("contact_title"),
            data.get("contact_email"),
            data.get("contact_phone"),
            data.get("contact_linkedin"),
            data.get("notes"),
        ))


def update_contact(contact_id: int, data: dict) -> None:
    with cursor() as cur:
        cur.execute("""
            UPDATE contacts SET
                contact_name = %s, contact_title = %s, contact_email = %s,
                contact_phone = %s, contact_linkedin = %s, notes = %s
            WHERE id = %s
        """, (
            data["contact_name"],
            data.get("contact_title"),
            data.get("contact_email"),
            data.get("contact_phone"),
            data.get("contact_linkedin"),
            data.get("notes"),
            contact_id,
        ))


def delete_contact(contact_id: int) -> None:
    with cursor() as cur:
        cur.execute("DELETE FROM contacts WHERE id = %s", (contact_id,))


# ---------------------------------------------------------------------------
# Follow-ups
# ---------------------------------------------------------------------------

def get_follow_ups(job_id: int | None = None, completed: bool | None = None) -> list[dict]:
    query = """
        SELECT f.*, j.company_name, j.role_title
        FROM follow_ups f
        JOIN jobs j ON f.job_id = j.id
    """
    conditions: list[str] = []
    params: list = []

    if job_id is not None:
        conditions.append("f.job_id = %s")
        params.append(job_id)
    if completed is not None:
        conditions.append("f.completed = %s")
        params.append(completed)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY f.follow_up_date ASC"

    with cursor() as cur:
        cur.execute(query, params)
        return [dict(r) for r in cur.fetchall()]


def add_follow_up(data: dict) -> None:
    with cursor() as cur:
        cur.execute("""
            INSERT INTO follow_ups (job_id, follow_up_date, follow_up_type, completed, notes)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            data["job_id"],
            data["follow_up_date"],
            data.get("follow_up_type", "Email"),
            bool(data.get("completed")),
            data.get("notes"),
        ))


def update_follow_up(follow_up_id: int, data: dict) -> None:
    with cursor() as cur:
        cur.execute("""
            UPDATE follow_ups SET
                follow_up_date = %s, follow_up_type = %s,
                completed = %s, notes = %s
            WHERE id = %s
        """, (
            data["follow_up_date"],
            data["follow_up_type"],
            bool(data.get("completed")),
            data.get("notes"),
            follow_up_id,
        ))


def toggle_follow_up(follow_up_id: int) -> None:
    with cursor() as cur:
        cur.execute(
            "UPDATE follow_ups SET completed = NOT completed WHERE id = %s",
            (follow_up_id,),
        )


def delete_follow_up(follow_up_id: int) -> None:
    with cursor() as cur:
        cur.execute("DELETE FROM follow_ups WHERE id = %s", (follow_up_id,))


# ---------------------------------------------------------------------------
# Interview stages
# ---------------------------------------------------------------------------

def get_interview_stages(job_id: int) -> list[dict]:
    with cursor() as cur:
        cur.execute(
            "SELECT * FROM interview_stages WHERE job_id = %s ORDER BY stage_date ASC",
            (job_id,),
        )
        return [dict(r) for r in cur.fetchall()]


def add_interview_stage(data: dict) -> None:
    with cursor() as cur:
        cur.execute("""
            INSERT INTO interview_stages
                (job_id, stage_name, stage_date, interviewer_name, format, status, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            data["job_id"],
            data["stage_name"],
            data["stage_date"],
            data.get("interviewer_name"),
            data.get("format", "Video"),
            data.get("status", "Scheduled"),
            data.get("notes"),
        ))


def update_interview_stage(stage_id: int, data: dict) -> None:
    with cursor() as cur:
        cur.execute("""
            UPDATE interview_stages SET
                stage_name = %s, stage_date = %s, interviewer_name = %s,
                format = %s, status = %s, notes = %s
            WHERE id = %s
        """, (
            data["stage_name"],
            data["stage_date"],
            data.get("interviewer_name"),
            data.get("format"),
            data.get("status"),
            data.get("notes"),
            stage_id,
        ))


def delete_interview_stage(stage_id: int) -> None:
    with cursor() as cur:
        cur.execute("DELETE FROM interview_stages WHERE id = %s", (stage_id,))


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

def get_applications_over_time() -> list[dict]:
    with cursor() as cur:
        cur.execute("""
            SELECT to_char(date_added, 'IYYY-IW') AS week, COUNT(*) AS count
            FROM jobs
            GROUP BY week
            ORDER BY week
        """)
        return [dict(r) for r in cur.fetchall()]


def get_status_counts() -> list[dict]:
    with cursor() as cur:
        cur.execute("""
            SELECT status, COUNT(*) AS count
            FROM jobs
            GROUP BY status
            ORDER BY count DESC
        """)
        return [dict(r) for r in cur.fetchall()]


def get_source_stats() -> list[dict]:
    with cursor() as cur:
        cur.execute("""
            SELECT
                source,
                COUNT(*) AS total,
                SUM(CASE WHEN status IN (
                    'Interview','Technical Assessment','Final Round','Offer'
                ) THEN 1 ELSE 0 END) AS interviews,
                SUM(CASE WHEN status = 'Offer' THEN 1 ELSE 0 END) AS offers
            FROM jobs
            GROUP BY source
            ORDER BY total DESC
        """)
        return [dict(r) for r in cur.fetchall()]


def get_upcoming_items(days: int = 7) -> tuple[list[dict], list[dict]]:
    today = date.today()
    end = today + timedelta(days=days)

    with cursor() as cur:
        cur.execute("""
            SELECT f.*, j.company_name, j.role_title
            FROM follow_ups f
            JOIN jobs j ON f.job_id = j.id
            WHERE f.follow_up_date BETWEEN %s AND %s AND NOT f.completed
            ORDER BY f.follow_up_date ASC
        """, (today, end))
        follow_ups = [dict(r) for r in cur.fetchall()]

        cur.execute("""
            SELECT i.*, j.company_name, j.role_title
            FROM interview_stages i
            JOIN jobs j ON i.job_id = j.id
            WHERE i.stage_date BETWEEN %s AND %s AND i.status = 'Scheduled'
            ORDER BY i.stage_date ASC
        """, (today, end))
        interviews = [dict(r) for r in cur.fetchall()]

    return follow_ups, interviews


def get_avg_time_in_stages() -> list[dict]:
    with cursor() as cur:
        cur.execute("""
            SELECT
                status,
                AVG((CURRENT_DATE - date_added)) AS avg_days,
                COUNT(*) AS count
            FROM jobs
            GROUP BY status
        """)
        return [dict(r) for r in cur.fetchall()]


def get_response_times() -> list[dict]:
    """Avg days from date_applied to first_response_date, grouped by source."""
    with cursor() as cur:
        cur.execute("""
            SELECT
                source,
                AVG((first_response_date - date_applied)) AS avg_days,
                COUNT(*) AS count
            FROM jobs
            WHERE date_applied IS NOT NULL
              AND first_response_date IS NOT NULL
            GROUP BY source
            ORDER BY avg_days ASC
        """)
        return [dict(r) for r in cur.fetchall()]


def get_weekly_applied_count() -> int:
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    with cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) AS cnt FROM jobs
            WHERE date_applied >= %s
              AND status NOT IN ('Researching', 'Ready to Apply', 'Withdrawn')
        """, (week_start,))
        row = cur.fetchone()
        return row["cnt"] if row else 0


# ---------------------------------------------------------------------------
# New Leads / triage
# ---------------------------------------------------------------------------

def get_new_leads(days: int = 7, sort_by: str = "imported_desc") -> list[dict]:
    """
    sort_by options:
      imported_desc  — most recently added to DB first (created_at DESC)
      imported_asc   — oldest import first (created_at ASC)
      date_added_desc — job posting date newest first
      priority       — High → Medium, then created_at DESC
    """
    cutoff = date.today() - timedelta(days=days)

    order_clause = {
        "imported_desc":   "created_at DESC NULLS LAST",
        "imported_asc":    "created_at ASC NULLS LAST",
        "date_added_desc": "date_added DESC",
        "priority":        "CASE priority WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END, created_at DESC NULLS LAST",
    }.get(sort_by, "created_at DESC NULLS LAST")

    with cursor() as cur:
        cur.execute(f"""
            SELECT * FROM jobs
            WHERE status = 'Researching'
              AND priority != 'Low'
              AND date_added >= %s
            ORDER BY {order_clause}
        """, (cutoff,))
        return [dict(r) for r in cur.fetchall()]


def count_new_leads(days: int = 7) -> int:
    cutoff = date.today() - timedelta(days=days)
    with cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) AS cnt FROM jobs
            WHERE status = 'Researching'
              AND priority != 'Low'
              AND date_added >= %s
        """, (cutoff,))
        row = cur.fetchone()
        return row["cnt"] if row else 0


def count_jobs_today() -> int:
    with cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM jobs WHERE date_added = CURRENT_DATE"
        )
        row = cur.fetchone()
        return row["cnt"] if row else 0


def triage_job(job_id: int, status: str, priority: str | None = None) -> None:
    with cursor() as cur:
        if priority is not None:
            cur.execute(
                "UPDATE jobs SET status = %s, priority = %s, updated_at = NOW() WHERE id = %s",
                (status, priority, job_id),
            )
        else:
            cur.execute(
                "UPDATE jobs SET status = %s, updated_at = NOW() WHERE id = %s",
                (status, job_id),
            )


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

def get_setting(key: str, default=None):
    with cursor() as cur:
        cur.execute("SELECT value FROM settings WHERE key = %s", (key,))
        row = cur.fetchone()
        return row["value"] if row else default


def set_setting(key: str, value) -> None:
    with cursor() as cur:
        cur.execute(
            """
            INSERT INTO settings (key, value) VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """,
            (key, str(value)),
        )


# ---------------------------------------------------------------------------
# Ingestion helpers (new — not in database.py)
# ---------------------------------------------------------------------------

def get_last_ingestion_run() -> dict | None:
    with cursor() as cur:
        cur.execute("""
            SELECT * FROM ingestion_runs
            ORDER BY started_at DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        return dict(row) if row else None


def get_ingestion_runs(limit: int = 20) -> list[dict]:
    with cursor() as cur:
        cur.execute(
            "SELECT * FROM ingestion_runs ORDER BY started_at DESC LIMIT %s",
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]


def start_ingestion_run(source: str = "all") -> int:
    with cursor() as cur:
        cur.execute(
            "INSERT INTO ingestion_runs (source) VALUES (%s) RETURNING id",
            (source,),
        )
        return cur.fetchone()["id"]


def complete_ingestion_run(run_id: int, report: dict) -> None:
    with cursor() as cur:
        cur.execute("""
            UPDATE ingestion_runs SET
                completed_at  = NOW(),
                status        = %s,
                jobs_found    = %s,
                jobs_created  = %s,
                jobs_updated  = %s,
                jobs_skipped  = %s,
                error_count   = %s,
                run_notes     = %s
            WHERE id = %s
        """, (
            report.get("status", "completed"),
            report.get("jobs_found", 0),
            report.get("jobs_created", 0),
            report.get("jobs_updated", 0),
            report.get("jobs_skipped", 0),
            report.get("error_count", 0),
            report.get("run_notes"),
            run_id,
        ))


def load_jobs_for_dedup() -> list[dict]:
    """Return lightweight job rows used for deduplication checks."""
    with cursor() as cur:
        cur.execute("""
            SELECT id, company_name, role_title, dedupe_fingerprint, external_job_id
            FROM jobs
            ORDER BY id DESC
        """)
        return [dict(r) for r in cur.fetchall()]


def upsert_ingested_job(data: dict) -> tuple[int, str]:
    """
    Insert or update a job that came from the ingestion pipeline.

    Returns (job_id, action) where action is 'created' | 'updated' | 'skipped'.
    Matches on dedupe_fingerprint if provided, else (company_name, role_title).
    """
    fingerprint = data.get("dedupe_fingerprint")

    with cursor() as cur:
        # Try fingerprint match first
        existing_id = None
        if fingerprint:
            cur.execute(
                "SELECT id FROM jobs WHERE dedupe_fingerprint = %s LIMIT 1",
                (fingerprint,),
            )
            row = cur.fetchone()
            if row:
                existing_id = row["id"]

        # Fall back to company+title match
        if existing_id is None:
            cur.execute(
                "SELECT id FROM jobs WHERE company_name = %s AND role_title = %s LIMIT 1",
                (data["company_name"], data["role_title"]),
            )
            row = cur.fetchone()
            if row:
                existing_id = row["id"]

        if existing_id is not None:
            # Only update mutable fields; don't overwrite user edits to status/priority
            cur.execute("""
                UPDATE jobs SET
                    job_url           = COALESCE(%s, job_url),
                    description_raw   = COALESCE(%s, description_raw),
                    dedupe_fingerprint = COALESCE(%s, dedupe_fingerprint),
                    updated_at        = NOW()
                WHERE id = %s
            """, (
                data.get("job_url"),
                data.get("description_raw"),
                fingerprint,
                existing_id,
            ))
            return existing_id, "updated"

        # New job
        cur.execute("""
            INSERT INTO jobs (
                company_name, role_title, status, date_added, date_applied,
                salary_min, salary_max, location, work_type, source,
                job_url, notes, priority,
                dedupe_fingerprint, external_job_id, description_raw
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s
            )
            RETURNING id
        """, (
            data["company_name"],
            data["role_title"],
            data.get("status", "Researching"),
            data.get("date_added", date.today()),
            data.get("date_applied"),
            data.get("salary_min"),
            data.get("salary_max"),
            data.get("location"),
            data.get("work_type", "Remote"),
            data.get("source", "Other"),
            data.get("job_url"),
            data.get("notes"),
            data.get("priority", "Medium"),
            fingerprint,
            data.get("external_job_id"),
            data.get("description_raw"),
        ))
        new_id = cur.fetchone()["id"]
        return new_id, "created"
