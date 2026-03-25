"""
job_scraper.py — Search job APIs and import results into jobs.db

Usage:
    python job_scraper.py                          # Run all profiles
    python job_scraper.py --profile "Disaster Recovery / Emergency Management"
    python job_scraper.py --dry-run                # Preview without writing
    python job_scraper.py --force                  # Skip deduplication
    python job_scraper.py --import-csv jobs.csv    # Import from CSV
    python job_scraper.py --db /path/to/jobs.db    # Custom DB path
"""

import argparse
import csv
import json
import logging
import sqlite3
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import requests
from rapidfuzz import fuzz, process

from company_watcher import run_company_watcher

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------

SCRIPT_DIR   = Path(__file__).parent
DEFAULT_DB   = SCRIPT_DIR.parent / "jobs.db"
CONFIG_FILE  = SCRIPT_DIR / "search_config.json"
LOGS_DIR     = SCRIPT_DIR / "scraper_logs"

DEDUP_THRESHOLD  = 85   # score >= this → duplicate
REVIEW_THRESHOLD = 70   # score >= this → flag for review

WORK_TYPE_KEYWORDS = {
    "Remote":  ["remote", "work from home", "wfh", "fully remote", "100% remote", "anywhere"],
    "Hybrid":  ["hybrid", "partially remote", "flexible location", "flex work"],
    "On-site": ["on-site", "onsite", "in-office", "in office", "on site"],
}

VALID_STATUSES = [
    "Researching", "Ready to Apply", "Applied", "Phone Screen",
    "Interview", "Technical Assessment", "Final Round",
    "Offer", "Rejected", "Withdrawn", "Ghosted",
]

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config(path: Path = CONFIG_FILE) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with open(path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def detect_work_type(text: str) -> str:
    lower = text.lower()
    for work_type, keywords in WORK_TYPE_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            return work_type
    return "On-site"


def to_annual_salary(val) -> Optional[int]:
    """Convert a raw salary value to an integer annual figure."""
    if val is None:
        return None
    try:
        v = float(val)
        if v <= 0:
            return None
        if v < 500:        # looks hourly
            v = v * 2080
        elif v < 5000:     # looks weekly
            v = v * 52
        elif v < 20000:    # looks monthly
            v = v * 12
        return int(v)
    except (ValueError, TypeError):
        return None


def score_priority(company: str, salary_min, salary_max, profile: dict) -> str:
    targets = [t.lower() for t in profile.get("target_companies", [])]
    company_lower = company.lower()
    if any(t in company_lower or company_lower in t for t in targets):
        return "High"
    sal = salary_min or salary_max
    if sal and sal >= 100_000:
        return "High"
    return profile.get("default_priority", "Medium")


# ---------------------------------------------------------------------------
# API response normalisers
# ---------------------------------------------------------------------------

def normalize_jsearch(raw: dict, profile: dict, query: str) -> dict:
    company  = (raw.get("employer_name") or "").strip()
    title    = (raw.get("job_title") or "").strip()
    desc     = raw.get("job_description") or ""

    city     = raw.get("job_city") or ""
    state    = raw.get("job_state") or ""
    location = ", ".join(p for p in [city, state] if p) or raw.get("job_country") or "Unknown"

    if raw.get("job_is_remote"):
        work_type = "Remote"
    else:
        work_type = detect_work_type(f"{title} {desc} {location}")

    sal_min = to_annual_salary(raw.get("job_min_salary"))
    sal_max = to_annual_salary(raw.get("job_max_salary"))

    return {
        "company_name": company,
        "role_title":   title,
        "status":       "Researching",
        "date_added":   date.today().isoformat(),
        "date_applied": None,
        "salary_min":   sal_min,
        "salary_max":   sal_max,
        "location":     location,
        "work_type":    work_type,
        "source":       "JSearch API",
        "job_url":      raw.get("job_apply_link"),
        "notes":        f"Imported via scraper — query: \"{query}\"",
        "priority":     score_priority(company, sal_min, sal_max, profile),
    }


def normalize_adzuna(raw: dict, profile: dict, query: str) -> dict:
    company  = (raw.get("company", {}).get("display_name") or "").strip()
    title    = (raw.get("title") or "").strip()
    desc     = raw.get("description") or ""
    location = raw.get("location", {}).get("display_name") or "Unknown"

    work_type = detect_work_type(f"{title} {desc} {location}")
    sal_min   = to_annual_salary(raw.get("salary_min"))
    sal_max   = to_annual_salary(raw.get("salary_max"))

    return {
        "company_name": company,
        "role_title":   title,
        "status":       "Researching",
        "date_added":   date.today().isoformat(),
        "date_applied": None,
        "salary_min":   sal_min,
        "salary_max":   sal_max,
        "location":     location,
        "work_type":    work_type,
        "source":       "Adzuna",
        "job_url":      raw.get("redirect_url"),
        "notes":        f"Imported via scraper — query: \"{query}\"",
        "priority":     score_priority(company, sal_min, sal_max, profile),
    }


# ---------------------------------------------------------------------------
# API clients
# ---------------------------------------------------------------------------

class JSearchClient:
    def __init__(self, cfg: dict):
        self.key      = cfg["api_key"]
        self.base_url = cfg.get("base_url", "https://jsearch.p.rapidapi.com")
        self.headers  = {
            "X-RapidAPI-Key":  self.key,
            "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
        }

    def search(self, query: str, num_results: int = 20, days_posted: int = 30) -> list[dict]:
        results   = []
        per_page  = min(num_results, 10)
        max_pages = -(-num_results // per_page)   # ceiling division

        for page in range(1, max_pages + 1):
            try:
                resp = requests.get(
                    f"{self.base_url}/search",
                    headers=self.headers,
                    params={
                        "query":       query,
                        "page":        str(page),
                        "num_pages":   "1",
                        "date_posted": "month" if days_posted >= 30 else "week",
                    },
                    timeout=15,
                )
                resp.raise_for_status()
                batch = resp.json().get("data") or []
                if not batch:
                    break
                results.extend(batch)
                if len(results) >= num_results:
                    break
                time.sleep(1.2)     # stay well within rate limits
            except requests.RequestException as exc:
                log.error(f"  JSearch error on page {page} for '{query}': {exc}")
                break

        return results[:num_results]


class AdzunaClient:
    def __init__(self, cfg: dict):
        self.app_id   = cfg["app_id"]
        self.app_key  = cfg["app_key"]
        self.country  = cfg.get("country", "us")

    def search(self, query: str, num_results: int = 20, days_posted: int = 30) -> list[dict]:
        try:
            resp = requests.get(
                f"https://api.adzuna.com/v1/api/jobs/{self.country}/search/1",
                params={
                    "app_id":           self.app_id,
                    "app_key":          self.app_key,
                    "what":             query,
                    "results_per_page": min(num_results, 50),
                    "max_days_old":     days_posted,
                    "content-type":     "application/json",
                },
                timeout=15,
            )
            resp.raise_for_status()
            return (resp.json().get("results") or [])[:num_results]
        except requests.RequestException as exc:
            log.error(f"  Adzuna error for '{query}': {exc}")
            return []


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def _job_key(company: str, title: str) -> str:
    return f"{company.lower().strip()} | {title.lower().strip()}"


def deduplicate(
    incoming: list[dict],
    existing: list[dict],
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Returns (new_jobs, duplicates, flagged_for_review).
    Deduplicates both against the DB and within the incoming batch.
    """
    existing_keys = [_job_key(j["company_name"], j["role_title"]) for j in existing]
    seen: set[str] = set()
    new_jobs, duplicates, flagged = [], [], []

    for job in incoming:
        key = _job_key(job["company_name"], job["role_title"])

        # Within-batch dedup
        if key in seen:
            duplicates.append(job)
            continue
        seen.add(key)

        if not existing_keys:
            new_jobs.append(job)
            continue

        best = process.extractOne(key, existing_keys, scorer=fuzz.token_sort_ratio)
        score = best[1] if best else 0

        if score >= DEDUP_THRESHOLD:
            duplicates.append(job)
        elif score >= REVIEW_THRESHOLD:
            job["_fuzzy_match"] = f"{best[0]}  (score: {score})"
            flagged.append(job)
        else:
            new_jobs.append(job)

    return new_jobs, duplicates, flagged


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def load_existing_jobs(db_path: Path) -> list[dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT id, company_name, role_title FROM jobs").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def insert_jobs(jobs: list[dict], db_path: Path) -> int:
    if not jobs:
        return 0
    conn = sqlite3.connect(db_path)
    imported = 0
    for job in jobs:
        try:
            conn.execute("""
                INSERT INTO jobs (
                    company_name, role_title, status, date_added, date_applied,
                    salary_min, salary_max, location, work_type, source,
                    job_url, notes, priority
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job["company_name"],   job["role_title"],   job["status"],
                job["date_added"],     job.get("date_applied"),
                job.get("salary_min"), job.get("salary_max"),
                job.get("location"),   job.get("work_type", "Remote"),
                job.get("source", "Other"), job.get("job_url"),
                job.get("notes"),      job.get("priority", "Medium"),
            ))
            imported += 1
        except sqlite3.Error as exc:
            log.error(f"  DB insert failed ({job.get('company_name')}): {exc}")
    conn.commit()
    conn.close()
    return imported


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def save_report(report: dict) -> Path:
    LOGS_DIR.mkdir(exist_ok=True)
    filename = datetime.now().strftime("%Y-%m-%d_%H-%M") + ".json"
    path = LOGS_DIR / filename
    with open(path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    log.info(f"Report saved → {path}")
    return path


def print_summary(report: dict):
    w = 50
    print("\n" + "=" * w)
    print("  SCRAPER RUN SUMMARY")
    print("=" * w)
    print(f"  Profiles run:       {report.get('profiles_run', '—')}")
    print(f"  Queries executed:   {report.get('queries_executed', '—')}")
    print(f"  Raw results found:  {report.get('total_found', '—')}")
    print(f"  After filters:      {report.get('after_filter', report.get('total_found', '—'))}")
    prefix = "[DRY RUN] Would add" if report.get("dry_run") else "New jobs added"
    print(f"  {prefix}:     {report.get('imported', 0)}")
    print(f"  Duplicates skipped: {report.get('duplicates', 0)}")
    print(f"  Flagged for review: {report.get('flagged', 0)}")

    if report.get("flagged_jobs"):
        print(f"\n  {'-' * (w - 2)}")
        print("  Flagged for review (possible near-duplicates):")
        for j in report["flagged_jobs"]:
            print(f"    • {j['company_name']}  |  {j['role_title']}")
            if j.get("_fuzzy_match"):
                print(f"      Possible match: {j['_fuzzy_match']}")

    print("=" * w + "\n")


# ---------------------------------------------------------------------------
# CSV import
# ---------------------------------------------------------------------------

# Maps common CSV header variations → DB column names
_CSV_MAP = {
    "company":        "company_name",
    "company_name":   "company_name",
    "employer":       "company_name",
    "role":           "role_title",
    "title":          "role_title",
    "job_title":      "role_title",
    "role_title":     "role_title",
    "position":       "role_title",
    "status":         "status",
    "location":       "location",
    "work_type":      "work_type",
    "remote":         "work_type",
    "source":         "source",
    "url":            "job_url",
    "job_url":        "job_url",
    "link":           "job_url",
    "apply_url":      "job_url",
    "salary_min":     "salary_min",
    "min_salary":     "salary_min",
    "salary_max":     "salary_max",
    "max_salary":     "salary_max",
    "notes":          "notes",
    "priority":       "priority",
    "date_applied":   "date_applied",
}


def import_csv(csv_path: Path, db_path: Path, dry_run: bool = False, force: bool = False) -> dict:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    log.info(f"Read {len(rows)} rows from {csv_path.name}")

    jobs = []
    for row in rows:
        job: dict = {
            "status":     "Researching",
            "date_added": date.today().isoformat(),
            "work_type":  "Remote",
            "source":     "Other",
            "priority":   "Medium",
        }
        for col, val in row.items():
            key = _CSV_MAP.get(col.lower().strip().replace(" ", "_"))
            if key and val and str(val).strip():
                job[key] = str(val).strip()

        if not job.get("company_name") or not job.get("role_title"):
            log.warning(f"  Skipping row (missing company/title): {dict(row)}")
            continue

        if job.get("status") not in VALID_STATUSES:
            job["status"] = "Researching"

        job["salary_min"] = to_annual_salary(job.get("salary_min"))
        job["salary_max"] = to_annual_salary(job.get("salary_max"))
        jobs.append(job)

    existing = [] if force else load_existing_jobs(db_path)
    new_jobs, dupes, flagged = deduplicate(jobs, existing)

    imported = len(new_jobs) if dry_run else insert_jobs(new_jobs, db_path)

    report = {
        "timestamp":      datetime.now().isoformat(),
        "source":         str(csv_path),
        "dry_run":        dry_run,
        "profiles_run":   1,
        "queries_executed": 1,
        "total_found":    len(jobs),
        "after_filter":   len(jobs),
        "imported":       imported,
        "duplicates":     len(dupes),
        "flagged":        len(flagged),
        "flagged_jobs":   flagged,
    }
    print_summary(report)
    save_report(report)
    return report


# ---------------------------------------------------------------------------
# Main scraper orchestrator
# ---------------------------------------------------------------------------

class Scraper:
    def __init__(self, config: dict, db_path: Path, dry_run: bool = False,
                 force: bool = False, companies_only: bool = False):
        self.config         = config
        self.db_path        = db_path
        self.dry_run        = dry_run
        self.force          = force
        self.companies_only = companies_only
        self.filters        = config.get("filters", {})
        self.clients        = {} if companies_only else self._init_clients()

    def _init_clients(self) -> dict:
        clients = {}
        for name, cfg in self.config.get("apis", {}).items():
            if not cfg.get("enabled"):
                continue
            if name == "jsearch":
                key = cfg.get("api_key", "")
                if key and key != "YOUR_RAPIDAPI_KEY_HERE":
                    clients["jsearch"] = JSearchClient(cfg)
                    log.info("API enabled: JSearch (RapidAPI)")
                else:
                    log.warning("JSearch enabled but api_key not set — skipping.")
            elif name == "adzuna":
                app_id = cfg.get("app_id", "")
                if app_id and app_id != "YOUR_ADZUNA_APP_ID":
                    clients["adzuna"] = AdzunaClient(cfg)
                    log.info("API enabled: Adzuna")
                else:
                    log.warning("Adzuna enabled but credentials not set — skipping.")

        if not clients:
            raise RuntimeError(
                "No APIs configured. Open search_config.json, set your API key(s), "
                "and set enabled=true."
            )
        return clients

    def _apply_filters(self, jobs: list[dict]) -> list[dict]:
        exclude    = [kw.lower() for kw in self.filters.get("exclude_keywords", [])]
        min_salary = self.filters.get("min_salary")
        work_types = [w.lower() for w in self.filters.get("work_types", [])]

        kept = []
        for job in jobs:
            title = job["role_title"].lower()

            if any(kw in title for kw in exclude):
                continue
            if min_salary and job.get("salary_max") and job["salary_max"] < min_salary:
                continue
            if work_types and job["work_type"].lower() not in work_types:
                continue
            kept.append(job)

        return kept

    def run(self, profile_name: str = None) -> dict:
        existing    = [] if self.force else load_existing_jobs(self.db_path)
        all_raw:    list[dict] = []
        queries_run = 0

        # ── API keyword search (JSearch / Adzuna) ──────────────────────────
        if not self.companies_only and self.clients:
            profiles = self.config.get("search_profiles", [])
            if profile_name:
                profiles = [p for p in profiles if p["name"] == profile_name]
                if not profiles:
                    raise ValueError(f"Profile '{profile_name}' not found in config.")

            max_per_query = self.filters.get("max_results_per_query", 20)
            days_posted   = self.filters.get("days_posted", 30)

            for profile in profiles:
                log.info(f"\nProfile: {profile['name']}")
                for query in profile.get("queries", []):
                    log.info(f"  Query: \"{query}\"")
                    for api_name, client in self.clients.items():
                        try:
                            raw = client.search(query, max_per_query, days_posted)
                            log.info(f"    {api_name}: {len(raw)} results")
                            if api_name == "jsearch":
                                normalised = [normalize_jsearch(r, profile, query) for r in raw]
                            else:
                                normalised = [normalize_adzuna(r, profile, query) for r in raw]
                            all_raw.extend(normalised)
                            queries_run += 1
                            time.sleep(1)
                        except Exception as exc:
                            log.error(f"    Error ({api_name}, '{query}'): {exc}")

        # ── Company watcher ────────────────────────────────────────────────
        # Company watcher results bypass work_type/salary filters — the companies
        # were hand-picked, so any keyword-matching role there is worth seeing.
        companies = self.config.get("target_companies", [])
        company_jobs: list[dict] = []
        if companies:
            log.info(f"\n── Company Watcher ({len(companies)} companies) ──")
            company_jobs = run_company_watcher(companies)
            log.info(f"Company watcher total: {len(company_jobs)} matching jobs found")

        # Apply full filters only to API results; company jobs skip work_type/salary
        api_filtered   = self._apply_filters(all_raw)
        # Still exclude title keywords from company results (e.g. intern, VP)
        exclude = [kw.lower() for kw in self.filters.get("exclude_keywords", [])]
        co_filtered = [
            j for j in company_jobs
            if not any(kw in j["role_title"].lower() for kw in exclude)
        ]

        filtered = api_filtered + co_filtered
        log.info(f"\nFiltering: {len(all_raw)} → {len(filtered)} results")

        new_jobs, dupes, flagged = deduplicate(filtered, existing)

        imported = len(new_jobs) if self.dry_run else insert_jobs(new_jobs, self.db_path)
        if self.dry_run:
            log.info(f"[DRY RUN] Would import {imported} jobs — nothing written.")
        else:
            log.info(f"Imported {imported} new jobs into {self.db_path}")

        report = {
            "timestamp":        datetime.now().isoformat(),
            "dry_run":          self.dry_run,
            "profiles_run":     0 if self.companies_only else len(self.config.get("search_profiles", [])),
            "queries_executed": queries_run,
            "total_found":      len(all_raw),
            "after_filter":     len(filtered),
            "imported":         imported,
            "duplicates":       len(dupes),
            "flagged":          len(flagged),
            "flagged_jobs":     flagged,
        }
        print_summary(report)
        save_report(report)
        return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Search job APIs and import results into jobs.db",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--profile",        metavar="NAME",     help="Run a single search profile by name")
    parser.add_argument("--dry-run",        action="store_true", help="Preview results without writing to DB")
    parser.add_argument("--force",          action="store_true", help="Import all results, skip deduplication")
    parser.add_argument("--companies-only", action="store_true", help="Skip API keyword search; run company watcher only")
    parser.add_argument("--db",             metavar="PATH",     default=str(DEFAULT_DB), help="Path to jobs.db")
    parser.add_argument("--config",         metavar="PATH",     default=str(CONFIG_FILE), help="Path to search_config.json")
    parser.add_argument("--import-csv",     metavar="CSV_PATH", help="Import jobs from a CSV file")
    args = parser.parse_args()

    db_path     = Path(args.db)
    config_path = Path(args.config)

    if not db_path.exists():
        log.error(f"Database not found: {db_path}\nRun the Streamlit app once to create it, or use --db to specify the path.")
        raise SystemExit(1)

    if args.import_csv:
        import_csv(Path(args.import_csv), db_path, dry_run=args.dry_run, force=args.force)
        return

    config  = load_config(config_path)
    scraper = Scraper(config, db_path, dry_run=args.dry_run, force=args.force,
                      companies_only=args.companies_only)
    scraper.run(profile_name=args.profile)


if __name__ == "__main__":
    main()
