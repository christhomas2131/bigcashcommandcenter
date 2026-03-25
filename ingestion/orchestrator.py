"""
ingestion/orchestrator.py — Main ingestion pipeline.

Orchestrates API keyword search + company watcher, deduplicates against
the Postgres jobs table, records the run in ingestion_runs, and upserts
new/updated jobs via db.repository.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path

import config
from db import repository as repo
from ingestion.dedup import deduplicate
from ingestion.sources.company_watcher import run_company_watcher
from ingestion.sources.jsearch import JSearchClient
from ingestion.sources.adzuna import AdzunaClient
from ingestion.sources.usajobs import USAJobsClient
from ingestion.sources.normalize import normalize_jsearch, normalize_adzuna, normalize_usajobs

log = logging.getLogger(__name__)

WORK_TYPE_KEYWORDS_LOWER = ["remote", "hybrid"]


# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------

def _apply_api_filters(jobs: list[dict], filters: dict) -> list[dict]:
    """Full filter set: exclude keywords + work_type + min salary."""
    exclude    = [kw.lower() for kw in filters.get("exclude_keywords", [])]
    min_salary = filters.get("min_salary")
    work_types = [w.lower() for w in filters.get("work_types", [])]

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


def _apply_title_filter(jobs: list[dict], filters: dict) -> list[dict]:
    """Exclude-keywords only (used for company-watcher results)."""
    exclude = [kw.lower() for kw in filters.get("exclude_keywords", [])]
    return [j for j in jobs if not any(kw in j["role_title"].lower() for kw in exclude)]


# ---------------------------------------------------------------------------
# Client initialisation
# ---------------------------------------------------------------------------

def _init_clients(api_config: dict) -> dict:
    clients = {}

    jsearch_cfg = api_config.get("jsearch", {})
    if jsearch_cfg.get("enabled"):
        key = jsearch_cfg.get("api_key") or config.JSEARCH_API_KEY
        if key and key not in ("", "YOUR_RAPIDAPI_KEY_HERE"):
            clients["jsearch"] = JSearchClient(key, jsearch_cfg.get("base_url", "https://jsearch.p.rapidapi.com"))
            log.info("API enabled: JSearch")
        else:
            log.warning("JSearch enabled but no api_key — skipping.")

    adzuna_cfg = api_config.get("adzuna", {})
    if adzuna_cfg.get("enabled"):
        app_id = adzuna_cfg.get("app_id") or config.ADZUNA_APP_ID
        app_key = adzuna_cfg.get("app_key") or config.ADZUNA_APP_KEY
        if app_id and app_id not in ("", "YOUR_ADZUNA_APP_ID"):
            clients["adzuna"] = AdzunaClient(app_id, app_key, adzuna_cfg.get("country", "us"))
            log.info("API enabled: Adzuna")
        else:
            log.warning("Adzuna enabled but credentials not set — skipping.")

    usajobs_cfg = api_config.get("usajobs", {})
    if usajobs_cfg.get("enabled"):
        email   = usajobs_cfg.get("email") or config.USAJOBS_EMAIL
        api_key = usajobs_cfg.get("api_key") or config.USAJOBS_API_KEY
        if email and api_key and api_key not in ("", "YOUR_USAJOBS_API_KEY"):
            clients["usajobs"] = USAJobsClient(email, api_key)
            log.info("API enabled: USAJobs")
        else:
            log.warning("USAJobs enabled but credentials not set — skipping.")

    return clients


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run(
    search_config_path: str | None = None,
    profile_name: str | None = None,
    companies_only: bool = False,
    dry_run: bool = False,
) -> dict:
    """
    Run the full ingestion pipeline.  Returns a summary report dict.

    search_config_path — override default from config.INGESTION_SEARCH_CONFIG
    profile_name       — run a single search profile by name
    companies_only     — skip API keyword search; only run company watcher
    dry_run            — fetch + dedup but don't write to DB
    """
    cfg_path = Path(search_config_path or config.INGESTION_SEARCH_CONFIG)
    if not cfg_path.exists():
        raise FileNotFoundError(f"search_config.json not found: {cfg_path}")
    with open(cfg_path) as f:
        search_cfg = json.load(f)

    filters     = search_cfg.get("filters", {})
    api_config  = search_cfg.get("apis", {})
    companies   = search_cfg.get("target_companies", [])

    # Record the run start (skipped in dry-run mode)
    run_id: int | None = None
    if not dry_run:
        run_id = repo.start_ingestion_run(source="all" if not companies_only else "companies")

    report: dict = {
        "timestamp":        datetime.now().isoformat(),
        "dry_run":          dry_run,
        "profiles_run":     0,
        "queries_executed": 0,
        "jobs_found":       0,
        "jobs_created":     0,
        "jobs_updated":     0,
        "jobs_skipped":     0,
        "error_count":      0,
        "flagged":          0,
        "flagged_jobs":     [],
    }

    try:
        existing = repo.load_jobs_for_dedup()
        all_normalised: list[dict] = []

        # ── API keyword search ─────────────────────────────────────────────
        if not companies_only:
            clients = _init_clients(api_config)
            profiles = search_cfg.get("search_profiles", [])
            if profile_name:
                profiles = [p for p in profiles if p["name"] == profile_name]
                if not profiles:
                    raise ValueError(f"Profile '{profile_name}' not found in config.")

            max_per_query = filters.get("max_results_per_query", 20)
            days_posted   = filters.get("days_posted", 30)

            for profile in profiles:
                log.info(f"\nProfile: {profile['name']}")
                for query in profile.get("queries", []):
                    log.info(f"  Query: \"{query}\"")
                    for api_name, client in clients.items():
                        try:
                            raw = client.search(query, max_per_query, days_posted)
                            log.info(f"    {api_name}: {len(raw)} results")
                            if api_name == "jsearch":
                                normalised = [normalize_jsearch(r, profile, query) for r in raw]
                            elif api_name == "usajobs":
                                normalised = [normalize_usajobs(r, query) for r in raw]
                            else:
                                normalised = [normalize_adzuna(r, profile, query) for r in raw]
                            all_normalised.extend(normalised)
                            report["queries_executed"] += 1
                            time.sleep(1)
                        except Exception as exc:
                            log.error(f"    Error ({api_name}, '{query}'): {exc}")
                            report["error_count"] += 1

            report["profiles_run"] = len(profiles)

        # ── USAJobs extra (federal-specific) queries ───────────────────────
        usajobs_client = clients.get("usajobs")
        if usajobs_client:
            usajobs_cfg   = api_config.get("usajobs", {})
            extra_queries = usajobs_cfg.get("extra_queries", [])
            location      = usajobs_cfg.get("location")
            for eq in extra_queries:
                keyword = eq.get("keyword", "")
                if not keyword:
                    continue
                log.info(f'  USAJobs extra: "{keyword}"')
                try:
                    raw = usajobs_client.search(keyword, max_per_query, days_posted, location=location)
                    log.info(f"    usajobs: {len(raw)} results")
                    all_normalised.extend([normalize_usajobs(r, keyword) for r in raw])
                    report["queries_executed"] += 1
                    time.sleep(0.5)
                except Exception as exc:
                    log.error(f"    USAJobs extra error ('{keyword}'): {exc}")
                    report["error_count"] += 1

        # ── Company watcher ────────────────────────────────────────────────
        company_jobs: list[dict] = []
        if companies:
            log.info(f"\n── Company Watcher ({len(companies)} companies) ──")
            try:
                company_jobs = run_company_watcher(companies)
                log.info(f"Company watcher total: {len(company_jobs)} matching jobs")
            except Exception as exc:
                log.error(f"Company watcher failed: {exc}")
                report["error_count"] += 1

        # ── Filtering ─────────────────────────────────────────────────────
        api_filtered = _apply_api_filters(all_normalised, filters)
        co_filtered  = _apply_title_filter(company_jobs, filters)
        combined     = api_filtered + co_filtered

        report["jobs_found"] = len(combined)
        log.info(f"\nFiltering: {len(all_normalised) + len(company_jobs)} → {len(combined)}")

        # ── Deduplication ─────────────────────────────────────────────────
        new_jobs, duplicates, flagged = deduplicate(combined, existing)
        report["jobs_skipped"] = len(duplicates)
        report["flagged"]      = len(flagged)
        report["flagged_jobs"] = flagged

        log.info(
            f"Dedup: {len(new_jobs)} new, {len(duplicates)} dupes, {len(flagged)} flagged"
        )

        # ── Persist ───────────────────────────────────────────────────────
        if not dry_run:
            for job in new_jobs + flagged:  # flagged still get saved, just noted
                try:
                    _, action = repo.upsert_ingested_job(job)
                    if action == "created":
                        report["jobs_created"] += 1
                    elif action == "updated":
                        report["jobs_updated"] += 1
                except Exception as exc:
                    log.error(f"  DB upsert failed ({job.get('company_name')}): {exc}")
                    report["error_count"] += 1
        else:
            report["jobs_created"] = len(new_jobs)
            log.info(f"[DRY RUN] Would create {len(new_jobs)} jobs — nothing written.")

    except Exception as exc:
        log.error(f"Ingestion failed: {exc}")
        report["error_count"] += 1
        if run_id is not None:
            repo.complete_ingestion_run(run_id, {**report, "status": "error"})
        raise

    # ── Finalise run record ────────────────────────────────────────────────
    if run_id is not None:
        repo.complete_ingestion_run(run_id, {
            "status":        "completed",
            "jobs_found":    report["jobs_found"],
            "jobs_created":  report["jobs_created"],
            "jobs_updated":  report["jobs_updated"],
            "jobs_skipped":  report["jobs_skipped"],
            "error_count":   report["error_count"],
            "run_notes":     f"flagged={report['flagged']}",
        })

    _print_summary(report)
    return report


# ---------------------------------------------------------------------------
# Summary printer
# ---------------------------------------------------------------------------

def _print_summary(report: dict) -> None:
    w = 52
    print("\n" + "=" * w)
    print("  INGESTION RUN SUMMARY")
    print("=" * w)
    print(f"  Profiles run:       {report.get('profiles_run', '—')}")
    print(f"  Queries executed:   {report.get('queries_executed', '—')}")
    print(f"  Jobs found:         {report.get('jobs_found', '—')}")
    prefix = "[DRY RUN] Would create" if report.get("dry_run") else "Jobs created"
    print(f"  {prefix}:    {report.get('jobs_created', 0)}")
    print(f"  Jobs updated:       {report.get('jobs_updated', 0)}")
    print(f"  Duplicates skipped: {report.get('jobs_skipped', 0)}")
    print(f"  Flagged for review: {report.get('flagged', 0)}")
    print(f"  Errors:             {report.get('error_count', 0)}")

    flagged_jobs = report.get("flagged_jobs", [])
    if flagged_jobs:
        print(f"\n  {'-' * (w - 2)}")
        print("  Flagged (possible near-duplicates):")
        for j in flagged_jobs[:10]:
            print(f"    • {j['company_name']}  |  {j['role_title']}")
            if j.get("_fuzzy_match"):
                print(f"      Possible match: {j['_fuzzy_match']}")

    print("=" * w + "\n")
