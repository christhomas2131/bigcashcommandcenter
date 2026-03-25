"""
ingestion/dedup.py — Fuzzy deduplication against the existing jobs table.

Priority order:
  1. Exact fingerprint match (URL hash)  → duplicate
  2. Exact external_job_id match         → duplicate
  3. Exact company + title match         → duplicate
  4. Fuzzy score >= DEDUP_THRESHOLD (90) → duplicate
  5. Fuzzy score >= REVIEW_THRESHOLD (70)→ flagged for review
  6. Otherwise                           → new
"""

from __future__ import annotations

from rapidfuzz import fuzz, process

DEDUP_THRESHOLD  = 90   # raised from 85 — reduces false positives
REVIEW_THRESHOLD = 70


def _job_key(company: str, title: str) -> str:
    return f"{company.lower().strip()} | {title.lower().strip()}"


def deduplicate(
    incoming: list[dict],
    existing: list[dict],
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Returns (new_jobs, duplicates, flagged_for_review).

    existing rows must have at minimum: company_name, role_title.
    Optionally: dedupe_fingerprint, external_job_id.
    """
    # Build lookup sets for fast exact matching
    existing_fingerprints = {
        j["dedupe_fingerprint"]
        for j in existing
        if j.get("dedupe_fingerprint")
    }
    existing_ext_ids = {
        j["external_job_id"]
        for j in existing
        if j.get("external_job_id")
    }
    existing_keys = [_job_key(j["company_name"], j["role_title"]) for j in existing]
    existing_keys_set = set(existing_keys)

    seen_fingerprints: set[str] = set()
    seen_keys: set[str] = set()

    new_jobs: list[dict] = []
    duplicates: list[dict] = []
    flagged: list[dict] = []

    for job in incoming:
        fingerprint = job.get("dedupe_fingerprint")
        ext_id      = job.get("external_job_id")
        key         = _job_key(job["company_name"], job["role_title"])

        # ── Within-batch dedup ────────────────────────────────────────────
        if fingerprint and fingerprint in seen_fingerprints:
            duplicates.append(job)
            continue
        if key in seen_keys:
            duplicates.append(job)
            continue

        if fingerprint:
            seen_fingerprints.add(fingerprint)
        seen_keys.add(key)

        # ── Exact fingerprint match against DB ────────────────────────────
        if fingerprint and fingerprint in existing_fingerprints:
            duplicates.append(job)
            continue

        # ── Exact external_job_id match ───────────────────────────────────
        if ext_id and ext_id in existing_ext_ids:
            duplicates.append(job)
            continue

        # ── Exact company+title match ─────────────────────────────────────
        if key in existing_keys_set:
            duplicates.append(job)
            continue

        # ── No existing jobs to fuzzy-match against ───────────────────────
        if not existing_keys:
            new_jobs.append(job)
            continue

        # ── Fuzzy match ───────────────────────────────────────────────────
        best  = process.extractOne(key, existing_keys, scorer=fuzz.token_sort_ratio)
        score = best[1] if best else 0

        if score >= DEDUP_THRESHOLD:
            duplicates.append(job)
        elif score >= REVIEW_THRESHOLD:
            job = dict(job)
            job["_fuzzy_match"] = f"{best[0]}  (score: {score})"
            flagged.append(job)
        else:
            new_jobs.append(job)

    return new_jobs, duplicates, flagged
