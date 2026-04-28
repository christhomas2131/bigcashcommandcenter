"""
ingestion/enrichment.py - Optional Claude relevance scoring for ingested jobs.

This pass is intentionally conservative: if Claude is disabled, unavailable, or
returns an unexpected response, jobs continue through the pipeline unchanged.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

import requests

import config

log = logging.getLogger(__name__)

_ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"


def _extract_json(text: str) -> Any:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    start = cleaned.find("[")
    end = cleaned.rfind("]")
    if start != -1 and end != -1 and end > start:
        cleaned = cleaned[start:end + 1]
    return json.loads(cleaned)


def _payload_for_job(idx: int, job: dict) -> dict:
    desc = (job.get("description_raw") or job.get("notes") or "")[:900]
    return {
        "idx": idx,
        "company": job.get("company_name", ""),
        "title": job.get("role_title", ""),
        "location": job.get("location", ""),
        "work_type": job.get("work_type", ""),
        "source": job.get("source", ""),
        "description": desc,
    }


def _score_batch(jobs: list[dict], settings: dict) -> dict[int, dict]:
    prompt_jobs = [_payload_for_job(i, job) for i, job in enumerate(jobs)]
    system_prompt = (
        "You score jobs for a public job board focused on Chris Ball's search: "
        "disaster recovery and emergency management, FEMA public assistance, "
        "hazard mitigation, CDBG-DR, grants management, floodplain/NFIP/CFM, "
        "GovTech/public sector SaaS, implementation/customer success, solutions "
        "consulting, solutions engineering, sales engineering, and technical "
        "account management. Favor remote, hybrid, Bay Area, California, and "
        "public sector roles. Penalize internships, VP/executive roles, unrelated "
        "engineering roles, and roles requiring active security clearance."
    )
    user_prompt = (
        "Return only a JSON array. For each job, include: "
        "idx, keep (boolean), score (0-100), priority (High|Medium|Low), "
        "reason (short string). Jobs:\n"
        f"{json.dumps(prompt_jobs, ensure_ascii=True)}"
    )

    resp = requests.post(
        _ANTHROPIC_URL,
        headers={
            "x-api-key": config.ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": settings.get("model") or config.ANTHROPIC_MODEL,
            "max_tokens": int(settings.get("max_tokens", 1200)),
            "temperature": 0,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
        },
        timeout=int(settings.get("timeout_seconds", 30)),
    )
    resp.raise_for_status()
    payload = resp.json()
    content = payload.get("content") or []
    text = "".join(part.get("text", "") for part in content if part.get("type") == "text")
    decisions = _extract_json(text)
    return {int(item["idx"]): item for item in decisions if "idx" in item}


def enrich_jobs_with_claude(jobs: list[dict], settings: dict) -> tuple[list[dict], dict]:
    stats = {"reviewed": 0, "dropped": 0, "errors": 0}
    if not jobs or not settings.get("enabled"):
        return jobs, stats
    if not config.ANTHROPIC_API_KEY:
        log.warning("Claude enrichment enabled but ANTHROPIC_API_KEY is not set - skipping.")
        return jobs, stats

    max_jobs = int(settings.get("max_jobs_per_run", 75))
    batch_size = max(1, min(int(settings.get("batch_size", 10)), 20))
    min_score = int(settings.get("min_score", 45))
    high_priority_score = int(settings.get("high_priority_score", 85))

    reviewed = jobs[:max_jobs]
    passthrough = jobs[max_jobs:]
    kept: list[dict] = []

    for start in range(0, len(reviewed), batch_size):
        batch = reviewed[start:start + batch_size]
        try:
            decisions = _score_batch(batch, settings)
        except Exception as exc:
            log.warning(f"Claude enrichment failed for batch {start // batch_size + 1}: {exc}")
            stats["errors"] += 1
            kept.extend(batch)
            continue

        for idx, job in enumerate(batch):
            decision = decisions.get(idx)
            if not decision:
                kept.append(job)
                continue

            stats["reviewed"] += 1
            score = int(decision.get("score") or 0)
            keep = bool(decision.get("keep", True)) and score >= min_score
            if not keep:
                stats["dropped"] += 1
                continue

            enriched = dict(job)
            priority = decision.get("priority") or enriched.get("priority") or "Medium"
            if score >= high_priority_score:
                priority = "High"
            enriched["priority"] = priority

            reason = str(decision.get("reason") or "").strip()
            note = f"Claude relevance: {score}/100"
            if reason:
                note = f"{note} - {reason[:180]}"
            existing_notes = enriched.get("notes") or ""
            enriched["notes"] = f"{existing_notes}\n{note}".strip()
            kept.append(enriched)

    kept.extend(passthrough)
    return kept, stats
