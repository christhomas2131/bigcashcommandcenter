"""
ingestion/sources/normalize.py — Shared normalisation helpers for all API sources.
"""

from __future__ import annotations

import hashlib
from datetime import date
from typing import Optional


WORK_TYPE_KEYWORDS = {
    "Remote":  ["remote", "work from home", "wfh", "fully remote", "100% remote", "anywhere"],
    "Hybrid":  ["hybrid", "partially remote", "flexible location", "flex work"],
    "On-site": ["on-site", "onsite", "in-office", "in office", "on site"],
}


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


def make_fingerprint(company: str, title: str, url: str = "") -> str:
    """Stable MD5 hash used for deduplication across ingestion runs."""
    key = f"{company.lower().strip()}|{title.lower().strip()}|{url.strip()}"
    return hashlib.md5(key.encode()).hexdigest()


def normalize_jsearch(raw: dict, profile: dict, query: str) -> dict:
    company  = (raw.get("employer_name") or "").strip()
    title    = (raw.get("job_title") or "").strip()
    desc     = raw.get("job_description") or ""

    city     = raw.get("job_city") or ""
    state    = raw.get("job_state") or ""
    location = ", ".join(p for p in [city, state] if p) or raw.get("job_country") or "Unknown"

    work_type = "Remote" if raw.get("job_is_remote") else detect_work_type(f"{title} {desc} {location}")

    sal_min = to_annual_salary(raw.get("job_min_salary"))
    sal_max = to_annual_salary(raw.get("job_max_salary"))
    job_url = raw.get("job_apply_link") or ""

    return {
        "company_name":      company,
        "role_title":        title,
        "status":            "Researching",
        "date_added":        date.today(),
        "date_applied":      None,
        "salary_min":        sal_min,
        "salary_max":        sal_max,
        "location":          location,
        "work_type":         work_type,
        "source":            "JSearch API",
        "job_url":           job_url,
        "notes":             f'Imported via scraper — query: "{query}"',
        "priority":          score_priority(company, sal_min, sal_max, profile),
        "external_job_id":   raw.get("job_id"),
        "description_raw":   desc,
        "dedupe_fingerprint": make_fingerprint(company, title, job_url),
    }


def normalize_usajobs(raw: dict, query: str) -> dict:
    """Normalize a USAJobs SearchResultItem."""
    obj      = raw.get("MatchedObjectDescriptor", {})
    title    = (obj.get("PositionTitle") or "").strip()
    company  = (obj.get("OrganizationName") or "").strip()
    location = (obj.get("PositionLocationDisplay") or "").strip()
    job_url  = (obj.get("PositionURI") or "").strip()
    ext_id   = (obj.get("PositionID") or "").strip()

    # Salary
    rem = obj.get("PositionRemuneration", [{}])
    sal_min = to_annual_salary(rem[0].get("MinimumRange")) if rem else None
    sal_max = to_annual_salary(rem[0].get("MaximumRange")) if rem else None

    # Work type — USAJobs has a Telework field
    details   = obj.get("UserArea", {}).get("Details", {})
    telework  = (details.get("Telework") or "").lower()
    if "full" in telework or "100" in telework:
        work_type = "Remote"
    elif "eligible" in telework or "partial" in telework:
        work_type = "Hybrid"
    else:
        work_type = detect_work_type(f"{title} {location}")

    desc = (details.get("JobSummary") or "").strip()

    # Priority — federal consulting firms are High
    high_orgs = ["fema", "hud", "epa", "cdbg", "usace", "army corps"]
    priority = "High" if any(o in company.lower() for o in high_orgs) else "Medium"

    return {
        "company_name":       company,
        "role_title":         title,
        "status":             "Researching",
        "date_added":         date.today(),
        "date_applied":       None,
        "salary_min":         sal_min,
        "salary_max":         sal_max,
        "location":           location,
        "work_type":          work_type,
        "source":             "USAJobs",
        "job_url":            job_url,
        "notes":              f'Imported via USAJobs — query: "{query}"',
        "priority":           priority,
        "external_job_id":    ext_id,
        "description_raw":    desc,
        "dedupe_fingerprint": make_fingerprint(company, title, job_url),
    }


def normalize_adzuna(raw: dict, profile: dict, query: str) -> dict:
    company  = (raw.get("company", {}).get("display_name") or "").strip()
    title    = (raw.get("title") or "").strip()
    desc     = raw.get("description") or ""
    location = raw.get("location", {}).get("display_name") or "Unknown"
    job_url  = raw.get("redirect_url") or ""

    work_type = detect_work_type(f"{title} {desc} {location}")
    sal_min   = to_annual_salary(raw.get("salary_min"))
    sal_max   = to_annual_salary(raw.get("salary_max"))

    return {
        "company_name":      company,
        "role_title":        title,
        "status":            "Researching",
        "date_added":        date.today(),
        "date_applied":      None,
        "salary_min":        sal_min,
        "salary_max":        sal_max,
        "location":          location,
        "work_type":         work_type,
        "source":            "Adzuna",
        "job_url":           job_url,
        "notes":             f'Imported via scraper — query: "{query}"',
        "priority":          score_priority(company, sal_min, sal_max, profile),
        "external_job_id":   raw.get("id"),
        "description_raw":   desc,
        "dedupe_fingerprint": make_fingerprint(company, title, job_url),
    }
