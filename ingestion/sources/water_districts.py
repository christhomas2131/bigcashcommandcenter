"""
ingestion/sources/water_districts.py

Scrapes job listings from regional water district career pages using the
Firecrawl SDK (required because these agencies use JS-rendered ATS platforms
such as NEOGOV, Workday, and SmartRecruiters that plain HTML scraping cannot
reach).

Requires FIRECRAWL_API_KEY in environment — silently skips if not set.
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import date
from urllib.parse import urljoin

import config
from ingestion.sources.normalize import detect_work_type

log = logging.getLogger(__name__)

WATER_DISTRICTS = [
    {
        "name": "Harris County Flood Control District",
        "url": "https://www.governmentjobs.com/careers/hcfcd",
        "state": "TX",
        "keywords": ["floodplain", "flood", "stormwater", "hydraulic", "engineer", "hydrology", "cfm", "water", "environmental"],
    },
    {
        "name": "South Florida Water Management District",
        "url": "https://careers.sfwmd.gov/",
        "state": "FL",
        "keywords": ["water", "flood", "floodplain", "environmental", "hydraulic", "hydrology", "engineer", "scientist"],
    },
    {
        "name": "Metropolitan Water Reclamation District of Chicago",
        "url": "https://www.governmentjobs.com/careers/mwrd",
        "state": "IL",
        "keywords": ["water", "engineer", "environmental", "hydraulic", "stormwater", "flood"],
    },
    {
        "name": "Tarrant Regional Water District",
        "url": "https://www.governmentjobs.com/careers/trwd",
        "state": "TX",
        "keywords": ["water", "engineer", "environmental", "floodplain", "hydraulic", "stormwater"],
    },
    {
        "name": "East Bay Municipal Utility District",
        "url": "https://www.governmentjobs.com/careers/ebmud",
        "state": "CA",
        "keywords": ["water", "engineer", "environmental", "hydraulic", "stormwater", "floodplain"],
    },
    {
        "name": "San Francisco Public Utilities Commission",
        "url": "https://jobs.smartrecruiters.com/CityAndCountyOfSanFrancisco1",
        "state": "CA",
        "keywords": ["water", "stormwater", "flood", "hydraulic", "engineer", "environmental"],
    },
    {
        "name": "Denver Water",
        "url": "https://www.denverwater.org/about-us/careers/current-openings",
        "state": "CO",
        "keywords": ["water", "engineer", "environmental", "hydraulic", "stormwater"],
    },
    {
        "name": "Miami-Dade Water and Sewer",
        "url": "https://www.miamidade.gov/jobs/",
        "state": "FL",
        "keywords": ["water", "engineer", "environmental", "hydraulic", "flood", "stormwater"],
    },
    {
        "name": "Arizona Department of Water Resources",
        "url": "https://www.governmentjobs.com/careers/adwr",
        "state": "AZ",
        "keywords": ["water", "floodplain", "hydraulic", "hydrology", "engineer", "cfm", "flood"],
    },
]

EXCLUDE_KEYWORDS = ["intern", "internship", "student", "vice president", "vp", "chief", "director"]


def _fingerprint(company: str, title: str, url: str) -> str:
    key = f"{company.lower().strip()}|{title.lower().strip()}|{url.strip()}"
    return hashlib.md5(key.encode()).hexdigest()


def _clean_title(raw: str) -> str:
    title = re.sub(r"\s+", " ", raw).strip(" -|:\t\r\n")
    title = re.sub(r"^(apply now|view job|job details|learn more)\s*[:|-]?\s*", "", title, flags=re.I)
    return title[:160].strip()


def _matches(title: str, extra: str, keywords: list[str]) -> bool:
    text = f"{title} {extra}".lower()
    return (
        any(kw.lower() in text for kw in keywords)
        and not any(ex in text for ex in EXCLUDE_KEYWORDS)
    )


def _make_job(district: dict, title: str, url: str, page_url: str) -> dict:
    return {
        "company_name":       district["name"],
        "role_title":         title,
        "status":             "Researching",
        "date_added":         date.today(),
        "date_applied":       None,
        "salary_min":         None,
        "salary_max":         None,
        "location":           district["state"],
        "work_type":          detect_work_type(title),
        "source":             "Water District",
        "job_url":            url,
        "notes":              f"Imported via Firecrawl from {page_url}",
        "priority":           "Medium",
        "external_job_id":    None,
        "description_raw":    "",
        "dedupe_fingerprint": _fingerprint(district["name"], title, url),
    }


def _extract_jobs(markdown: str, district: dict, max_jobs: int = 25) -> list[dict]:
    page_url = district["url"]
    keywords = district["keywords"]
    seen: set[str] = set()
    jobs: list[dict] = []

    for label, href in re.findall(r"\[([^\]]{3,180})\]\(([^)\s]+)\)", markdown):
        title = _clean_title(label)
        if not title:
            continue
        job_url = urljoin(page_url, href)
        if job_url in seen:
            continue
        if not _matches(title, job_url, keywords):
            continue
        seen.add(job_url)
        jobs.append(_make_job(district, title, job_url, page_url))
        if len(jobs) >= max_jobs:
            return jobs

    for line in markdown.splitlines():
        title = _clean_title(line)
        if len(title) < 8 or len(title) > 120:
            continue
        if not _matches(title, "", keywords):
            continue
        key = f"{district['name']}|{title}"
        if key in seen:
            continue
        seen.add(key)
        jobs.append(_make_job(district, title, page_url, page_url))
        if len(jobs) >= max_jobs:
            break

    return jobs


def scrape_water_districts(max_jobs_per_page: int = 25) -> list[dict]:
    """Scrape water district career pages via Firecrawl. Returns normalized job dicts."""
    if not config.FIRECRAWL_API_KEY:
        log.warning("FIRECRAWL_API_KEY not set — skipping water district sources.")
        return []

    try:
        from firecrawl import FirecrawlApp
    except ImportError:
        log.warning("firecrawl package not installed — skipping water districts.")
        return []

    app = FirecrawlApp(api_key=config.FIRECRAWL_API_KEY)
    all_jobs: list[dict] = []

    for district in WATER_DISTRICTS:
        try:
            log.info(f"Water districts: scraping {district['name']}...")
            result = app.scrape(district["url"], formats=["markdown"])
            markdown = result.markdown or "" if hasattr(result, "markdown") else (result or {}).get("markdown", "")
            jobs = _extract_jobs(markdown, district, max_jobs_per_page)
            log.info(f"  {district['name']}: {len(jobs)} matching jobs")
            all_jobs.extend(jobs)
        except Exception as exc:
            log.warning(f"  Water district ({district['name']}) failed: {exc}")

    return all_jobs
