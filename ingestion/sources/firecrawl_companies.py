"""
ingestion/sources/firecrawl_companies.py

Scrapes career pages for emergency-management and government-contracting firms
using the Firecrawl Python SDK. Returns normalized job dicts matching the
existing ingestion schema.

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

FIRECRAWL_COMPANIES = [
    {
        "name": "CDR Maguire",
        "url": "https://cdrmaguire.com/careers/",
        "keywords": ["disaster", "recovery", "debris", "damage", "assessment", "emergency"],
    },
    {
        "name": "Witt O'Brien's",
        "url": "https://wittobriens.com/careers/",
        "keywords": ["crisis", "emergency", "disaster", "recovery", "preparedness"],
    },
    {
        "name": "Aptive Resources",
        "url": "https://aptiveresources.com/careers",
        "keywords": ["FEMA", "emergency", "disaster", "federal"],
    },
    {
        "name": "IEM",
        "url": "https://www.iem.com/careers/",
        "keywords": ["emergency", "homeland", "disaster", "FEMA", "preparedness"],
    },
    {
        "name": "Arcadis",
        "url": "https://careers.arcadis.com/",
        "keywords": ["resilience", "climate", "emergency", "disaster", "infrastructure"],
    },
    {
        "name": "AECOM",
        "url": "https://jobs.aecom.com/",
        "keywords": ["resilience", "disaster", "recovery", "emergency", "infrastructure"],
    },
    {
        "name": "Tetra Tech",
        "url": "https://www.tetratech.com/careers",
        "keywords": ["disaster", "recovery", "environmental", "emergency", "resilience"],
    },
    {
        "name": "Michael Baker International",
        "url": "https://www.mbakerintl.com/careers",
        "keywords": ["FEMA", "hazard", "mitigation", "disaster", "emergency"],
    },
    {
        "name": "Stantec",
        "url": "https://www.stantec.com/en/careers",
        "keywords": ["resilience", "disaster", "infrastructure", "emergency"],
    },
    {
        "name": "JEO Consulting",
        "url": "https://www.jeo.com/careers",
        "keywords": ["floodplain", "disaster", "recovery", "mitigation", "emergency"],
    },
    {
        "name": "Jacobs",
        "url": "https://careers.jacobs.com/",
        "keywords": ["resilience", "infrastructure", "disaster", "emergency", "critical"],
    },
    {
        "name": "Wood PLC",
        "url": "https://www.woodplc.com/careers",
        "keywords": ["environmental", "disaster", "recovery", "infrastructure"],
    },
    {
        "name": "Freese and Nichols",
        "url": "https://www.freese.com/careers",
        "keywords": ["water", "floodplain", "hazard", "mitigation", "disaster"],
    },
    {
        "name": "Kimley-Horn",
        "url": "https://www.kimley-horn.com/careers",
        "keywords": ["stormwater", "resilience", "infrastructure", "disaster"],
    },
    {
        "name": "ManTech",
        "url": "https://www.mantech.com/careers",
        "keywords": ["FEMA", "DHS", "emergency", "federal", "homeland"],
    },
    {
        "name": "Peraton",
        "url": "https://www.peraton.com/careers",
        "keywords": ["federal", "mission", "emergency", "homeland", "FEMA"],
    },
    {
        "name": "GDIT",
        "url": "https://gdit.com/careers",
        "keywords": ["FEMA", "federal", "emergency", "IT", "mission"],
    },
    {
        "name": "ESA",
        "url": "https://www.esassoc.com/careers",
        "keywords": ["environmental", "climate", "resilience", "NEPA", "planning"],
    },
    # CFM / water resources firms
    {
        "name": "WEST Consultants",
        "url": "https://www.westconsultants.com/careers",
        "keywords": ["floodplain", "hydrology", "hydraulic", "CFM", "water", "HEC-RAS", "stormwater"],
    },
    {
        "name": "Mead & Hunt",
        "url": "https://www.meadhunt.com/careers",
        "keywords": ["floodplain", "stormwater", "water resources", "hydraulic", "CFM"],
    },
    {
        "name": "HR Green",
        "url": "https://www.hrgreen.com/careers",
        "keywords": ["floodplain", "stormwater", "water", "CFM", "hydrology"],
    },
    {
        "name": "Ayres Associates",
        "url": "https://www.ayresassociates.com/careers",
        "keywords": ["floodplain", "water resources", "hydraulic", "hydrology", "CFM"],
    },
    {
        "name": "HDR",
        "url": "https://www.hdrinc.com/careers",
        "keywords": ["water resources", "floodplain", "stormwater", "hydraulic", "CFM"],
    },
    {
        "name": "SNC-Lavalin (Atkins)",
        "url": "https://www.snclavalin.com/en/careers",
        "keywords": ["water resources", "flood risk", "floodplain", "hydraulic"],
    },
]

EXCLUDE_KEYWORDS = [
    "intern",
    "internship",
    "student",
    "senior director",
    "vice president",
    "vp",
    "chief",
    "clearance",
    "ts/sci",
    "top secret",
]


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


def _make_job(company: str, title: str, url: str, page_url: str, context: str = "") -> dict:
    return {
        "company_name":       company,
        "role_title":         title,
        "status":             "Researching",
        "date_added":         date.today(),
        "date_applied":       None,
        "salary_min":         None,
        "salary_max":         None,
        "location":           "",
        "work_type":          detect_work_type(f"{title} {context}"),
        "source":             "Firecrawl",
        "job_url":            url,
        "notes":              f"Imported via Firecrawl from {page_url}",
        "priority":           "Medium",
        "external_job_id":    None,
        "description_raw":    context[:1200],
        "dedupe_fingerprint": _fingerprint(company, title, url),
    }


def _extract_jobs(markdown: str, company: dict, max_jobs: int = 25) -> list[dict]:
    name = company["name"]
    page_url = company["url"]
    keywords = company["keywords"]
    seen: set[str] = set()
    jobs: list[dict] = []

    # Prefer linked job titles (markdown [text](url) pattern)
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
        jobs.append(_make_job(name, title, job_url, page_url))
        if len(jobs) >= max_jobs:
            return jobs

    # Fall back to plain-text lines for pages that render cards without links
    for line in markdown.splitlines():
        title = _clean_title(line)
        if len(title) < 8 or len(title) > 120:
            continue
        if not _matches(title, "", keywords):
            continue
        key = f"{name}|{title}|{page_url}"
        if key in seen:
            continue
        seen.add(key)
        jobs.append(_make_job(name, title, page_url, page_url, context=line))
        if len(jobs) >= max_jobs:
            break

    return jobs


def scrape_firecrawl_companies(max_jobs_per_page: int = 25) -> list[dict]:
    """Scrape all FIRECRAWL_COMPANIES career pages. Returns normalized job dicts."""
    if not config.FIRECRAWL_API_KEY:
        log.warning("FIRECRAWL_API_KEY not set — skipping Firecrawl company sources.")
        return []

    try:
        from firecrawl import FirecrawlApp
    except ImportError:
        log.warning("firecrawl package not installed — skipping. Run: pip install firecrawl-py")
        return []

    app = FirecrawlApp(api_key=config.FIRECRAWL_API_KEY)
    all_jobs: list[dict] = []

    for company in FIRECRAWL_COMPANIES:
        try:
            log.info(f"Firecrawl companies: scraping {company['name']}...")
            result = app.scrape(company["url"], formats=["markdown"])
            markdown = result.markdown or "" if hasattr(result, "markdown") else (result or {}).get("markdown", "")

            jobs = _extract_jobs(markdown, company, max_jobs_per_page)
            log.info(f"  {company['name']}: {len(jobs)} matching jobs")
            all_jobs.extend(jobs)
        except Exception as exc:
            log.warning(f"  Firecrawl companies ({company['name']}) failed: {exc}")

    return all_jobs
