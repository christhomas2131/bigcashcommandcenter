"""
ingestion/sources/tavily_discovery.py

Uses Tavily AI search to discover job listings broadly across the web —
company sites, government postings, niche boards, etc.
Returns normalized job dicts matching the existing ingestion schema.

Requires TAVILY_API_KEY in environment — silently skips if not set.
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import date

import config
from ingestion.sources.normalize import detect_work_type

log = logging.getLogger(__name__)

TAVILY_SEARCH_QUERIES = [
    # Core emergency management
    "emergency management jobs hiring",
    "disaster recovery specialist positions",
    "FEMA jobs opportunities",
    "hazard mitigation planner openings",
    "emergency preparedness coordinator jobs",
    # Floodplain / CFM
    "certified floodplain manager jobs",
    "floodplain administrator positions",
    "CFM jobs hiring",
    "hydraulic engineer floodplain jobs",
    # GovTech / Federal
    "emergency operations center jobs",
    "homeland security emergency management jobs",
    "public safety emergency management jobs",
    # Consulting
    "disaster recovery consultant jobs",
    "emergency management consulting positions",
    "FEMA contractor jobs hiring",
    # Recency emphasis
    "emergency management jobs posted this week",
    "disaster recovery jobs latest openings",
]

# Job board aggregators — Tavily returns their search-results pages, not actual jobs
_EXCLUDED_DOMAINS = [
    "linkedin.com", "indeed.com", "glassdoor.com", "ziprecruiter.com",
    "governmentjobs.com", "monster.com", "careerbuilder.com", "simplyhired.com",
    "snagajob.com", "dice.com", "theladders.com", "salary.com", "jobs.com",
    "hiring.com", "talent.com", "jooble.org", "jobberman.com", "adzuna.com",
    "jobgether.com", "handshake.com", "wayup.com", "internshala.com",
    "careerjet.com", "joblist.com", "ladders.com", "payscale.com",
    "builtin.com", "wellfound.com", "jobsearch.about.com",
]

# Regex patterns that identify search-results pages, not actual job postings
_GARBAGE_TITLE_PATTERNS = [
    re.compile(r"\|\s*(indeed|glassdoor|ziprecruiter|monster|careerbuilder|governmentjobs)", re.I),
    re.compile(r"-\s*(glassdoor|indeed|ziprecruiter|monster)", re.I),
    re.compile(r"\d+\s+.{3,40}\s+jobs?\s+(in|near)\b", re.I),   # "26 Emergency jobs in NY"
    re.compile(r"jobs?,\s+employment", re.I),                     # "Jobs, Employment"
    re.compile(r"\(now hiring\)", re.I),
    re.compile(r"\bhiring\s+now\b.{0,20}\d{4}", re.I),            # "Hiring Now May 2026"
    re.compile(r"^\$[\d,]+-\$[\d,]+/hr\s+.+jobs", re.I),         # "$16-$27/hr Remote Jobs"
    re.compile(r"\bwork opportunities at\b", re.I),
    re.compile(r"\bjobs?\s+&\s+careers?\b", re.I),
    re.compile(r"\bopen positions at\b", re.I),
    re.compile(r"\bcareers?\s+at\s+\w+", re.I),                   # "Careers at Company" (generic page)
    re.compile(r"\bjoin our team\b", re.I),
]


def _is_aggregator_url(url: str) -> bool:
    url_lower = url.lower()
    return any(domain in url_lower for domain in _EXCLUDED_DOMAINS)


def _is_garbage_title(title: str) -> bool:
    return any(p.search(title) for p in _GARBAGE_TITLE_PATTERNS)


def _fingerprint(company: str, title: str, url: str) -> str:
    key = f"{company.lower().strip()}|{title.lower().strip()}|{url.strip()}"
    return hashlib.md5(key.encode()).hexdigest()


def _company_from_url(url: str) -> str:
    """Best-effort company name from URL domain."""
    match = re.search(r"https?://(?:www\.|jobs\.)?([^/.]+)", url)
    return match.group(1).title() if match else "Unknown"


def _location_from_content(content: str) -> str:
    for pattern in [r"(?:Location|City):\s*([^\n,]+)", r"\b([A-Z][a-z]+,\s*[A-Z]{2})\b"]:
        m = re.search(pattern, content)
        if m:
            return m.group(1).strip()
    return ""


def _make_job(title: str, url: str, company: str, location: str, content: str) -> dict:
    return {
        "company_name":       company,
        "role_title":         title[:160].strip(),
        "status":             "Researching",
        "date_added":         date.today(),
        "date_applied":       None,
        "salary_min":         None,
        "salary_max":         None,
        "location":           location,
        "work_type":          detect_work_type(f"{title} {content}"),
        "source":             "Tavily Discovery",
        "job_url":            url,
        "notes":              "Discovered via Tavily AI search",
        "priority":           "Medium",
        "external_job_id":    None,
        "description_raw":    content[:1200],
        "dedupe_fingerprint": _fingerprint(company, title, url),
    }


def scrape_tavily(
    queries: list[str] | None = None,
    max_results_per_query: int = 10,
) -> list[dict]:
    """Run Tavily searches and return normalized job dicts."""
    if not config.TAVILY_API_KEY:
        log.warning("TAVILY_API_KEY not set — skipping Tavily discovery.")
        return []

    try:
        from tavily import TavilyClient
    except ImportError:
        log.warning("tavily-python not installed — skipping. Run: pip install tavily-python")
        return []

    client = TavilyClient(api_key=config.TAVILY_API_KEY)
    search_queries = queries or TAVILY_SEARCH_QUERIES
    seen_urls: set[str] = set()
    all_jobs: list[dict] = []
    skipped = 0

    for query in search_queries:
        try:
            log.info(f"Tavily: {query!r}")
            response = client.search(
                query=query,
                search_depth="advanced",
                max_results=max_results_per_query,
                exclude_domains=_EXCLUDED_DOMAINS,
            )
            for result in response.get("results", []):
                url     = (result.get("url") or "").strip()
                title   = (result.get("title") or "").strip()
                content = (result.get("content") or "")

                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                if _is_aggregator_url(url):
                    skipped += 1
                    continue
                if _is_garbage_title(title):
                    skipped += 1
                    continue

                company  = _company_from_url(url)
                location = _location_from_content(content)
                all_jobs.append(_make_job(title, url, company, location, content))

        except Exception as exc:
            log.warning(f"Tavily search failed for {query!r}: {exc}")

    log.info(f"Tavily: {len(all_jobs)} jobs kept, {skipped} aggregator/garbage pages dropped")
    return all_jobs
