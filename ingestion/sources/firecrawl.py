"""
ingestion/sources/firecrawl.py - Firecrawl-backed fallback source.

Use this for career pages that do not expose a clean ATS API. It scrapes a
configured page, extracts likely job links from markdown, and normalizes the
matches into the same shape as the other ingestion sources.
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import date
from urllib.parse import urljoin

import requests

import config
from ingestion.sources.normalize import detect_work_type

log = logging.getLogger(__name__)

ROLE_KEYWORDS = [
    "disaster recovery",
    "emergency management",
    "fema",
    "public assistance",
    "hazard mitigation",
    "grant",
    "grants management",
    "cdbg",
    "resilience",
    "floodplain",
    "nfip",
    "program manager",
    "project manager",
    "implementation",
    "customer success",
    "solutions consultant",
    "solutions engineer",
    "sales engineer",
    "technical account manager",
    "public sector",
    "government",
    "govtech",
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


def _matches(title: str, extra: str = "", search_terms: list[str] | None = None) -> bool:
    text = f"{title} {extra}".lower()
    include_terms = [t.lower() for t in (search_terms or ROLE_KEYWORDS)]
    return any(term in text for term in include_terms) and not any(
        term in text for term in EXCLUDE_KEYWORDS
    )


def _clean_title(raw: str) -> str:
    title = re.sub(r"\s+", " ", raw).strip(" -|:\t\r\n")
    title = re.sub(r"^(apply now|view job|job details|learn more)\s*[:|-]?\s*", "", title, flags=re.I)
    return title[:160].strip()


def _job(company: str, title: str, url: str, page_url: str, context: str = "") -> dict:
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


class FirecrawlClient:
    def __init__(self, api_key: str, base_url: str = "https://api.firecrawl.dev"):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def scrape_markdown(self, url: str, timeout_ms: int = 30000) -> str:
        resp = requests.post(
            f"{self.base_url}/v1/scrape",
            headers=self.headers,
            json={
                "url": url,
                "formats": ["markdown"],
                "onlyMainContent": True,
                "timeout": timeout_ms,
            },
            timeout=max(15, timeout_ms // 1000 + 10),
        )
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data") or payload
        return data.get("markdown") or data.get("content") or ""


def _extract_jobs_from_markdown(
    markdown: str,
    page: dict,
    max_jobs: int,
) -> list[dict]:
    company = page["name"]
    page_url = page["url"]
    search_terms = page.get("search_terms") or ROLE_KEYWORDS
    seen_urls: set[str] = set()
    jobs: list[dict] = []

    links = re.findall(r"\[([^\]]{3,180})\]\(([^)\s]+)\)", markdown)
    for label, href in links:
        title = _clean_title(label)
        if not title:
            continue
        job_url = urljoin(page_url, href)
        if job_url in seen_urls:
            continue
        if not _matches(title, job_url, search_terms):
            continue
        seen_urls.add(job_url)
        jobs.append(_job(company, title, job_url, page_url))
        if len(jobs) >= max_jobs:
            return jobs

    # Some career pages render cards as plain text. Capture likely title lines
    # so Firecrawl still adds value when links are not visible in markdown.
    for line in markdown.splitlines():
        title = _clean_title(line)
        if len(title) < 8 or len(title) > 120:
            continue
        if not _matches(title, "", search_terms):
            continue
        synthetic_url = page_url
        synthetic_key = f"{company}|{title}|{synthetic_url}"
        if synthetic_key in seen_urls:
            continue
        seen_urls.add(synthetic_key)
        jobs.append(_job(company, title, synthetic_url, page_url, context=line))
        if len(jobs) >= max_jobs:
            break

    return jobs


def run_firecrawl_pages(settings: dict) -> list[dict]:
    if not settings.get("enabled"):
        return []
    if not config.FIRECRAWL_API_KEY:
        log.warning("Firecrawl enabled but FIRECRAWL_API_KEY is not set - skipping.")
        return []

    pages = settings.get("pages") or []
    if not pages:
        log.info("Firecrawl enabled but no pages configured.")
        return []

    client = FirecrawlClient(config.FIRECRAWL_API_KEY, config.FIRECRAWL_BASE_URL)
    max_jobs = int(settings.get("max_jobs_per_page", 25))
    timeout_ms = int(settings.get("timeout_ms", 30000))

    all_jobs: list[dict] = []
    for page in pages:
        name = page.get("name")
        url = page.get("url")
        if not name or not url:
            log.warning("Firecrawl page missing name or url - skipping.")
            continue
        try:
            markdown = client.scrape_markdown(url, timeout_ms=timeout_ms)
            jobs = _extract_jobs_from_markdown(markdown, page, max_jobs)
            log.info(f"  Firecrawl ({name}): {len(jobs)} matching")
            all_jobs.extend(jobs)
        except Exception as exc:
            log.warning(f"  Firecrawl ({name}) failed: {exc}")

    return all_jobs
