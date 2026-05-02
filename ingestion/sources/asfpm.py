"""
ingestion/sources/asfpm.py — Scraper for ASFPM Career Center
https://careers.floods.org/jobs

No API key required. Uses requests + BeautifulSoup.
Scrapes paginated job listings from the Association of State Floodplain
Managers career center.
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
from datetime import date, timedelta

import urllib3
import cloudscraper
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger(__name__)

BASE_URL   = "https://careers.floods.org"
SEARCH_URL = "https://careers.floods.org/jobs"


def _parse_posted_date(text: str) -> date:
    text = text.strip().lower()
    if "today" in text or "hour" in text or "just" in text:
        return date.today()
    if "yesterday" in text:
        return date.today() - timedelta(days=1)
    m = re.search(r"(\d+)\s*day", text)
    if m:
        return date.today() - timedelta(days=int(m.group(1)))
    m = re.search(r"(\d+)\s*week", text)
    if m:
        return date.today() - timedelta(weeks=int(m.group(1)))
    m = re.search(r"(\d+)\s*month", text)
    if m:
        return date.today() - timedelta(days=int(m.group(1)) * 30)
    return date.today()


def _parse_work_type(text: str) -> str:
    lower = text.lower()
    if "remote" in lower:
        return "Remote"
    if "hybrid" in lower:
        return "Hybrid"
    return "On-site"


def _fingerprint(company: str, title: str, url: str) -> str:
    raw = f"{company.lower()}|{title.lower()}|{url.lower()}"
    return hashlib.md5(raw.encode()).hexdigest()


def _parse_page(html: str) -> list[dict]:
    """Extract job dicts from one page of ASFPM results."""
    soup = BeautifulSoup(html, "html.parser")
    jobs = []

    # ASFPM uses a standard job board — try several common container patterns
    # Pattern 1: article or div with data-job or job-listing class
    containers = (
        soup.find_all("article", class_=re.compile(r"job", re.I))
        or soup.find_all("div", class_=re.compile(r"job[-_]?listing|job[-_]?item|job[-_]?card|joblisting", re.I))
        or soup.find_all("li", class_=re.compile(r"job", re.I))
    )

    for tile in containers:
        try:
            # Title — look for a heading or prominent link
            title_el = (
                tile.find(["h2", "h3", "h4"], class_=re.compile(r"title|position|role", re.I))
                or tile.find(["h2", "h3", "h4"])
                or tile.find("a", href=re.compile(r"/job|/career|/position", re.I))
            )
            title = title_el.get_text(strip=True) if title_el else ""

            # URL
            a_tag = tile.find("a", href=True)
            job_url = ""
            if a_tag:
                href = a_tag["href"]
                job_url = href if href.startswith("http") else BASE_URL + href

            # Company / organization
            company_el = tile.find(class_=re.compile(r"company|employer|organization|org", re.I))
            company = company_el.get_text(strip=True) if company_el else "Unknown"

            # Location
            loc_el = tile.find(class_=re.compile(r"location|city|geo", re.I))
            location = loc_el.get_text(strip=True) if loc_el else ""

            # Work type from location text or explicit tag
            wt_el = tile.find(class_=re.compile(r"remote|work.?type|job.?type", re.I))
            wt_text = wt_el.get_text(strip=True) if wt_el else location
            work_type = _parse_work_type(wt_text)

            # Posted date
            date_el = tile.find(class_=re.compile(r"date|posted|age", re.I))
            date_added = _parse_posted_date(date_el.get_text()) if date_el else date.today()

            # External ID from URL slug or data attribute
            ext_id = ""
            if job_url:
                slug_match = re.search(r"/(\d+)(?:/|$)", job_url)
                ext_id = f"asfpm_{slug_match.group(1)}" if slug_match else ""

            if not title:
                continue

            jobs.append({
                "company_name":       company,
                "role_title":         title,
                "status":             "Researching",
                "date_added":         date_added,
                "date_applied":       None,
                "salary_min":         None,
                "salary_max":         None,
                "location":           location,
                "work_type":          work_type,
                "source":             "ASFPM",
                "job_url":            job_url,
                "notes":              "",
                "priority":           "Medium",
                "external_job_id":    ext_id,
                "description_raw":    "",
                "dedupe_fingerprint": _fingerprint(company, title, job_url),
            })
        except Exception as exc:
            log.debug(f"ASFPM: skipped tile — {exc}")

    return jobs


def scrape_asfpm(max_pages: int = 4) -> list[dict]:
    """
    Scrape up to max_pages pages of ASFPM job listings.
    Returns a list of normalised job dicts.
    """
    session = cloudscraper.create_scraper()
    session.verify = False  # must be set on session, not per-request (Python 3.10+ ssl compat)
    all_jobs: list[dict] = []

    for pg in range(1, max_pages + 1):
        url = SEARCH_URL if pg == 1 else f"{SEARCH_URL}?page={pg}"
        try:
            resp = session.get(url, timeout=20)
            resp.raise_for_status()
        except Exception as exc:
            log.error(f"ASFPM: failed to fetch page {pg} — {exc}")
            break

        page_jobs = _parse_page(resp.text)
        log.info(f"  ASFPM page {pg}: {len(page_jobs)} jobs")

        if not page_jobs:
            break

        all_jobs.extend(page_jobs)
        time.sleep(1.5)

    # Deduplicate within the scraped batch
    seen = set()
    unique = []
    for j in all_jobs:
        key = j.get("external_job_id") or j["dedupe_fingerprint"]
        if key not in seen:
            seen.add(key)
            unique.append(j)

    log.info(f"ASFPM total: {len(unique)} unique jobs across {pg} pages")
    return unique
