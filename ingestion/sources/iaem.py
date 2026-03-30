"""
ingestion/sources/iaem.py — Scraper for IAEM Career Center
https://jobs.iaem.org/jobs/

No API key required. Uses requests + BeautifulSoup.
Scrapes paginated job listings from the International Association
of Emergency Managers job board.
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
from datetime import date, timedelta

import cloudscraper
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

BASE_URL   = "https://jobs.iaem.org"
SEARCH_URL = "https://jobs.iaem.org/jobs/"


def _parse_posted_date(text: str) -> date:
    """Convert 'X days ago', 'today', 'yesterday' etc. to a date."""
    text = text.strip().lower()
    if "today" in text or "hour" in text or "just" in text:
        return date.today()
    if "yesterday" in text:
        return date.today() - timedelta(days=1)
    m = re.search(r"(\d+)\s*day", text)
    if m:
        return date.today() - timedelta(days=int(m.group(1)))
    return date.today()


def _parse_work_type(text: str) -> str:
    """Parse '(remote)', '(hybrid)', '(on-site)' into standard values."""
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
    """Extract job dicts from one page of IAEM results."""
    soup = BeautifulSoup(html, "html.parser")
    jobs = []

    # Each job is in a div whose class contains 'job-tile-{id}'
    for tile in soup.find_all("div", class_=re.compile(r"\bjob-tile-\d+")):
        try:
            # IDs and fields from hidden inputs (already HTML-decoded by BS4)
            job_id_el  = tile.find("input", {"name": "job_id"})
            title_el   = tile.find("input", {"name": "job_Position"})
            company_el = tile.find("input", {"name": "job_company"})

            job_id  = job_id_el["value"].strip()  if job_id_el  else ""
            title   = title_el["value"].strip()   if title_el   else ""
            company = company_el["value"].strip()  if company_el else ""

            # Fallback to visible text if hidden inputs are missing
            if not title:
                a = tile.find("a", href=re.compile(r"^/job/"))
                title = a.get_text(strip=True) if a else ""
            if not company:
                co_div = tile.find(class_="job-company-row")
                company = co_div.get_text(strip=True) if co_div else ""

            # Job URL
            a_tag = tile.find("a", href=re.compile(r"^/job/"))
            job_url = BASE_URL + a_tag["href"] if a_tag else ""

            # Location — strip out the work-type span, then clean text
            loc_div = tile.find(class_="job-location")
            wt_span = tile.find(class_="workplace-location")
            if wt_span:
                wt_text = wt_span.get_text(strip=True)
                wt_span.decompose()
            else:
                wt_text = ""

            # Multi-location: collect dropdown items
            items = loc_div.find_all(class_="dropdown-item") if loc_div else []
            if items:
                location = "; ".join(
                    i.get_text(strip=True) for i in items if i.get_text(strip=True)
                )
            elif loc_div:
                location = loc_div.get_text(" ", strip=True)
            else:
                location = ""

            # Work type
            work_type = _parse_work_type(wt_text) if wt_text else "On-site"

            # Posted date
            date_div = tile.find(class_="job-posted-date")
            date_added = _parse_posted_date(date_div.get_text()) if date_div else date.today()

            if not title or not company:
                continue

            jobs.append({
                "company_name":      company,
                "role_title":        title,
                "status":            "Researching",
                "date_added":        date_added,
                "date_applied":      None,
                "salary_min":        None,
                "salary_max":        None,
                "location":          location,
                "work_type":         work_type,
                "source":            "IAEM",
                "job_url":           job_url,
                "notes":             "",
                "priority":          "Medium",
                "external_job_id":   f"iaem_{job_id}" if job_id else "",
                "description_raw":   "",
                "dedupe_fingerprint": _fingerprint(company, title, job_url),
            })
        except Exception as exc:
            log.debug(f"IAEM: skipped tile — {exc}")

    return jobs


def scrape_iaem(max_pages: int = 4) -> list[dict]:
    """
    Scrape up to max_pages pages of IAEM job listings.
    Returns a list of normalised job dicts.
    """
    session = cloudscraper.create_scraper()

    all_jobs: list[dict] = []

    for pg in range(1, max_pages + 1):
        url = SEARCH_URL if pg == 1 else f"{SEARCH_URL}?pg={pg}"
        try:
            resp = session.get(url, timeout=20)
            resp.raise_for_status()
        except Exception as exc:
            log.error(f"IAEM: failed to fetch page {pg} — {exc}")
            break

        page_jobs = _parse_page(resp.text)
        log.info(f"  IAEM page {pg}: {len(page_jobs)} jobs")

        if not page_jobs:
            break

        all_jobs.extend(page_jobs)
        time.sleep(1.5)

    # Deduplicate within the scraped batch by external_job_id
    seen = set()
    unique = []
    for j in all_jobs:
        key = j.get("external_job_id") or j["dedupe_fingerprint"]
        if key not in seen:
            seen.add(key)
            unique.append(j)

    log.info(f"IAEM total: {len(unique)} unique jobs across {pg} pages")
    return unique
