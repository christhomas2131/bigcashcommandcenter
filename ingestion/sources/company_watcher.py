"""
ingestion/sources/company_watcher.py — Scrape current openings from target company career pages.

Ported from job-scraper/company_watcher.py into the ingestion package.
Supported ATS: Breezy HR, Lever, Workday, iCIMS, KPMG, Deloitte.
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
from datetime import date
from typing import Optional

import requests

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Keyword filter
# ---------------------------------------------------------------------------

ROLE_KEYWORDS = [
    "disaster recovery",
    "emergency management",
    "fema",
    "public assistance",
    "hazard mitigation",
    "grant",
    "cdbg",
    "floodplain",
    "program manager",
    "project manager",
    "implementation",
    "customer success",
    "onboarding",
    "solutions consultant",
]

_CLEARANCE_EXCLUDE = [
    "clearance",
    "ts/sci",
    "top secret",
    "secret clearance",
    "classified",
    "dod ",
    "federal contract",
    "security clearance",
    "public trust clearance",
]


def _matches(title: str, extra: str = "") -> bool:
    text = f"{title} {extra}".lower()
    return any(kw in text for kw in ROLE_KEYWORDS)


def _is_clearance_or_federal(title: str) -> bool:
    t = title.lower()
    return any(term in t for term in _CLEARANCE_EXCLUDE)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def _detect_work_type(text: str) -> str:
    t = text.lower()
    if "hybrid" in t:
        return "Hybrid"
    if any(w in t for w in ["remote", "work from home", "wfh"]):
        return "Remote"
    return "On-site"


def _make_fingerprint(company: str, title: str, url: str = "") -> str:
    key = f"{company.lower().strip()}|{title.lower().strip()}|{url.strip()}"
    return hashlib.md5(key.encode()).hexdigest()


def _job(company: str, title: str, url: str, location: str = "",
         work_type: Optional[str] = None) -> dict:
    return {
        "company_name":       company,
        "role_title":         title,
        "status":             "Researching",
        "date_added":         date.today(),
        "date_applied":       None,
        "salary_min":         None,
        "salary_max":         None,
        "location":           location,
        "work_type":          work_type or _detect_work_type(f"{title} {location}"),
        "source":             "Company Site",
        "job_url":            url,
        "notes":              "Imported via company watcher",
        "priority":           "Medium",
        "external_job_id":    None,
        "description_raw":    None,
        "dedupe_fingerprint": _make_fingerprint(company, title, url),
    }


# ---------------------------------------------------------------------------
# Breezy HR
# ---------------------------------------------------------------------------

def scrape_breezy(company_name: str, slug: str) -> list[dict]:
    url = f"https://{slug}.breezy.hr/json"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"  Breezy ({company_name}): {e}")
        return []

    jobs = []
    for item in data:
        title = item.get("name", "").strip()
        if not _matches(title):
            continue
        loc_obj  = item.get("location", {})
        location = loc_obj.get("name", "") if isinstance(loc_obj, dict) else str(loc_obj)
        job_url  = f"https://{slug}.breezy.hr/p/{item.get('friendly_id', '')}"
        jobs.append(_job(company_name, title, job_url, location))

    log.info(f"  Breezy ({company_name}): {len(jobs)} matching")
    return jobs


# ---------------------------------------------------------------------------
# Lever
# ---------------------------------------------------------------------------

def scrape_lever(company_name: str, slug: str) -> list[dict]:
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"  Lever ({company_name}): {e}")
        return []

    jobs = []
    for item in data:
        title = item.get("text", "").strip()
        if not _matches(title):
            continue
        cats    = item.get("categories", {})
        loc     = cats.get("location", "")
        job_url = item.get("hostedUrl") or item.get("applyUrl") or ""
        jobs.append(_job(company_name, title, job_url, loc))

    log.info(f"  Lever ({company_name}): {len(jobs)} matching")
    return jobs


# ---------------------------------------------------------------------------
# Workday
# ---------------------------------------------------------------------------

def scrape_workday(company_name: str, tenant: str, wd_num: int, site: str) -> list[dict]:
    base    = f"https://{tenant}.wd{wd_num}.myworkdayjobs.com"
    api_url = f"{base}/wday/cxs/{tenant}/{site}/jobs"

    jobs   = []
    offset = 0
    limit  = 20

    while True:
        try:
            resp = requests.post(
                api_url,
                json={"limit": limit, "offset": offset, "searchText": "", "appliedFacets": {}},
                headers={**_HEADERS, "Content-Type": "application/json"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.error(f"  Workday ({company_name}): {e}")
            break

        postings = data.get("jobPostings", [])
        if not postings:
            break

        for item in postings:
            title = item.get("title", "").strip()
            if not _matches(title):
                continue
            location = item.get("locationsText", "")
            path     = item.get("externalPath", "")
            job_url  = f"{base}/{site}{path}" if path else ""
            jobs.append(_job(company_name, title, job_url, location))

        total   = data.get("total", 0)
        offset += limit
        if offset >= total:
            break
        time.sleep(0.5)

    log.info(f"  Workday ({company_name}): {len(jobs)} matching")
    return jobs


# ---------------------------------------------------------------------------
# iCIMS
# ---------------------------------------------------------------------------

def scrape_icims(company_name: str, subdomain: str) -> list[dict]:
    base = f"https://{subdomain}.icims.com"
    search_terms = [
        "disaster recovery", "emergency management",
        "program manager", "project manager",
        "implementation", "customer success",
    ]

    seen_urls: set[str] = set()
    jobs: list[dict] = []

    for term in search_terms:
        url = (
            f"{base}/jobs/search"
            f"?ss=1&searchKeyword={requests.utils.quote(term)}&pr=50&in_iframe=1"
        )
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=15)
            resp.raise_for_status()
            html = resp.text
        except Exception as e:
            log.warning(f"  iCIMS ({company_name}) [{term}]: {e}")
            continue

        row_blocks = re.findall(
            r'class="[^"]*iCIMS_JobsTable[^"]*".*?</tr>', html, re.DOTALL
        )
        if not row_blocks:
            row_blocks = re.findall(
                r'<a[^>]+href="([^"]+/jobs/\d+/[^"]+)"[^>]*>(.*?)</a>', html, re.DOTALL
            )
            for href, raw_title in row_blocks:
                title = re.sub(r"<[^>]+>", "", raw_title).strip()
                if not title or not _matches(title):
                    continue
                job_url = href if href.startswith("http") else base + href
                if job_url in seen_urls:
                    continue
                seen_urls.add(job_url)
                jobs.append(_job(company_name, title, job_url))
            time.sleep(1)
            continue

        for block in row_blocks:
            title_m = re.search(
                r'<a[^>]+href="([^"]+)"[^>]*>\s*<span[^>]*>(.*?)</span>', block, re.DOTALL
            )
            loc_m = re.search(
                r'class="[^"]*jobAttribute[^"]*"[^>]*>(.*?)</(?:td|span|div)>', block, re.DOTALL
            )
            if not title_m:
                continue
            href  = title_m.group(1)
            title = re.sub(r"<[^>]+>", "", title_m.group(2)).strip()
            loc   = re.sub(r"<[^>]+>", "", loc_m.group(1)).strip() if loc_m else ""

            if not title or not _matches(title):
                continue
            job_url = href if href.startswith("http") else base + href
            if job_url in seen_urls:
                continue
            seen_urls.add(job_url)
            jobs.append(_job(company_name, title, job_url, loc))

        time.sleep(1)

    log.info(f"  iCIMS ({company_name}): {len(jobs)} matching")
    return jobs


# ---------------------------------------------------------------------------
# KPMG
# ---------------------------------------------------------------------------

_KPMG_BOILERPLATE = {
    "experienced", "associate", "manager", "director", "internship",
    "contractor", "executive", "early career", "advisory", "audit",
    "tax", "federal", "business support services", "innovation & technology",
    "performance hub",
}


def _parse_kpmg_card(raw_html: str) -> tuple[str, str]:
    text  = re.sub(r"<[^>]+>", "\n", raw_html)
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    title = ""
    location = ""
    for line in lines:
        if "|" in line:
            location = line.split("|", 1)[1].strip()
        elif re.match(r"^\d+\s+locations?$", line, re.I):
            continue
        elif line.lower() not in _KPMG_BOILERPLATE and len(line) > 5 and not title:
            title = line
    return title, location


def scrape_kpmg(company_name: str, search_terms: list[str]) -> list[dict]:
    base = "https://kpmguscareers.com"
    seen_ids: set[str] = set()
    jobs: list[dict] = []

    for term in search_terms:
        page = 1
        while True:
            url = f"{base}/job-search/?keyword={requests.utils.quote(term)}&spage={page}"
            try:
                resp = requests.get(url, headers=_HEADERS, timeout=15)
                resp.raise_for_status()
                html = resp.text
            except Exception as e:
                log.warning(f"  KPMG [{term}] p{page}: {e}")
                break

            cards = re.findall(
                r'<a[^>]+href="(/jobdetail/\?jobId=(\d+))"[^>]*>(.*?)</a>',
                html, re.DOTALL,
            )
            if not cards:
                break

            found_new = False
            for href, job_id, inner in cards:
                if job_id in seen_ids:
                    continue
                title, location = _parse_kpmg_card(inner)
                if not title or not _matches(title):
                    continue
                if _is_clearance_or_federal(title):
                    continue
                seen_ids.add(job_id)
                found_new = True
                jobs.append(_job(company_name, title, f"{base}{href}", location))

            if not found_new or f"spage={page + 1}" not in html:
                break
            page += 1
            time.sleep(0.75)

        time.sleep(1)

    log.info(f"  KPMG: {len(jobs)} matching")
    return jobs


# ---------------------------------------------------------------------------
# Deloitte
# ---------------------------------------------------------------------------

def scrape_deloitte(company_name: str, search_terms: list[str]) -> list[dict]:
    base = "https://apply.deloitte.com"
    seen_ids: set[str] = set()
    jobs: list[dict] = []

    for term in search_terms:
        url = f"{base}/en_US/careers/SearchJobs/{requests.utils.quote(term)}"
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=15)
            resp.raise_for_status()
            html = resp.text
        except Exception as e:
            log.warning(f"  Deloitte [{term}]: {e}")
            continue

        blocks = re.findall(
            r'<h3[^>]*>\s*<a\s+href="([^"]+JobDetail[^"]+)"[^>]*>([^<]+)</a>\s*</h3>'
            r'(.*?)'
            r'<(?:h3|div\s+class="[^"]*job)',
            html, re.DOTALL,
        )

        if not blocks:
            links = re.findall(
                r'<a\s+href="(https://apply\.deloitte\.com[^"]+JobDetail/[^/]+/(\d+))"[^>]*>\s*([^<]+?)\s*</a>',
                html,
            )
            for href, job_id, title in links:
                title = title.strip()
                if not title or job_id in seen_ids:
                    continue
                if not _matches(title) or _is_clearance_or_federal(title):
                    continue
                seen_ids.add(job_id)
                jobs.append(_job(company_name, title, href))
            time.sleep(1)
            continue

        for href, title, after_html in blocks:
            title = title.strip()
            job_id_m = re.search(r"/(\d+)(?:\?|$)", href)
            job_id   = job_id_m.group(1) if job_id_m else href

            if job_id in seen_ids:
                continue
            if not _matches(title) or _is_clearance_or_federal(title):
                continue

            loc_m    = re.search(r"<p[^>]*>([^<]*\|[^<]*)</p>", after_html)
            location = ""
            if loc_m:
                parts    = loc_m.group(1).split("|")
                location = parts[-1].strip()

            seen_ids.add(job_id)
            full_url = href if href.startswith("http") else f"{base}{href}"
            jobs.append(_job(company_name, title, full_url, location))

        time.sleep(1)

    log.info(f"  Deloitte: {len(jobs)} matching")
    return jobs


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def run_company_watcher(companies: list[dict]) -> list[dict]:
    all_jobs: list[dict] = []

    for co in companies:
        ats  = co.get("ats", "").lower()
        name = co["name"]
        log.info(f"Watching: {name} ({ats})")

        try:
            if ats == "breezy":
                batch = scrape_breezy(name, co["slug"])
            elif ats == "lever":
                batch = scrape_lever(name, co["slug"])
            elif ats == "workday":
                batch = scrape_workday(name, co["tenant"], int(co["wd_num"]), co["site"])
            elif ats == "icims":
                batch = scrape_icims(name, co["subdomain"])
            elif ats == "kpmg":
                batch = scrape_kpmg(name, co.get("search_terms", []))
            elif ats == "deloitte":
                batch = scrape_deloitte(name, co.get("search_terms", []))
            else:
                log.warning(f"  Unknown ATS '{ats}' for {name} — skipping")
                continue

            all_jobs.extend(batch)
        except Exception as e:
            log.error(f"  Unhandled error for {name}: {e}")

        time.sleep(1)

    return all_jobs
