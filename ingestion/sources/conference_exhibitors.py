"""
ingestion/sources/conference_exhibitors.py

Scrapes career pages for AI conference exhibitors and filters to CX /
customer-facing roles only. Uses the Firecrawl SDK.

Supports multiple conferences — each has its own exhibitor list and
conference_source attribution. When the same job is found at multiple
conferences the dedup layer merges the attribution.

Requires FIRECRAWL_API_KEY in environment — silently skips if not set.
"""

from __future__ import annotations

import hashlib
import logging
import re
import time
from datetime import date
from urllib.parse import urljoin

import config
from ingestion.sources.normalize import detect_work_type

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Exhibitor lists
# ─────────────────────────────────────────────────────────────────────────────

DEEPLEARNING_AI_EXHIBITORS = [
    # AI Product Startups
    {"name": "LangChain",   "url": "https://www.langchain.com/careers"},
    {"name": "Cursor",      "url": "https://cursor.com/careers"},
    {"name": "Qdrant",      "url": "https://qdrant.tech/careers/"},
    {"name": "CrewAI",      "url": "https://www.crewai.com/careers"},
    {"name": "CopilotKit",  "url": "https://www.copilotkit.ai/careers"},
    {"name": "Apify",       "url": "https://apify.com/jobs"},
    {"name": "Vectara",     "url": "https://vectara.com/company/careers/"},
    {"name": "Chroma",      "url": "https://www.trychroma.com/careers"},
    {"name": "Reducto",     "url": "https://reducto.ai/careers"},
    {"name": "AI21 Labs",   "url": "https://www.ai21.com/careers/"},
    {"name": "Replit",      "url": "https://replit.com/careers"},
    {"name": "LandingAI",   "url": "https://landing.ai/careers"},
    {"name": "SambaNova",   "url": "https://sambanova.ai/careers"},
    {"name": "Tavily",      "url": "https://tavily.com/careers"},
    {"name": "Flower",      "url": "https://flower.ai/careers/"},
    {"name": "Prolific",    "url": "https://www.prolific.com/careers"},
    {"name": "CodeRabbit",  "url": "https://www.coderabbit.ai/careers"},
    {"name": "Actian",      "url": "https://www.actian.com/company/careers/"},
    {"name": "Giskard",     "url": "https://www.giskard.ai/careers"},
    {"name": "Unblocked",   "url": "https://getunblocked.com/careers"},
    # AI Infra / Data Platforms
    {"name": "Databricks",  "url": "https://www.databricks.com/company/careers/open-positions"},
    {"name": "Snowflake",   "url": "https://careers.snowflake.com/us/en"},
    {"name": "Datadog",     "url": "https://careers.datadoghq.com/"},
    {"name": "Elastic",     "url": "https://www.elastic.co/about/careers"},
    {"name": "Docker",      "url": "https://www.docker.com/careers/"},
    {"name": "Neo4j",       "url": "https://neo4j.com/careers/"},
    {"name": "Temporal",    "url": "https://temporal.io/careers"},
    {"name": "Box",         "url": "https://www.box.com/about-us/careers"},
    {"name": "Redis",       "url": "https://redis.io/careers/"},
    {"name": "JetBrains",   "url": "https://www.jetbrains.com/careers/jobs/"},
    {"name": "Sonar",       "url": "https://www.sonarsource.com/company/careers/"},
    {"name": "AMD",         "url": "https://careers.amd.com/careers-home/"},
    {"name": "Arm",         "url": "https://careers.arm.com/"},
]

HUMANX_EXHIBITORS = [
    # Title Sponsors / Top Tier
    {"name": "AWS",              "url": "https://www.amazon.jobs/en/teams/aws"},
    {"name": "DeepL",            "url": "https://www.deepl.com/en/careers"},
    {"name": "Google",           "url": "https://careers.google.com/"},
    {"name": "Lovable",          "url": "https://lovable.dev/careers"},
    {"name": "Nebius",           "url": "https://nebius.com/careers"},
    {"name": "NeuBird AI",       "url": "https://www.neubird.ai/careers"},
    {"name": "Okta",             "url": "https://www.okta.com/company/careers/"},
    {"name": "Oracle",           "url": "https://www.oracle.com/corporate/careers/"},
    {"name": "NVIDIA",           "url": "https://www.nvidia.com/en-us/about-nvidia/careers/"},
    {"name": "Vercel",           "url": "https://vercel.com/careers"},
    {"name": "WEKA",             "url": "https://www.weka.io/company/careers/"},
    # Platinum Sponsors
    {"name": "Baseten",          "url": "https://www.baseten.co/careers"},
    {"name": "Boomi",            "url": "https://boomi.com/company/careers/"},
    {"name": "Cedar",            "url": "https://www.cedar.com/careers"},
    {"name": "CoreWeave",        "url": "https://www.coreweave.com/careers"},
    {"name": "Cresta",           "url": "https://cresta.com/careers/"},
    {"name": "Fireworks AI",     "url": "https://fireworks.ai/careers"},
    {"name": "IBM",              "url": "https://www.ibm.com/employment/"},
    {"name": "IFS",              "url": "https://www.ifs.com/careers"},
    {"name": "Kong",             "url": "https://konghq.com/careers"},
    {"name": "Lambda",           "url": "https://lambda.ai/careers"},
    {"name": "Metronome",        "url": "https://metronome.com/careers"},
    {"name": "Micron",           "url": "https://www.micron.com/about/careers"},
    {"name": "Oxylabs",          "url": "https://oxylabs.io/careers"},
    {"name": "Sierra",           "url": "https://sierra.ai/careers"},
    {"name": "Snowflake",        "url": "https://careers.snowflake.com/us/en"},
    {"name": "Supermicro",       "url": "https://www.supermicro.com/en/about/careers"},
    {"name": "Together AI",      "url": "https://www.together.ai/careers"},
    {"name": "Twilio",           "url": "https://www.twilio.com/en-us/company/jobs"},
    {"name": "Vultr",            "url": "https://www.vultr.com/company/careers/"},
    # Sponsors
    {"name": "1Password",        "url": "https://1password.com/careers"},
    {"name": "Airbyte",          "url": "https://airbyte.com/careers"},
    {"name": "AMD",              "url": "https://careers.amd.com/careers-home/"},
    {"name": "Air",              "url": "https://air.inc/careers"},
    {"name": "CockroachDB",      "url": "https://www.cockroachlabs.com/careers/"},
    {"name": "Coder",            "url": "https://coder.com/careers"},
    {"name": "Creatio",          "url": "https://www.creatio.com/careers"},
    {"name": "CrewAI",           "url": "https://www.crewai.com/careers"},
    {"name": "Crusoe",           "url": "https://www.crusoe.ai/careers"},
    {"name": "Customer.io",      "url": "https://customer.io/careers"},
    {"name": "Databricks",       "url": "https://www.databricks.com/company/careers/open-positions"},
    {"name": "Dataiku",          "url": "https://www.dataiku.com/company/careers/"},
    {"name": "Deepgram",         "url": "https://deepgram.com/careers"},
    {"name": "Descope",          "url": "https://www.descope.com/careers"},
    {"name": "DigitalOcean",     "url": "https://www.digitalocean.com/careers"},
    {"name": "DX",               "url": "https://getdx.com/careers"},
    {"name": "E2B",              "url": "https://e2b.dev/careers"},
    {"name": "Elastic",          "url": "https://www.elastic.co/about/careers"},
    {"name": "ElevenLabs",       "url": "https://elevenlabs.io/careers"},
    {"name": "Encord",           "url": "https://encord.com/careers/"},
    {"name": "Exa",              "url": "https://exa.ai/careers"},
    {"name": "F5",               "url": "https://www.f5.com/company/careers"},
    {"name": "Factory",          "url": "https://www.factory.ai/careers"},
    {"name": "People.ai",        "url": "https://people.ai/careers/"},
    {"name": "Pigment",          "url": "https://www.pigment.com/careers"},
    {"name": "PlanetScale",      "url": "https://planetscale.com/careers"},
    {"name": "Prolific",         "url": "https://www.prolific.com/careers"},
    {"name": "Promptfoo",        "url": "https://www.promptfoo.dev/careers/"},
    {"name": "Red Hat",          "url": "https://www.redhat.com/en/jobs"},
    {"name": "Redis",            "url": "https://redis.io/careers/"},
    {"name": "Redpanda",         "url": "https://redpanda.com/careers"},
    {"name": "Refold AI",        "url": "https://refold.ai/careers"},
    {"name": "Regal",            "url": "https://www.regal.io/careers"},
    {"name": "Replicated",       "url": "https://www.replicated.com/careers/"},
    {"name": "Retool",           "url": "https://retool.com/careers"},
    {"name": "Rubrik",           "url": "https://www.rubrik.com/company/careers"},
    {"name": "Runpod",           "url": "https://www.runpod.io/careers"},
    {"name": "Salesforce",       "url": "https://www.salesforce.com/company/careers/"},
    {"name": "SambaNova",        "url": "https://sambanova.ai/careers"},
    {"name": "Samsara",          "url": "https://www.samsara.com/company/careers/"},
    {"name": "Scale AI",         "url": "https://scale.com/careers"},
    {"name": "Sentry",           "url": "https://sentry.io/careers/"},
    {"name": "Sigma",            "url": "https://www.sigmacomputing.com/company/careers"},
    {"name": "Softr",            "url": "https://www.softr.io/careers"},
    {"name": "SS&C Blue Prism",  "url": "https://www.blueprism.com/about/careers/"},
    {"name": "StackOne",         "url": "https://www.stackone.com/careers"},
    {"name": "Storylane",        "url": "https://www.storylane.io/careers"},
    {"name": "SurrealDB",        "url": "https://surrealdb.com/careers"},
    {"name": "Synthesia",        "url": "https://www.synthesia.io/careers"},
    {"name": "Sysdig",           "url": "https://sysdig.com/company/careers/"},
    {"name": "Tavus",            "url": "https://www.tavus.io/careers"},
    {"name": "TextQL",           "url": "https://textql.com/careers"},
    {"name": "TiDB",             "url": "https://www.pingcap.com/careers/"},
    {"name": "Tines",            "url": "https://www.tines.com/careers/"},
    {"name": "Tray.ai",          "url": "https://tray.ai/careers"},
    {"name": "Typeface",         "url": "https://www.typeface.ai/careers"},
    {"name": "Vanta",            "url": "https://www.vanta.com/company/careers"},
    {"name": "Vectara",          "url": "https://vectara.com/company/careers/"},
    {"name": "WorkOS",           "url": "https://workos.com/careers"},
    {"name": "You.com",          "url": "https://you.com/careers"},
    {"name": "Zendesk",          "url": "https://www.zendesk.com/jobs/"},
    {"name": "Zencoder",         "url": "https://zencoder.ai/careers"},
]

# ─────────────────────────────────────────────────────────────────────────────
# CX keyword filter — do NOT modify (shared across all conference sources)
# ─────────────────────────────────────────────────────────────────────────────

CX_KEYWORDS = [
    "customer success",
    "customer experience",
    "customer support",
    "customer service",
    "solutions engineer",
    "solutions architect",
    "solutions consultant",
    "sales engineer",
    "forward deployed",
    "forward-deployed",
    "implementation",
    "onboarding",
    "deployment specialist",
    "integration engineer",
    "professional services",
    "developer relations",
    "developer advocate",
    "technical account manager",
    "customer engineer",
    "applied engineer",
    "field engineer",
    "account manager",
    "account executive",
    "support engineer",
    "technical support",
]

_SKIP_PATTERNS = [
    "twitter", "linkedin", "github", "facebook", "youtube",
    "privacy", "terms", "cookie", "blog", "docs", "documentation",
    "pricing", "contact", "about us", "home", "menu",
]


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _is_cx(title: str) -> bool:
    t = title.lower()
    return any(kw in t for kw in CX_KEYWORDS)


def _fingerprint(company: str, title: str, url: str) -> str:
    key = f"{company.lower().strip()}|{title.lower().strip()}|{url.strip()}"
    return hashlib.md5(key.encode()).hexdigest()


def _parse_title_and_location(raw: str) -> tuple[str, str]:
    """
    Firecrawl career pages return lines like:
      **Title** \\ \\ Category•Full-time•City1; City2
    Split at the first backslash: left = title, right = metadata with location.
    """
    t = raw
    # Full markdown link [text](url) → text
    t = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", t)
    t = re.sub(r"\]\([^)]*\)", "", t)
    t = re.sub(r"^\[", "", t)
    t = re.sub(r"\*{1,3}", "", t)

    location = ""
    if "\\" in t:
        parts = t.split("\\", 1)
        title_part = parts[0]
        meta = parts[1].lstrip("\\ ").strip() if len(parts) > 1 else ""
        # meta looks like: "Category•Full-time•San Francisco; New York"
        # or "Solutions•Full-time•Remote" — location is the last •-segment(s)
        segments = [s.strip() for s in meta.split("•") if s.strip()]
        # Skip work-type-looking segments; take the rest as location
        _WORK_TYPE_WORDS = {"full-time", "part-time", "contract", "full time", "part time", "remote", "hybrid"}
        loc_parts = [s for s in segments[1:] if s.lower() not in _WORK_TYPE_WORDS and len(s) > 2]
        location = "; ".join(loc_parts)[:120]
        t = title_part

    t = re.sub(r"<[^>]+>", "", t)
    t = re.sub(r"\s+", " ", t).strip(" -|:\t\r\n")
    t = re.sub(r"^(apply now|view job|job details|learn more)\s*[:|-]?\s*", "", t, flags=re.I)
    return t[:160].strip(), location


def _clean_title(raw: str) -> str:
    title, _ = _parse_title_and_location(raw)
    return title


def _make_job(company: dict, title: str, url: str, page_url: str, conference_name: str, location: str = "") -> dict:
    return {
        "company_name":       company["name"],
        "role_title":         title,
        "status":             "Researching",
        "date_added":         date.today(),
        "date_applied":       None,
        "salary_min":         None,
        "salary_max":         None,
        "location":           location,
        "work_type":          detect_work_type(f"{title} {location}"),
        "source":             "Conference Exhibitor",
        "conference_source":  conference_name,
        "job_url":            url,
        "notes":              f"Imported via Firecrawl from {page_url}",
        "priority":           "Medium",
        "external_job_id":    None,
        "description_raw":    "",
        "dedupe_fingerprint": _fingerprint(company["name"], title, url),
    }


def _extract_jobs(markdown: str, company: dict, conference_name: str, max_jobs: int = 25) -> list[dict]:
    page_url = company["url"]
    seen: set[str] = set()
    jobs: list[dict] = []

    for label, href in re.findall(r"\[([^\]]{3,180})\]\(([^)\s]+)\)", markdown):
        title, location = _parse_title_and_location(label)
        if not title or len(title) < 8:
            continue
        if any(p in title.lower() or p in href.lower() for p in _SKIP_PATTERNS):
            continue
        if not _is_cx(title):
            continue
        job_url = urljoin(page_url, href)
        if job_url in seen:
            continue
        seen.add(job_url)
        jobs.append(_make_job(company, title, job_url, page_url, conference_name, location))
        if len(jobs) >= max_jobs:
            return jobs

    for line in markdown.splitlines():
        title, location = _parse_title_and_location(line)
        if len(title) < 8 or len(title) > 120:
            continue
        if not _is_cx(title):
            continue
        key = f"{company['name']}|{title}"
        if key in seen:
            continue
        seen.add(key)
        jobs.append(_make_job(company, title, page_url, page_url, conference_name, location))
        if len(jobs) >= max_jobs:
            break

    return jobs


# ─────────────────────────────────────────────────────────────────────────────
# Generic scraper
# ─────────────────────────────────────────────────────────────────────────────

def _scrape_exhibitor_list(
    exhibitors: list[dict],
    conference_name: str,
    app,
    max_jobs_per_page: int = 25,
) -> list[dict]:
    """Scrape one conference's exhibitor list. Reuses an already-initialised FirecrawlApp."""
    all_jobs: list[dict] = []
    for company in exhibitors:
        try:
            log.info(f"[{conference_name}] Scraping {company['name']}...")
            result   = app.scrape(company["url"], formats=["markdown"])
            markdown = result.markdown or "" if hasattr(result, "markdown") else (result or {}).get("markdown", "")
            jobs     = _extract_jobs(markdown, company, conference_name, max_jobs_per_page)
            log.info(f"  {company['name']}: {len(jobs)} CX jobs")
            all_jobs.extend(jobs)
        except Exception as exc:
            log.warning(f"  [{conference_name}] {company['name']} failed: {exc}")
        time.sleep(0.5)
    log.info(f"[{conference_name}] Total: {len(all_jobs)} CX jobs across {len(exhibitors)} companies")
    return all_jobs


# ─────────────────────────────────────────────────────────────────────────────
# Public entry points
# ─────────────────────────────────────────────────────────────────────────────

def scrape_deeplearning_ai(max_jobs_per_page: int = 25) -> list[dict]:
    """Scrape DeepLearning.AI exhibitor career pages for CX roles."""
    if not config.FIRECRAWL_API_KEY:
        log.warning("FIRECRAWL_API_KEY not set — skipping DeepLearning.AI scrape.")
        return []
    try:
        from firecrawl import FirecrawlApp
    except ImportError:
        log.warning("firecrawl package not installed — skipping.")
        return []
    app = FirecrawlApp(api_key=config.FIRECRAWL_API_KEY)
    return _scrape_exhibitor_list(DEEPLEARNING_AI_EXHIBITORS, "DeepLearning.AI", app, max_jobs_per_page)


def scrape_humanx(max_jobs_per_page: int = 25) -> list[dict]:
    """Scrape HumanX 2026 exhibitor career pages for CX roles."""
    if not config.FIRECRAWL_API_KEY:
        log.warning("FIRECRAWL_API_KEY not set — skipping HumanX scrape.")
        return []
    try:
        from firecrawl import FirecrawlApp
    except ImportError:
        log.warning("firecrawl package not installed — skipping.")
        return []
    app = FirecrawlApp(api_key=config.FIRECRAWL_API_KEY)
    return _scrape_exhibitor_list(HUMANX_EXHIBITORS, "HumanX 2026", app, max_jobs_per_page)


def scrape_conference_exhibitors(max_jobs_per_page: int = 25) -> list[dict]:
    """Scrape all conference exhibitor sources. Returns combined normalized job dicts."""
    if not config.FIRECRAWL_API_KEY:
        log.warning("FIRECRAWL_API_KEY not set — skipping all conference exhibitor scrapes.")
        return []
    try:
        from firecrawl import FirecrawlApp
    except ImportError:
        log.warning("firecrawl package not installed — skipping conference exhibitors.")
        return []

    app  = FirecrawlApp(api_key=config.FIRECRAWL_API_KEY)
    jobs = []
    jobs.extend(_scrape_exhibitor_list(DEEPLEARNING_AI_EXHIBITORS, "DeepLearning.AI", app, max_jobs_per_page))
    jobs.extend(_scrape_exhibitor_list(HUMANX_EXHIBITORS,          "HumanX 2026",     app, max_jobs_per_page))
    return jobs
