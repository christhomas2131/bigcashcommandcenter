"""
app.py — BIG CASH COMMAND CENTER | Public Job Board
"""

import base64 as _b64
import html as _html
import os
import re
from collections import Counter
from datetime import date, datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

import config
from db import repository as db
from db.connection import cursor as db_cursor
from db.schema import migrate

# ─────────────────────────────────────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Big Cash Command Center",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────────────────────────────────────
# DB init
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource
def init_db():
    migrate()
    with db_cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS email_subscribers (
                id            SERIAL PRIMARY KEY,
                email         TEXT NOT NULL UNIQUE,
                subscribed_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)

init_db()

# ─────────────────────────────────────────────────────────────────────────────
# Session state defaults
# ─────────────────────────────────────────────────────────────────────────────

_defaults = {
    "page":         "Analytics",
    "saved_jobs":   set(),
    "applied_jobs": set(),
    "role_filter":  "All",
    "sort_filter":  "Newest",
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Restore state from URL ────────────────────────────────────────────────────
# saved/applied: restore once per session only (protects in-session changes)
# page/role/q/company: apply every render so navigation links always work
_p = st.query_params
try:
    if not st.session_state.get("_initialized"):
        st.session_state._initialized = True
        if "saved" in _p:
            st.session_state.saved_jobs = {
                int(x) for x in _p["saved"].split(",") if x.strip()
            }
        if "applied" in _p:
            st.session_state.applied_jobs = {
                int(x) for x in _p["applied"].split(",") if x.strip()
            }

    # Navigation params — page is always applied (enables company links, shareable URLs)
    # role/q/days are only applied once on first load to avoid overriding in-session widget state
    _VALID_PAGES = ("Analytics", "All Jobs", "New This Week", "Saved", "Fetch New Jobs")
    if "page" in _p and _p["page"] in _VALID_PAGES:
        st.session_state.page = _p["page"]
    if not st.session_state.get("_url_params_loaded"):
        st.session_state["_url_params_loaded"] = True
        if "role" in _p:
            st.session_state["role_90"] = _p["role"]
            st.session_state["role_7"]  = _p["role"]
        if "q" in _p:
            st.session_state["search_90"] = _p["q"]
            st.session_state["search_7"]  = _p["q"]
        if "days" in _p:
            try:
                st.session_state["lb_90"] = int(_p["days"])
            except Exception:
                pass
except Exception:
    pass

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

PRIORITY_COLORS = {"High": "#EF4444", "Medium": "#F59E0B", "Low": "#6B7280"}

ROLE_CATEGORIES = {
    "DR / EM": [
        "disaster", "emergency", "fema", "hazard", "mitigation",
        "resilience", "public assistance", "cdbg", "recovery", "homeland",
        "grants management",
    ],
    "CFM": [
        "floodplain", "flood plain", "nfip", "cfm", "certified floodplain",
        "floodplain manager", "floodplain administrator", "flood insurance",
        "stormwater", "watershed", "flood mitigation",
    ],
    "GovTech": [
        "govtech", "government", "public sector", "federal", "civic",
        "municipal", "state and local",
    ],
    "Tech / Sales": [
        "solutions engineer", "sales engineer", "pre-sales", "account executive",
        "account manager", "customer success", "technical account",
        "partner solutions", "revenue enablement",
    ],
}

GENERIC_LOCS = {"us", "usa", "united states", "unknown", "n/a", "remote", ""}

STOP_WORDS = {
    "manager", "senior", "associate", "and", "of", "the", "for", "in", "a",
    "an", "to", "with", "at", "specialist", "coordinator", "lead", "director",
    "officer", "ii", "iii", "iv", "i", "jr", "sr",
}

_CHART = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#9CA3AF", family="Inter, system-ui, sans-serif", size=12),
    margin=dict(l=20, r=20, t=44, b=20),
)

# Cohesive 6-color palette — applied consistently across all charts
_PAL = ["#2DD4BF", "#A78BFA", "#FBBF24", "#FB7185", "#22D3EE", "#94A3B8"]

# Category → palette color (consistent mapping)
_CAT_COLOR = {
    "DR / EM":      _PAL[0],   # teal
    "CFM":          _PAL[3],   # rose
    "GovTech":      _PAL[1],   # purple
    "Tech / Sales": _PAL[2],   # amber
    "Other":        _PAL[5],   # slate
}

# Work type → palette color
_WT_COLOR = {
    "Remote":  _PAL[4],  # cyan
    "Hybrid":  _PAL[1],  # purple
    "On-Site": _PAL[2],  # amber
    "On-site": _PAL[2],
    "Other":   _PAL[5],
}

# Category → CSS class for card left border
_CAT_BORDER_CLASS = {
    "DR / EM":      "cat-dr",
    "CFM":          "cat-cfm",
    "GovTech":      "cat-gov",
    "Tech / Sales": "cat-tech",
}

# Priority → dot color
_PRI_DOT_COLOR = {"High": "#FB923C", "Medium": "#475569"}

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def relative_time(d) -> str:
    """Convert a date/string to a human-friendly relative string."""
    if d is None:
        return ""
    today = date.today()
    if isinstance(d, str):
        try:
            d = date.fromisoformat(d[:10])
        except Exception:
            return str(d)
    elif isinstance(d, datetime):
        d = d.date()
    delta = (today - d).days
    if delta < 0:  return "just now"
    if delta == 0: return "today"
    if delta == 1: return "yesterday"
    if delta < 7:  return f"{delta} days ago"
    if delta < 14: return "1 week ago"
    if delta < 30: return f"{delta // 7} weeks ago"
    if delta < 60: return "last month"
    return f"{delta // 30} months ago"


def classify(job: dict) -> str:
    text = f"{job.get('role_title','').lower()} {job.get('company_name','').lower()}"
    for cat, kws in ROLE_CATEGORIES.items():
        if any(k in text for k in kws):
            return cat
    return "Other"


def company_domain(name: str) -> str:
    clean = re.sub(
        r"\b(inc|llc|corp|ltd|co|group|consulting|technologies|technology|"
        r"solutions|services|systems|global|international|associates|partners|the)\b",
        "", name.lower(),
    )
    clean = re.sub(r"[^a-z0-9]", "", clean)
    return f"{clean}.com" if clean else ""


_INITIAL_COLORS = ["#7C3AED", "#3B82F6", "#10B981", "#F59E0B", "#EF4444", "#EC4899"]

def company_logo_html(name: str, domain: str) -> str:
    """Return an <img> with a colored-initial SVG fallback when Clearbit fails."""
    if not name:
        return ""
    initial = name[0].upper()
    color   = _INITIAL_COLORS[ord(initial) % len(_INITIAL_COLORS)]
    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="22" height="22">'
        f'<rect width="22" height="22" rx="5" fill="{color}"/>'
        f'<text x="11" y="16" text-anchor="middle" font-family="Inter,sans-serif" '
        f'font-size="12" font-weight="700" fill="white">{initial}</text>'
        f'</svg>'
    )
    import base64 as _b64i
    fallback = "data:image/svg+xml;base64," + _b64i.b64encode(svg.encode()).decode()
    if domain:
        return (
            f'<img src="https://logo.clearbit.com/{domain}" class="co-logo" '
            f'onerror="this.onerror=null;this.src=\'{fallback}\'" alt="">'
        )
    return f'<img src="{fallback}" class="co-logo" alt="">'


def week_counts(jobs: list) -> tuple:
    today = date.today()
    this  = sum(1 for j in jobs if j.get("date_added") and (today - j["date_added"]).days <= 7)
    last  = sum(1 for j in jobs if j.get("date_added") and 7 < (today - j["date_added"]).days <= 14)
    return this, last


def delta_html(this: int, last: int) -> str:
    diff = this - last
    if diff == 0 or last == 0:
        return ""
    arrow = "↑" if diff > 0 else "↓"
    color = "#10B981" if diff > 0 else "#EF4444"
    return (
        f'<div style="font-size:0.72rem;font-weight:700;color:{color};margin-top:3px;">'
        f'{arrow} {abs(diff)} vs last week</div>'
    )


@st.cache_data(ttl=300)
def load_leads(days: int = 90):
    return db.get_new_leads(days=days, sort_by="imported_desc")


def _persist():
    """Write saved/applied IDs into query params so state survives page refresh."""
    if st.session_state.saved_jobs:
        st.query_params["saved"] = ",".join(
            str(x) for x in sorted(st.session_state.saved_jobs)
        )
    elif "saved" in st.query_params:
        del st.query_params["saved"]
    if st.session_state.applied_jobs:
        st.query_params["applied"] = ",".join(
            str(x) for x in sorted(st.session_state.applied_jobs)
        )
    elif "applied" in st.query_params:
        del st.query_params["applied"]


@st.dialog("Job Details", width="large")
def show_job_modal(job: dict):
    """Full-detail modal for a single job listing."""
    jid       = job["id"]
    is_saved  = jid in st.session_state.saved_jobs
    is_applied= jid in st.session_state.applied_jobs
    job_url   = job.get("job_url") or ""
    domain    = company_domain(job.get("company_name", ""))
    logo      = company_logo_html(job.get("company_name", ""), domain)
    sal_lo    = job.get("salary_min")
    sal_hi    = job.get("salary_max")
    category  = classify(job)
    cat_color = _CAT_COLOR.get(category, _PAL[5])

    # Header row
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;">'
        f'  {logo}'
        f'  <div>'
        f'    <div style="font-size:1.2rem;font-weight:800;color:#F1F5F9;line-height:1.3;">'
        f'      {_html.escape(job.get("role_title",""))}'
        f'    </div>'
        f'    <div style="font-size:0.9rem;font-weight:600;color:#818CF8;margin-top:2px;">'
        f'      {_html.escape(job.get("company_name",""))}'
        f'    </div>'
        f'  </div>'
        f'  <span style="margin-left:auto;font-size:0.72rem;font-weight:700;color:{cat_color};'
        f'  background:rgba(0,0,0,0.3);border:1px solid {cat_color}44;padding:3px 10px;'
        f'  border-radius:20px;">{category}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # Meta grid
    m1, m2, m3 = st.columns(3)
    with m1:
        st.markdown(f'<div class="detail-label">Location</div>'
                    f'<div class="detail-value">{"📍 " + _html.escape(job.get("location","—"))}</div>',
                    unsafe_allow_html=True)
    with m2:
        wt = job.get("work_type") or "—"
        wt_color = "#67E8F9" if "remote" in wt.lower() else "#93C5FD"
        st.markdown(f'<div class="detail-label">Work Type</div>'
                    f'<div class="detail-value" style="color:{wt_color};">{_html.escape(wt)}</div>',
                    unsafe_allow_html=True)
    with m3:
        posted = relative_time(job.get("date_added"))
        st.markdown(f'<div class="detail-label">Posted</div>'
                    f'<div class="detail-value">{posted}</div>',
                    unsafe_allow_html=True)

    if sal_lo or sal_hi:
        lo = f"${sal_lo:,}" if sal_lo else "—"
        hi = f"${sal_hi:,}" if sal_hi else "—"
        st.markdown(f'<div style="margin-top:10px;" class="detail-label">Salary Range</div>'
                    f'<div class="detail-value" style="color:#FBBF24;">💰 {lo} – {hi}</div>',
                    unsafe_allow_html=True)

    # Description
    desc = job.get("description_raw") or job.get("notes") or ""
    if desc:
        st.markdown('<hr style="border:none;border-top:1px solid rgba(255,255,255,0.06);margin:14px 0;">', unsafe_allow_html=True)
        st.markdown('<div class="detail-label">Description</div>', unsafe_allow_html=True)
        st.markdown(
            f'<div style="font-size:0.82rem;color:#9CA3AF;line-height:1.6;max-height:220px;'
            f'overflow-y:auto;padding-right:4px;">{_html.escape(desc[:1200])}{"…" if len(desc)>1200 else ""}</div>',
            unsafe_allow_html=True,
        )

    st.markdown('<hr style="border:none;border-top:1px solid rgba(255,255,255,0.06);margin:14px 0;">', unsafe_allow_html=True)

    # Action row
    ac1, ac2, ac3 = st.columns(3)
    with ac1:
        save_lbl = "🔖 Saved" if is_saved else "🔖 Save"
        if st.button(save_lbl, key=f"modal_sv_{jid}", use_container_width=True):
            if is_saved:
                st.session_state.saved_jobs.discard(jid)
                st.session_state._toast = "Removed from saved"
            else:
                st.session_state.saved_jobs.add(jid)
                st.session_state._toast = "Job saved to your list"
            _persist()
            st.rerun()
    with ac2:
        app_lbl = "✓ Applied" if is_applied else "Mark Applied"
        if st.button(app_lbl, key=f"modal_ap_{jid}", use_container_width=True,
                     type="primary" if is_applied else "secondary"):
            if is_applied:
                st.session_state.applied_jobs.discard(jid)
                st.session_state._toast = "Application status removed"
            else:
                st.session_state.applied_jobs.add(jid)
                st.session_state._toast = "Marked as applied ✓"
            _persist()
            st.rerun()
    with ac3:
        if job_url:
            st.link_button("Apply →", job_url, use_container_width=True, type="primary")


def _sync_filters(role: str, q: str, days: int, page_key: str = "All Jobs"):
    """Keep filter state in the URL so views are shareable."""
    st.query_params["page"] = page_key
    if role != "All":
        st.query_params["role"] = role
    elif "role" in st.query_params:
        del st.query_params["role"]
    if q:
        st.query_params["q"] = q
    elif "q" in st.query_params:
        del st.query_params["q"]
    if days != 30:
        st.query_params["days"] = str(days)
    elif "days" in st.query_params:
        del st.query_params["days"]


# ─────────────────────────────────────────────────────────────────────────────
# CSS — Luxury Edition
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

/* ── Reset ───────────────────────────────────────────────────────────── */
*, *::before, *::after { box-sizing: border-box; }
html, body, [data-testid="stAppViewContainer"] { font-family: 'Inter', system-ui, sans-serif; }
[data-testid="collapsedControl"] { display: none !important; }
header[data-testid="stHeader"]   { display: none !important; }
[data-testid="stAppViewContainer"] > section > div:first-child { padding-top: 0 !important; }
.block-container {
    padding-top: 0 !important;
    max-width: 100% !important;
    padding-left: 1rem !important;
    padding-right: 1rem !important;
}

/* ── Scrollbar ───────────────────────────────────────────────────────── */
::-webkit-scrollbar              { width: 5px; height: 5px; }
::-webkit-scrollbar-track        { background: #080c16; }
::-webkit-scrollbar-thumb        { background: #7C3AED; border-radius: 4px; }
::-webkit-scrollbar-thumb:hover  { background: #9F67FF; }

/* ── Animations ──────────────────────────────────────────────────────── */
@keyframes gradientShift {
    0%   { background-position: 0% 50%; }
    50%  { background-position: 100% 50%; }
    100% { background-position: 0% 50%; }
}
@keyframes shimmer {
    0%   { background-position: -200% center; }
    100% { background-position:  200% center; }
}
@keyframes fadeIn {
    from { opacity: 0; transform: translateY(6px); }
    to   { opacity: 1; transform: translateY(0); }
}

/* ── Job cards ───────────────────────────────────────────────────────── */
.job-card {
    background: rgba(14, 17, 32, 0.85);
    backdrop-filter: blur(18px);
    -webkit-backdrop-filter: blur(18px);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 12px;
    padding: 18px 20px 14px;
    margin-bottom: 12px;
    transition: transform 0.15s ease, box-shadow 0.15s ease, border-color 0.15s ease;
    animation: fadeIn 0.3s ease;
    position: relative;
    overflow: hidden;
}
.job-card::before {
    content: "";
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, transparent, rgba(124,58,237,0.6), transparent);
    opacity: 0;
    transition: opacity 0.15s ease;
}
.job-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 10px 36px rgba(124,58,237,0.22), 0 2px 8px rgba(0,0,0,0.4);
    border-color: rgba(124,58,237,0.4);
}
.job-card:hover::before { opacity: 1; }

.card-title {
    font-size: 1rem;
    font-weight: 700;
    color: #F1F5F9;
    line-height: 1.35;
    margin-bottom: 3px;
    letter-spacing: -0.01em;
}
.card-company {
    font-size: 0.82rem;
    font-weight: 600;
    color: #818CF8;
    margin-bottom: 3px;
}
.card-location {
    font-size: 0.78rem;
    color: #4B5563;
    margin-bottom: 8px;
}
.card-tags { margin: 8px 0; }
.tag {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 20px;
    font-size: 0.68rem;
    font-weight: 700;
    margin-right: 4px;
    margin-bottom: 3px;
    letter-spacing: 0.02em;
}
.card-footer {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 10px;
    padding-top: 10px;
    border-top: 1px solid rgba(255,255,255,0.05);
}
.card-date {
    font-size: 0.7rem;
    color: #374151;
    font-weight: 500;
}
.apply-link {
    font-size: 0.78rem;
    font-weight: 700;
    color: #A78BFA;
    text-decoration: none;
    letter-spacing: 0.02em;
    transition: color 0.15s ease;
}
.apply-link:hover { color: #C4B5FD; }

/* ── Badge ───────────────────────────────────────────────────────────── */
.badge-new {
    display: inline-block;
    padding: 1px 7px;
    border-radius: 20px;
    font-size: 0.62rem;
    font-weight: 800;
    letter-spacing: 0.06em;
    background: linear-gradient(90deg, #F59E0B, #EF4444, #F59E0B);
    background-size: 200% auto;
    color: #fff;
    animation: shimmer 2.5s linear infinite;
    margin-left: 7px;
    vertical-align: middle;
    position: relative;
    top: -1px;
}

/* ── Company logo ────────────────────────────────────────────────────── */
.co-logo {
    width: 22px; height: 22px;
    border-radius: 5px;
    object-fit: contain;
    background: #fff;
    margin-right: 8px;
    flex-shrink: 0;
    vertical-align: middle;
}

/* ── Stat cards ──────────────────────────────────────────────────────── */
.stat-card {
    background: rgba(14,17,32,0.85);
    backdrop-filter: blur(14px);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 12px;
    padding: 20px 24px;
    margin-bottom: 12px;
    transition: border-color 0.15s ease;
}
.stat-card:hover { border-color: rgba(124,58,237,0.45); }
.stat-num   { font-size: 2.6rem; font-weight: 900; color: #F9FAFB; line-height: 1; letter-spacing: -0.03em; }
.stat-label { font-size: 0.72rem; font-weight: 500; color: #6B7280; margin-top: 5px; letter-spacing: 0.04em; text-transform: uppercase; }

/* ── Quick stats banner ──────────────────────────────────────────────── */
.stats-banner {
    padding: 8px 16px;
    border-radius: 8px;
    background: rgba(14,17,32,0.5);
    border: 1px solid rgba(255,255,255,0.05);
    margin-bottom: 14px;
    font-size: 0.78rem;
    color: #4B5563;
    font-weight: 500;
    letter-spacing: 0.01em;
}
.stats-banner span { color: #9CA3AF; font-weight: 600; }

/* ── Filter bar ──────────────────────────────────────────────────────── */
.filter-sep {
    border: none;
    border-bottom: 1px solid rgba(124,58,237,0.15);
    margin: 10px 0 16px;
}

/* ── Radio → chip style ──────────────────────────────────────────────── */
div[data-testid="stRadio"] > div:last-child {
    display: flex !important;
    flex-wrap: wrap !important;
    gap: 6px !important;
    align-items: center !important;
}
div[data-testid="stRadio"] > div:last-child > label {
    background: rgba(14,18,32,0.9) !important;
    border: 1px solid rgba(124,58,237,0.2) !important;
    border-radius: 20px !important;
    padding: 3px 13px !important;
    cursor: pointer !important;
    transition: all 0.15s ease !important;
    min-width: 0 !important;
}
div[data-testid="stRadio"] > div:last-child > label:hover {
    border-color: rgba(124,58,237,0.55) !important;
    background: rgba(124,58,237,0.08) !important;
}
/* Hide radio dot */
div[data-testid="stRadio"] > div:last-child > label > div:first-child {
    display: none !important;
}
/* Label text */
div[data-testid="stRadio"] > div:last-child > label > div:last-child p {
    color: #6B7280 !important;
    font-size: 0.76rem !important;
    font-weight: 600 !important;
    margin: 0 !important;
    padding: 0 !important;
    font-family: 'Inter', system-ui, sans-serif !important;
}
/* Active chip */
div[data-testid="stRadio"] > div:last-child > label:has(input:checked) {
    background: rgba(124,58,237,0.22) !important;
    border-color: #7C3AED !important;
}
div[data-testid="stRadio"] > div:last-child > label:has(input:checked) > div:last-child p {
    color: #E0D9FF !important;
}
/* Hide radio field label when we don't want it */
.hide-label > div[data-testid="stWidgetLabel"] { display: none; }

/* ── Nav buttons ─────────────────────────────────────────────────────── */
section[data-testid="column"]:first-child button {
    text-align: left !important;
    justify-content: flex-start !important;
    font-size: 0.82rem !important;
    font-weight: 600 !important;
    border-radius: 8px !important;
    padding: 6px 10px !important;
    margin-bottom: 2px !important;
    transition: all 0.15s ease !important;
}

/* ── Email section ───────────────────────────────────────────────────── */
.email-section {
    background: linear-gradient(135deg, rgba(124,58,237,0.12), rgba(59,130,246,0.08));
    border: 1px solid rgba(124,58,237,0.28);
    border-radius: 14px;
    padding: 22px 26px;
    margin-top: 20px;
}

/* ── Card action buttons — compact ───────────────────────────────────── */
.card-actions button {
    padding: 4px 8px !important;
    font-size: 0.72rem !important;
    font-weight: 600 !important;
    border-radius: 6px !important;
    min-height: 0 !important;
    height: 28px !important;
    line-height: 1 !important;
}

/* ── Priority left-border accents on cards ───────────────────────────── */
.job-card.pri-high   { border-left: 3px solid #FB923C; }
.job-card.pri-medium { border-left: 3px solid #475569; }
.job-card.cat-dr     { border-left: 3px solid #2DD4BF; }
.job-card.cat-cfm    { border-left: 3px solid #FB7185; }
.job-card.cat-gov    { border-left: 3px solid #A78BFA; }
.job-card.cat-tech   { border-left: 3px solid #FBBF24; }

/* Priority dot (top-right of card when category also present) */
.pri-dot {
    position: absolute;
    top: 10px;
    right: 12px;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    flex-shrink: 0;
}

/* ── Details expander discoverability ────────────────────────────────── */
details summary {
    color: #6B7280 !important;
    font-size: 0.75rem !important;
    font-weight: 600 !important;
    cursor: pointer !important;
    transition: color 0.15s ease !important;
    user-select: none;
}
details summary:hover { color: #A78BFA !important; }
details[open] summary { color: #A78BFA !important; }
/* Streamlit expander wrapper */
div[data-testid="stExpander"] > details > summary > span {
    font-weight: 700 !important;
}
div[data-testid="stExpander"] > details > summary:hover > span {
    color: #A78BFA !important;
    text-decoration: underline !important;
    text-underline-offset: 3px !important;
}

/* ── Sub-view tabs (Saved / Applied) ─────────────────────────────────── */
.sv-tab {
    display: inline-block;
    padding: 4px 14px;
    border-radius: 20px;
    font-size: 0.75rem;
    font-weight: 700;
    cursor: pointer;
    border: 1px solid rgba(124,58,237,0.2);
    color: #6B7280;
    margin-right: 6px;
    transition: all 0.15s ease;
}
.sv-tab.active {
    background: rgba(124,58,237,0.22);
    border-color: #7C3AED;
    color: #E0D9FF;
}

/* ── Toast notifications ─────────────────────────────────────────────── */
@keyframes toastIn  { from { opacity:0; transform:translateY(12px); } to { opacity:1; transform:translateY(0); } }
@keyframes toastOut { from { opacity:1; } to { opacity:0; } }
.toast-container {
    position: fixed;
    bottom: 24px;
    right: 24px;
    z-index: 9999;
    display: flex;
    flex-direction: column;
    gap: 8px;
    pointer-events: none;
}
.toast {
    background: rgba(18,22,40,0.96);
    backdrop-filter: blur(12px);
    border: 1px solid rgba(124,58,237,0.35);
    border-radius: 10px;
    padding: 10px 16px;
    color: #E2E8F0;
    font-size: 0.78rem;
    font-weight: 600;
    font-family: 'Inter', system-ui, sans-serif;
    box-shadow: 0 8px 24px rgba(0,0,0,0.4);
    animation: toastIn 0.2s ease forwards;
    pointer-events: auto;
    cursor: pointer;
    max-width: 280px;
    white-space: nowrap;
}

/* ── Job detail dialog ───────────────────────────────────────────────── */
.detail-section { margin-bottom: 14px; }
.detail-label {
    font-size: 0.65rem;
    font-weight: 700;
    color: #4B5563;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    margin-bottom: 3px;
}
.detail-value {
    font-size: 0.9rem;
    font-weight: 600;
    color: #E2E8F0;
}

/* ── Company link in card ────────────────────────────────────────────── */
.co-link {
    cursor: pointer;
    transition: color 0.12s ease;
}
.co-link:hover { color: #A78BFA !important; text-decoration: underline dotted; }

/* ── Mobile responsive ───────────────────────────────────────────────── */
@media (max-width: 768px) {
    /* Container — prevent horizontal scroll, comfortable padding */
    .main .block-container, .block-container {
        padding-left: 0.75rem !important;
        padding-right: 0.75rem !important;
        max-width: 100% !important;
    }

    /* Stack all horizontal blocks vertically */
    [data-testid="stHorizontalBlock"] {
        flex-wrap: wrap !important;
    }

    /* All columns go full-width */
    [data-testid="stHorizontalBlock"] > [data-testid="stVerticalBlock"],
    [data-testid="stHorizontalBlock"] > div[data-testid="column"],
    [data-testid="column"],
    section[data-testid="column"] {
        flex: 0 0 100% !important;
        min-width: 100% !important;
        max-width: 100% !important;
        width: 100% !important;
    }

    /* Header — collapse to single centered column, fix negative margins, hide face images */
    #bccc-header, .bccc-header {
        margin-left: 0 !important;
        margin-right: 0 !important;
        grid-template-columns: 1fr !important;
        justify-items: center;
        gap: 4px;
        padding: 6px;
    }
    .bccc-face-left,
    .bccc-face-right,
    #bccc-header img { display: none !important; }
    .bccc-title {
        font-size: 1rem !important;
        letter-spacing: 0.1em !important;
        white-space: normal !important;
        overflow: visible !important;
        text-overflow: unset !important;
    }
    .bccc-subtitle { font-size: 0.75rem !important; }

    /* Typography scale */
    h1 { font-size: 1.5rem !important; }
    h2 { font-size: 1.2rem !important; }
    h3 { font-size: 1.1rem !important; }

    /* Tighten element spacing */
    .element-container { margin-bottom: 0.5rem !important; }

    /* Buttons — minimum 44px tap target */
    .stButton > button {
        min-height: 44px !important;
        font-size: 0.85rem !important;
    }

    /* Apply link — minimum 44px tap target */
    .apply-link {
        display: inline-flex !important;
        align-items: center !important;
        min-height: 44px !important;
        padding: 0 !important;
    }

    /* Stat cards — tighter padding, smaller numbers */
    .stat-card   { padding: 14px 16px !important; }
    .stat-num    { font-size: 1.8rem !important; }
    .stat-label  { font-size: 0.65rem !important; }

    /* Activity cards */
    .activity-card { padding: 10px 14px !important; }
    .activity-num  { font-size: 1.5rem !important; }

    /* Job cards — tighter internal padding */
    .job-card { padding: 12px 12px 10px !important; }

    /* Filter chips — tighter but still tappable */
    div[data-testid="stRadio"] > div:last-child {
        flex-wrap: wrap !important;
        gap: 4px !important;
    }
    div[data-testid="stRadio"] > div:last-child > label {
        padding: 2px 10px !important;
        min-height: 36px !important;
        display: inline-flex !important;
        align-items: center !important;
    }
    div[data-testid="stRadio"] > div:last-child > label > div:last-child p {
        font-size: 0.7rem !important;
    }

    /* Stats banner — smaller text, allow wrap */
    .stats-banner {
        font-size: 0.7rem !important;
        white-space: normal !important;
        line-height: 1.6 !important;
    }

    /* Email section — tighter padding */
    .email-section { padding: 16px !important; }
}

/* ── Header — class-based layout ────────────────────────────────────── */
.bccc-header {
    display: grid;
    grid-template-columns: auto minmax(0, 1fr) auto;
    align-items: center;
    text-align: center;
    border-top: 2px solid #7C3AED;
    border-bottom: 2px solid #7C3AED;
    padding: 0;
    margin: 0 -1rem 14px -1rem;
    gap: 5px;
}
.bccc-center {
    min-width: 0;
    width: 100%;
    text-align: center;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 5px;
    padding: 8px 0;
    line-height: normal;
}
.bccc-title {
    letter-spacing: 0.22em;
    font-size: 1.5rem;
    font-weight: 900;
    color: #F1F5F9;
    font-family: 'Inter', system-ui, sans-serif;
    text-shadow: 0 0 24px rgba(124,58,237,0.8), 0 0 60px rgba(59,130,246,0.35);
    white-space: nowrap;
    width: 100%;
}
.bccc-subtitle {
    font-size: 1.03rem;
    font-style: italic;
    font-weight: 400;
    color: #6B7280;
    letter-spacing: 0.05em;
}
.bccc-face {
    height: 96px;
    width: auto;
    object-fit: contain;
    display: block;
    opacity: 0.95;
}

/* ── Your Activity cards ─────────────────────────────────────────────── */
.activity-card {
    background: rgba(124,58,237,0.06);
    border: 1px solid rgba(124,58,237,0.2);
    border-left: 3px solid #7C3AED;
    border-radius: 10px;
    padding: 14px 18px;
    margin-bottom: 8px;
}
.activity-num   { font-size: 2rem; font-weight: 900; color: #F9FAFB; line-height: 1; letter-spacing: -0.03em; }
.activity-label { font-size: 0.68rem; font-weight: 500; color: #6B7280; margin-top: 4px; letter-spacing: 0.04em; text-transform: uppercase; }

</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Header — animated gradient + rebrand
# ─────────────────────────────────────────────────────────────────────────────

_face_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "chris_face.png")
_face_b64  = ""
if os.path.exists(_face_path):
    with open(_face_path, "rb") as _f:
        _face_b64 = _b64.b64encode(_f.read()).decode()
_img_left  = f'<img src="data:image/png;base64,{_face_b64}" class="bccc-face bccc-face-left" alt="">'  if _face_b64 else ""
_img_right = f'<img src="data:image/png;base64,{_face_b64}" class="bccc-face bccc-face-right" alt="">' if _face_b64 else ""

st.markdown(f"""
<style>
  div[data-testid="stMarkdownContainer"]:has(#bccc-header) {{
      margin: 0 !important; padding: 0 !important; line-height: 0;
  }}
  #bccc-header {{
      background: linear-gradient(270deg, #0d0220, #0a1628, #100820, #0a1628, #0d0220);
      background-size: 400% 400%;
      animation: gradientShift 10s ease infinite;
  }}
</style>
<div id="bccc-header" class="bccc-header">
    {_img_left}
    <a href="/" target="_self" class="bccc-center" style="text-decoration:none;cursor:pointer;min-width:0;width:100%;text-align:center;">
        <div class="bccc-title">BIG CASH COMMAND CENTER</div>
        <div class="bccc-subtitle">you want jobs? shit, we got jobs</div>
    </a>
    {_img_right}
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Data
# ─────────────────────────────────────────────────────────────────────────────

all_jobs  = load_leads(90)
leads_7d  = [j for j in all_jobs if j.get("date_added") and (date.today() - j["date_added"]).days <= 7]
saved_jobs = [j for j in all_jobs if j["id"] in st.session_state.saved_jobs]


# ─────────────────────────────────────────────────────────────────────────────
# Nav
# ─────────────────────────────────────────────────────────────────────────────

nav_col, content_col = st.columns([1, 4])

def render_nav(col):
    with col:
        n_saved = len(st.session_state.saved_jobs)
        pages = [
            ("Analytics",                          "📊"),
            (f"All Jobs ({len(all_jobs)})",        "💼"),
            (f"New This Week ({len(leads_7d)})",    "✨"),
            (f"Saved ({n_saved})",                  "🔖"),
            ("Fetch New Jobs",                       "⚙️"),
        ]
        for label, icon in pages:
            base   = label.split(" (")[0]
            active = st.session_state.page == base
            if st.button(
                f"{icon}  {label}",
                key=f"nav_{base}",
                use_container_width=True,
                type="primary" if active else "secondary",
            ):
                st.session_state.page = base
                st.query_params.clear()
                components.html(
                    "<script>window.parent.document.querySelector('section.main').scrollTop=0;</script>",
                    height=0,
                )
                st.rerun()
        st.markdown('<hr style="border:none;border-top:1px solid rgba(124,58,237,0.15);margin:12px 0 10px;">', unsafe_allow_html=True)
        st.caption(date.today().strftime("%B %d, %Y"))

render_nav(nav_col)
page = st.session_state.page

# ── Toast renderer ────────────────────────────────────────────────────────────
_toast_msg = st.session_state.pop("_toast", None)
if _toast_msg:
    components.html(f"""
    <div class="toast-container">
        <div class="toast" id="t1" onclick="this.remove()">{_html.escape(_toast_msg)}</div>
    </div>
    <script>
        setTimeout(()=>{{
            var t=document.getElementById('t1');
            if(t){{t.style.animation='toastOut 0.3s ease forwards';setTimeout(()=>t.remove(),300);}}
        }}, 2500);
    </script>
    <style>
        @keyframes toastIn  {{from{{opacity:0;transform:translateY(12px)}}to{{opacity:1;transform:translateY(0)}}}}
        @keyframes toastOut {{from{{opacity:1}}to{{opacity:0}}}}
        .toast-container{{position:fixed;bottom:24px;right:24px;z-index:9999;pointer-events:none;}}
        .toast{{background:rgba(18,22,40,0.96);backdrop-filter:blur(12px);border:1px solid rgba(124,58,237,0.35);
                border-radius:10px;padding:10px 16px;color:#E2E8F0;font-size:0.78rem;font-weight:600;
                font-family:Inter,sans-serif;box-shadow:0 8px 24px rgba(0,0,0,0.4);
                animation:toastIn 0.2s ease forwards;pointer-events:auto;cursor:pointer;}}
        @media (max-width:768px) {{
            .toast-container{{bottom:auto;top:16px;left:16px;right:16px;width:auto;}}
            .toast{{max-width:100%;white-space:normal;}}
        }}
    </style>
    """, height=0)

# ─────────────────────────────────────────────────────────────────────────────
# Shared: render_job_cards
# ─────────────────────────────────────────────────────────────────────────────

_PAGE_SIZE = 30

def render_job_cards(jobs: list, key_prefix: str = ""):
    if not jobs:
        st.markdown(
            '<div style="text-align:center;padding:48px 0;color:#374151;">'
            '<div style="font-size:2rem;margin-bottom:10px;">🔍</div>'
            '<div style="font-size:0.9rem;font-weight:600;">No jobs match your filters.</div>'
            '<div style="font-size:0.78rem;margin-top:4px;">Try expanding the lookback window or changing the role type.</div>'
            '</div>',
            unsafe_allow_html=True,
        )
        return

    # Pagination
    _limit_key = f"_limit_{key_prefix}"
    if _limit_key not in st.session_state:
        st.session_state[_limit_key] = _PAGE_SIZE
    visible = jobs[: st.session_state[_limit_key]]

    card_cols = st.columns(3, gap="medium")

    for i, job in enumerate(visible):
        col       = card_cols[i % 3]
        jid       = job["id"]
        pc        = PRIORITY_COLORS.get(job.get("priority", "Medium"), "#6B7280")
        work_type = job.get("work_type") or ""
        location  = job.get("location")  or ""
        source    = job.get("source")    or ""
        job_url   = job.get("job_url")   or ""
        is_saved  = jid in st.session_state.saved_jobs
        is_applied= jid in st.session_state.applied_jobs
        new_today = job.get("date_added") and (date.today() - job["date_added"]).days == 0
        rel_date  = relative_time(job.get("date_added"))
        domain    = company_domain(job.get("company_name", ""))

        # Escape for safe HTML insertion
        safe_title   = _html.escape(job.get("role_title", ""))
        safe_company = _html.escape(job.get("company_name", ""))
        safe_loc     = _html.escape(location)

        logo_html    = company_logo_html(job.get("company_name", ""), domain)

        category     = classify(job)
        cat_cls      = _CAT_BORDER_CLASS.get(category, "")
        priority_val = job.get("priority", "")
        # If category has a border class, use priority dot; otherwise fall back to priority border
        if cat_cls:
            card_cls  = f"job-card {cat_cls}"
            pri_dot   = (f'<span class="pri-dot" style="background:{_PRI_DOT_COLOR[priority_val]};" '
                         f'title="{priority_val} Priority"></span>') if priority_val in _PRI_DOT_COLOR else ""
        else:
            pri_cls   = "pri-high" if priority_val == "High" else ("pri-medium" if priority_val == "Medium" else "")
            card_cls  = f"job-card {pri_cls}".strip()
            pri_dot   = ""

        new_badge    = ""
        wt_bg        = "#0d2035" if "remote" in work_type.lower() else "#1a1f35"
        wt_color     = "#67E8F9" if "remote" in work_type.lower() else "#93C5FD"
        wt_tag       = f'<span class="tag" style="background:{wt_bg};color:{wt_color};">{_html.escape(work_type)}</span>' if work_type else ""
        src_tag      = f'<span class="tag" style="background:#0d2218;color:#6EE7B7;">{_html.escape(source)}</span>' if source else ""
        applied_tag  = '<span class="tag" style="background:#064e3b;color:#34D399;">✓ Applied</span>' if is_applied else ""
        apply_html   = f'<a href="{job_url}" target="_blank" class="apply-link">Apply →</a>' if job_url else '<span style="color:#374151;font-size:0.72rem;">No link</span>'

        # Company name → clickable link that filters to that company
        co_encoded = _html.escape(job.get("company_name", "")).replace(" ", "+")
        co_link_html = (
            f'<a class="card-company co-link" '
            f'href="?page=New+Leads&q={co_encoded}" target="_self">'
            f'{safe_company}</a>'
        )


        with col:
            st.markdown(
                f'<div class="{card_cls}">'
                f'  {pri_dot}'
                f'  <div style="display:flex;align-items:flex-start;margin-bottom:6px;">'
                f'    {logo_html}'
                f'    <div style="flex:1;min-width:0;">'
                f'      <div class="card-title">{safe_title}{new_badge}</div>'
                f'      {co_link_html}'
                f'    </div>'
                f'  </div>'
                f'  <div class="card-location">{"📍 " + safe_loc if safe_loc else ""}</div>'
                f'  <div class="card-tags">{wt_tag}{src_tag}{applied_tag}</div>'
                f'  <div class="card-footer">'
                f'    <span class="card-date">Posted {rel_date}</span>'
                f'    {apply_html}'
                f'  </div>'
                f'</div>',
                unsafe_allow_html=True,
            )

            # Compact action row — Save / Applied / Details
            a1, a2, a3 = st.columns([2, 2, 1])
            with a1:
                save_lbl = "🔖 Saved" if is_saved else "🔖 Save"
                if st.button(save_lbl, key=f"{key_prefix}_sv_{jid}", use_container_width=True):
                    if is_saved:
                        st.session_state.saved_jobs.discard(jid)
                        st.session_state._toast = "Removed from saved"
                    else:
                        st.session_state.saved_jobs.add(jid)
                        st.session_state._toast = "Job saved to your list"
                    _persist()
                    st.rerun()
            with a2:
                app_lbl = "✓ Applied" if is_applied else "Mark Applied"
                if st.button(app_lbl, key=f"{key_prefix}_ap_{jid}", use_container_width=True):
                    if is_applied:
                        st.session_state.applied_jobs.discard(jid)
                        st.session_state._toast = "Application status removed"
                    else:
                        st.session_state.applied_jobs.add(jid)
                        st.session_state._toast = "Marked as applied ✓"
                    _persist()
                    st.rerun()
            with a3:
                if st.button("⤢", key=f"{key_prefix}_dt_{jid}", use_container_width=True,
                             help="View full details"):
                    show_job_modal(job)


    # Show more / counter
    remaining = len(jobs) - len(visible)
    if remaining > 0:
        st.markdown("<br>", unsafe_allow_html=True)
        bc, _ = st.columns([1, 3])
        with bc:
            if st.button(
                f"Show {min(remaining, _PAGE_SIZE)} more  ({len(visible)} of {len(jobs)})",
                key=f"more_{key_prefix}",
                use_container_width=True,
            ):
                st.session_state[_limit_key] += _PAGE_SIZE
                st.rerun()
    else:
        st.markdown(
            f'<div style="text-align:center;padding:14px 0;font-size:0.72rem;color:#374151;">'
            f'Showing all {len(jobs)} listings</div>',
            unsafe_allow_html=True,
        )

# ─────────────────────────────────────────────────────────────────────────────
# Page: Analytics
# ─────────────────────────────────────────────────────────────────────────────

def page_analytics():
    jobs = all_jobs
    if not jobs:
        st.info("No data yet — fetch new jobs first.")
        return

    df    = pd.DataFrame(jobs)
    today = date.today()
    df["category"] = df.apply(classify, axis=1)

    # ── Your Activity ──────────────────────────────────────────────────────
    n_sv  = len(st.session_state.saved_jobs)
    n_ap  = len(st.session_state.applied_jobs)
    n_new = sum(1 for j in jobs if j.get("date_added") and (today - j["date_added"]).days <= 7)
    st.markdown("#### 👤 Your Activity")
    ac1, ac2, ac3 = st.columns(3)
    for col, num, label, hint in [
        (ac1, n_sv,  "SAVED",              "Jobs bookmarked this session"),
        (ac2, n_ap,  "APPLIED",            "Applications tracked this session"),
        (ac3, n_new, "NEW MATCHES THIS WEEK", "Fresh listings added in the last 7 days"),
    ]:
        with col:
            st.markdown(
                f'<div class="activity-card">'
                f'  <div class="activity-num">{num}</div>'
                f'  <div class="activity-label" title="{hint}">{label}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
    if not n_sv and not n_ap:
        st.caption("Save or apply to jobs on the All Jobs page to track your activity here.")
    st.markdown('<hr style="border:none;border-top:1px solid rgba(124,58,237,0.12);margin:16px 0;">', unsafe_allow_html=True)

    this_week, last_week = week_counts(jobs)
    total_remote  = int(df["work_type"].str.lower().eq("remote").sum())
    remote_pct    = round(total_remote / len(df) * 100) if df.shape[0] else 0

    # Total listings: this 90d vs prior 90d
    total_now  = len(df)
    total_prev = sum(1 for j in jobs if j.get("date_added") and
                     90 < (today - j["date_added"]).days <= 180)

    # Time-to-stale
    ages      = [(today - j["date_added"]).days for j in jobs if j.get("date_added")]
    avg_age   = round(sum(ages) / len(ages)) if ages else 0
    freshness = "🟢" if avg_age < 7 else "🟡" if avg_age < 14 else "🔴"

    # ── Stat row ──────────────────────────────────────────────────────────
    m1, m2, m3, m4 = st.columns(4)
    cards = [
        (m1, total_now,        "TOTAL LISTINGS (90D)", delta_html(total_now, total_prev)),
        (m2, this_week,        "ADDED THIS WEEK",       delta_html(this_week, last_week)),
        (m3, f"{remote_pct}%", "REMOTE ROLES",          None),
        (m4, f"{freshness} {avg_age}d", "AVG LISTING AGE", None),
    ]
    for col, num, label, dh in cards:
        with col:
            st.markdown(
                f'<div class="stat-card">'
                f'  <div class="stat-num">{num}</div>'
                f'  <div class="stat-label">{label}</div>'
                f'  {dh or ""}'
                f'</div>',
                unsafe_allow_html=True,
            )

    st.markdown('<hr style="border:none;border-top:1px solid rgba(124,58,237,0.12);margin:16px 0;">', unsafe_allow_html=True)

    # ── Market Pulse ──────────────────────────────────────────────────────
    st.markdown("#### 📡 Market Pulse")
    pulse_rows = []
    for cat in list(ROLE_CATEGORIES.keys()) + ["Other"]:
        cat_jobs = [j for j in jobs if classify(j) == cat]
        tw, lw   = week_counts(cat_jobs)
        if lw > 0:
            pct  = min(round((tw - lw) / lw * 100), 999)
        else:
            pct  = 100 if tw > 0 else 0
        pulse_rows.append({"Category": cat, "This Week": tw, "Last Week": lw, "pct": pct, "new": lw == 0 and tw > 0})
    pulse_df = pd.DataFrame(pulse_rows)

    p_chart, p_badges = st.columns([3, 1])
    with p_chart:
        fig = go.Figure()
        fig.add_bar(x=pulse_df["Category"], y=pulse_df["Last Week"],
                    name="Last Week", marker_color=f"rgba(148,163,184,0.35)")
        fig.add_bar(x=pulse_df["Category"], y=pulse_df["This Week"],
                    name="This Week",  marker_color=_PAL[1])
        fig.update_layout(**_CHART, barmode="group",
                          title=dict(text="New Listings: This Week vs Last", y=0.97),
                          legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h",
                                      x=0.5, xanchor="center", y=-0.18))
        fig.update_layout(margin=dict(l=20, r=20, t=52, b=40))
        fig.update_xaxes(showgrid=False)
        fig.update_yaxes(gridcolor="rgba(255,255,255,0.04)")
        st.plotly_chart(fig, use_container_width=True, key="an_pulse")
    with p_badges:
        st.markdown("<br>", unsafe_allow_html=True)
        for _, row in pulse_df.iterrows():
            if row["new"]:
                label = "NEW"
                color = "#F59E0B"
            elif row["pct"] >= 0:
                label = f"↑ {row['pct']}%"
                color = "#10B981"
            else:
                label = f"↓ {abs(row['pct'])}%"
                color = "#EF4444"
            st.markdown(
                f'<div style="padding:9px 0;border-bottom:1px solid rgba(255,255,255,0.04);">'
                f'  <div style="font-size:0.75rem;color:#9CA3AF;font-weight:600;">{row["Category"]}</div>'
                f'  <div style="font-size:1.15rem;font-weight:800;color:{color};">{label}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    st.markdown('<hr style="border:none;border-top:1px solid rgba(124,58,237,0.12);margin:16px 0;">', unsafe_allow_html=True)

    # ── Jobs Added Per Day (full width) ──────────────────────────────────
    if "date_added" in df.columns:
        daily = (df.groupby("date_added").size()
                   .reset_index(name="count")
                   .sort_values("date_added")
                   .tail(30))
        fig = px.area(daily, x="date_added", y="count",
                      title="Jobs Added Per Day (last 30d)",
                      color_discrete_sequence=["#7C3AED"])
        fig.update_layout(**_CHART)
        fig.update_traces(line_color="#7C3AED", fillcolor="rgba(124,58,237,0.15)")
        fig.update_xaxes(showgrid=False)
        fig.update_yaxes(gridcolor="rgba(255,255,255,0.04)")
        st.plotly_chart(fig, use_container_width=True, key="an_daily")

    r2l, r2r = st.columns(2)

    # ── Top 15 Companies ──────────────────────────────────────────────────
    with r2l:
        top_cos = df["company_name"].value_counts().head(15).reset_index()
        top_cos.columns = ["company", "count"]
        top_cos["company"] = top_cos["company"].str[:32]   # truncate long names
        fig = px.bar(top_cos, x="count", y="company", orientation="h",
                     title="Top 15 Companies Hiring",
                     color="count",
                     color_continuous_scale=[_PAL[5], _PAL[4], _PAL[1]])
        fig.update_layout(**_CHART, yaxis=dict(autorange="reversed"),
                          coloraxis_showscale=False)
        fig.update_xaxes(showgrid=False)
        fig.update_yaxes(gridcolor="rgba(255,255,255,0.04)")
        fig.update_traces(hovertemplate="%{y}<br>%{x} listings<extra></extra>")
        st.plotly_chart(fig, use_container_width=True, key="an_companies")

    # ── Role Category Donut ───────────────────────────────────────────────
    with r2r:
        cat_counts = df["category"].value_counts().reset_index()
        cat_counts.columns = ["category", "count"]
        fig = px.pie(cat_counts, names="category", values="count",
                     title="Role Category Breakdown",
                     color_discrete_map=_CAT_COLOR,
                     hole=0.52)
        fig.update_layout(**_CHART)
        fig.update_traces(textfont_color="#F9FAFB",
                          hovertemplate="%{label}: %{value} jobs (%{percent})<extra></extra>")
        st.plotly_chart(fig, use_container_width=True, key="an_cat_donut")

    r3l, r3r = st.columns(2)

    # ── Source Donut ──────────────────────────────────────────────────────
    with r3l:
        src = df["source"].value_counts().reset_index()
        src.columns = ["source", "count"]
        fig = px.pie(src, names="source", values="count",
                     title="Jobs by Source",
                     color_discrete_sequence=_PAL,
                     hole=0.52)
        fig.update_layout(**_CHART)
        fig.update_traces(textfont_color="#F9FAFB")
        st.plotly_chart(fig, use_container_width=True, key="an_source")

    r4l, r4r = st.columns(2)

    # ── Top Locations (gray for generic) ──────────────────────────────────
    with r4l:
        if "location" in df.columns:
            loc_counts = (df["location"].dropna()
                                        .value_counts()
                                        .head(14)
                                        .reset_index())
            loc_counts.columns = ["location", "count"]
            loc_counts["color"] = loc_counts["location"].apply(
                lambda x: _PAL[5] if x.strip().lower() in GENERIC_LOCS else _PAL[0]
            )
            fig = px.bar(loc_counts, x="count", y="location", orientation="h",
                         title="📍 Top Locations",
                         color="color",
                         color_discrete_map="identity")
            fig.update_layout(**_CHART, yaxis=dict(autorange="reversed"),
                              showlegend=False)
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(gridcolor="rgba(255,255,255,0.04)")
            st.plotly_chart(fig, use_container_width=True, key="an_locs")

    # ── Salary distribution + median ─────────────────────────────────────
    with r4r:
        sal_df = df[df["salary_max"].notna() & (df["salary_max"] > 0)].copy()
        if len(sal_df) > 5:
            median_sal = sal_df["salary_max"].median()
            fig = px.histogram(sal_df, x="salary_max", nbins=22,
                               title="💰 Salary Max Distribution",
                               color_discrete_sequence=[_PAL[1]],
                               labels={"salary_max": "Salary ($)"})
            fig.add_vline(
                x=median_sal,
                line_dash="dash",
                line_color="#F59E0B",
                line_width=1.5,
                annotation_text=f"  Median ${median_sal:,.0f}",
                annotation_font_color="#F59E0B",
                annotation_font_size=11,
            )
            fig.update_layout(**_CHART)
            fig.update_xaxes(showgrid=False)
            fig.update_yaxes(gridcolor="rgba(255,255,255,0.04)")
            st.plotly_chart(fig, use_container_width=True, key="an_salary")
        else:
            st.info("Not enough salary data to chart yet.")

    # ── Salary Benchmarks by Category ────────────────────────────────────
    if len(sal_df) > 5:
        sal_cat = sal_df.groupby("category")["salary_max"].mean().reset_index()
        sal_cat.columns = ["category", "avg"]
        sal_cat["avg"] = sal_cat["avg"].round(0)
        sal_cat = sal_cat.sort_values("avg")
        fig = px.bar(sal_cat, x="avg", y="category", orientation="h",
                     title="💰 Avg Salary Max by Role Category",
                     color="avg",
                     color_continuous_scale=[_PAL[5], _PAL[4], _PAL[0]],
                     labels={"avg": "Avg Salary ($)"})
        fig.update_layout(**_CHART, coloraxis_showscale=False)
        fig.update_xaxes(showgrid=False)
        fig.update_yaxes(gridcolor="rgba(255,255,255,0.04)")
        st.plotly_chart(fig, use_container_width=True, key="an_sal_cat")

    # ── Hot Keywords — HORIZONTAL ─────────────────────────────────────────
    st.markdown('<hr style="border:none;border-top:1px solid rgba(124,58,237,0.12);margin:16px 0;">', unsafe_allow_html=True)
    words = []
    for title in df["role_title"].dropna():
        words.extend([w.lower() for w in title.split()
                      if w.lower() not in STOP_WORDS and len(w) > 3])
    wdf = pd.DataFrame(Counter(words).most_common(22), columns=["keyword", "count"])
    wdf = wdf.sort_values("count")                      # ascending for horizontal
    fig = px.bar(wdf, x="count", y="keyword", orientation="h",
                 title="🔥 Hot Keywords in Job Titles",
                 color="count",
                 color_continuous_scale=[_PAL[4], _PAL[1], _PAL[3]],
                 height=520)
    fig.update_layout(**_CHART, coloraxis_showscale=False)
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor="rgba(255,255,255,0.04)")
    st.plotly_chart(fig, use_container_width=True, key="an_keywords")

    # ── Email digest ──────────────────────────────────────────────────────
    st.markdown(
        '<div class="email-section">'
        '  <div style="font-size:1rem;font-weight:800;color:#F1F5F9;margin-bottom:5px;">📬 Weekly Job Alerts</div>'
        '  <div style="font-size:0.78rem;color:#6B7280;">Fresh DR/EM, GovTech &amp; Tech roles every Monday. No spam.</div>'
        '</div>',
        unsafe_allow_html=True,
    )
    e1, e2 = st.columns([4, 1])
    with e1:
        email_in = st.text_input("Email", placeholder="your@email.com",
                                 label_visibility="collapsed", key="email_sub")
    with e2:
        if st.button("Subscribe", type="primary", use_container_width=True):
            if email_in and "@" in email_in:
                try:
                    with db_cursor() as cur:
                        cur.execute(
                            "INSERT INTO email_subscribers (email) VALUES (%s) ON CONFLICT (email) DO NOTHING",
                            (email_in.strip().lower(),),
                        )
                    st.success("You're in 🎉")
                except Exception:
                    st.error("Something went wrong — try again.")
            else:
                st.warning("Enter a valid email.")


# ─────────────────────────────────────────────────────────────────────────────
# Page: Leads (shared by All Jobs + New This Week)
# ─────────────────────────────────────────────────────────────────────────────

def page_leads(days: int):
    heading = "New This Week" if days == 7 else "All Jobs"
    st.markdown(f"### {heading}")

    # ── Row 1: search + lookback + refresh ────────────────────────────────
    fc1, fc2, fc3 = st.columns([4, 1, 1])
    with fc1:
        search = st.text_input(
            "Search jobs",
            placeholder="Company, title, location…",
            label_visibility="collapsed",
            key=f"search_{days}",
        )
    with fc2:
        if days == 7:
            days_opt = 7
        else:
            days_opt = st.selectbox(
                "Lookback", [7, 14, 30, 60, 90], index=2,
                format_func=lambda d: f"{d}d",
                key=f"lb_{days}",
            )
    with fc3:
        if st.button("⟳ Refresh", key=f"refresh_{days}", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # ── Row 2: role · work type · source · sort ───────────────────────────
    _sources = sorted({j.get("source") or "Unknown" for j in all_jobs if j.get("source")})
    ff1, ff2, ff3, ff4 = st.columns([3, 2, 2, 2])
    with ff1:
        role_opt = st.radio(
            "Role", ["All"] + list(ROLE_CATEGORIES.keys()),
            horizontal=True, key=f"role_{days}",
        )
    with ff2:
        wt_opt = st.radio(
            "Work Type", ["All", "Remote", "Hybrid", "On-Site"],
            horizontal=True, key=f"wt_{days}",
        )
    with ff3:
        src_opt = st.radio(
            "Source", ["All"] + _sources,
            horizontal=True, key=f"src_{days}",
        )
    with ff4:
        sort_opt = st.radio(
            "Sort", ["Newest", "Priority", "Date Posted"],
            horizontal=True, key=f"sort_{days}",
        )

    # Salary slider with live label
    sal_jobs = [j for j in all_jobs if j.get("salary_max") and j["salary_max"] > 0]
    sal_range = None
    if sal_jobs:
        lo_v = int(min(j["salary_max"] for j in sal_jobs))
        hi_v = int(max(j["salary_max"] for j in sal_jobs))
        if lo_v < hi_v:
            sal_range = st.slider(
                "Salary max ($)", lo_v, hi_v, (lo_v, hi_v),
                format="$%d", key=f"sal_{days}",
            )
            lo_lbl = f"${sal_range[0]:,}" if sal_range else f"${lo_v:,}"
            hi_lbl = "Any" if (not sal_range or sal_range[1] >= hi_v) else f"${sal_range[1]:,}"
            st.markdown(
                f'<div style="font-size:0.7rem;color:#6B7280;margin-top:-10px;margin-bottom:4px;">'
                f'Salary range: <span style="color:#9CA3AF;font-weight:600;">{lo_lbl} – {hi_lbl}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

    st.markdown('<hr class="filter-sep">', unsafe_allow_html=True)

    # ── Load + filter ─────────────────────────────────────────────────────
    # Reset pagination when filters change
    _filter_sig = (search, role_opt, wt_opt, src_opt, sort_opt, days_opt, sal_range)
    _sig_key = f"_sig_l{days}"
    if st.session_state.get(_sig_key) != _filter_sig:
        st.session_state[_sig_key] = _filter_sig
        st.session_state[f"_limit_l{days}"] = _PAGE_SIZE

    jobs = load_leads(days_opt)

    if role_opt != "All":
        kws  = ROLE_CATEGORIES[role_opt]
        jobs = [j for j in jobs if any(
            k in (j.get("role_title") or "").lower() or
            k in (j.get("company_name") or "").lower()
            for k in kws
        )]

    if wt_opt != "All":
        jobs = [j for j in jobs if (j.get("work_type") or "").lower() == wt_opt.lower()]

    if src_opt != "All":
        jobs = [j for j in jobs if (j.get("source") or "Unknown") == src_opt]

    if search:
        q    = search.lower()
        jobs = [j for j in jobs if
                q in (j.get("company_name") or "").lower() or
                q in (j.get("role_title") or "").lower() or
                q in (j.get("location") or "").lower()]

    if sal_range:
        jobs = [j for j in jobs if
                not j.get("salary_max") or
                sal_range[0] <= j["salary_max"] <= sal_range[1]]

    # ── Sort ──────────────────────────────────────────────────────────────
    _pri_order = {"High": 0, "Medium": 1, "Low": 2}
    if sort_opt == "Priority":
        jobs = sorted(jobs, key=lambda j: _pri_order.get(j.get("priority", "Medium"), 1))
    elif sort_opt == "Date Posted":
        jobs = sorted(jobs, key=lambda j: j.get("date_added") or date.min, reverse=True)
    # "Newest" = default DB order (imported_desc), already sorted

    # ── Quick stats banner ────────────────────────────────────────────────
    n_remote    = sum(1 for j in jobs if (j.get("work_type") or "").lower() == "remote")
    n_this_week = sum(1 for j in jobs if j.get("date_added") and (date.today() - j["date_added"]).days <= 7)
    n_today     = sum(1 for j in jobs if j.get("date_added") and (date.today() - j["date_added"]).days == 0)

    banner_parts = [
        f'<span>{len(jobs)}</span> listings',
        f'<span>{n_this_week}</span> this week',
        f'<span>{n_remote}</span> remote',
    ]
    if n_today:
        banner_parts.append(f'<span style="color:#F59E0B;">🔥 {n_today} today</span>')

    # Sync active filters into URL for shareability + refresh persistence
    _sync_filters(role_opt, search, days_opt,
                  page_key="New This Week" if days == 7 else "All Jobs")

    b1, b2 = st.columns([5, 1])
    with b1:
        st.markdown(
            f'<div class="stats-banner">{" &nbsp;·&nbsp; ".join(banner_parts)}</div>',
            unsafe_allow_html=True,
        )
    with b2:
        if jobs:
            csv = pd.DataFrame(jobs)[
                ["company_name", "role_title", "location", "work_type",
                 "priority", "salary_min", "salary_max", "source", "job_url", "date_added"]
            ].to_csv(index=False)
            st.download_button(
                "CSV",
                data=csv,
                file_name=f"leads_{date.today()}.csv",
                mime="text/csv",
                key=f"csv_{days}",
                use_container_width=True,
            )

    render_job_cards(jobs, key_prefix=f"l{days}")


# ─────────────────────────────────────────────────────────────────────────────
# Page: Saved Jobs
# ─────────────────────────────────────────────────────────────────────────────

def page_saved():
    n_saved   = len(st.session_state.saved_jobs)
    n_applied = len(st.session_state.applied_jobs)

    # Sub-view toggle
    if "saved_tab" not in st.session_state:
        st.session_state.saved_tab = "Saved"

    t1, t2, _, clr = st.columns([2, 2, 3, 1])
    with t1:
        if st.button(f"🔖 Saved ({n_saved})", key="tab_saved",
                     type="primary" if st.session_state.saved_tab == "Saved" else "secondary",
                     use_container_width=True):
            st.session_state.saved_tab = "Saved"
            st.rerun()
    with t2:
        if st.button(f"✓ Applied ({n_applied})", key="tab_applied",
                     type="primary" if st.session_state.saved_tab == "Applied" else "secondary",
                     use_container_width=True):
            st.session_state.saved_tab = "Applied"
            st.rerun()
    with clr:
        if st.button("Clear", type="secondary", use_container_width=True):
            if st.session_state.saved_tab == "Saved":
                st.session_state.saved_jobs = set()
            else:
                st.session_state.applied_jobs = set()
            _persist()
            st.rerun()

    st.markdown('<hr style="border:none;border-top:1px solid rgba(124,58,237,0.12);margin:10px 0 14px;">', unsafe_allow_html=True)

    if st.session_state.saved_tab == "Saved":
        if not n_saved:
            st.markdown(
                '<div style="text-align:center;padding:60px 0;">'
                '  <div style="font-size:2.5rem;margin-bottom:12px;">🔖</div>'
                '  <div style="font-size:0.95rem;font-weight:700;color:#6B7280;">No saved jobs yet</div>'
                '  <div style="font-size:0.78rem;margin-top:6px;color:#374151;">'
                '    Hit <strong style="color:#A78BFA;">🔖 Save</strong> on any listing.</div>'
                '</div>', unsafe_allow_html=True)
            return
        jobs = [j for j in all_jobs if j["id"] in st.session_state.saved_jobs]
        render_job_cards(jobs, key_prefix="sv")
    else:
        if not n_applied:
            st.markdown(
                '<div style="text-align:center;padding:60px 0;">'
                '  <div style="font-size:2.5rem;margin-bottom:12px;">✅</div>'
                '  <div style="font-size:0.95rem;font-weight:700;color:#6B7280;">No applications tracked yet</div>'
                '  <div style="font-size:0.78rem;margin-top:6px;color:#374151;">'
                '    Hit <strong style="color:#34D399;">Mark Applied</strong> on any listing.</div>'
                '</div>', unsafe_allow_html=True)
            return
        jobs = [j for j in all_jobs if j["id"] in st.session_state.applied_jobs]
        render_job_cards(jobs, key_prefix="ap")


# ─────────────────────────────────────────────────────────────────────────────
# Page: Fetch New Jobs
# ─────────────────────────────────────────────────────────────────────────────

def page_ingestion():
    st.markdown("### ⚙️ Fetch New Jobs")

    c1, c2 = st.columns([3, 1])
    with c1:
        st.info("Trigger a manual run to pull fresh listings from Adzuna and USAJobs.")
    with c2:
        dry_run = st.checkbox("Dry run", value=False, help="Fetch jobs but don't write to DB")

    if st.button("▶ Fetch New Jobs", type="primary", use_container_width=True):
        from ingestion import orchestrator
        with st.spinner("Fetching jobs…"):
            try:
                report = orchestrator.run(dry_run=dry_run)
                st.success(
                    f"Done — {report['jobs_created']} new · "
                    f"{report['jobs_updated']} updated · "
                    f"{report['jobs_skipped']} dupes skipped"
                )
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Failed to fetch jobs: {e}")

    # ── Run history ────────────────────────────────────────────────────────
    st.markdown('<hr style="border:none;border-top:1px solid rgba(124,58,237,0.12);margin:20px 0 14px;">', unsafe_allow_html=True)
    st.markdown("#### 📋 Recent Runs")
    try:
        runs = db.get_ingestion_runs(limit=15)
    except Exception:
        runs = []

    if not runs:
        st.caption("No runs recorded yet.")
    else:
        _status_color = {"completed": "#10B981", "failed": "#EF4444", "running": "#F59E0B"}
        for r in runs:
            started  = r.get("started_at")
            finished = r.get("completed_at")
            status   = r.get("status") or "unknown"
            sc       = _status_color.get(status, "#6B7280")
            dur      = ""
            if started and finished:
                secs = int((finished - started).total_seconds())
                dur  = f"{secs}s" if secs < 60 else f"{secs//60}m {secs%60}s"
            ts = started.strftime("%b %d %H:%M") if started else "—"

            st.markdown(
                f'<div style="display:flex;align-items:center;gap:14px;padding:9px 14px;'
                f'background:rgba(14,17,32,0.7);border:1px solid rgba(255,255,255,0.05);'
                f'border-radius:8px;margin-bottom:6px;font-size:0.78rem;">'
                f'  <span style="color:{sc};font-weight:800;min-width:72px;">{status.upper()}</span>'
                f'  <span style="color:#9CA3AF;min-width:90px;">{ts}</span>'
                f'  <span style="color:#F1F5F9;font-weight:600;">+{r.get("jobs_created",0)} new</span>'
                f'  <span style="color:#6B7280;">{r.get("jobs_updated",0)} updated</span>'
                f'  <span style="color:#6B7280;">{r.get("jobs_skipped",0)} dupes</span>'
                f'  <span style="color:#374151;margin-left:auto;">{dur}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )


# ─────────────────────────────────────────────────────────────────────────────
# Route
# ─────────────────────────────────────────────────────────────────────────────

with content_col:
    if page == "Analytics":
        page_analytics()
    elif page.startswith("All Jobs"):
        page_leads(days=90)
    elif page.startswith("New This Week"):
        page_leads(days=7)
    elif page.startswith("Saved"):
        page_saved()
    elif page == "Fetch New Jobs":
        page_ingestion()
