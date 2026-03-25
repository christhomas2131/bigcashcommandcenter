"""
app.py — Public Job Board

A shared job board for Disaster Recovery, Emergency Management, GovTech,
and Tech roles. Powered by automated ingestion from Adzuna and USAJobs.
"""

import os
from datetime import datetime

import streamlit as st

import config
from db import repository as db
from db.schema import migrate

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Public Job Board",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# DB init
# ---------------------------------------------------------------------------

@st.cache_resource
def init_db():
    migrate()

init_db()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

STATUS_COLORS = {
    "Researching":          "#6B7280",
    "Ready to Apply":       "#3B82F6",
    "Applied":              "#F59E0B",
    "Phone Screen":         "#8B5CF6",
    "Interview":            "#EC4899",
    "Technical Assessment": "#06B6D4",
    "Final Round":          "#10B981",
    "Offer":                "#22C55E",
    "Rejected":             "#EF4444",
    "Ghosted":              "#4B5563",
}

PRIORITY_COLORS = {
    "High":   "#EF4444",
    "Medium": "#F59E0B",
    "Low":    "#6B7280",
}

ROLE_FILTER_KEYWORDS = {
    "DR/EM": [
        "disaster", "emergency", "fema", "hazard", "mitigation",
        "flood", "resilience", "public assistance", "cdbg", "recovery",
        "homeland", "grants management",
    ],
    "GovTech": [
        "govtech", "government", "public sector", "federal", "civic",
        "state and local", "municipal",
    ],
    "Tech / Sales": [
        "solutions engineer", "sales engineer", "pre-sales", "account executive",
        "account manager", "customer success", "technical account",
        "partner solutions", "revenue enablement",
    ],
}

# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

st.markdown("""
<style>
    [data-testid="collapsedControl"] { display: none; }
    .block-container { padding-top: 1rem; }

    .job-card {
        background: #1E2130;
        border-radius: 8px;
        padding: 12px 14px;
        margin-bottom: 10px;
        border-left: 3px solid #3B82F6;
    }
    .job-card-title {
        font-weight: 700;
        font-size: 0.9rem;
        color: #F9FAFB;
        margin-bottom: 2px;
    }
    .job-card-meta {
        font-size: 0.78rem;
        color: #9CA3AF;
    }
    .tag {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 0.7rem;
        font-weight: 600;
        margin-right: 4px;
        margin-top: 4px;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

st.markdown("""
<div style="
    background: linear-gradient(90deg, #1a0533, #0a1628, #1a0533);
    border-top: 2px solid #7C3AED;
    border-bottom: 2px solid #7C3AED;
    padding: 14px 24px;
    margin-bottom: 16px;
    text-align: center;
">
    <div style="
        letter-spacing: 0.18em;
        font-size: 1.4rem;
        font-weight: 900;
        color: #F9FAFB;
        text-shadow: 0 0 20px #7C3AED, 0 0 40px #3B82F6;
        font-family: monospace;
    ">PUBLIC JOB BOARD</div>
    <div style="color:#9CA3AF;font-size:0.8rem;margin-top:4px;">
        Disaster Recovery · Emergency Management · GovTech · Tech Sales
    </div>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Controls
# ---------------------------------------------------------------------------

col_search, col_filter, col_sort, col_days = st.columns([3, 2, 2, 1])

with col_search:
    search = st.text_input("Search", placeholder="Company, title, or location…", label_visibility="collapsed")

with col_filter:
    role_filter = st.radio("Role type", ["All", "DR/EM", "GovTech", "Tech / Sales"], horizontal=True)

with col_sort:
    sort_opt = st.radio("Sort by", ["Newest", "Priority", "Date Posted"], horizontal=True)

with col_days:
    days_opt = st.selectbox("Lookback", [7, 14, 30, 60, 90], index=2, format_func=lambda d: f"{d}d")

sort_map = {
    "Newest":      "imported_desc",
    "Priority":    "priority",
    "Date Posted": "date_added_desc",
}

# ---------------------------------------------------------------------------
# Load + filter leads
# ---------------------------------------------------------------------------

leads = db.get_new_leads(days=days_opt, sort_by=sort_map[sort_opt])

if role_filter != "All":
    kws = ROLE_FILTER_KEYWORDS[role_filter]
    leads = [
        j for j in leads
        if any(k in (j.get("role_title") or "").lower()
               or k in (j.get("company_name") or "").lower()
               for k in kws)
    ]

if search:
    q = search.lower()
    leads = [
        j for j in leads
        if q in (j.get("company_name") or "").lower()
        or q in (j.get("role_title") or "").lower()
        or q in (j.get("location") or "").lower()
    ]

# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------

st.caption(f"{len(leads)} jobs found")

if not leads:
    st.info("No jobs match your filters. Try expanding the lookback window or changing the role type.")
    st.stop()

# Render cards in 3 columns
card_cols = st.columns(3)

for i, job in enumerate(leads):
    col = card_cols[i % 3]
    pc = PRIORITY_COLORS.get(job.get("priority", "Medium"), "#6B7280")
    work_type = job.get("work_type") or ""
    location  = job.get("location") or ""
    source    = job.get("source") or ""
    job_url   = job.get("job_url") or ""
    date_added = job.get("date_added") or ""

    with col:
        st.markdown(
            f'<div class="job-card">'
            f'<div class="job-card-title">{job["role_title"]}</div>'
            f'<div class="job-card-meta">{job["company_name"]}</div>'
            f'<div class="job-card-meta" style="margin-top:4px;">{location}</div>'
            f'<div style="margin-top:6px;">'
            f'<span class="tag" style="background:#1E3A5F;color:#93C5FD;">{work_type}</span>'
            f'<span class="tag" style="background:#1F2937;color:{pc};">{job.get("priority","")} Pri</span>'
            f'{"<span class=\\"tag\\" style=\\"background:#1a2e1a;color:#6EE7B7;\\">" + source + "</span>" if source else ""}'
            f'</div>'
            f'<div class="job-card-meta" style="margin-top:6px;">Posted: {date_added}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
        if job_url:
            st.link_button("Apply →", job_url, use_container_width=True)
