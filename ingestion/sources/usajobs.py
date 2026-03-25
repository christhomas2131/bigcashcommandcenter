"""
ingestion/sources/usajobs.py — USAJobs.gov API client.

Free API — register at https://developer.usajobs.gov/ to get a key.
Requires two headers: User-Agent (your email) and Authorization-Key.

Great source for FEMA, disaster recovery, emergency management, and
grants management roles in federal and consulting contexts.
"""

from __future__ import annotations

import logging
import time

import requests

log = logging.getLogger(__name__)

_BASE_URL = "https://data.usajobs.gov/api/search"


class USAJobsClient:
    def __init__(self, email: str, api_key: str):
        self.headers = {
            "Host":              "data.usajobs.gov",
            "User-Agent":        email,
            "Authorization-Key": api_key,
        }

    def search(self, keyword: str, num_results: int = 25, days_posted: int = 30,
               remote_only: bool = False, location: str | None = None) -> list[dict]:
        results = []
        page    = 1
        per_page = min(num_results, 25)

        while len(results) < num_results:
            params: dict = {
                "Keyword":        keyword,
                "DatePosted":     days_posted,
                "ResultsPerPage": per_page,
                "Page":           page,
            }
            if remote_only:
                params["RemoteIndicator"] = "True"
            if location:
                params["LocationDescriptions"] = location

            try:
                resp = requests.get(_BASE_URL, headers=self.headers, params=params, timeout=15)
                resp.raise_for_status()
                data = resp.json()
            except requests.RequestException as exc:
                log.error(f"  USAJobs error for '{keyword}' page {page}: {exc}")
                break

            items = (
                data.get("SearchResult", {})
                    .get("SearchResultItems", [])
            )
            if not items:
                break

            results.extend(items)
            total = int(data.get("SearchResult", {}).get("SearchResultCount", 0))
            if len(results) >= total or len(results) >= num_results:
                break

            page += 1
            time.sleep(0.5)

        return results[:num_results]
