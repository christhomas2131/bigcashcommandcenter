"""
ingestion/sources/adzuna.py — Adzuna API client.
"""

from __future__ import annotations

import logging

import requests

log = logging.getLogger(__name__)


class AdzunaClient:
    def __init__(self, app_id: str, app_key: str, country: str = "us"):
        self.app_id  = app_id
        self.app_key = app_key
        self.country = country

    def search(self, query: str, num_results: int = 20, days_posted: int = 30) -> list[dict]:
        try:
            resp = requests.get(
                f"https://api.adzuna.com/v1/api/jobs/{self.country}/search/1",
                params={
                    "app_id":           self.app_id,
                    "app_key":          self.app_key,
                    "what":             query,
                    "results_per_page": min(num_results, 50),
                    "max_days_old":     days_posted,
                    "content-type":     "application/json",
                },
                timeout=15,
            )
            resp.raise_for_status()
            return (resp.json().get("results") or [])[:num_results]
        except requests.RequestException as exc:
            log.error(f"  Adzuna error for '{query}': {exc}")
            return []
