"""
ingestion/sources/jsearch.py — JSearch (RapidAPI) client.
"""

from __future__ import annotations

import logging
import time

import requests

log = logging.getLogger(__name__)


class JSearchClient:
    def __init__(self, api_key: str, base_url: str = "https://jsearch.p.rapidapi.com"):
        self.key      = api_key
        self.base_url = base_url
        self.headers  = {
            "X-RapidAPI-Key":  self.key,
            "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
        }

    def search(self, query: str, num_results: int = 20, days_posted: int = 30) -> list[dict]:
        results   = []
        per_page  = min(num_results, 10)
        max_pages = -(-num_results // per_page)   # ceiling division

        for page in range(1, max_pages + 1):
            try:
                resp = requests.get(
                    f"{self.base_url}/search",
                    headers=self.headers,
                    params={
                        "query":       query,
                        "page":        str(page),
                        "num_pages":   "1",
                        "date_posted": "month" if days_posted >= 30 else "week",
                    },
                    timeout=15,
                )
                resp.raise_for_status()
                batch = resp.json().get("data") or []
                if not batch:
                    break
                results.extend(batch)
                if len(results) >= num_results:
                    break
                time.sleep(1.2)
            except requests.RequestException as exc:
                log.error(f"  JSearch error on page {page} for '{query}': {exc}")
                break

        return results[:num_results]
