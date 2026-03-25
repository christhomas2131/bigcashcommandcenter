"""
ingestion/run_ingestion.py — CLI entry point for the ingestion pipeline.

Called by the Render cron job or manually:
    python -m ingestion.run_ingestion
    python -m ingestion.run_ingestion --dry-run
    python -m ingestion.run_ingestion --companies-only
    python -m ingestion.run_ingestion --profile "Disaster Recovery / Emergency Management"
"""

import argparse
import logging
import sys

from db.schema import migrate
from ingestion import orchestrator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the job ingestion pipeline against Postgres.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--profile", metavar="NAME",
        help="Run a single search profile by name",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch and dedup but do not write to DB",
    )
    parser.add_argument(
        "--companies-only", action="store_true",
        help="Skip API keyword search; run company watcher only",
    )
    parser.add_argument(
        "--config", metavar="PATH",
        help="Path to search_config.json (overrides INGESTION_SEARCH_CONFIG env var)",
    )
    args = parser.parse_args()

    # Ensure schema is up to date before running
    migrate()

    try:
        orchestrator.run(
            search_config_path=args.config,
            profile_name=args.profile,
            companies_only=args.companies_only,
            dry_run=args.dry_run,
        )
    except Exception as exc:
        logging.error(f"Ingestion aborted: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
