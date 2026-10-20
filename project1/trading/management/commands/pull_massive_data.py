"""
pull_massive_data.py -- Massive fetch smoke test (Component: data_ingestion)

Django management command (``python manage.py pull_massive_data``) that
exercises ``MassiveAPIClient.fetch_bulk_momentum_data`` end to end
against the live Massive REST API and drops into the debugger to
inspect the result.

Invariants:
    - Read-only: fetches prices, writes nothing to the database.
    - Development use only; it blocks on an interactive breakpoint.
"""

from django.core.management.base import BaseCommand
from trading.services.massive_client import MassiveAPIClient
from datetime import date


class Command(BaseCommand):
    """
    Fetch momentum reference closes for a fixed ticker pair and date.

    Pipeline stage: manual check of data ingestion, run before wiring
    the client into momentum scoring.
    """

    def handle(self, *args, **options):
        """
        Run the bulk momentum fetch and open a debugger on the result.

        Parameters
        ----------
        *args, **options
            Supplied by Django's command runner; unused.
        """
        # Requires MASSIVE_API_KEY in the environment; the client raises
        # ValueError at construction if it is missing.
        client = MassiveAPIClient()
        
        # TODO(data-ingestion): take tickers and calculation date as
        # command arguments instead of hard-coding them.
        result = client.fetch_bulk_momentum_data(["AAPL", "NVDA", "TSLA"], date(2026, 5, 12))
        print(result)
        # NOTE: blocks for interactive inspection; remove before this
        # command runs unattended (cron, CI).
        breakpoint()
