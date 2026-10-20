"""
massive_client.py -- Massive market-data client (Component: data_ingestion)

Wraps the Massive REST API to supply daily OHLCV bars and the two
reference closes the momentum ranking needs: the close ~12 months ago
and the close ~1 month ago. The window deliberately skips the most
recent month, which is dominated by short-term reversal rather than
momentum.

Data source: Massive REST only (no websocket). Primary path for the
momentum snapshot is the grouped daily endpoint (one call returns the
whole market for one date); per-ticker aggregates are the fallback.

Invariants:
    - Every outbound request passes through ``_rate_limit`` first.
    - Prices are split/dividend adjusted unless a caller opts out.
    - A ticker with no resolvable price yields None, never 0.0, so
      downstream code can tell "missing" from "worthless".
"""

from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging
import time
from urllib3.exceptions import MaxRetryError

from django.conf import settings
import pandas as pd
from massive import RESTClient
from massive.exceptions import BadResponse

logger = logging.getLogger(__name__)


class MassiveAPIClient:
    """
    Rate-limited, caching client for Massive daily aggregates.

    Pipeline stage: data ingestion, upstream of momentum scoring.
    Lifecycle: the response cache and the request-time window live on
    the instance, so they are lost when it is discarded and are not
    shared across processes. Not thread-safe: concurrent callers would
    race on ``request_times`` and could exceed the rate limit.

    Parameters
    ----------
    api_key : str, optional
        Massive API key. Defaults to ``settings.MASSIVE_API_KEY``, which
        is read from the environment.
    requests_per_minute : int, default 5
        Client-side request budget per rolling 60 s window. 5 matches
        the Massive free-tier quota.

    Raises
    ------
    ValueError
        If no API key is supplied and none is configured.
    """

    def __init__(self, api_key: str = None, requests_per_minute: int = 5):
        self.api_key = api_key or settings.MASSIVE_API_KEY
        if not self.api_key:
            raise ValueError("Massive API key is required")
        self.requests_per_minute = requests_per_minute
        self.client = RESTClient(self.api_key)
        self._cache = {}  # per-instance; unbounded, no expiry
        self.request_times = []  # epoch seconds of recent requests

    def _get_cache_key(self, ticker: str, start_date: str, end_date: str) -> str:
        """
        Build the cache key for a per-ticker aggregates request.

        NOTE: multiplier, timespan and adjusted are not part of the key,
        so requests differing only in those share one cache entry.
        """
        return f"{ticker}:{start_date}:{end_date}"

    def _rate_limit(self):
        """
        Block until another request fits in the rolling 60 s window.

        Sliding-window limiter: keeps the timestamps of requests made in
        the last minute and, once the budget is spent, sleeps until the
        oldest one ages out (plus 1 s of slack for clock jitter).
        """
        current_time = time.time()
        self.request_times = [t for t in self.request_times if current_time - t < 60]

        if len(self.request_times) >= self.requests_per_minute:
            sleep_time = 60 - (current_time - self.request_times[0]) + 1
            if sleep_time > 0:
                logger.info(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)

        self.request_times.append(current_time)

    # ─────────────────────────────────────────────────────────────────────────
    # Per-ticker aggregates
    # ─────────────────────────────────────────────────────────────────────────

    def fetch_stock_data(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        multiplier: int = 1,
        timespan: str = "day",
        adjusted: bool = True,
        limit: int = 50000,
        use_cache: bool = True,
        max_retries: int = 3,
        retry_delay: int = 60,
    ) -> List[Dict]:
        """
        Fetch OHLCV bars for one ticker over a date range 
        (start_date to end_date).

        Retries only on HTTP 429 (surfaced by urllib3 as MaxRetryError);
        every other error is raised immediately, since retrying a bad
        ticker or malformed request would just burn the rate budget.

        Parameters
        ----------
        ticker : str
            Exchange ticker, e.g. 'AAPL'.
        start_date, end_date : str
            Inclusive range in 'YYYY-MM-DD' format.
        multiplier : int, default 1
            Bar size in units of ``timespan``.
        timespan : str, default 'day'
            Bar unit as accepted by Massive ('minute', 'hour', 'day', ...).
        adjusted : bool, default True
            Return split/dividend-adjusted prices.
        limit : int, default 50000
            Max base aggregates per page; 50000 is the API maximum.
        use_cache : bool, default True
            Serve from and populate the in-memory cache.
        max_retries : int, default 3
            Total attempts, including the first, on HTTP 429.
        retry_delay : int, default 60
            Seconds to wait after a 429; one full rate-limit window.

        Returns
        -------
        list of dict
            One dict per bar with keys date, open, high, low, close,
            volume, vwap, transactions. vwap and transactions may be None.

        Raises
        ------
        MaxRetryError
            If 429s persist past ``max_retries`` or the error is not a 429.
        ValueError, TypeError, BadResponse
            On invalid input or a non-success API response.
        """
        cache_key = self._get_cache_key(ticker, start_date, end_date)

        if use_cache and (cache_key in self._cache):
            logger.info(f"Using cached data for {ticker}")
            return self._cache[cache_key]

        # NOTE: rate limit is applied once per call, not per retry; the
        # retry_delay sleep is what spaces out the retried attempts.
        self._rate_limit()

        for attempt in range(max_retries):
            try:
                aggs = []
                # list_aggs paginates transparently, so a single call may
                # issue several HTTP requests for long ranges.
                for agg in self.client.list_aggs(
                    ticker,
                    multiplier,
                    timespan,
                    start_date,
                    end_date,
                    adjusted=adjusted,
                    limit=limit,
                ):
                    aggs.append(
                        {
                            # Massive timestamps are epoch milliseconds.
                            "date": datetime.fromtimestamp(agg.timestamp / 1000).date(),
                            "open": agg.open,
                            "high": agg.high,
                            "low": agg.low,
                            "close": agg.close,
                            "volume": agg.volume,
                            # Optional fields: absent on some bars.
                            "vwap": getattr(agg, "vwap", None),
                            "transactions": getattr(agg, "transactions", None),
                        }
                    )

                if use_cache:
                    self._cache[cache_key] = aggs

                logger.info(f"Fetched {len(aggs)} data points for {ticker}")
                return aggs

            except MaxRetryError as e:
                if "429" in str(e) and attempt < max_retries - 1:
                    logger.warning(
                        f"Rate limited on attempt {attempt + 1} for {ticker}, retrying in {retry_delay} seconds"
                    )
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"Max retries exceeded for {ticker}: {str(e)}")
                    raise
            except (ValueError, TypeError, BadResponse) as e:
                logger.error(f"Error fetching data for {ticker}: {str(e)}")
                raise

    # ─────────────────────────────────────────────────────────────────────────
    # Momentum snapshot (12-month and 1-month reference closes)
    # ─────────────────────────────────────────────────────────────────────────

    def fetch_bulk_momentum_data(
        self, tickers: List[str], calculation_date: datetime = None
    ) -> Dict[str, Dict[str, Optional[float]]]:
        """
        Fetch the 12-month and 1-month reference closes for many tickers.

        Uses the grouped daily endpoint, which returns every US stock for
        one date in a single call, so the happy path costs 2 requests
        regardless of universe size. Reference dates are approximated as
        calculation_date - 365 days and - 30 days. Tickers missing on
        those exact dates (weekend, holiday, halt) are retried on nearby
        dates; if the grouped endpoint itself fails, the whole fetch
        falls back to per-ticker aggregates.

        Parameters
        ----------
        tickers : list of str
            Universe to price.
        calculation_date : datetime or date, optional
            Anchor date for the lookbacks. Defaults to today.

        Returns
        -------
        dict
            ``{ticker: {'price_12m': float | None,
                        'price_1m': float | None}}``. Every input
            ticker is present; None means no price could be resolved.
        """
        if calculation_date is None:
            calculation_date = datetime.now().date()

        twelve_months_ago = calculation_date - timedelta(days=365)
        one_month_ago = calculation_date - timedelta(days=30)

        logger.info(
            f"TRUE bulk fetching momentum data for {len(tickers)} stocks using grouped daily API"
        )
        logger.info(f"Target dates: 12m={twelve_months_ago}, 1m={one_month_ago}")

        # Pre-seed so tickers absent from both responses still appear in
        # the result with explicit Nones.
        momentum_data = {}
        for ticker in tickers:
            momentum_data[ticker] = {"price_12m": None, "price_1m": None}

        try:
            self._rate_limit()

            logger.info(
                f"Fetching grouped daily data for 12 months ago ({twelve_months_ago})"
            )
            twelve_month_data = self.client.get_grouped_daily_aggs(
                date=twelve_months_ago, adjusted=True
            )

            # The response covers the whole market; keep only our universe.
            twelve_month_prices = {}
            for agg in twelve_month_data:
                if hasattr(agg, "ticker") and agg.ticker in tickers:
                    twelve_month_prices[agg.ticker] = float(agg.close)

            logger.info(f"Found 12-month prices for {len(twelve_month_prices)} stocks")

            self._rate_limit()

            logger.info(
                f"Fetching grouped daily data for 1 month ago ({one_month_ago})"
            )
            one_month_data = self.client.get_grouped_daily_aggs(
                date=one_month_ago, adjusted=True
            )

            one_month_prices = {}
            for agg in one_month_data:
                if hasattr(agg, "ticker") and agg.ticker in tickers:
                    one_month_prices[agg.ticker] = float(agg.close)

            logger.info(f"Found 1-month prices for {len(one_month_prices)} stocks")

            for ticker in tickers:
                momentum_data[ticker] = {
                    "price_12m": twelve_month_prices.get(ticker),
                    "price_1m": one_month_prices.get(ticker),
                }

                logger.debug(
                    f"Momentum data for {ticker}: "
                    f"12m=${momentum_data[ticker]['price_12m']}, "
                    f"1m=${momentum_data[ticker]['price_1m']}"
                )

            # A reference date landing on a non-trading day returns an
            # empty or partial market; resolve those tickers from nearby
            # dates instead.
            missing_tickers = [
                t
                for t in tickers
                if not momentum_data[t]["price_12m"] or not momentum_data[t]["price_1m"]
            ]
            if missing_tickers:
                logger.info(
                    f"Handling {len(missing_tickers)} stocks with missing data using fallback dates"
                )
                momentum_data = self._handle_missing_data_fallback(
                    momentum_data, missing_tickers, calculation_date
                )

            logger.info(f"Successfully fetched momentum data using only 2 API calls!")

        except Exception as e:
            logger.error(
                f"Error with grouped daily API, falling back to individual calls: {str(e)}"
            )
            # Fallback: per-ticker aggregates. Far slower (one request per
            # ticker under the same rate budget) but independent of the
            # grouped endpoint.
            return self._fetch_bulk_momentum_data_fallback(tickers, calculation_date)

        return momentum_data

    def _fetch_bulk_momentum_data_fallback(
        self, tickers: List[str], calculation_date: datetime
    ) -> Dict[str, Dict[str, Optional[float]]]:
        """
        Resolve momentum reference closes via per-ticker aggregates.

        Used only when the grouped daily endpoint fails. Fetches one
        range per ticker that spans both reference dates, then picks the
        bar nearest each date. Returns the same shape as
        ``fetch_bulk_momentum_data``.
        """
        logger.warning("Using fallback method - this will be slower")

        twelve_months_ago = calculation_date - timedelta(days=365)
        one_month_ago = calculation_date - timedelta(days=30)

        # One range covers both reference dates, so each ticker costs a
        # single request. The 30-day lead guarantees trading days exist
        # before the 12-month date; the 7-day tail is slack at the end.
        start_date = twelve_months_ago - timedelta(days=30)
        end_date = calculation_date + timedelta(days=7)

        logger.info(
            f"Fallback: fetching momentum data for {len(tickers)} stocks from {start_date} to {end_date}"
        )

        all_data = self.fetch_multiple_stocks(
            tickers=tickers,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            batch_size=5,  # one batch fills the 5 req/min budget
            delay_between_batches=15,
        )

        momentum_data = {}

        for ticker in tickers:
            data = all_data.get(ticker, [])
            if not data:
                momentum_data[ticker] = {"price_12m": None, "price_1m": None}
                continue

            df = pd.DataFrame(data)
            if df.empty:
                momentum_data[ticker] = {"price_12m": None, "price_1m": None}
                continue

            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date")

            # FIXME(data-ingestion): _find_closest_price is not defined on
            # this class, so this fallback raises AttributeError when hit.
            price_12m = self._find_closest_price(df, twelve_months_ago)
            price_1m = self._find_closest_price(df, one_month_ago)

            momentum_data[ticker] = {"price_12m": price_12m, "price_1m": price_1m}

            logger.debug(
                f"Momentum data for {ticker}: 12m=${price_12m}, 1m=${price_1m}"
            )

        return momentum_data

    def _handle_missing_data_fallback(
        self,
        momentum_data: Dict[str, Dict[str, Optional[float]]],
        missing_tickers: List[str],
        calculation_date: datetime,
    ) -> Dict[str, Dict[str, Optional[float]]]:
        """
        Fill missing reference closes from dates near the targets.

        Probes grouped daily data at offsets up to +/-7 days, nearest
        first, and only fills slots that are still empty, so the closest
        available trading day wins. Mutates and returns ``momentum_data``.
        Worst case costs 28 requests (14 offsets x 2 reference dates),
        roughly 6 minutes under the default 5 req/min budget.
        """
        twelve_months_ago = calculation_date - timedelta(days=365)
        one_month_ago = calculation_date - timedelta(days=30)

        # Nearest-first, forward-biased at each distance: +1..+3 covers a
        # weekend or a holiday-extended weekend before looking backward.
        for days_offset in [1, 2, 3, -1, -2, -3, 4, 5, 6, 7, -4, -5, -6, -7]:
            if not missing_tickers:
                break

            fallback_12m_date = twelve_months_ago + timedelta(days=days_offset)
            try:
                self._rate_limit()
                fallback_12m_data = self.client.get_grouped_daily_aggs(
                    date=fallback_12m_date, adjusted=True
                )

                for agg in fallback_12m_data:
                    if hasattr(agg, "ticker") and agg.ticker in missing_tickers:
                        if not momentum_data[agg.ticker]["price_12m"]:
                            momentum_data[agg.ticker]["price_12m"] = float(agg.close)
                            logger.debug(
                                f"Found 12m price for {agg.ticker} on {fallback_12m_date}"
                            )
            # FIXME(data-ingestion): bare except hides auth and network
            # failures as well as the expected no-data responses.
            except:
                pass

            fallback_1m_date = one_month_ago + timedelta(days=days_offset)
            try:
                self._rate_limit()
                fallback_1m_data = self.client.get_grouped_daily_aggs(
                    date=fallback_1m_date, adjusted=True
                )

                for agg in fallback_1m_data:
                    if hasattr(agg, "ticker") and agg.ticker in missing_tickers:
                        if not momentum_data[agg.ticker]["price_1m"]:
                            momentum_data[agg.ticker]["price_1m"] = float(agg.close)
                            logger.debug(
                                f"Found 1m price for {agg.ticker} on {fallback_1m_date}"
                            )
            except:
                pass

            # Shrink the set so the loop can exit early once all resolve.
            missing_tickers = [
                t
                for t in missing_tickers
                if not momentum_data[t]["price_12m"] or not momentum_data[t]["price_1m"]
            ]

        return momentum_data

    # ─────────────────────────────────────────────────────────────────────────
    # Batch and point-in-time helpers
    # ─────────────────────────────────────────────────────────────────────────

    def fetch_multiple_stocks(
        self,
        tickers: List[str],
        start_date: str,
        end_date: str,
        adjusted: bool = True,
        batch_size: int = 10,
        delay_between_batches: int = 12,
    ) -> Dict[str, List[Dict]]:
        """
        Fetch OHLCV bars for many tickers, batched to respect rate limits.

        A failure on one ticker is logged and recorded as an empty list
        so the rest of the batch still completes.

        Parameters
        ----------
        tickers : list of str
            Tickers to fetch.
        start_date, end_date : str
            Inclusive range in 'YYYY-MM-DD' format.
        adjusted : bool, default True
            Return split/dividend-adjusted prices.
        batch_size : int, default 10
            Tickers per batch.
        delay_between_batches : int, default 12
            Extra seconds to sleep between batches, on top of the
            per-request limiter.

        Returns
        -------
        dict
            ``{ticker: list of bar dicts}`` as returned by
            ``fetch_stock_data``; an empty list marks a failed ticker.
        """
        results = {}
        total_tickers = len(tickers)

        logger.info(
            f"Fetching data for {total_tickers} stocks in batches of {batch_size}"
        )

        for i in range(0, total_tickers, batch_size):
            batch_tickers = tickers[i : i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_tickers + batch_size - 1) // batch_size  # ceil

            logger.info(
                f"Processing batch {batch_num}/{total_batches} ({len(batch_tickers)} stocks)"
            )

            for ticker in batch_tickers:
                try:
                    results[ticker] = self.fetch_stock_data(
                        ticker=ticker,
                        start_date=start_date,
                        end_date=end_date,
                        adjusted=adjusted,
                    )
                    logger.info(f"Successfully fetched data for {ticker}")
                except (MaxRetryError, ValueError, TypeError, BadResponse) as e:
                    logger.error(f"Failed to fetch data for {ticker}: {str(e)}")
                    results[ticker] = []

            # No sleep after the final batch.
            if i + batch_size < total_tickers:
                logger.info(
                    f"Waiting {delay_between_batches} seconds before next batch..."
                )
                time.sleep(delay_between_batches)

        return results

    def get_price_on_date(
        self, ticker: str, target_date: datetime, tolerance_days: int = 7
    ) -> Optional[float]:
        """
        Return the close nearest to ``target_date`` for one ticker.

        Searches +/- ``tolerance_days`` around the target so weekends and
        holidays still resolve. Ties in distance go to the earlier bar.

        Parameters
        ----------
        ticker : str
            Exchange ticker, e.g. 'AAPL'.
        target_date : datetime or date
            Date to price.
        tolerance_days : int, default 7
            Half-width of the search window in calendar days.

        Returns
        -------
        float or None
            Nearest close, or None if no bar exists in the window or the
            request fails with ValueError or TypeError.
        """
        start_date = target_date - timedelta(days=tolerance_days)
        end_date = target_date + timedelta(days=tolerance_days)

        try:
            data = self.fetch_stock_data(
                ticker=ticker,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
            )

            if not data:
                return None

            # Bar dates are datetime.date; normalize a datetime target so
            # the subtraction below is date - date.
            target_date_obj = (
                target_date if hasattr(target_date, "date") else target_date
            )
            if hasattr(target_date_obj, "date"):
                target_date_obj = target_date_obj.date()

            closest_data = min(
                data, key=lambda x: abs((x["date"] - target_date_obj).days)
            )

            return float(closest_data["close"])

        except (ValueError, TypeError) as e:
            logger.error(f"Error getting price for {ticker} on {target_date}: {str(e)}")
            return None


def get_massive_client() -> MassiveAPIClient:
    """
    Return a new client configured from Django settings.

    Each call builds a fresh instance, so the response cache and
    rate-limit window are not shared between callers.
    """
    return MassiveAPIClient()
