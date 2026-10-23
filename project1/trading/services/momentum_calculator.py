"""
momentum_calculator.py -- 12-1 momentum scoring (Component: feature_pipeline)

Computes the classic "12-1" cross-sectional momentum signal for every stock
in the universe, persists one MomentumScore per (stock, calculation_date),
and assigns ranks and quintiles that the strategy engine turns into trades.

    momentum = (P_1m - P_12m) / P_12m

where P_12m is the close ~365 calendar days before calculation_date and
P_1m the close ~30 days before. The most recent month is skipped on
purpose: short-horizon returns tend to reverse and would contaminate the
signal.

Invariants:
    - Output contract: quintile 1 = strongest momentum (BUY candidates),
      quintile 5 = weakest (SELL candidates).
    - A stock missing either reference price gets no score rather than
      a fabricated one.
    - Primary data path is the Massive grouped-daily bulk fetch; the
      per-ticker path is a fallback only.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from decimal import Decimal
import time     
import logging


from django.conf import settings
from django.utils import timezone
import numpy as np
import pandas as pd

from trading.services.massive_client import get_massive_client
from trading.models import Stock, PriceData, MomentumScore

logger = logging.getLogger(__name__)


class MomentumCalculator:
    """
    Scores, ranks and buckets the stock universe by 12-1 momentum.

    Pipeline stage: post-ingestion, pre-signal. Reads prices from the
    local PriceData table or the Massive API and writes MomentumScore
    rows; it never places orders.
    Lifecycle: stateless apart from the Massive client; a new instance
    per rebalance is safe.
    """

    def __init__(self):
        self.massive_client = get_massive_client()
        # NOTE: both settings are read but unused; the window is hardcoded
        # as 365 / 30 calendar days throughout this module.
        self.lookback_months = getattr(settings, "MOMENTUM_LOOKBACK_MONTHS", 12)
        self.skip_months = getattr(settings, "MOMENTUM_SKIP_MONTHS", 1)

    # ─────────────────────────────────────────────────────────────────────────
    # Single-stock momentum
    # ─────────────────────────────────────────────────────────────────────────

    def calculate_momentum_for_stock(
        self, stock: Stock, calculation_date: datetime = None
    ) -> Optional[Decimal]:
        """
        Return the 12-1 momentum of one stock, or None if unpriceable.

        Each reference price is looked up in the local database first and
        fetched from the Massive API only on a miss.

        Parameters
        ----------
        stock : Stock
            Stock to score.
        calculation_date : datetime, optional
            Scoring date. Defaults to today.

        Returns
        -------
        Decimal or None
            (P_1m - P_12m) / P_12m, e.g. Decimal('0.25') for +25 %.
            None if either price is missing or P_12m is not positive.
        """
        if calculation_date is None:
            calculation_date = timezone.now().date()

        # Calendar-day approximations of 12 and 1 months; weekends and
        # holidays are absorbed by the lookup tolerance.
        twelve_months_ago = calculation_date - timedelta(days=365)
        one_month_ago = calculation_date - timedelta(days=30)

        try:
            # DB first: free and not rate limited.
            price_12m = self._get_price_from_db(
                stock, twelve_months_ago, tolerance_days=7
            )
            price_1m = self._get_price_from_db(stock, one_month_ago, tolerance_days=7)

            # API fallback costs one request per missing price against the
            # 5 req/min free-tier budget.
            if not price_12m:
                price_12m = self._get_price_from_api(stock.ticker, twelve_months_ago)

            if not price_1m:
                price_1m = self._get_price_from_api(stock.ticker, one_month_ago)

            # Guard against division by zero and corrupt non-positive
            # prices.
            if price_12m and price_1m and price_12m > 0:
                momentum = (price_1m - price_12m) / price_12m
                return Decimal(str(momentum))

            logger.warning(
                f"Could not calculate momentum for {stock.ticker}: "
                f"price_12m={price_12m}, price_1m={price_1m}"
            )
            return None

        except ValueError:
            logger.error(
                f"Error calculating momentum for {stock.ticker}: Invalid price data"
            )
            return None

    def _get_price_from_db(
        self, stock: Stock, target_date: datetime, tolerance_days: int = 7
    ) -> Optional[Decimal]:
        start_date = target_date - timedelta(days=tolerance_days)
        end_date = target_date + timedelta(days=tolerance_days)

        # NOTE: picks the earliest bar in the +/- tolerance window, not the
        # one nearest target_date, so the price can be up to 7 days early.
        price_data = (
            stock.price_data.filter(date__gte=start_date, date__lte=end_date)
            .order_by("date")
            .first()
        )

        return price_data.close if price_data else None

    def _get_price_from_api(
        self, ticker: str, target_date: datetime
    ) -> Optional[Decimal]:
        try:
            # Massive REST, daily bars: close nearest target_date within
            # the client's default 7-day tolerance.
            price = self.massive_client.get_price_on_date(ticker, target_date)
            # Massive returns float; Decimal(str()) keeps the printed value
            # instead of its binary approximation.
            return Decimal(str(price)) if price else None
        except (ValueError, TypeError):
            logger.error(
                f"Error fetching price from API for {ticker}: Invalid API response"
            )
            return None

    # ─────────────────────────────────────────────────────────────────────────
    # Universe-wide scoring
    # ─────────────────────────────────────────────────────────────────────────

    def calculate_momentum_scores_bulk(
        self, stock_list: List[Stock] = None, calculation_date: datetime = None
    ) -> List[MomentumScore]:
        """
        Score the whole universe from one bulk fetch and persist results.

        Primary path: Massive grouped-daily endpoint via
        fetch_bulk_momentum_data, about two requests for any universe
        size. If that raises, falls back to per-ticker scoring.

        Parameters
        ----------
        stock_list : list of Stock, optional
            Stocks to score. Defaults to every active Stock.
        calculation_date : datetime, optional
            Scoring date. Defaults to today.

        Returns
        -------
        list of MomentumScore
            One saved row per stock that had both reference prices.
            Unpriceable stocks are logged and omitted.
        """
        if calculation_date is None:
            calculation_date = timezone.now().date()

        if stock_list is None:
            stock_list = Stock.objects.filter(is_active=True)

        momentum_scores = []
        tickers = [stock.ticker for stock in stock_list]

        logger.info(
            f"Calculating momentum scores for {len(stock_list)} stocks using bulk API fetch"
        )

        try:
            # Shape: {ticker: {"price_12m": float | None,
            #                  "price_1m": float | None}}
            bulk_momentum_data = self.massive_client.fetch_bulk_momentum_data(
                tickers=tickers, calculation_date=calculation_date
            )

            logger.info(
                f"Successfully fetched bulk momentum data for {len(bulk_momentum_data)} stocks"
            )

            for stock in stock_list:
                # Per-stock try: one bad ticker must not abort the
                # universe-wide run.
                try:
                    ticker_data = bulk_momentum_data.get(stock.ticker, {})
                    price_12m = ticker_data.get("price_12m")
                    price_1m = ticker_data.get("price_1m")

                    if price_12m and price_1m and price_12m > 0:
                        momentum = (price_1m - price_12m) / price_12m
                        momentum_decimal = Decimal(str(momentum))

                        # Upsert keyed on (stock, calculation_date) makes
                        # re-running a rebalance for the same day
                        # idempotent.
                        momentum_score, created = (
                            MomentumScore.objects.update_or_create(
                                stock=stock,
                                calculation_date=calculation_date,
                                defaults={
                                    "momentum_score": momentum_decimal,
                                    "period_start": calculation_date
                                    - timedelta(days=365),
                                    "period_end": calculation_date - timedelta(days=30),
                                },
                            )
                        )
                        momentum_scores.append(momentum_score)

                        action = "Created" if created else "Updated"
                        logger.info(
                            f"{action} momentum score for {stock.ticker}: {momentum_decimal:.6f}"
                        )
                    else:
                        logger.warning(
                            f"Could not calculate momentum for {stock.ticker}: "
                            f"price_12m={price_12m}, price_1m={price_1m}"
                        )

                except (ValueError, TypeError) as e:
                    logger.error(
                        f"Error processing momentum for {stock.ticker}: {str(e)}"
                    )

            logger.info(
                f"Processed {len(momentum_scores)} momentum scores successfully"
            )

        except Exception as e:
            logger.error(f"Error in bulk momentum calculation: {str(e)}")
            # Fallback: per-ticker path, much slower (roughly N/5 minutes
            # for N tickers at the free-tier rate limit).
            logger.info("Falling back to individual API calls...")
            return self._calculate_momentum_scores_individual(
                stock_list, calculation_date
            )

        return momentum_scores

    def _calculate_momentum_scores_individual(
        self, stock_list: List[Stock], calculation_date: datetime
    ) -> List[MomentumScore]:
        """
        Fallback method using individual API calls with enhanced rate limiting
        """
        momentum_scores = []

        logger.info(
            f"Using individual API calls for {len(stock_list)} stocks with rate limiting"
        )

        for i, stock in enumerate(stock_list):
            try:
                momentum = self.calculate_momentum_for_stock(stock, calculation_date)

                if momentum is not None:
                    # Same idempotent upsert as the bulk path.
                    momentum_score, created = MomentumScore.objects.update_or_create(
                        stock=stock,
                        calculation_date=calculation_date,
                        defaults={
                            "momentum_score": momentum,
                            "period_start": calculation_date - timedelta(days=365),
                            "period_end": calculation_date - timedelta(days=30),
                        },
                    )
                    momentum_scores.append(momentum_score)

                    if created:
                        logger.info(
                            f"Created momentum score for {stock.ticker}: {momentum}"
                        )
                    else:
                        logger.info(
                            f"Updated momentum score for {stock.ticker}: {momentum}"
                        )

                if (i + 1) % 5 == 0:
                    logger.info(f"Processed {i + 1}/{len(stock_list)} stocks")

                # Extra throttle on top of the client's own rate limiter;
                # skipped after the last stock.
                if (i + 1) % 3 == 0 and i + 1 < len(stock_list):
                    time.sleep(2)

            except Exception as e:
                logger.error(f"Error processing {stock.ticker}: {str(e)}")

        return momentum_scores

    # ─────────────────────────────────────────────────────────────────────────
    # Ranking and quintile selection
    # ─────────────────────────────────────────────────────────────────────────

    def rank_stocks_by_momentum(
        self, calculation_date: datetime = None
    ) -> List[MomentumScore]:
        """
        Assign rank and quintile to every score for a date.

        Quintiles are equal-count buckets by rank: quintile_size = N // 5,
        and the remainder N % 5 falls into quintile 5. Rank 1 is the
        highest momentum.

        Parameters
        ----------
        calculation_date : datetime, optional
            Date whose scores are ranked. Defaults to today.

        Returns
        -------
        QuerySet of MomentumScore
            All scores for the date, highest momentum first.
        """
        if calculation_date is None:
            calculation_date = timezone.now().date()

        # Writes rank, quintile and is_top_quintile on each row; the
        # quintile queries below depend on these stored fields.
        MomentumScore.calculate_quintiles_for_date(calculation_date)

        return MomentumScore.objects.filter(calculation_date=calculation_date).order_by(
            "-momentum_score"
        )

    def get_top_quintile_stocks(self, calculation_date: datetime = None) -> List[Stock]:
        """
        Return quintile-1 stocks (BUY candidates), strongest first.

        Parameters
        ----------
        calculation_date : datetime, optional
            Date whose quintiles are read. Defaults to today.

        Returns
        -------
        list of Stock
            Empty if rank_stocks_by_momentum has not run for the date.
        """
        if calculation_date is None:
            calculation_date = timezone.now().date()

        # select_related avoids one extra query per score when .stock is
        # read below.
        momentum_scores = (
            MomentumScore.objects.filter(
                calculation_date=calculation_date, is_top_quintile=True
            )
            .select_related("stock")
            .order_by("-momentum_score")
        )

        return [score.stock for score in momentum_scores]

    def get_bottom_quintile_stocks(
        self, calculation_date: datetime = None
    ) -> List[Stock]:
        """
        Return quintile-5 stocks (SELL candidates), weakest first.

        Parameters
        ----------
        calculation_date : datetime, optional
            Date whose quintiles are read. Defaults to today.

        Returns
        -------
        list of Stock
            Empty if rank_stocks_by_momentum has not run for the date.
        """
        if calculation_date is None:
            calculation_date = timezone.now().date()

        momentum_scores = (
            MomentumScore.objects.filter(calculation_date=calculation_date, quintile=5)
            .select_related("stock")
            .order_by("momentum_score")
        )

        return [score.stock for score in momentum_scores]

    # ─────────────────────────────────────────────────────────────────────────
    # Universe and price-history maintenance
    # ─────────────────────────────────────────────────────────────────────────

    def update_stock_universe(self, tickers: List[str] = None) -> List[Stock]:
        """
        Make sure every ticker in the universe has a Stock row.

        Existing stocks are left untouched; nothing is deactivated, so
        names that leave the index stay in the universe.

        Parameters
        ----------
        tickers : list of str, optional
            Tickers to register. Defaults to the S&P 500 constituents.

        Returns
        -------
        list of Stock
            One Stock per input ticker, in input order.
        """
        if tickers is None:
            # FIXME(sp500-tickers): MassiveClient has no get_sp500_tickers
            # method, so this default path raises AttributeError.
            tickers = self.massive_client.get_sp500_tickers()

        stocks = []
        for ticker in tickers:
            # Ticker doubles as the name until real metadata is loaded.
            stock, created = Stock.objects.get_or_create(
                ticker=ticker, defaults={"name": ticker, "is_active": True}
            )
            stocks.append(stock)

            if created:
                logger.info(f"Added new stock: {ticker}")

        return stocks

    def backfill_price_data(self, stock: Stock, days_back: int = 420) -> int:
        """
        Store missing daily bars so momentum can be computed from the DB.

        The 420-day default covers the 365-day lookback plus the 7-day
        lookup tolerance, with margin for holidays.

        Parameters
        ----------
        stock : Stock
            Stock whose history is backfilled.
        days_back : int, default 420
            Calendar days of history to request, ending today.

        Returns
        -------
        int
            Number of new PriceData rows inserted; 0 on a parse error.
        """
        end_date = timezone.now().date()
        start_date = end_date - timedelta(days=days_back)

        try:
            # Dates already stored are skipped so a re-run inserts only
            # the gaps.
            existing_data = stock.price_data.filter(date__gte=start_date).values_list(
                "date", flat=True
            )

            # Massive REST, daily OHLCV. Bars are split/dividend-adjusted
            # by default, which is why close is also stored as
            # adjusted_close below.
            api_data = self.massive_client.fetch_stock_data(
                ticker=stock.ticker,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
            )

            new_records = 0
            for data_point in api_data:
                if data_point["date"] not in existing_data:
                    PriceData.objects.create(
                        stock=stock,
                        date=data_point["date"],
                        open_price=Decimal(str(data_point["open"])),
                        high=Decimal(str(data_point["high"])),
                        low=Decimal(str(data_point["low"])),
                        close=Decimal(str(data_point["close"])),
                        volume=data_point["volume"],
                        adjusted_close=Decimal(str(data_point["close"])),
                    )
                    new_records += 1

            logger.info(f"Backfilled {new_records} price records for {stock.ticker}")
            return new_records

        except (ValueError, TypeError):
            logger.error(
                f"Error backfilling data for {stock.ticker}: Invalid price data format"
            )
            return 0

    # ─────────────────────────────────────────────────────────────────────────
    # Diagnostics
    # ─────────────────────────────────────────────────────────────────────────

    def get_momentum_statistics(self, calculation_date: datetime = None) -> Dict:
        """
        Return the cross-sectional distribution of scores for a date.

        Parameters
        ----------
        calculation_date : datetime, optional
            Date whose scores are summarised. Defaults to today.

        Returns
        -------
        dict
            Count, mean, median, population std, min and max of the
            scores, plus the 80th and 20th percentiles as approximate
            quintile cut-offs. Empty dict if no scores exist.
        """
        if calculation_date is None:
            calculation_date = timezone.now().date()

        scores = MomentumScore.objects.filter(calculation_date=calculation_date)

        if not scores.exists():
            return {}

        momentum_values = [float(score.momentum_score) for score in scores]

        # Percentile thresholds interpolate, while stored quintiles are
        # assigned by rank count, so the two can disagree at the edges.
        return {
            "total_stocks": len(momentum_values),
            "mean_momentum": np.mean(momentum_values),
            "median_momentum": np.median(momentum_values),
            "std_momentum": np.std(momentum_values),
            "min_momentum": min(momentum_values),
            "max_momentum": max(momentum_values),
            "top_quintile_threshold": np.percentile(momentum_values, 80),
            "bottom_quintile_threshold": np.percentile(momentum_values, 20),
        }

    def validate_momentum_calculation(
        self, stock: Stock, calculation_date: datetime = None
    ) -> Dict:
        """
        Report local data coverage and the computed score for one stock.

        Parameters
        ----------
        stock : Stock
            Stock to check.
        calculation_date : datetime, optional
            Scoring date. Defaults to today.

        Returns
        -------
        dict
            Ticker, date, number of stored bars in the lookback window,
            a has_sufficient_data flag and the momentum score (None if
            unpriceable). Adds 'error' if scoring raised.
        """
        if calculation_date is None:
            calculation_date = timezone.now().date()

        twelve_months_ago = calculation_date - timedelta(days=365)
        one_month_ago = calculation_date - timedelta(days=30)

        validation_result = {
            "stock": stock.ticker,
            "calculation_date": calculation_date,
            "has_sufficient_data": False,
            "price_12m": None,
            "price_1m": None,
            "momentum_score": None,
            "data_points_available": 0,
        }

        data_count = stock.price_data.filter(
            date__gte=twelve_months_ago, date__lte=calculation_date
        ).count()

        # FIXME(data-threshold): 365 calendar days hold only ~252 trading
        # days, so the 280-bar threshold can never be met and this flag
        # is always False.
        validation_result["data_points_available"] = data_count
        validation_result["has_sufficient_data"] = data_count >= 280

        # May hit the Massive API when the DB lacks a reference price.
        try:
            validation_result["momentum_score"] = self.calculate_momentum_for_stock(
                stock, calculation_date
            )
        except (ValueError, TypeError) as e:
            validation_result["error"] = str(e)

        return validation_result


def get_momentum_calculator() -> MomentumCalculator:
    """
    Return a fresh MomentumCalculator.

    Returns
    -------
    MomentumCalculator
        New instance with its own Massive client.
    """
    return MomentumCalculator()
