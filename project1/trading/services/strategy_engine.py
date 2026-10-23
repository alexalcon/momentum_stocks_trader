"""
strategy_engine.py -- Momentum rebalance orchestrator (Component: strategy)

Runs one end-to-end rebalance of a Portfolio: refresh the stock universe,
score 12-1 momentum, rank into quintiles, turn quintile membership into
BUY/SELL TradingSignals, and hand those signals to the SnapTrade executor.
Every run is recorded as a RebalanceEvent.

Invariants:
    - Long-only. BUY candidates come from quintile 1 (highest momentum);
      SELL candidates are held positions that fell to quintile 5.
    - Positions in quintiles 2-4 are held, not traded. This band avoids
      churning names that drift just below the top quintile.
    - SELL orders are submitted before BUY orders so sale proceeds can
      fund the new positions.
    - New positions are equal-weight; there is no conviction or
      volatility scaling.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from decimal import Decimal
import logging

from django.conf import settings
from django.utils import timezone

from trading.services.snaptrade_client import get_trading_executor
from trading.services.momentum_calculator import get_momentum_calculator
from trading.models import Stock, TradingSignal, RebalanceEvent
from portfolio.models import Portfolio, Position

logger = logging.getLogger(__name__)


class MomentumTradingStrategy:
    """
    Cross-sectional momentum strategy bound to a single Portfolio.

    Pipeline stage: post-scoring, pre-execution. Owns signal generation
    and delegates scoring to MomentumCalculator and order placement to
    TradingExecutor.
    Lifecycle: one instance per portfolio per rebalance run.
    """

    def __init__(self, portfolio: Portfolio):
        self.portfolio = portfolio
        self.trading_executor = get_trading_executor()
        self.momentum_calculator = get_momentum_calculator()
        # One of {'weekly', 'monthly'}; any other value disables the
        # schedule check in should_rebalance.
        self.rebalance_frequency = getattr(settings, "REBALANCE_FREQUENCY", "weekly")

    # ─────────────────────────────────────────────────────────────────────────
    # Rebalance orchestration
    # ─────────────────────────────────────────────────────────────────────────

    def should_rebalance(self) -> bool:
        """
        Return True when enough time has passed since the last rebalance.

        Only COMPLETED events count, so a run that crashed mid-way does not
        push the schedule back. The lookup is global, not per portfolio.

        Returns
        -------
        bool
            True if no rebalance has completed yet, or if at least 7
            (weekly) or 30 (monthly) days have passed since the last one.
            False for an unrecognised rebalance_frequency.
        """
        # look up the most recent completed rebalance event
        last_rebalance = (
            RebalanceEvent.objects.filter(execution_status="COMPLETED")
            .order_by("-date")
            .first()
        )

        # no rebalance has been completed yet
        if not last_rebalance:
            return True

        days_since_rebalance = (timezone.now().date() - last_rebalance.date).days

        if self.rebalance_frequency == "weekly":
            return days_since_rebalance >= 7
        elif self.rebalance_frequency == "monthly":
            return days_since_rebalance >= 30
        else:
            return False

    def execute_rebalance(self, calculation_date: datetime = None) -> RebalanceEvent:
        """
        Run the full score -> rank -> signal -> trade pipeline once.

        Mutates rebalance state: a new RebalanceEvent goes IN_PROGRESS ->
        COMPLETED. An exception at any step propagates and leaves the event
        IN_PROGRESS; nothing marks it FAILED.

        Parameters
        ----------
        calculation_date : datetime, optional
            Date the momentum scores are computed for. Defaults to today.

        Returns
        -------
        RebalanceEvent
            The completed event, with signal counts and the portfolio
            value after trading.
        """
        if calculation_date is None:
            calculation_date = timezone.now().date()

        logger.info(f"Starting rebalance for {calculation_date}")

        # Audit row is created first so every attempt is visible, even
        # one that fails before any trade is placed.
        rebalance_event = RebalanceEvent.objects.create(
            date=calculation_date,
            total_stocks_analyzed=0,
            buy_signals_generated=0,
            sell_signals_generated=0,
            execution_status="IN_PROGRESS",
        )

        # Step 1: refresh the universe and persist one MomentumScore per
        # stock that has both reference prices.
        stocks = self.momentum_calculator.update_stock_universe()
        momentum_scores = self.momentum_calculator.calculate_momentum_scores_bulk(
            stocks, calculation_date
        )

        rebalance_event.total_stocks_analyzed = len(momentum_scores)

        # Step 2: assign rank and quintile to every score for this date.
        # Called for its side effect; the get_*_quintile_stocks queries in
        # step 3 read the stored quintile fields.
        ranked_scores = self.momentum_calculator.rank_stocks_by_momentum(
            calculation_date
        )

        # Step 3: quintile membership -> BUY/SELL signals.
        buy_signals, sell_signals = self.generate_trading_signals(calculation_date)

        rebalance_event.buy_signals_generated = len(buy_signals)
        rebalance_event.sell_signals_generated = len(sell_signals)
        rebalance_event.save()

        # Step 4: submit orders, sells first.
        self.execute_trading_signals(buy_signals, sell_signals, rebalance_event)

        # Step 5: market orders may still be unfilled here, so this value
        # reflects submitted, not settled, trades.
        self.portfolio.calculate_total_value()
        self.portfolio.save()

        rebalance_event.total_portfolio_value = self.portfolio.total_value
        rebalance_event.execution_status = "COMPLETED"
        rebalance_event.completed_at = timezone.now()
        rebalance_event.save()

        logger.info(f"Rebalance completed successfully for {calculation_date}")

        return rebalance_event

    # ─────────────────────────────────────────────────────────────────────────
    # Signal generation and execution
    # ─────────────────────────────────────────────────────────────────────────

    def generate_trading_signals(
        self, calculation_date: datetime = None
    ) -> Tuple[List[TradingSignal], List[TradingSignal]]:
        """
        Turn quintile membership into persisted BUY and SELL signals.

        Signal rules:
            - SELL: a held stock whose score is in quintile 5.
            - BUY: a quintile-1 stock not already held, sized at an equal
              share of the cash available after the 5 % buffer.
            - Held stocks in quintiles 2-4 get no signal (hold band).

        Requires rank_stocks_by_momentum to have run for calculation_date.

        Parameters
        ----------
        calculation_date : datetime, optional
            Date whose quintiles are read. Defaults to today.

        Returns
        -------
        tuple of (list of TradingSignal, list of TradingSignal)
            (buy_signals, sell_signals), already saved to the database.
        """
        if calculation_date is None:
            calculation_date = timezone.now().date()

        current_positions = {
            pos.stock: pos for pos in self.portfolio.get_current_positions()
        }

        # Quintile 1, ordered strongest momentum first.
        top_quintile_stocks = self.momentum_calculator.get_top_quintile_stocks(
            calculation_date
        )

        # Quintile 5, ordered weakest momentum first.
        bottom_quintile_stocks = self.momentum_calculator.get_bottom_quintile_stocks(
            calculation_date
        )

        buy_signals = []
        sell_signals = []

        # Long-only: a bottom-quintile stock is only actionable if held.
        for stock in bottom_quintile_stocks:
            if stock in current_positions:
                position = current_positions[stock]
                momentum_score = stock.momentum_scores.filter(
                    calculation_date=calculation_date
                ).first()

                # Full exit: the signal targets the whole position.
                sell_signal = TradingSignal.objects.create(
                    stock=stock,
                    signal_date=calculation_date,
                    signal_type="SELL",
                    momentum_score=momentum_score,
                    target_quantity=position.quantity,
                    target_value=position.current_value,
                    reason=f"Stock in bottom quintile (rank {momentum_score.rank if momentum_score else 'N/A'})",
                )
                sell_signals.append(sell_signal)

        # 95 % of current cash; sale proceeds are not included at this
        # point, so BUY target_value is a conservative estimate.
        available_cash = self.trading_executor.get_available_cash_for_trading(
            self.portfolio
        )

        # Existing holdings are not topped up back to equal weight.
        buy_candidates = [
            stock for stock in top_quintile_stocks if stock not in current_positions
        ]

        if buy_candidates and available_cash > 0:
            allocation_per_stock = available_cash / len(buy_candidates)

            for stock in buy_candidates:
                momentum_score = stock.momentum_scores.filter(
                    calculation_date=calculation_date
                ).first()

                buy_signal = TradingSignal.objects.create(
                    stock=stock,
                    signal_date=calculation_date,
                    signal_type="BUY",
                    momentum_score=momentum_score,
                    target_value=allocation_per_stock,
                    reason=f"Stock in top quintile (rank {momentum_score.rank if momentum_score else 'N/A'})",
                )
                buy_signals.append(buy_signal)

        logger.info(
            f"Generated {len(buy_signals)} buy signals and {len(sell_signals)} sell signals"
        )

        return buy_signals, sell_signals

    def execute_trading_signals(
        self,
        buy_signals: List[TradingSignal],
        sell_signals: List[TradingSignal],
        rebalance_event: RebalanceEvent,
    ):
        """
        Submit the signals as orders through the SnapTrade executor.

        Signals are flagged is_executed once their orders are submitted;
        that means "sent to the broker", not "filled".

        Parameters
        ----------
        buy_signals : list of TradingSignal
            BUY signals from generate_trading_signals.
        sell_signals : list of TradingSignal
            SELL signals from generate_trading_signals.
        rebalance_event : RebalanceEvent
            Current run. Accepted for traceability but not used yet.
        """
        # Sells go first so their proceeds can fund the buys.
        # FIXME(user-secret): user_secret is not passed, and the executor
        # raises ValueError without it, so this call cannot succeed.
        sell_stocks = [signal.stock for signal in sell_signals]
        sell_trades = self.trading_executor.execute_sell_orders(
            self.portfolio, sell_stocks
        )

        for signal in sell_signals:
            signal.is_executed = True
            signal.executed_at = timezone.now()
            signal.save()

        # Buying power = cash + expected sale proceeds at the last synced
        # price. The sells are market orders that may not have filled yet,
        # and the 5 % cash buffer is not applied here.
        total_sell_value = sum(
            signal.target_value for signal in sell_signals if signal.target_value
        )

        available_for_buying = self.portfolio.current_cash + total_sell_value

        # The executor re-splits this total equally, so BUY signal
        # target_value is informational only.
        buy_stocks = [signal.stock for signal in buy_signals]
        buy_trades = []
        if buy_stocks and available_for_buying > 0:
            buy_trades = self.trading_executor.execute_buy_orders(
                self.portfolio, buy_stocks, available_for_buying
            )

            for signal in buy_signals:
                signal.is_executed = True
                signal.executed_at = timezone.now()
                signal.save()

        logger.info(
            f"Executed {len(sell_trades)} sell orders and {len(buy_trades)} buy orders"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Reporting, validation and backtesting
    # ─────────────────────────────────────────────────────────────────────────

    def get_strategy_performance(self, days_back: int = 30) -> Dict:
        """
        Summarise strategy activity and return over a trailing window.

        Parameters
        ----------
        days_back : int, default 30
            Length of the trailing window in calendar days.

        Returns
        -------
        dict
            Counts of completed rebalances and executed BUY/SELL signals,
            current value, cash and open positions, and a 'returns' entry.
            'returns' is None when fewer than two PerformanceMetric rows
            exist in the window.
        """
        start_date = timezone.now().date() - timedelta(days=days_back)

        # Rebalance and signal counts are global, not filtered by
        # portfolio; only the PerformanceMetric query is portfolio-scoped.
        recent_rebalances = RebalanceEvent.objects.filter(
            date__gte=start_date, execution_status="COMPLETED"
        ).order_by("-date")

        recent_signals = TradingSignal.objects.filter(
            signal_date__gte=start_date, is_executed=True
        )

        performance_metrics = self.portfolio.performance_metrics.filter(
            date__gte=start_date
        ).order_by("-date")

        buy_signals_count = recent_signals.filter(signal_type="BUY").count()
        sell_signals_count = recent_signals.filter(signal_type="SELL").count()

        # Simple point-to-point return between the earliest and latest
        # snapshots: (V_end - V_start) / V_start * 100. Deposits and
        # withdrawals are not adjusted for.
        returns_data = None
        if performance_metrics.count() >= 2:
            latest = performance_metrics.first()
            earliest = performance_metrics.last()

            if earliest.total_value > 0:
                period_return = (
                    (latest.total_value - earliest.total_value) / earliest.total_value
                ) * 100

                returns_data = {
                    "period_return_percent": float(period_return),
                    "start_value": float(earliest.total_value),
                    "end_value": float(latest.total_value),
                    "start_date": earliest.date,
                    "end_date": latest.date,
                }

        return {
            "rebalance_events": recent_rebalances.count(),
            "buy_signals": buy_signals_count,
            "sell_signals": sell_signals_count,
            "current_portfolio_value": float(self.portfolio.total_value),
            "current_cash": float(self.portfolio.current_cash),
            "active_positions": self.portfolio.positions.filter(quantity__gt=0).count(),
            "returns": returns_data,
        }

    def validate_strategy_setup(self) -> Dict:
        """
        Pre-flight check of credentials and data before a rebalance.

        Issues make the setup invalid; warnings are reported but do not
        block a run.

        Returns
        -------
        dict
            {'is_valid': bool, 'issues': list of str,
            'warnings': list of str}.
        """
        validation_result = {"is_valid": True, "issues": [], "warnings": []}

        # Blocking: without a linked brokerage account no order can be
        # placed.
        if not self.portfolio.snaptrade_user_id:
            validation_result["issues"].append("Portfolio missing SnapTrade user ID")
            validation_result["is_valid"] = False

        if not self.portfolio.snaptrade_account_id:
            validation_result["issues"].append("Portfolio missing SnapTrade account ID")
            validation_result["is_valid"] = False

        # Blocking: app credentials for the data feed and the broker. Only
        # presence is checked, not that the keys actually work.
        if not settings.MASSIVE_API_KEY:
            validation_result["issues"].append("Massive API key not configured")
            validation_result["is_valid"] = False

        if not settings.SNAPTRADE_CLIENT_ID or not settings.SNAPTRADE_CLIENT_SECRET:
            validation_result["issues"].append(
                "SnapTrade API credentials not configured"
            )
            validation_result["is_valid"] = False

        # Non-blocking: execute_rebalance repopulates both.
        active_stocks = Stock.objects.filter(is_active=True).count()
        if active_stocks == 0:
            validation_result["warnings"].append("No active stocks in database")

        # Only today's scores are checked, so this warns on any day before
        # the first rebalance of the day.
        recent_scores = self.momentum_calculator.get_momentum_statistics()
        if not recent_scores:
            validation_result["warnings"].append("No recent momentum scores calculated")

        return validation_result

    def run_backtest(
        self,
        start_date: datetime,
        end_date: datetime,
        initial_capital: Decimal = Decimal("100000"),
    ) -> Dict:
        """
        Placeholder backtest; returns an empty result with an error note.

        Parameters
        ----------
        start_date : datetime
            First date of the simulated period.
        end_date : datetime
            Last date of the simulated period.
        initial_capital : Decimal, default Decimal('100000')
            Starting portfolio value for the simulation.

        Returns
        -------
        dict
            Result skeleton with final_value and total_return at 0 and
            'error' set to "Backtest implementation pending".
        """
        logger.info(f"Running backtest from {start_date} to {end_date}")

        results = {
            "start_date": start_date,
            "end_date": end_date,
            "initial_capital": float(initial_capital),
            "final_value": 0,
            "total_return": 0,
            "trades_executed": 0,
            "error": None,
        }

        # TODO(backtest): simulate the strategy from initial_capital:
        #   1. step through each rebalance date in [start_date, end_date];
        #   2. score momentum using only prices known on that date;
        #   3. apply the same quintile signal rules with simulated fills;
        #   4. record portfolio value after every rebalance.
        # Step 2 must avoid look-ahead bias, so it cannot reuse the live
        # MomentumScore rows.

        results["error"] = "Backtest implementation pending"

        return results


def get_strategy_engine(portfolio: Portfolio) -> MomentumTradingStrategy:
    """
    Return a strategy instance bound to the given portfolio.

    Parameters
    ----------
    portfolio : Portfolio
        Portfolio the strategy will rebalance.

    Returns
    -------
    MomentumTradingStrategy
        New instance with its own calculator and executor.
    """
    return MomentumTradingStrategy(portfolio)
