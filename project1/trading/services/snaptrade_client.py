"""
snaptrade_client.py -- Brokerage execution layer (Component: order_router)

Bridges the Django 'trading' models and the brokerage account linked through
SnapTrade. Responsibilities: sync broker-side positions and cash into the
local Portfolio, submit market BUY/SELL orders for a rebalance, and poll
order status until each Trade reaches a terminal state.

Invariants:
    - Every broker call requires the per-user SnapTrade secret; it is
      passed in by the caller and never persisted by this module.
    - A Trade row is written as PENDING before its order is submitted, so
      every broker order has a local audit record.
    - BUY orders are sized by notional dollar value (equal weight); SELL
      orders liquidate the whole-share part of the current position.
    - Order failures are re-raised so the UI surfaces broker errors rather
      than reporting a partial rebalance as successful.
"""

from datetime import datetime
from typing import List, Optional
from decimal import Decimal
import logging

from django.conf import settings
from snaptrade_client import SnapTrade, SnapTradeAuth

from trading.models import Stock
from portfolio.models import Trade
from portfolio.models import Portfolio
from portfolio.models import Position

logger = logging.getLogger(__name__)


class TradingExecutor:
    """
    Thin wrapper over the SnapTrade SDK that owns order submission and
    portfolio reconciliation for one or more linked brokerage accounts.

    Pipeline stage: post-strategy, execution and reconciliation.
    Lifecycle: stateless apart from the SDK client; a new instance per
    request (via get_trading_executor) is safe.
    """

    def __init__(self):
        # App-level credentials. Source: Django settings, populated from
        # the environment. Per-user secrets are supplied per call instead.
        self.snaptrade = SnapTrade(
            auth=SnapTradeAuth.commercial_api_key(
                consumer_key=settings.SNAPTRADE_CLIENT_SECRET,
                client_id=settings.SNAPTRADE_CLIENT_ID,
            ),
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Reconciliation -- broker positions and cash into the local Portfolio
    # ─────────────────────────────────────────────────────────────────────────

    def sync_portfolio_positions(
        self, portfolio: Portfolio, user_secret: str = None
    ) -> List[Position]:
        """
        Pull positions and cash from the broker and upsert them locally.

        The broker is treated as the source of truth: local Position rows
        are overwritten with broker quantities and prices. Positions that
        no longer exist at the broker are not deleted here.

        Parameters
        ----------
        portfolio : Portfolio
            Portfolio linked to a SnapTrade user and account.
        user_secret : str, optional
            Per-user SnapTrade secret. Required despite the default.

        Returns
        -------
        list of Position
            Positions created or updated during this sync.

        Raises
        ------
        ValueError
            If the portfolio is not linked to SnapTrade, the user secret
            is missing, or the broker payload cannot be parsed.
        """
        if not portfolio.snaptrade_user_id or not portfolio.snaptrade_account_id:
            raise ValueError("Portfolio must have SnapTrade user and account IDs")

        if not user_secret:
            raise ValueError("SnapTrade user secret is required for API operations")

        try:
            # SnapTrade REST: GET /accounts/{accountId}/positions
            positions_response = (
                self.snaptrade.account_information.get_user_account_positions(
                    user_id=portfolio.snaptrade_user_id,
                    user_secret=user_secret,
                    account_id=portfolio.snaptrade_account_id,
                )
            )

            # The SDK wraps the payload in an ApiResponseFor200 object.
            positions_data = positions_response.body
            synced_positions = []

            for pos_data in positions_data:
                # Ticker lives at symbol -> symbol -> raw_symbol in the
                # SnapTrade position schema.
                symbol = (
                    pos_data.get("symbol", {}).get("symbol", {}).get("raw_symbol", "")
                )
                quantity = float(pos_data.get("units", 0))
                average_cost = float(pos_data.get("average_purchase_price", 0))
                current_price = float(pos_data.get("price", 0))

                # Long-only system: skip unparseable rows and short or
                # zero positions rather than mirroring them locally.
                if not symbol or quantity <= 0:
                    continue

                # Broker may hold tickers outside the local universe;
                # register them so the position has a Stock to point to.
                stock, _ = Stock.objects.get_or_create(
                    ticker=symbol, defaults={"name": symbol, "is_active": True}
                )

                # Decimal(str(...)) avoids binary-float artifacts in money
                # fields.
                position, created = Position.objects.update_or_create(
                    portfolio=portfolio,
                    stock=stock,
                    defaults={
                        "quantity": quantity,
                        "average_cost": Decimal(str(average_cost)),
                        "current_price": Decimal(str(current_price)),
                    },
                )

                position.update_current_value()
                position.save()
                synced_positions.append(position)

                logger.info(
                    f"{'Created' if created else 'Updated'} position: {symbol} - {quantity} shares"
                )

            # SnapTrade REST: GET /accounts/{accountId}/balances
            balance_response = (
                self.snaptrade.account_information.get_user_account_balance(
                    user_id=portfolio.snaptrade_user_id,
                    user_secret=user_secret,
                    account_id=portfolio.snaptrade_account_id,
                )
            )

            balance_data = balance_response.body
            if (
                balance_data
                and isinstance(balance_data, list)
                and len(balance_data) > 0
            ):
                # One entry per currency; the first is assumed to be the
                # USD balance for a US equities account.
                balance_item = balance_data[0]
                cash_balance = float(balance_item.get("cash", 0))
                portfolio.current_cash = Decimal(str(cash_balance))
                portfolio.calculate_total_value()
                portfolio.save()
                logger.info(f"Updated portfolio cash balance to ${cash_balance}")

            return synced_positions

        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Error syncing portfolio positions: {str(e)}")
            raise

    # ─────────────────────────────────────────────────────────────────────────
    # Order execution -- rebalance BUY and SELL legs
    # ─────────────────────────────────────────────────────────────────────────

    def execute_buy_orders(
        self,
        portfolio: Portfolio,
        buy_list: List[Stock],
        total_value: Decimal,
        user_secret: str = None,
    ) -> List[Trade]:
        """
        Submit equal-weight notional market BUY orders for each stock.

        Orders are submitted sequentially. The first failure aborts the
        loop, so earlier orders stay live and later stocks are skipped;
        the failing Trade row is left in PENDING.

        Parameters
        ----------
        portfolio : Portfolio
            Portfolio linked to a SnapTrade user and account.
        buy_list : list of Stock
            Stocks selected by the strategy for this rebalance.
        total_value : Decimal
            Dollar amount to deploy across all of buy_list.
        user_secret : str, optional
            Per-user SnapTrade secret. Required despite the default.

        Returns
        -------
        list of Trade
            Trades in SUBMITTED state, one per order placed.

        Raises
        ------
        ValueError
            If the user secret is missing.
        Exception
            If any order submission fails; wraps the broker error.
        """
        if not buy_list:
            return []

        if not user_secret:
            raise ValueError("SnapTrade user secret is required for trading operations")

        # Equal-weight sizing: no conviction or volatility scaling.
        allocation_per_stock = total_value / len(buy_list)
        executed_trades = []

        for stock in buy_list:
            try:
                # Record intent before submission so a broker-side order
                # can never exist without a local audit row.
                trade = Trade.objects.create(
                    portfolio=portfolio,
                    stock=stock,
                    trade_type="BUY",
                    quantity=0,  # unknown until fill; broker sizes notional
                    price=allocation_per_stock,  # dollar allocation, not quote
                    status="PENDING",
                )

                # SnapTrade REST: POST /trade/place (force order, no
                # impact preview). Notional sizing lets the broker buy
                # fractional shares for the exact dollar allocation.
                order_response = self.snaptrade.trading.place_force_order(
                    user_id=portfolio.snaptrade_user_id,
                    user_secret=user_secret,
                    body={
                        "account_id": portfolio.snaptrade_account_id,
                        "action": "BUY",
                        "order_type": "Market",
                        "price": None,  # market order carries no limit
                        "stop": None,
                        "time_in_force": "Day",
                        "notional_value": float(
                            allocation_per_stock
                        ),
                        "symbol": stock.ticker,
                    },
                )

                # Tolerate SDK versions that return the payload unwrapped.
                order_result = (
                    order_response.body
                    if hasattr(order_response, "body")
                    else order_response
                )

                # Broker order id is the key used later by
                # update_trade_status to poll for fills.
                trade.external_order_id = (
                    order_result.get("id", "") if isinstance(order_result, dict) else ""
                )
                trade.status = "SUBMITTED"
                trade.submitted_at = datetime.now()
                trade.save()

                executed_trades.append(trade)
                logger.info(
                    f"Submitted buy order: ${float(allocation_per_stock)} notional value of {stock.ticker}"
                )

            except Exception as e:
                logger.error(f"Error executing buy order for {stock.ticker}: {str(e)}")
                # Fail loudly so the UI shows the broker error instead of
                # a silently partial rebalance.
                raise Exception(f"Buy order failed for {stock.ticker}: {str(e)}")

        return executed_trades

    def execute_sell_orders(
        self, portfolio: Portfolio, sell_list: List[Stock], user_secret: str = None
    ) -> List[Trade]:
        """
        Submit market SELL orders that liquidate each listed position.

        Stocks without a positive local position are skipped. As with
        BUY orders, the first failure aborts the remaining sells.

        Parameters
        ----------
        portfolio : Portfolio
            Portfolio linked to a SnapTrade user and account.
        sell_list : list of Stock
            Stocks the strategy has dropped from the target holdings.
        user_secret : str, optional
            Per-user SnapTrade secret. Required despite the default.

        Returns
        -------
        list of Trade
            Trades in SUBMITTED state, one per order placed.

        Raises
        ------
        ValueError
            If the user secret is missing.
        Exception
            If any order submission fails; wraps the broker error.
        """
        if not sell_list:
            return []

        if not user_secret:
            raise ValueError("SnapTrade user secret is required for trading operations")

        executed_trades = []
        # Local positions drive sell quantities; run
        # sync_portfolio_positions first so they match the broker.
        current_positions = {
            pos.stock.ticker: pos for pos in portfolio.get_current_positions()
        }

        for stock in sell_list:
            if stock.ticker not in current_positions:
                continue

            position = current_positions[stock.ticker]
            if position.quantity <= 0:
                continue

            try:
                # Record intent before submission so a broker-side order
                # can never exist without a local audit row.
                trade = Trade.objects.create(
                    portfolio=portfolio,
                    stock=stock,
                    trade_type="SELL",
                    quantity=position.quantity,
                    price=position.current_price,  # last synced, not fill
                    status="PENDING",
                )

                # SnapTrade REST: POST /trade/place (force order, no
                # impact preview). Units are truncated to whole shares, so
                # any fractional remainder from notional buys stays held.
                order_response = self.snaptrade.trading.place_force_order(
                    user_id=portfolio.snaptrade_user_id,
                    user_secret=user_secret,
                    body={
                        "account_id": portfolio.snaptrade_account_id,
                        "action": "SELL",
                        "order_type": "Market",
                        "price": None,  # market order carries no limit
                        "stop": None,
                        "time_in_force": "Day",
                        "units": int(position.quantity),
                        "symbol": stock.ticker,
                    },
                )

                # Tolerate SDK versions that return the payload unwrapped.
                order_result = (
                    order_response.body
                    if hasattr(order_response, "body")
                    else order_response
                )

                # Broker order id is the key used later by
                # update_trade_status to poll for fills.
                trade.external_order_id = (
                    order_result.get("id", "") if isinstance(order_result, dict) else ""
                )
                trade.status = "SUBMITTED"
                trade.submitted_at = datetime.now()
                trade.save()

                executed_trades.append(trade)
                logger.info(
                    f"Submitted sell order: {position.quantity} shares of {stock.ticker}"
                )

            except Exception as e:
                logger.error(f"Error executing sell order for {stock.ticker}: {str(e)}")
                # Fail loudly so the UI shows the broker error instead of
                # a silently partial rebalance.
                raise Exception(f"Sell order failed for {stock.ticker}: {str(e)}")

        return executed_trades

    # ─────────────────────────────────────────────────────────────────────────
    # Order lifecycle -- status polling and fill application
    # ─────────────────────────────────────────────────────────────────────────

    def update_trade_status(self, trade: Trade, user_secret: str = None) -> bool:
        """
        Refresh a Trade from the broker and apply its fill on completion.

        Mutates trade state: SUBMITTED -> broker-reported state. On FILLED,
        the linked Position is updated via Trade.update_position().

        Parameters
        ----------
        trade : Trade
            Trade previously submitted by this executor.
        user_secret : str, optional
            Per-user SnapTrade secret. Required despite the default.

        Returns
        -------
        bool
            True if the broker was queried and the trade saved; False if
            the trade is terminal, has no broker id, or parsing failed.

        Raises
        ------
        ValueError
            If the user secret is missing for a pollable trade.
        """
        # Terminal states never change again, and a trade without a broker
        # id was never accepted, so there is nothing to poll.
        if not trade.external_order_id or trade.status in [
            "FILLED",
            "CANCELLED",
            "REJECTED",
        ]:
            return False

        if not user_secret:
            raise ValueError(
                "SnapTrade user secret is required for trade status updates"
            )

        try:
            order_status = self.snaptrade.trading.get_order_status(
                user_id=trade.portfolio.snaptrade_user_id,
                user_secret=user_secret,
                account_id=trade.portfolio.snaptrade_account_id,
                order_id=trade.external_order_id,
            )

            old_status = trade.status
            trade.status = order_status.get("state", trade.status)

            # Notional BUYs are created with quantity=0; the real share
            # count and price are only known from the broker fill.
            if order_status.get("filled_units"):
                trade.filled_quantity = float(order_status["filled_units"])

            if order_status.get("executed_price"):
                trade.filled_price = Decimal(str(order_status["executed_price"]))

            if trade.status == "FILLED":
                trade.filled_at = datetime.now()
                trade.update_position()

            trade.save()

            if old_status != trade.status:
                logger.info(
                    f"Trade {trade.id} status updated: {old_status} -> {trade.status}"
                )

            return True

        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Error updating trade status for trade {trade.id}: {str(e)}")
            return False

    # ─────────────────────────────────────────────────────────────────────────
    # Helpers -- pricing and cash allocation
    # ─────────────────────────────────────────────────────────────────────────

    def _get_current_stock_price(self, ticker: str) -> Optional[Decimal]:
        # TODO(realtime-feed): replace the Massive daily price lookup with a
        # real-time quote before using this for order sizing.
        try:
            # Local import keeps the Massive client out of module load for
            # callers that only need order execution.
            from trading.services.massive_client import get_massive_client

            massive_client = get_massive_client()
            price = massive_client.get_price_on_date(ticker, datetime.now())
            return Decimal(str(price)) if price else None
        except (ValueError, TypeError) as e:
            logger.error(f"Error getting current price for {ticker}: {str(e)}")
            return None

    def get_available_cash_for_trading(self, portfolio: Portfolio) -> Decimal:
        """
        Return the cash that a rebalance may deploy.

        Holds back 5 % of cash to absorb slippage on market orders and
        price drift between sizing and fill, avoiding rejected BUYs.

        Parameters
        ----------
        portfolio : Portfolio
            Portfolio whose current_cash was refreshed by the last sync.

        Returns
        -------
        Decimal
            95 % of portfolio.current_cash.
        """
        return portfolio.current_cash * Decimal("0.95")


def get_trading_executor() -> TradingExecutor:
    """
    Return a fresh TradingExecutor configured from Django settings.

    Returns
    -------
    TradingExecutor
        New executor instance; callers should not share it across users.
    """
    return TradingExecutor()
