"""Generate realistic trade execution data for securities."""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta

import pandas as pd

from data_generation.models.schemas import Currency, Exchange, OrderSide, TradeStatus


class TradeGenerator:
    """Generates trade execution records linked to orders."""

    EXECUTION_VENUES = [
        "NYSE_MATCHING_ENGINE",
        "NASDAQ_CROSS",
        "BATS_BZX",
        "IEX_DPEG",
        "ARCA_MIDPOINT",
        "DARK_POOL_SIGMA",
        "DARK_POOL_CROSSFINDER",
        "INTERNAL_CROSS",
    ]
    TRADER_IDS = [f"TRADER-{i:04d}" for i in range(1, 101)]
    PORTFOLIO_IDS = [f"PORT-{i:04d}" for i in range(1, 51)]

    def __init__(self, instruments_df: pd.DataFrame, num_trades: int = 100_000, seed: int = 42):
        self.instruments = instruments_df
        self.num_trades = num_trades
        self.rng = random.Random(seed)

    def generate(self, start_date: str = "2024-01-02", end_date: str = "2024-12-31") -> pd.DataFrame:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        trading_days = []
        current = start
        while current <= end:
            if current.weekday() < 5:
                trading_days.append(current)
            current += timedelta(days=1)

        tradeable = self.instruments[
            self.instruments["asset_class"].isin(["EQUITY", "ETF", "OPTION"])
            & self.instruments["is_active"]
        ]

        trades = []
        for _ in range(self.num_trades):
            inst = tradeable.iloc[self.rng.randint(0, len(tradeable) - 1)]
            day = self.rng.choice(trading_days)

            hour = self.rng.randint(9, 15)
            minute = self.rng.randint(0, 59) if hour > 9 else self.rng.randint(30, 59)
            second = self.rng.randint(0, 59)
            microsecond = self.rng.randint(0, 999999)
            executed_at = day.replace(hour=hour, minute=minute, second=second, microsecond=microsecond)

            trade_date = day.date()
            settlement_date = trade_date + timedelta(days=2)  # T+2 settlement
            # Skip weekends for settlement
            while settlement_date.weekday() >= 5:
                settlement_date += timedelta(days=1)

            side = self.rng.choice([OrderSide.BUY, OrderSide.SELL])
            quantity = self.rng.choice([100, 200, 300, 500, 1000, 2000, 5000])
            price = round(self.rng.uniform(15, 800), 4)
            notional_value = round(quantity * price, 2)

            # Commission model: per-share with minimum
            commission_per_share = self.rng.uniform(0.003, 0.01)
            commission = round(max(1.0, quantity * commission_per_share), 2)

            # Exchange/SEC fees
            fees = round(notional_value * 0.0000229 + quantity * 0.000119, 2)

            # Net amount
            if side == OrderSide.BUY:
                net_amount = round(-(notional_value + commission + fees), 2)
            else:
                net_amount = round(notional_value - commission - fees, 2)

            status = self.rng.choices(
                [TradeStatus.EXECUTED, TradeStatus.SETTLED, TradeStatus.FAILED, TradeStatus.CANCELLED],
                weights=[40, 55, 3, 2],
                k=1,
            )[0]

            venue = self.rng.choices(
                self.EXECUTION_VENUES,
                weights=[25, 20, 15, 10, 10, 8, 7, 5],
                k=1,
            )[0]

            trades.append(
                {
                    "trade_id": f"TRD-{uuid.UUID(int=self.rng.getrandbits(128)).hex[:12].upper()}",
                    "order_id": f"ORD-{uuid.UUID(int=self.rng.getrandbits(128)).hex[:12].upper()}",
                    "instrument_id": inst["instrument_id"],
                    "symbol": inst["symbol"],
                    "portfolio_id": self.rng.choice(self.PORTFOLIO_IDS),
                    "trader_id": self.rng.choice(self.TRADER_IDS),
                    "side": side.value,
                    "quantity": quantity,
                    "price": price,
                    "notional_value": notional_value,
                    "commission": commission,
                    "fees": fees,
                    "net_amount": net_amount,
                    "exchange": inst.get("exchange", Exchange.NYSE.value),
                    "currency": inst.get("currency", Currency.USD.value),
                    "status": status.value,
                    "execution_venue": venue,
                    "settlement_date": str(settlement_date),
                    "trade_date": str(trade_date),
                    "executed_at": executed_at.isoformat(),
                }
            )

        return pd.DataFrame(trades)
