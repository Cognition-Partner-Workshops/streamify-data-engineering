"""Generate realistic order book data for securities trading."""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta

import pandas as pd

from data_generation.models.schemas import Currency, Exchange, OrderSide, OrderStatus, OrderType


class OrderGenerator:
    """Generates order flow data across multiple portfolios and traders."""

    TRADER_IDS = [f"TRADER-{i:04d}" for i in range(1, 101)]
    PORTFOLIO_IDS = [f"PORT-{i:04d}" for i in range(1, 51)]
    TIME_IN_FORCE_OPTIONS = ["DAY", "GTC", "IOC", "FOK"]

    def __init__(self, instruments_df: pd.DataFrame, num_orders: int = 150_000, seed: int = 42):
        self.instruments = instruments_df
        self.num_orders = num_orders
        self.rng = random.Random(seed)

    def generate(self, start_date: str = "2024-01-02", end_date: str = "2024-12-31") -> pd.DataFrame:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        # Generate trading days
        trading_days = []
        current = start
        while current <= end:
            if current.weekday() < 5:
                trading_days.append(current)
            current += timedelta(days=1)

        # Focus on tradeable instruments (equities + ETFs)
        tradeable = self.instruments[
            self.instruments["asset_class"].isin(["EQUITY", "ETF", "OPTION"])
            & self.instruments["is_active"]
        ]

        orders = []
        for i in range(self.num_orders):
            inst = tradeable.iloc[self.rng.randint(0, len(tradeable) - 1)]
            day = self.rng.choice(trading_days)

            # Random time during market hours (09:30 - 16:00)
            hour = self.rng.randint(9, 15)
            minute = self.rng.randint(0, 59) if hour > 9 else self.rng.randint(30, 59)
            second = self.rng.randint(0, 59)
            microsecond = self.rng.randint(0, 999999)
            created_at = day.replace(hour=hour, minute=minute, second=second, microsecond=microsecond)

            side = self.rng.choice([OrderSide.BUY, OrderSide.SELL])
            order_type = self.rng.choices(
                [OrderType.MARKET, OrderType.LIMIT, OrderType.STOP, OrderType.STOP_LIMIT],
                weights=[30, 50, 12, 8],
                k=1,
            )[0]

            # Generate a realistic base price
            base_price = self.rng.uniform(20, 500)
            quantity = self.rng.choice([100, 200, 500, 1000, 2000, 5000]) * self.rng.randint(1, 5)

            # Price fields based on order type
            price = None
            limit_price = None
            stop_price = None

            if order_type == OrderType.MARKET:
                price = round(base_price, 2)
            elif order_type == OrderType.LIMIT:
                offset = base_price * self.rng.uniform(0.001, 0.02)
                limit_price = round(base_price + (offset if side == OrderSide.BUY else -offset), 2)
                price = limit_price
            elif order_type == OrderType.STOP:
                offset = base_price * self.rng.uniform(0.02, 0.05)
                stop_price = round(base_price - (offset if side == OrderSide.BUY else -offset), 2)
                price = stop_price
            elif order_type == OrderType.STOP_LIMIT:
                offset = base_price * self.rng.uniform(0.02, 0.05)
                stop_price = round(base_price - (offset if side == OrderSide.BUY else -offset), 2)
                limit_price = round(stop_price * (1 + self.rng.uniform(-0.005, 0.005)), 2)
                price = limit_price

            # Order status with realistic distribution
            status = self.rng.choices(
                [
                    OrderStatus.FILLED,
                    OrderStatus.PARTIALLY_FILLED,
                    OrderStatus.CANCELLED,
                    OrderStatus.NEW,
                    OrderStatus.REJECTED,
                    OrderStatus.EXPIRED,
                ],
                weights=[55, 10, 15, 8, 7, 5],
                k=1,
            )[0]

            filled_quantity = 0
            avg_fill_price = None
            if status == OrderStatus.FILLED:
                filled_quantity = quantity
                avg_fill_price = round(base_price * (1 + self.rng.uniform(-0.002, 0.002)), 2)
            elif status == OrderStatus.PARTIALLY_FILLED:
                filled_quantity = int(quantity * self.rng.uniform(0.1, 0.9))
                avg_fill_price = round(base_price * (1 + self.rng.uniform(-0.003, 0.003)), 2)

            updated_at = created_at + timedelta(seconds=self.rng.randint(0, 3600))
            tif = self.rng.choices(
                self.TIME_IN_FORCE_OPTIONS, weights=[60, 25, 10, 5], k=1
            )[0]

            orders.append(
                {
                    "order_id": f"ORD-{uuid.UUID(int=self.rng.getrandbits(128)).hex[:12].upper()}",
                    "instrument_id": inst["instrument_id"],
                    "symbol": inst["symbol"],
                    "portfolio_id": self.rng.choice(self.PORTFOLIO_IDS),
                    "trader_id": self.rng.choice(self.TRADER_IDS),
                    "side": side.value,
                    "order_type": order_type.value,
                    "quantity": quantity,
                    "price": price,
                    "limit_price": limit_price,
                    "stop_price": stop_price,
                    "filled_quantity": filled_quantity,
                    "avg_fill_price": avg_fill_price,
                    "status": status.value,
                    "exchange": inst.get("exchange", Exchange.NYSE.value),
                    "currency": inst.get("currency", Currency.USD.value),
                    "created_at": created_at.isoformat(),
                    "updated_at": updated_at.isoformat(),
                    "time_in_force": tif,
                }
            )

        return pd.DataFrame(orders)
