"""Generate realistic OHLCV market data with bid/ask spreads."""

from __future__ import annotations

import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from data_generation.models.schemas import Exchange


class MarketDataGenerator:
    """Generates intraday OHLCV market data ticks for a universe of instruments."""

    def __init__(self, instruments_df: pd.DataFrame, num_records: int = 500_000, seed: int = 42):
        self.instruments = instruments_df
        self.num_records = num_records
        self.rng = random.Random(seed)
        np.random.seed(seed)

    def _generate_price_series(self, start_price: float, n_ticks: int, volatility: float) -> np.ndarray:
        """Geometric Brownian Motion price simulation."""
        dt = 1.0 / 252 / 78  # ~78 five-minute bars per trading day
        mu = 0.0001  # slight positive drift
        returns = np.random.normal(mu * dt, volatility * np.sqrt(dt), n_ticks)
        prices = start_price * np.cumprod(1 + returns)
        return np.round(prices, 4)

    def generate(self, start_date: str = "2024-01-02", end_date: str = "2024-12-31") -> pd.DataFrame:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        # Generate trading days (weekdays only)
        trading_days = []
        current = start
        while current <= end:
            if current.weekday() < 5:  # Mon-Fri
                trading_days.append(current)
            current += timedelta(days=1)

        # 5-minute bars from 09:30 to 16:00 = 78 bars per day
        bar_times = []
        for hour in range(9, 16):
            start_min = 30 if hour == 9 else 0
            end_min = 60 if hour < 16 else 1  # last bar at 16:00
            for minute in range(start_min, end_min, 5):
                bar_times.append((hour, minute))

        # Select instruments with higher weight to active equities/ETFs
        active_instruments = self.instruments[self.instruments["is_active"]].copy()
        equity_etf = active_instruments[active_instruments["asset_class"].isin(["EQUITY", "ETF"])]

        records_per_instrument = max(1, self.num_records // max(1, len(equity_etf)))
        total_bars = len(trading_days) * len(bar_times)
        sample_rate = min(1.0, records_per_instrument / total_bars)

        all_records = []
        record_count = 0

        for _, inst in equity_etf.iterrows():
            if record_count >= self.num_records:
                break

            # Assign a realistic starting price based on asset class
            if inst["asset_class"] == "ETF":
                start_price = self.rng.uniform(30, 500)
                vol = self.rng.uniform(0.10, 0.25)
            else:
                start_price = self.rng.uniform(15, 800)
                vol = self.rng.uniform(0.15, 0.60)

            prices = self._generate_price_series(start_price, total_bars, vol)
            bar_idx = 0

            for day in trading_days:
                if record_count >= self.num_records:
                    break
                for hour, minute in bar_times:
                    if record_count >= self.num_records:
                        break
                    if bar_idx >= len(prices):
                        break

                    # Probabilistic sampling to control output size
                    if self.rng.random() > sample_rate:
                        bar_idx += 1
                        continue

                    price = prices[bar_idx]
                    bar_idx += 1

                    # Generate OHLCV from the base price
                    intra_vol = vol * 0.002
                    open_price = round(price * (1 + self.rng.uniform(-intra_vol, intra_vol)), 4)
                    high_price = round(max(open_price, price) * (1 + abs(self.rng.gauss(0, intra_vol))), 4)
                    low_price = round(min(open_price, price) * (1 - abs(self.rng.gauss(0, intra_vol))), 4)
                    close_price = round(price, 4)

                    volume = int(abs(self.rng.gauss(50000, 30000))) + 100
                    num_trades = max(1, int(volume / self.rng.uniform(50, 200)))
                    vwap = round((open_price + high_price + low_price + close_price) / 4, 4)

                    # Bid-ask spread
                    spread_bps = self.rng.uniform(1, 15)  # 1-15 basis points
                    spread = round(close_price * spread_bps / 10000, 4)
                    bid_price = round(close_price - spread / 2, 4)
                    ask_price = round(close_price + spread / 2, 4)
                    bid_size = self.rng.randint(100, 5000)
                    ask_size = self.rng.randint(100, 5000)

                    ts = day.replace(hour=hour, minute=minute, second=0, microsecond=0)

                    all_records.append(
                        {
                            "instrument_id": inst["instrument_id"],
                            "symbol": inst["symbol"],
                            "exchange": inst.get("exchange", Exchange.NYSE.value),
                            "timestamp": ts.isoformat(),
                            "open": open_price,
                            "high": high_price,
                            "low": low_price,
                            "close": close_price,
                            "volume": volume,
                            "vwap": vwap,
                            "num_trades": num_trades,
                            "bid_price": bid_price,
                            "ask_price": ask_price,
                            "bid_size": bid_size,
                            "ask_size": ask_size,
                            "spread": spread,
                        }
                    )
                    record_count += 1

        df = pd.DataFrame(all_records)
        return df
