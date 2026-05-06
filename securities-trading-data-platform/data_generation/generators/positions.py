"""Generate end-of-day portfolio position snapshots."""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta

import pandas as pd

from data_generation.models.schemas import Currency


class PositionGenerator:
    """Generates daily position snapshots for portfolios across instruments."""

    PORTFOLIO_IDS = [f"PORT-{i:04d}" for i in range(1, 51)]

    def __init__(self, instruments_df: pd.DataFrame, num_portfolios: int = 50, seed: int = 42):
        self.instruments = instruments_df
        self.num_portfolios = num_portfolios
        self.rng = random.Random(seed)

    def generate(self, start_date: str = "2024-01-02", end_date: str = "2024-12-31") -> pd.DataFrame:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()

        # Generate month-end snapshot dates
        snapshot_dates = []
        current = start
        while current <= end:
            # Find last business day of each month
            if current.month == 12:
                next_month = date(current.year + 1, 1, 1)
            else:
                next_month = date(current.year, current.month + 1, 1)
            last_day = next_month - timedelta(days=1)
            while last_day.weekday() >= 5:
                last_day -= timedelta(days=1)
            if last_day not in snapshot_dates and start <= last_day <= end:
                snapshot_dates.append(last_day)
            current = date(
                current.year + (1 if current.month == 12 else 0),
                (current.month % 12) + 1,
                1,
            )

        tradeable = self.instruments[
            self.instruments["asset_class"].isin(["EQUITY", "ETF"])
            & self.instruments["is_active"]
        ]

        positions = []
        position_id = 1

        for portfolio_id in self.PORTFOLIO_IDS[: self.num_portfolios]:
            # Each portfolio holds 10-30 instruments
            num_holdings = self.rng.randint(10, min(30, len(tradeable)))
            holdings = tradeable.sample(n=num_holdings, random_state=self.rng.randint(0, 99999))

            # Generate portfolio allocation weights
            raw_weights = [self.rng.uniform(0.5, 5.0) for _ in range(num_holdings)]
            total_weight = sum(raw_weights)
            weights = [w / total_weight for w in raw_weights]

            portfolio_nav = self.rng.uniform(5_000_000, 500_000_000)  # $5M to $500M

            for snap_date in snapshot_dates:
                # Slight monthly NAV drift
                portfolio_nav *= 1 + self.rng.uniform(-0.03, 0.04)

                for idx, (_, inst) in enumerate(holdings.iterrows()):
                    weight = weights[idx]
                    target_value = portfolio_nav * weight

                    current_price = round(self.rng.uniform(20, 600), 4)
                    quantity = max(1, int(target_value / current_price))
                    market_value = round(quantity * current_price, 2)

                    # Cost basis with some drift from current
                    cost_drift = self.rng.uniform(-0.15, 0.10)
                    avg_cost = round(current_price * (1 + cost_drift), 4)
                    unrealized_pnl = round((current_price - avg_cost) * quantity, 2)
                    realized_pnl = round(self.rng.uniform(-50000, 100000), 2)
                    total_pnl = round(unrealized_pnl + realized_pnl, 2)

                    actual_weight = round(market_value / portfolio_nav * 100, 4) if portfolio_nav > 0 else 0

                    positions.append(
                        {
                            "position_id": f"POS-{position_id:08d}",
                            "portfolio_id": portfolio_id,
                            "instrument_id": inst["instrument_id"],
                            "symbol": inst["symbol"],
                            "quantity": quantity,
                            "avg_cost": avg_cost,
                            "current_price": current_price,
                            "market_value": market_value,
                            "unrealized_pnl": unrealized_pnl,
                            "realized_pnl": realized_pnl,
                            "total_pnl": total_pnl,
                            "weight": actual_weight,
                            "currency": inst.get("currency", Currency.USD.value),
                            "as_of_date": str(snap_date),
                            "last_updated": datetime.combine(
                                snap_date, datetime.min.time()
                            ).replace(hour=16, minute=30).isoformat(),
                        }
                    )
                    position_id += 1

        return pd.DataFrame(positions)
