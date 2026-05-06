"""Pydantic schemas defining the structure of each data domain."""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum

from pydantic import BaseModel, Field

# ── Enums ─────────────────────────────────────────────────────────────────────


class AssetClass(str, Enum):
    EQUITY = "EQUITY"
    FIXED_INCOME = "FIXED_INCOME"
    OPTION = "OPTION"
    ETF = "ETF"
    FUTURES = "FUTURES"


class Exchange(str, Enum):
    NYSE = "NYSE"
    NASDAQ = "NASDAQ"
    CBOE = "CBOE"
    CME = "CME"
    ARCA = "ARCA"
    BATS = "BATS"
    IEX = "IEX"


class Currency(str, Enum):
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    JPY = "JPY"
    CAD = "CAD"


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"


class OrderStatus(str, Enum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class TradeStatus(str, Enum):
    EXECUTED = "EXECUTED"
    SETTLED = "SETTLED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class Sector(str, Enum):
    TECHNOLOGY = "Technology"
    HEALTHCARE = "Healthcare"
    FINANCIALS = "Financials"
    ENERGY = "Energy"
    CONSUMER_DISCRETIONARY = "Consumer Discretionary"
    CONSUMER_STAPLES = "Consumer Staples"
    INDUSTRIALS = "Industrials"
    MATERIALS = "Materials"
    REAL_ESTATE = "Real Estate"
    UTILITIES = "Utilities"
    COMMUNICATION_SERVICES = "Communication Services"


# ── Data Models ───────────────────────────────────────────────────────────────


class Instrument(BaseModel):
    instrument_id: str = Field(description="Unique instrument identifier")
    symbol: str = Field(description="Ticker symbol")
    name: str = Field(description="Full company/instrument name")
    asset_class: AssetClass
    exchange: Exchange
    currency: Currency
    sector: Sector | None = None
    isin: str | None = Field(default=None, description="International Securities Identification Number")
    cusip: str | None = Field(default=None, description="CUSIP identifier")
    lot_size: int = Field(default=100, description="Standard lot size")
    tick_size: float = Field(default=0.01, description="Minimum price increment")
    is_active: bool = True
    listed_date: date | None = None
    expiry_date: date | None = None  # for options/futures


class Order(BaseModel):
    order_id: str
    instrument_id: str
    symbol: str
    portfolio_id: str
    trader_id: str
    side: OrderSide
    order_type: OrderType
    quantity: int
    price: float | None = None  # None for market orders
    limit_price: float | None = None
    stop_price: float | None = None
    filled_quantity: int = 0
    avg_fill_price: float | None = None
    status: OrderStatus
    exchange: Exchange
    currency: Currency
    created_at: datetime
    updated_at: datetime
    time_in_force: str = "DAY"  # DAY, GTC, IOC, FOK


class Trade(BaseModel):
    trade_id: str
    order_id: str
    instrument_id: str
    symbol: str
    portfolio_id: str
    trader_id: str
    side: OrderSide
    quantity: int
    price: float
    notional_value: float
    commission: float
    fees: float
    net_amount: float
    exchange: Exchange
    currency: Currency
    status: TradeStatus
    execution_venue: str
    settlement_date: date
    trade_date: date
    executed_at: datetime


class MarketData(BaseModel):
    instrument_id: str
    symbol: str
    exchange: Exchange
    timestamp: datetime
    open_price: float = Field(alias="open")
    high_price: float = Field(alias="high")
    low_price: float = Field(alias="low")
    close_price: float = Field(alias="close")
    volume: int
    vwap: float
    num_trades: int
    bid_price: float
    ask_price: float
    bid_size: int
    ask_size: int
    spread: float

    model_config = {"populate_by_name": True}


class Position(BaseModel):
    position_id: str
    portfolio_id: str
    instrument_id: str
    symbol: str
    quantity: int
    avg_cost: float
    current_price: float
    market_value: float
    unrealized_pnl: float
    realized_pnl: float
    total_pnl: float
    weight: float  # portfolio weight percentage
    currency: Currency
    as_of_date: date
    last_updated: datetime
