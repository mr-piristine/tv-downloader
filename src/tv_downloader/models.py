from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
from typing import List, Union

class Interval(Enum):
    # Minutes
    M1 = "1"
    M3 = "3"
    M5 = "5"
    M15 = "15"
    M30 = "30"
    M45 = "45"
    
    # Hours
    H1 = "60"
    H2 = "120"
    H3 = "180"
    H4 = "240"
    
    # Days / Weeks / Months
    D1 = "1D"
    W1 = "1W"
    MN1 = "1M"

    @classmethod
    def from_str(cls, value: str) -> "Interval":
        """Converts a shorthand string like 'H1' or 'D1' into a valid Enum instance."""
        try:
            return cls[value.upper()]
        except KeyError:
            raise ValueError(f"Unsupported interval '{value}'. Choose from {[e.name for e in cls]}")

@dataclass(frozen=True)
class Asset:
    symbol: str
    exchange: str

    @property
    def tv_ticker(self) -> str:
        return f"{self.exchange.upper()}:{self.symbol.upper()}"

@dataclass(frozen=True)
class Candle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float

@dataclass
class HistoryResultSet:
    asset: Asset
    interval: Interval
    candles: List[Candle] = field(default_factory=list)
    source: str = "TradingView"

    @property
    def symbol(self) -> str:
        return self.asset.symbol

    @property
    def venue(self) -> str:
        return self.asset.exchange

    @property
    def time_period(self) -> str:
        # This will now return your preferred string format (e.g., 'H1', 'D1')
        return self.interval.name