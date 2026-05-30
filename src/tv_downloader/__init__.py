"""
tv_downloader
~~~~~~~~~~~~~

A modular, object-oriented historical data downloader for TradingView.
This library operates strictly on a synchronous Connect -> Fetch -> Disconnect lifecycle.

Basic Usage:
    >>> from tv_downloader.models import Asset, Interval
    >>> from tv_downloader.engine import HistoricalDownloader
    >>> downloader = HistoricalDownloader()
    >>> candles = downloader.fetch(Asset("AAPL", "NASDAQ"), Interval.daily)
"""

from .__about__ import __doc__, __version__
from .models import Asset, Interval, Candle
from .engine import HistoricalDownloader
from .session import TVSession
from .ingestor import IngestedEvidence, JSONEvidenceIngestor

__all__ = [
    "Asset", 
    "Interval", 
    "Candle", 
    "HistoricalDownloader", 
    "TVSession",
    "__version__"
]