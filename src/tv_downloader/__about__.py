# tv_downloader/__about__.py

__version__ = "0.1.0"
__author__ = "S.P."
__email__ = "sprstn@gmail.com"
__license__ = "Creative Commons - CC0 1.0 Universal"
__summary__ = "A modular, object-oriented historical data downloader for TradingView."

# You can keep your main documentation here as a clean string asset
__doc__ = """
tv_downloader
~~~~~~~~~~~~~

An object-wise historical data engine built to replace monolithic TradingView scraping utilities.
Operates strictly on a synchronous Connect -> Fetch -> Disconnect lifecycle.

Key Components:
- tv_downloader.models: Pure domain entities (Asset, Interval, Candle)
- tv_downloader.engine: The core WebSocket communication framework
- tv_downloader.adapters: Transformation layer for Pandas and data science suites
"""