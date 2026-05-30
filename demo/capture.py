from datetime import datetime
from tv_downloader.models import Asset
from tv_downloader.session import TVSession
from tv_downloader.engine import HistoricalDownloader
from tv_downloader.adapters import JSONEvidenceAdapter, PolarsAdapter

# 1. Track your runtime download parameters
download_config = {
    "n_bars": 3,
    "environment": "production",
    "triggered_by": "automated_risk_runner",
    "pipeline_run_id": "run_98234"
}

# 2. Execute extraction
session = TVSession()
downloader = HistoricalDownloader(session=session)
gold_asset = Asset(symbol="XAUUSD", exchange="OANDA")

print("Fetching historical data envelope...")
result_set = downloader.fetch(
    asset=gold_asset, 
    interval="D1", 
    n_bars=download_config["n_bars"]
)


# 3. Print the extracted data using the Polars Adapter
PolarsAdapter.print_dataframe(result_set)


# 4. Write out to the evidence directory
target_dir = "C:/acme/tv-downloader/evidence_store"
JSONEvidenceAdapter.to_json_evidence(
    result_set=result_set, 
    directory=target_dir, 
    download_params=download_config
)