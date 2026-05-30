from pathlib import Path
from tv_downloader.ingestor import JSONEvidenceIngestor

# Path to our target source of truth file
evidence_file = Path("C:/acme/tv-downloader/evidence_store/evidence_oanda_xauusd_d1_1780128160.json")

# Unpack the decoupled components safely
evidence = JSONEvidenceIngestor.load_and_verify(evidence_file)

# 1. Routing to UI / Console Logging Components
print(f"🔒 Verified Artifact: {evidence_file.name}")
print(f"   Algorithm:         {evidence.integrity['hash_method_name']} (v{evidence.integrity['hash_version']})")
print(f"   Fingerprint:       {evidence.integrity['content_hash']}")
print(f"   Pipeline ID:       {evidence.download_parameters['pipeline_run_id']}\n")

# 2. Routing to Analytical / Mathematical Components
# The risk engine or calculator receives just the raw matrix data frame
raw_polars_df = evidence.df
print("--- Raw Time Series Frame ---")
print(raw_polars_df)