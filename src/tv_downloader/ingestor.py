import json
from pathlib import Path
from typing import Dict, Any, NamedTuple
import polars as pl
import xxhash

class IngestedEvidence(NamedTuple):
    """Structured, immutable envelope separating raw time-series data 
    from audit-trail configurations.
    """
    df: pl.DataFrame
    metadata: Dict[str, Any]
    download_parameters: Dict[str, Any]
    integrity: Dict[str, Any]


class JSONEvidenceIngestor:
    
    @staticmethod
    def _generate_hash(data_string: str) -> str:
        """System-wide unified 128-bit verification hash."""
        return xxhash.xxh128(data_string).hexdigest()

    @classmethod
    def load_and_verify(cls, file_path: str | Path) -> IngestedEvidence:
        """Loads a JSON evidence source file, cryptographically verifies its 
        integrity, and returns a cleanly decoupled IngestedEvidence envelope.
        """
        path = Path(file_path)
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)

        # 1. Extract distinct tracking and integrity layers
        integrity = payload.get("integrity", {})
        expected_content_hash = integrity.get("content_hash")
        
        metadata = payload.get("metadata", {})
        download_params = payload.get("download_parameters", {})
        data_rows = payload.get("data", [])

        # 2. Cryptographic Audit: Row-Level Verification
        row_hashes_pool = []
        for row in data_rows:
            raw_row_string = f"{row['datetime']}|{row['open']}|{row['high']}|{row['low']}|{row['close']}|{row['volume']}"
            calculated_row_hash = cls._generate_hash(raw_row_string)
            
            if calculated_row_hash != row["row_hash"]:
                raise ValueError(f"Row-level integrity corruption detected at timestamp {row['datetime']}!")
            
            row_hashes_pool.append(calculated_row_hash)

        # 3. Cryptographic Audit: Dataset-Level Verification
        content_fingerprint_base = {
            "metadata": {
                "symbol": metadata.get("symbol"),
                "venue": metadata.get("venue"),
                "time_period": metadata.get("time_period"),
                "source": metadata.get("source")
            },
            "download_parameters": download_params,
            "row_hashes": row_hashes_pool
        }
        
        stable_content_string = json.dumps(content_fingerprint_base, sort_keys=True)
        calculated_content_hash = cls._generate_hash(stable_content_string)

        if calculated_content_hash != expected_content_hash:
            raise ValueError("Dataset-level content_hash verification failed! File has been modified.")

        # 4. Process data rows into a clean Polars DataFrame (excluding row_hashes)
        # Convert datetime strings to concrete datetime primitives natively
        df = pl.DataFrame(data_rows).drop("row_hash").with_columns(
            pl.col("datetime").str.to_datetime()
        )

        # 5. Return decoupled components inside a type-safe NamedTuple container
        return IngestedEvidence(
            df=df,
            metadata=metadata,
            download_parameters=download_params,
            integrity=integrity
        )