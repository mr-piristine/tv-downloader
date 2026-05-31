    
import json
import time
from pathlib import Path
from typing import Dict, Any
from datetime import datetime
import polars as pl
from ..models import HistoryResultSet
from ..hasher import EvidenceHasher  # <-- Modern separate class import

class JSONEvidenceAdapter:

    @staticmethod
    def to_json_evidence(
        result_set: HistoryResultSet, 
        directory: str, 
        download_params: Dict[str, Any] = None
    ) -> str:
        """Serializes the complete HistoryResultSet envelope using EvidenceHasher signatures."""
        if download_params is None:
            download_params = {}

        dir_path = Path(directory)
        dir_path.mkdir(parents=True, exist_ok=True)

        structured_candles = []
        row_hashes_pool = []

        # 1. Row-level hashing using the standalone hasher
        for c in result_set.candles:
            ts_str = c.timestamp.isoformat()
            row_hash = EvidenceHasher.generate_row_hash(ts_str, c.open, c.high, c.low, c.close, c.volume)
            row_hashes_pool.append(row_hash)

            structured_candles.append({
                "datetime": ts_str, "open": c.open, "high": c.high, "low": c.low, "close": c.close, "volume": c.volume,
                "row_hash": row_hash
            })

        metadata_payload = {
            "symbol": result_set.symbol, "venue": result_set.venue, "time_period": result_set.time_period, "source": result_set.source
        }
        
        # 2. Content-level hashing using the standalone hasher
        content_fingerprint_base = {
            "metadata": metadata_payload, "download_parameters": download_params, "row_hashes": row_hashes_pool
        }
        stable_content_string = json.dumps(content_fingerprint_base, sort_keys=True)
        content_hash = EvidenceHasher.generate_content_hash(stable_content_string)

        # 3. Build out payload map incorporating dynamic metadata descriptors
        evidence_payload = {
            "integrity": {
                "content_hash": content_hash,
                **EvidenceHasher.get_xxhash_metadata()
            },
            "metadata": {
                **metadata_payload,
                "exported_at": datetime.now().isoformat() + "Z"
            },
            "download_parameters": download_params,
            "data": structured_candles
        }

        run_timestamp = int(time.time())
        filename = f"evidence_{result_set.venue}_{result_set.symbol}_{result_set.time_period}_{run_timestamp}.json".lower()
        full_path = dir_path / filename 

        with full_path.open("w", encoding="utf-8") as f:
            json.dump(evidence_payload, f, indent=4, ensure_ascii=False)

        return str(full_path)