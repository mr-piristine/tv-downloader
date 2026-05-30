import hashlib
import xxhash

class EvidenceHasher:
    """Unified system cryptographic facility for calculating data integrity 
    signatures and Data Vault 2.0 business keys.
    """

    @staticmethod
    def get_xxhash_metadata() -> dict[str, str]:
        """Exposes xxhash system framework tracking parameters."""
        return {
            "hash_algorithm": xxhash.__name__,
            "hash_version": xxhash.VERSION,
            "hash_method_name": "XXH3_128"
        }

    @staticmethod
    def generate_row_hash(timestamp_iso: str, open_p: float, high: float, low: float, close: float, volume: float) -> str:
        """Calculates a deterministic 128-bit integrity fingerprint for a single time-series bar."""
        raw_row_string = f"{timestamp_iso}|{open_p}|{high}|{low}|{close}|{volume}"
        return xxhash.xxh128(raw_row_string).hexdigest()

    @staticmethod
    def generate_content_hash(stable_json_string: str) -> str:
        """Calculates a deterministic 128-bit integrity signature for an entire dataset."""
        return xxhash.xxh128(stable_json_string).hexdigest()

    @staticmethod
    def generate_dv2_hash_key(*business_keys: str) -> str:
        """Derives a deterministic Data Vault 2.0 component key string.
        
        Uses standard MD5 to output stable 32-character string representations 
        perfect for distributed relational index alignments.
        """
        # Formulate composite structure matching exact business component layout
        composite_string = "|".join(str(key).strip().lower() for key in business_keys)
        return hashlib.md5(composite_string.encode("utf-8")).hexdigest()