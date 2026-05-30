
# ARCHITECTURE NOTES

---

## 1. System Architecture

The pipeline uses a decoupled, event-driven architecture to break GitHub API mutations down into isolated workloads, preventing system bottlenecks.

```
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────────────┐
│ GitHub REST/v4  ├─────►│ Ingestion Workers├─────►│  Evidence Store (S3)    │
│ Streaming API   │      │ (Token Rotating) │      │  (Immutable JSON Audit) │
└─────────────────┘      └──────────────────┘      └────────────┬────────────┘
                                                                │
                                                                ▼
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────────────┐
│ Core Analytical │◄─────┤ Core DB Adapter  │◄─────┤   Verification Engine   │
│ Warehouse Tables│      │  (Polars/PSQL)   │      │   (Independent Hasher)  │
└─────────────────┘      └──────────────────┘      └─────────────────────────┘

```

### Components Layout

1. **Ingestion Loop (Workers):** Distributed stateless containers that fetch targeted entities (e.g., Pull Requests, Commits, Repositories). They rotate GitHub OAuth tokens dynamically to avoid secondary rate limits.
2. **Evidence Store (Write-Ahead Log):** Upstream extractions are stored as flat, signed JSON files in an object store (e.g., AWS S3 or a local secure directory). This acts as the unalterable **Source of Truth (SoT)**.
3. **Verification Layer (`EvidenceHasher`):** A standalone component that recalculates file signatures on the fly. It also creates deterministic Data Vault 2.0 keys before database insertion, allowing components to process data completely in parallel.
4. **Relational Database Sink (`psycopg3`):** Streams verified data chunks directly into a PostgreSQL cluster using high-performance `COPY` operations, bypassing slow, row-by-row `INSERT` blocks.

---

## 2. Cryptographic Core (`EvidenceHasher`)

This dedicated component decouples business identifier creation from database constraints. It calculates row-level data signatures using `XXH3_128` for speed and Data Vault 2.0 Hub/Link identifiers using `MD5` to ensure stable indexing.

```python
import hashlib
import xxhash

class EvidenceHasher:
    """Unified system cryptographic facility for calculating data integrity 
    signatures and Data Vault 2.0 business keys.
    """

    @staticmethod
    def get_xxhash_metadata() -> dict[str, str]:
        return {
            "hash_algorithm": xxhash.__name__,
            "hash_version": xxhash.VERSION,
            "hash_method_name": "XXH3_128"
        }

    @staticmethod
    def generate_entity_row_hash(raw_payload_string: str) -> str:
        """Calculates a deterministic 128-bit integrity fingerprint for a single API record payload."""
        return xxhash.xxh128(raw_payload_string).hexdigest()

    @staticmethod
    def generate_dv2_hash_key(*business_keys: str) -> str:
        """Derives a deterministic Data Vault 2.0 component key string.
        
        Uses standard MD5 to output stable 32-character string representations 
        perfect for distributed relational index alignments.
        """
        composite_string = "|".join(str(key).strip().lower() for key in business_keys)
        return hashlib.md5(composite_string.encode("utf-8")).hexdigest()

```

---

## 3. Data Vault 2.0 Database Schema

By replacing traditional sequence increments (`SERIAL`/`BIGSERIAL`) with deterministic hash keys, you avoid table lock bottlenecks. Distributed workers can compute primary keys natively in memory before sending bulk data streams to the database.

```sql
-- ============================================================================
-- 1. HUBS (Core Business Entities)
-- ============================================================================

-- Hub Organization
CREATE TABLE IF NOT EXISTS hub_organizations (
    hk_organization CHAR(32) PRIMARY KEY,     -- md5(org_name)
    org_name VARCHAR(255) NOT NULL,
    load_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    record_source VARCHAR(100) NOT NULL
);

-- Hub Repository
CREATE TABLE IF NOT EXISTS hub_repositories (
    hk_repository CHAR(32) PRIMARY KEY,       -- md5(org_name | repo_name)
    repo_name VARCHAR(255) NOT NULL,
    load_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    record_source VARCHAR(100) NOT NULL
);

-- Hub Users (Committers, Authors, Assignees)
CREATE TABLE IF NOT EXISTS hub_users (
    hk_user CHAR(32) PRIMARY KEY,             -- md5(github_username)
    github_username VARCHAR(150) NOT NULL,
    load_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    record_source VARCHAR(100) NOT NULL
);


-- ============================================================================
-- 2. LINKS (Entity Interrelationships)
-- ============================================================================

-- Link Organization to Repository
CREATE TABLE IF NOT EXISTS link_organization_repositories (
    hk_link_org_repo CHAR(32) PRIMARY KEY,    -- md5(hk_organization | hk_repository)
    hk_organization CHAR(32) NOT NULL REFERENCES hub_organizations(hk_organization),
    hk_repository CHAR(32) NOT NULL REFERENCES hub_repositories(hk_repository),
    load_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    record_source VARCHAR(100) NOT NULL
);

-- Link Audit Traceability (Tracks Pipeline Runs and Origin Files)
CREATE TABLE IF NOT EXISTS link_execution_audit (
    hk_link_execution CHAR(32) PRIMARY KEY,   -- md5(pipeline_run_id | evidence_filename)
    pipeline_run_id VARCHAR(100) NOT NULL,
    evidence_filename VARCHAR(255) NOT NULL,
    load_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    record_source VARCHAR(100) NOT NULL
);


-- ============================================================================
-- 3. SATELLITES (Mutable descriptive metadata states over time)
-- ============================================================================

-- Sat Repositories Descriptive State
CREATE TABLE IF NOT EXISTS sat_repository_details (
    hk_repository CHAR(32) NOT NULL REFERENCES hub_repositories(hk_repository),
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    star_count INT NOT NULL,
    fork_count INT NOT NULL,
    open_issues_count INT NOT NULL,
    row_hash CHAR(32) NOT NULL,                -- xxhash tracking record mutation deltas
    hk_link_execution CHAR(32) NOT NULL REFERENCES link_execution_audit(hk_link_execution),
    is_duplicate_at_stage BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (hk_repository, timestamp)
);

```

---

## 4. DB Adapter Layer Implementation (`PostgresAdapter`)

This adapter reads a verified Polars DataFrame out of the evidence store, generates Data Vault tracking keys in memory, and pipes the data into PostgreSQL using an optimized `COPY` instruction.

```python
import io
from pathlib import Path
from typing import Dict, Any, List
import polars as pl
import psycopg
from .hasher import EvidenceHasher

class PostgresGitHubIndexerAdapter:
    """Enterprise Data Vault 2.0 Persistence Engine for GitHub Indexing payloads."""

    def __init__(self, connection_string: str):
        self.conn_str = connection_string

    def persist_repository_metrics(
        self, 
        file_path: str | Path,
        df: pl.DataFrame, 
        metadata: Dict[str, Any], 
        download_params: Dict[str, Any],
        row_hashes: List[str]
    ) -> None:
        """Pipes repository state metrics directly into Data Vault structures."""
        if df.is_empty():
            return

        filename_str = Path(file_path).name
        pipeline_run_id = download_params.get("pipeline_run_id", "manual_run")
        record_src = metadata.get("source", "GitHub_API_v3")

        # 1. Generate Business Entity Hash Keys completely in memory
        hk_org = EvidenceHasher.generate_dv2_hash_key(metadata["org_name"])
        hk_repo = EvidenceHasher.generate_dv2_hash_key(metadata["org_name"], metadata["repo_name"])
        hk_link_org_repo = EvidenceHasher.generate_dv2_hash_key(hk_org, hk_repo)
        hk_link_execution = EvidenceHasher.generate_dv2_hash_key(pipeline_run_id, filename_str)

        with psycopg.connect(self.conn_str) as conn:
            with conn.cursor() as cur:
                
                # --- PHASE 1: Populate Hubs (Idempotent) ---
                cur.execute(
                    "INSERT INTO hub_organizations VALUES (%s, %s, NOW(), %s) ON CONFLICT DO NOTHING;",
                    (hk_org, metadata["org_name"], record_src)
                )
                cur.execute(
                    "INSERT INTO hub_repositories VALUES (%s, %s, NOW(), %s) ON CONFLICT DO NOTHING;",
                    (hk_repo, metadata["repo_name"], record_src)
                )

                # --- PHASE 2: Populate Links (Idempotent) ---
                cur.execute(
                    "INSERT INTO link_organization_repositories VALUES (%s, %s, %s, NOW(), %s) ON CONFLICT DO NOTHING;",
                    (hk_link_org_repo, hk_org, hk_repo, record_src)
                )
                cur.execute(
                    "INSERT INTO link_execution_audit VALUES (%s, %s, %s, NOW(), %s) ON CONFLICT DO NOTHING;",
                    (hk_link_execution, pipeline_run_id, filename_str, record_src)
                )

                # --- PHASE 3: Stream Records into Satellite via temporary staging ---
                satellite_df = df.with_columns([
                    pl.lit(hk_repo).alias("hk_repository"),
                    pl.Series("row_hash", row_hashes),
                    pl.lit(hk_link_execution).alias("hk_link_execution"),
                    pl.lit(False).alias("is_duplicate_at_stage")
                ]).select([
                    "hk_repository", "datetime", "star_count", "fork_count", 
                    "open_issues_count", "row_hash", "hk_link_execution", "is_duplicate_at_stage"
                ])

                cur.execute(
                    """
                    CREATE TEMPORARY TABLE temp_sat_repo_stage (
                        hk_repository CHAR(32), timestamp TIMESTAMP, star_count INT, fork_count INT,
                        open_issues_count INT, row_hash CHAR(32), hk_link_execution CHAR(32),
                        is_duplicate_at_stage BOOLEAN
                    ) ON COMMIT DROP;
                    """
                )

                csv_buffer = io.StringIO()
                satellite_df.write_csv(csv_buffer, include_header=False)
                csv_buffer.seek(0)

                with cur.copy("COPY temp_sat_repo_stage FROM STDIN WITH CSV") as copy_op:
                    copy_op.write(csv_buffer.read())

                # --- PHASE 4: Update Duplicate Status & Insert ---
                cur.execute(
                    """
                    UPDATE temp_sat_repo_stage t 
                    SET is_duplicate_at_stage = TRUE 
                    FROM sat_repository_details s 
                    WHERE s.hk_repository = t.hk_repository AND s.timestamp = t.timestamp;
                    """
                )

                cur.execute(
                    """
                    INSERT INTO sat_repository_details 
                    SELECT * FROM temp_sat_repo_stage
                    ON CONFLICT (hk_repository, timestamp) DO UPDATE SET
                        star_count = EXCLUDED.star_count, fork_count = EXCLUDED.fork_count,
                        open_issues_count = EXCLUDED.open_issues_count, row_hash = EXCLUDED.row_hash,
                        hk_link_execution = EXCLUDED.hk_link_execution, is_duplicate_at_stage = EXCLUDED.is_duplicate_at_stage;
                    """
                )

```

---

## 5. Benefits

* **Horizontal Scale Out:** Because primary keys are hash identities calculated deterministically in worker memory, ingestion processing can scale across multiple worker containers without any cross-talk or locking bottlenecks.
* **Audit Trail Traceability:** Any metrics record in the database can be traced directly back to its origin file via `hk_link_execution`. This makes verification straightforward during data audits.
* **Timeline Lineage:** The architecture handles updates cleanly. Changes over time are simply appended to the satellite table, preserving historical records for chronological analysis.