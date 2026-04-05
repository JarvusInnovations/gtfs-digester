# GTFS Schedule Archiving — Architecture Plan

## Overview

Add GTFS schedule archiving to the gtfs-realtime-archiver ecosystem. Three deliverables:

1. **`gtfs-digester`** — shared Python package for canonicalization, fingerprinting, and change detection of GTFS Schedule feeds
2. **Dagster schedule ingestion pipeline** — new assets in `gtfs-realtime-archiver` that use gtfs-digester to discover, ingest, and store GTFS schedules
3. **Historical backfill** — load historical feeds from Mobility Database to cover existing RT archive date ranges

## 1. gtfs-digester Package

### What It Does

Takes a GTFS zip (or directory of .txt files) and produces:

- A **content fingerprint** (BLAKE3 merkle hash) that is identical for semantically identical feeds regardless of zip metadata, file ordering, CSV whitespace, quoting, or time formatting
- **Canonical Arrow tables** for each file — normalized, sorted by primary key, all values as strings
- **Per-file hashes** for efficient hierarchical change detection
- **Archive diffs** — added/removed/modified files and rows

### Key Design Decision: Preserve Unknown Files and Columns

The experiment dropped unknown files/columns. This was widely disliked and is wrong for an archival tool. The new implementation:

- **Unknown files** (e.g., `fare_leg_rules.txt`, `modifications.txt`, agency extensions): preserved in output and included in fingerprint. Sorted by filename, rows sorted lexicographically (no primary key knowledge).
- **Unknown columns** in known files (e.g., `tts_stop_name` in `stops.txt`): preserved, placed after spec-defined columns in alphabetical order. Included in fingerprint.
- **Known files** (the 9 implemented + more from the full GTFS spec): normalized with primary key sorting, time zero-padding, spec column ordering.

This means the fingerprint reflects the **complete content** of the feed, not just the parts we understand.

### Module Structure

```
src/gtfs_digester/
  __init__.py
  schema.py          # GTFS file schemas (primary keys, column order, sort rules)
  normalize.py       # Value normalization (whitespace, time zero-padding)
  file.py            # GTFSFile: parse CSV → canonical Arrow table → BLAKE3 hash
  archive.py         # GTFSArchive: load zip/dir → fingerprint → diff → write
  fingerprint.py     # ArchiveFingerprint: merkle tree, JSON serialization
  diff.py            # ArchiveDiff/FileDiff: hierarchical change detection
  storage.py         # ExplodedStorage: read/write exploded format to cloud/local
  metadata.py        # Feed metadata: provenance, validity dates, row counts
```

### Ported from Experiment (with fixes)

- `schema.py` — expand from 9 to all 32+ GTFS files, add unknown file handling
- `normalize.py` — unchanged (whitespace strip, time zero-pad)
- `file.py` — add unknown column preservation (after spec columns, alphabetical)
- `archive.py` — add unknown file preservation, directory loading
- `fingerprint.py` — unchanged (BLAKE3 merkle)
- `diff.py` — unchanged (hierarchical primary-key diffing)

### New

- `storage.py` — exploded format for cloud storage (the unimplemented Phase 4 from the experiment)
- `metadata.py` — provenance tracking (source URL, download timestamp, raw SHA256)

## 2. Storage Architecture

### No Raw Zip Bucket

For RT data, the two-bucket pattern (`protobuf.gtfsrt.io` → `parquet.gtfsrt.io`) exists because raw protobufs accumulate continuously and compaction is lossy. GTFS schedules are different:

- Each feed version is a single download event
- The digester's canonicalization is **lossless** (all files/columns preserved)
- `metadata.json` records the `source_sha256` of the original zip for provenance
- The original zip can be re-downloaded from the source URL or Mobility Database if ever needed

No raw zip archival. If we later want it, `raw.gtfsrt.io` is the right bucket name.

### Proposed: Version-First Parquet in `parquet.gtfsrt.io/schedules/`

Store schedule data alongside the RT parquet data in the existing public bucket under a `schedules/` root. Each feed version is a **self-contained directory** keyed by its digester fingerprint.

```
parquet.gtfsrt.io/
  # Existing RT data (unchanged)
  vehicle_positions/date=YYYY-MM-DD/base64url=.../data.parquet
  trip_updates/date=YYYY-MM-DD/base64url=.../data.parquet
  service_alerts/date=YYYY-MM-DD/base64url=.../data.parquet
  feeds.parquet
  inventory.json

  # New: GTFS schedule data
  schedules/
    # Per-feed directory, keyed by schedule_url (same base64url encoding as RT)
    base64url={base64url-of-schedule-url}/
      # Each feed version is a self-contained directory
      _feed_digest={fingerprint}/
        agency.parquet
        stops.parquet
        routes.parquet
        trips.parquet
        stop_times.parquet
        calendar.parquet
        calendar_dates.parquet
        shapes.parquet
        feed_info.parquet
        # ... all files from the feed, including unknown/extension files
        metadata.json       # version metadata + digester info (see below)

  # Updated: schedule metadata added to existing feeds.parquet
  feeds.parquet                 # add schedule columns (fingerprint, date_retrieved, validity dates)

  # Updated: schedule info added to inventory
  inventory.json                # add schedule_versions array per feed
```

### `metadata.json` Contents

Each version's `metadata.json` is a single JSON document with all provenance and digester info:

```json
{
  "_feed_digest": "v1:abc123...",
  "schedule_url": "https://www3.septa.org/developer/google_bus.zip",
  "date_retrieved": "2026-03-28T00:31:25Z",
  "feed_start_date": "2026-03-29",
  "feed_end_date": "2026-06-14",
  "source_sha256": "def456...",
  "digester_version": "v1",
  "file_hashes": {
    "stops.txt": "v1:...",
    "routes.txt": "v1:...",
    "trips.txt": "v1:..."
  },
  "file_row_counts": {
    "stops.txt": 9245,
    "routes.txt": 142,
    "trips.txt": 48320
  }
}
```

Human-readable, inspectable with `cat` or `jq`. DuckDB can glob across versions:

```sql
SELECT * FROM read_json_auto(
  'http://parquet.gtfsrt.io/schedules/base64url={b64}/_feed_digest=*/metadata.json',
  hive_partitioning=true
);
```

This is the **commit marker** — written last after all table parquets. If `metadata.json` exists, the version is complete.

### Why This Layout

1. **Version-first directories**: a feed version is a single prefix (`_feed_digest={fp}/`). Listing, copying, replicating, or deleting a version is one operation. The unit of work at the archiver level is the version, not the table.
2. **Self-contained**: everything about a version lives in one directory — table data, metadata, provenance. No cross-references needed.
3. **Hive-compatible**: DuckDB reads `_feed_digest=*/*.parquet` and gets `_feed_digest` as a column. Consumers that need all versions of a table can glob across versions.
4. **Atomic writes**: each ingestion writes N table files + `metadata.json` last. A failed write leaves old data untouched. `metadata.json` presence = complete version.
5. **Content-addressed**: same fingerprint = same path. Re-ingesting an identical feed overwrites with identical data — safe idempotent.
6. **Parquet not CSV**: consumers query directly with DuckDB/BigQuery. The digester produces Arrow tables — parquet is the natural serialization.
7. **Colocated with RT**: single bucket serves both RT and schedule data. One endpoint for consumers.
8. **`base64url` consistency**: same encoding as RT feeds. The `schedule_url` from `agencies.yaml` maps directly to the path.

### Consumer Query Pattern

DuckDB consumers (Transit Lake's dbt-gtfs, BigQuery, ad-hoc) read like:

```sql
-- All versions of stops for a feed
SELECT * FROM read_parquet(
  'http://parquet.gtfsrt.io/schedules/base64url={b64}/_feed_digest=*/stops.parquet',
  hive_partitioning=true
);

-- Specific version
SELECT * FROM read_parquet(
  'http://parquet.gtfsrt.io/schedules/base64url={b64}/_feed_digest={fp}/stops.parquet'
);

-- All version metadata for a feed
SELECT * FROM read_json_auto(
  'http://parquet.gtfsrt.io/schedules/base64url={b64}/_feed_digest=*/metadata.json',
  hive_partitioning=true
);
```

### Alignment with Transit Lake

Transit Lake's `bin/load-gtfs` currently produces a table-first Hive layout in agency buckets:

```
gs://{agency}.transitlake.io/raw/gtfs-schedule/
  stops/_feed_hash={hash}/*.parquet
  feed_meta/{timestamp}.parquet
```

The archiver uses a version-first layout (all tables under `_feed_digest={fp}/`). When Transit Lake replicates from the archiver, it will need to either:

- Adopt the version-first layout (cleaner, matches the archiver as source of truth)
- Reshape into its current table-first layout during replication

The naming difference (`_feed_digest` vs `_feed_hash`) also needs resolution. Options:

- Use `_feed_digest` everywhere (simplest — just copy)
- Recompute `_feed_hash` from content (preserves independence but redundant)

These decisions are deferred to the Transit Lake integration phase.

## 3. Dagster Pipeline Design

### Assets

Two assets in a new `gtfs_schedule` group, plus extensions to existing metadata assets.

**Asset 1: `gtfs_schedule_check`** (unpartitioned, scheduled)

- Runs daily at 5am UTC (after RT compaction at 2am, inventory at 4am)
- Reads `agencies.yaml` from Secret Manager
- For each unique `schedule_url` across all agencies/systems:
  - Downloads the zip (with auth if needed, reusing archiver's secret resolution)
  - Runs gtfs-digester to compute fingerprint
  - Checks for existing `_feed_digest={fp}/metadata.json` in parquet bucket
  - If new: registers Dagster dynamic partition, stores digester result in memory/output
- Yields `MaterializeResult` with list of new fingerprints discovered
- Metadata: urls_checked, new_feeds_found, unchanged_feeds, errors

**Asset 2: `gtfs_schedule_ingest`** (partitioned by `schedule_url:fingerprint`)

- Triggered by sensor watching `gtfs_schedule_check`
- Downloads the zip fresh and runs gtfs-digester (same as check — digester is fast and cheap, no need for intermediate storage)
- Writes each table as parquet to `parquet.gtfsrt.io/schedules/base64url={b64}/_feed_digest={fp}/{table}.parquet`
- Writes `metadata.json` last (commit marker — contains provenance, digester info, file hashes)

### Why Split Check and Ingest?

The digester runs twice (once in check, once in ingest) — this is intentional. The digester is fast and the cost is a redundant download+hash, not a redundant heavy computation. The architectural benefits of splitting:

- **Check is unpartitioned, ingest is partitioned**: check runs for all feeds in one scheduled pass; ingest runs independently per new version. A download failure for one feed doesn't block processing of others.
- **Re-runnable**: ingest can be manually re-triggered for a specific feed version (useful for backfill, debugging, or recovery from partial writes).
- **No intermediate state**: check doesn't need to store the zip anywhere between assets. Ingest downloads fresh. No temp bucket, no cleanup, no orphaned files.
- **Clean Dagster observability**: "did anything change?" is a separate question from "process the change." The sensor bridges them naturally.

### Extending Existing Metadata Assets

Rather than a separate index asset, **extend the existing `feeds_metadata` and `inventory` assets** to also scan schedule data:

- **`feeds_metadata`** (already runs daily at 4am): add schedule columns to `feeds.parquet` — `schedule_feed_digest`, `schedule_date_retrieved`, `schedule_start_date`, `schedule_end_date` per feed. Reads `metadata.json` files from the schedules prefix.
- **`inventory`** (already runs daily at 4am): add `schedule_versions` array to each feed entry in `inventory.json`.
- **Dependency**: add `gtfs_schedule_ingest` as an optional upstream so these re-run when new schedules land (via sensor or schedule ordering — inventory already runs after compaction).

This avoids a third asset writing to the same files, and keeps the metadata pipeline as a single source of truth for `feeds.parquet` and `inventory.json`.

### Schedule & Sensor

```python
# Schedule: check all feeds daily at 5am UTC
@dg.schedule(cron_schedule="0 5 * * *", job=gtfs_schedule_check_job)
def gtfs_schedule_check_schedule(): ...

# Sensor: trigger ingest for each new fingerprint
@dg.sensor(monitored_assets=[AssetKey("gtfs_schedule_check")])
def gtfs_schedule_new_feed_sensor(): ...
```

### Partitioning

Dynamic partitions keyed by `{base64url}:{fingerprint}`. This allows:

- Re-running ingest for a specific feed version
- Parallelizing across multiple new feeds discovered in one check
- Backfill of historical feeds (manually add partitions)

### Auth Resolution

The archiver already resolves `schedule_url` per agency/system with auth inheritance. The schedule check asset reuses `flatten_agencies()` to get the full feed list with resolved auth configs. For feeds requiring API keys (Metrolink, Houston, AC Transit, etc.), the same Secret Manager integration applies.

## 4. Historical Backfill

### Mobility Database Integration

We've already downloaded 25 historical GTFS zips from MobilityDB to `.scratch/gtfs-schedules-historical/`. The backfill process:

1. **One-time script** (not a Dagster asset): iterate over downloaded zips, run gtfs-digester on each, upload to parquet bucket
2. **Set `date_retrieved`** from the MobilityDB `downloaded_at` field (not current time) — this is critical for correct feed validity window computation
3. **Deduplication**: if two MobilityDB snapshots produce the same fingerprint, only one version directory is created

### Feed Coverage Needed

| Agency | RT Archive | Schedule Coverage Needed |
|--------|-----------|------------------------|
| SEPTA Bus | Jan 4 – Mar 26 | Dec 14, 2025 – Mar 29, 2026 (7 MDB datasets) |
| MTA Bus (6 boroughs) | Mar 1 – Mar 26 | Jan 3 – Apr 12, 2026 (18 MDB datasets) |

After deduplication by fingerprint, we expect ~3-4 unique feed versions per agency (vs 25 downloaded zips).

### Backfill Script

```bash
# Run from gtfs-realtime-archiver repo
python -m dagster_pipeline.scripts.backfill_schedules \
  --source-dir .scratch/gtfs-schedules-historical/ \
  --metadata-source mobility-database \
  --dry-run
```

The script:

1. Iterates over zip files organized by MDB feed ID
2. Maps MDB feed IDs to archiver `schedule_url`s (via a mapping file or agencies.yaml lookup)
3. Runs gtfs-digester on each
4. Uploads deduplicated results to parquet bucket
5. Registers dynamic partitions in Dagster for manual re-processing if needed

## 5. Transit Lake Integration (Direction, Not Detail)

Three possible approaches for how Transit Lake pulls archived schedules from gtfsrt.io:

### Approach A: Direct Read from gtfsrt.io

Transit Lake's `dbt-gtfs` package reads parquet directly from `parquet.gtfsrt.io/schedules/`. The `metadata.json` files tell it which fingerprints are available. `bin/load-gtfs` downloads the parquet files instead of processing zips itself.

**Pro**: No data duplication. Single source of truth.
**Con**: Couples Transit Lake's dbt runs to gtfsrt.io availability.

### Approach B: Replicate to Agency Bucket

A Transit Lake connector (or `bin/load-gtfs` enhancement) copies the relevant schedule parquet from gtfsrt.io into the agency's own bucket at `gs://{agency}.transitlake.io/raw/gtfs-schedule/`. This is essentially what `bin/load-gtfs` does today but reading from the archive instead of processing zips.

**Pro**: Agency bucket is self-contained. dbt runs are independent of gtfsrt.io.
**Con**: Data duplication. Need a sync mechanism.

### Approach C: GTFS Schedule Connector

Model schedule archiving as a Transit Lake "connector" — similar to how GTFS-RT archiving is already a connector type. The connector config points to the gtfsrt.io archive and the pipeline automatically syncs new schedule versions into the agency bucket when detected.

**Pro**: Fits the Transit Lake product model. Users see schedule updates in the pipeline UI.
**Con**: More product surface area to build.

**Recommendation**: Start with **Approach B** (simple replication) for the immediate need, design toward **Approach C** (connector) as the product vision. Approach A is a useful intermediate for development/testing.

## 6. Implementation Order

### Phase 1: gtfs-digester core (this repo)

- [ ] Port schema.py with unknown file/column preservation
- [ ] Port normalize.py, file.py, fingerprint.py, diff.py
- [ ] Port archive.py with directory loading support
- [ ] Add comprehensive tests against real feeds (SEPTA, MTA, RFTA)
- [ ] Publish to PyPI or use as git dependency

### Phase 2: Storage layer

- [ ] Implement `storage.py` — exploded parquet write/read with fsspec
- [ ] Implement `metadata.py` — provenance tracking, metadata.json generation
- [ ] Test against GCS (parquet.gtfsrt.io bucket)

### Phase 3: Dagster integration (in gtfs-realtime-archiver)

- [ ] Add gtfs-digester dependency to dagster-deploy group
- [ ] Implement `gtfs_schedule_check` asset
- [ ] Implement `gtfs_schedule_ingest` asset
- [ ] Extend `feeds_metadata` + `inventory` assets with schedule data
- [ ] Add schedule + sensor
- [ ] Terraform: BigQuery external tables for schedule data

### Phase 4: Historical backfill

- [ ] Write backfill script using downloaded MDB zips
- [ ] Map MDB feed IDs to agencies.yaml schedule_urls
- [ ] Run backfill, verify deduplication
- [ ] Verify coverage against RT archive date ranges

### Phase 5: Transit Lake integration (separate planning session)

- [ ] Decide on Approach A/B/C
- [ ] Update bin/load-gtfs or add connector
- [ ] Verify dbt-gtfs works against archived schedule data
