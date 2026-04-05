# gtfs-digester — Agent Guide

## What This Is

A Python package for canonicalizing, fingerprinting, and diffing GTFS Schedule feeds. Content-addressed: identical feeds produce identical fingerprints regardless of zip packaging, file ordering, CSV formatting, or whitespace.

Part of the gtfs-realtime-archiver ecosystem. Used by the Dagster schedule ingestion pipeline to detect changed feeds and store them as exploded parquet.

## Package Manager

Uses `uv` for dependency management. Never edit `pyproject.toml` directly for dependencies:

```bash
uv add <package>           # runtime dependency
uv add --group dev <pkg>   # dev dependency
```

## Running Tests

```bash
uv run pytest tests/ -k "not Real"   # unit tests only (~0.5s)
uv run pytest tests/                  # full suite including real feeds (~25min)
```

Real feed tests require SEPTA GTFS zips in `/home/chris/transit-lake/.scratch/gtfs-schedules-historical/mdb-502/`.

## Architecture

```
src/gtfs_digester/
  schema.py       # GTFS file schemas (26 files, primary keys, sort rules)
  normalize.py    # Vectorized value normalization (PyArrow compute)
  file.py         # GTFSFile: CSV → canonical Arrow table → BLAKE3 hash
  archive.py      # GTFSArchive: zip/dir → fingerprint → diff
  fingerprint.py  # ArchiveFingerprint: BLAKE3 merkle tree, v1: versioned
  diff.py         # ArchiveDiff/FileDiff: hierarchical PK-based diffing
  metadata.py     # FeedMetadata: provenance, metadata.json generation
  storage.py      # write_exploded/read: version-first parquet via fsspec
```

## Key Design Decisions

- **Preserve unknown files and columns** — never drop data. Unknown files sort lexicographically, unknown columns go after spec columns alphabetically.
- **All data stays as strings** in Arrow — no type coercion. Numeric casts only for sorting (temporary columns, dropped after).
- **BLAKE3** for hashing (fast, deterministic). Versioned with `v1:` prefix for algorithm evolution.
- **`_feed_digest`** is the Hive partition key in storage paths. Distinct from Transit Lake's `_feed_hash`.
- **Primary key columns may be absent** — sort and validate using only PK columns present in the table (e.g., `transfers.txt` optional columns).
- **Empty CSV files are valid** — return empty Arrow table, not an error.

## Storage Layout

```
base_path/
  _feed_digest={v1:abc...}/
    stops.parquet
    routes.parquet
    ...
    metadata.json    # commit marker, written last
```

## Commit Practices

- Conventional commits: `feat:`, `fix:`, `test:`, `docs:`, `chore:`, `refactor:`
- No scope needed (single package)
