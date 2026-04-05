# gtfs-digester

Canonicalization, fingerprinting, and change detection for GTFS Schedule feeds.

## What It Does

Takes a GTFS zip and produces:

- A **content fingerprint** (BLAKE3 merkle hash) — identical for semantically identical feeds regardless of zip metadata, file ordering, CSV whitespace, or time formatting
- **Canonical Arrow tables** for each file — normalized, sorted by primary key, all values as strings
- **Per-file hashes** for efficient hierarchical change detection
- **Archive diffs** — added/removed/modified files and rows by primary key
- **Exploded parquet storage** — write/read from local or cloud (GCS, S3) via fsspec

All files and columns are preserved, including non-standard ones.

## Install

```bash
pip install gtfs-digester
# or
uv add gtfs-digester
```

## Quick Start

```python
from gtfs_digester import GTFSArchive

# Load and fingerprint
archive = GTFSArchive.from_zip("google_transit.zip")
print(archive.fingerprint.hex())  # v1:abc123...

# Access canonical tables
stops = archive.arrow_table("stops.txt")
print(stops.num_rows)

# Compare two feed versions
old = GTFSArchive.from_zip("old.zip")
new = GTFSArchive.from_zip("new.zip")
diff = old.diff(new)
print(diff.is_identical)
for f in diff.modified_files:
    fd = diff.file_diff(f)
    print(f"{f}: {fd.summary()}")

# Write as exploded parquet
from gtfs_digester import write_exploded
write_exploded(archive, "gs://bucket/schedules/feed-1", schedule_url="https://...")
```

## How Fingerprinting Works

1. Each `.txt` file is parsed to a PyArrow table (all strings)
2. Columns reordered per GTFS spec (unknown columns preserved, sorted alphabetically after spec columns)
3. Values normalized: whitespace stripped, times zero-padded (`9:05:00` → `09:05:00`)
4. Rows sorted by primary key (numeric columns sorted numerically, not lexicographically)
5. Canonical CSV serialized and BLAKE3 hashed per file
6. Archive fingerprint = BLAKE3 of sorted `filename:hash` pairs (merkle tree)

Unknown files (not in the GTFS spec) are preserved with lexicographic row sorting.

## Storage Layout

`write_exploded()` produces a version-first directory:

```
base_path/
  _feed_digest={v1:abc...}/
    agency.parquet
    stops.parquet
    routes.parquet
    trips.parquet
    stop_times.parquet
    ...
    metadata.json       # provenance + digester info, written last (commit marker)
```

DuckDB reads across versions with hive partitioning:

```sql
SELECT * FROM read_parquet('base_path/_feed_digest=*/stops.parquet', hive_partitioning=true);
```

## Development

```bash
uv sync --group dev
uv run pytest tests/ -k "not Real"   # unit tests (~0.5s)
uv run pytest tests/                  # includes real feed integration tests
```

## License

MIT
