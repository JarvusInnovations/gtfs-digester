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

## CLI

```bash
# Fingerprint a feed
gtfs-digester digest google_transit.zip
# Fingerprint: v1:abc123...
# Files: 11
#   agency.txt        1 rows
#   stops.txt      9245 rows
#   ...

# JSON output or just the hash
gtfs-digester digest --json google_transit.zip
gtfs-digester digest --quiet google_transit.zip

# Diff two feeds (exit code 1 if different)
gtfs-digester diff old.zip new.zip

# Write as exploded parquet
gtfs-digester write google_transit.zip ./output --schedule-url https://example.com/gtfs.zip

# Produce a normalized GTFS zip
gtfs-digester normalize messy.zip clean.zip
```

## Python API

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

### Tables already in memory

`GTFSArchive.from_tables()` builds an archive from in-memory tables with no zip
and no CSV round-trip. Each value is a `pyarrow.Table` or any object that exports
an Arrow stream (`__arrow_c_stream__`), such as a polars DataFrame. Every column
must be a string type (`string`, `large_string` or `string_view`).

```python
import polars as pl
from gtfs_digester import GTFSArchive

frames = {"stops.txt": stops_df, "stop_times.txt": stop_times_df}  # all-string polars frames
archive = GTFSArchive.from_tables(frames)
print(archive.fingerprint.hex())
```

The fingerprint equals the one `from_zip()` gives for a zip whose files parse to
the same strings. Parsers differ on malformed CSV, so the strings can differ: polars
keeps a blank line as a row of nulls and fills short rows, while `from_zip()` (pyarrow)
skips blank lines and skips a file with short rows.

## How Fingerprinting Works

1. Each `.txt` file is parsed to a PyArrow table (all strings)
2. Columns reordered per GTFS spec (unknown columns preserved, sorted alphabetically after spec columns)
3. Values normalized: whitespace stripped, times zero-padded (`9:05:00` → `09:05:00`)
4. Rows sorted by primary key (numeric columns sorted numerically, not lexicographically)
5. Canonical CSV serialized and BLAKE3 hashed per file
6. Archive fingerprint = BLAKE3 of sorted `filename:hash` pairs (merkle tree)

Unknown files (not in the GTFS spec) are preserved with lexicographic row sorting.

## Canonical CSV

The canonical CSV is the byte form each file hash covers. Every implementation
that computes `v1:` fingerprints must write these bytes exactly; a single byte of
difference changes the digest.

| Property | Rule |
| --- | --- |
| Encoding | UTF-8, no byte-order mark (a BOM in the input is removed). |
| Rows | A header row of column names, then one row per record. |
| Header row | Column names are trimmed of leading and trailing whitespace when the table is built (`from_zip`, `from_directory`, `from_csv_bytes`, `from_arrow_table` and `from_tables` all trim; a `GTFSFile` built directly with its constructor keeps its names as given). After that, names are written exactly as they are, empty and repeated names included (a trailing-comma header stays `stop_id,stop_name,`), with the same quoting rules as values. |
| Column order | Known files: the spec columns that are present, in spec order, then any other columns sorted by code point. Unknown files: all columns sorted by code point. |
| Row order | Known files: by the primary key columns, then by the remaining columns, ascending; columns the schema marks numeric compare as numbers, the rest as strings. Unknown files: by every column, as strings. |
| Values | Strings only, after normalization: leading and trailing space, tab, CR and LF removed; times in time columns zero-padded (`9:05:00` becomes `09:05:00`). No other change: numbers keep their written form. |
| Delimiter | Comma (`,`). |
| Line endings | LF (`\n`) after each row except the last. No trailing newline. |
| Quoting | A field is enclosed in double quotes when it contains a comma, a double quote, CR or LF. A double quote inside a field is written twice (`"Say ""hi"""`). No other field is quoted: spaces, tabs and every other character are written as is. |
| Line breaks inside a field | Kept byte for byte inside the quotes: CR, LF and CRLF are not converted. |
| Empty value | Written as nothing (`a,,b`), except when it is the only field in the row (a single-column table), where it is written as `""` so the row is not a blank line. |
| Null | The same as an empty value. Canonical tables hold no nulls (canonicalization turns null into an empty string), and the writer treats any null as an empty string. |
| Empty tables | A table with columns and no rows is the header row alone, with no newline. A table with no columns is zero bytes. |

These are the bytes Python's `csv.writer(out, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)`
writes with the default dialect (quote character `"`, doubled quotes, no escape
character), with the final `\n` removed. That writer is the reference
(`gtfs_digester.file._reference_canonical_csv`).

File hash: `v1:` + BLAKE3 hex of the canonical CSV bytes. Archive hash: `v1:` +
BLAKE3 hex of the lines `filename:v1:<hex>`, sorted by filename and joined with
`\n` (no trailing newline), encoded as UTF-8.

### How the bytes are produced

`GTFSFile.fingerprint_hash()` streams the canonical CSV into BLAKE3 a slice of
200,000 rows at a time: each zero-copy slice of the Arrow table is written by
polars `write_csv(quote_style="necessary", line_terminator="\n")`. Polars quotes
the same characters as the reference. It writes an empty string as `""` and a
null as nothing, which is the reverse of the reference for empty values, so
empty strings become null before writing in tables with more than one column,
and nulls become empty strings in single-column tables. The header row is written by the
reference writer, so column names come out exactly (polars would rename an empty
name). The slices reach polars under positional names (`c0`, `c1`, …), so tables
with empty or repeated column names, which only the `GTFSFile` constructor can
hold, stream the same bytes as the reference. No Python object is created per value. Tables with non-string columns fall
back to the reference writer.

`tests/test_golden.py` pins the root and per-file digests of seven public feeds
and hand-made edge cases (under `tests/fixtures/`), recorded from the reference
writer in gtfs-digester 0.4.0. It fails if any canonical byte moves.
`tests/test_streamed_csv.py` compares the streamed writer with the reference on
generated tables.

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
