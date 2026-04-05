"""Exploded parquet storage: write/read archives as version-first directories.

Supports local filesystem and cloud storage (GCS, S3) via fsspec.

Layout per the PLAN.md:
    base_path/
      _fingerprint={fp}/
        agency.parquet
        stops.parquet
        ...
        metadata.json       # commit marker, written last
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq

from .archive import GTFSArchive
from .metadata import FeedMetadata


def write_exploded(
    archive: GTFSArchive,
    base_path: str,
    schedule_url: str,
    date_retrieved: str | datetime | None = None,
    source_sha256: str | None = None,
    filesystem: fsspec.AbstractFileSystem | None = None,
) -> FeedMetadata:
    """Write an archive as exploded parquet to a version-first directory.

    Creates:
        {base_path}/_fingerprint={fp}/{table}.parquet  (one per file)
        {base_path}/_fingerprint={fp}/metadata.json    (commit marker, last)

    Args:
        archive: The digested archive to write.
        base_path: Root path for this feed (contains _fingerprint= dirs).
            For GCS: "gs://bucket/schedules/base64url=abc123"
            For local: "/path/to/output"
        schedule_url: Source URL for provenance.
        date_retrieved: When the feed was downloaded. Defaults to now.
        source_sha256: SHA256 of original zip bytes.
        filesystem: fsspec filesystem. If None, inferred from base_path.

    Returns:
        The FeedMetadata that was written.
    """
    if filesystem is None:
        filesystem, base_path = _resolve_fs(base_path)

    fp = archive.fingerprint
    version_dir = f"{base_path}/_fingerprint={fp.root_hash}"

    # Ensure directory exists
    filesystem.mkdirs(version_dir, exist_ok=True)

    # Write each table as parquet
    for filename in sorted(archive.filenames):
        gtfs_file = archive[filename]
        table_name = filename.removesuffix(".txt")
        parquet_path = f"{version_dir}/{table_name}.parquet"

        buf = pa.BufferOutputStream()
        pq.write_table(gtfs_file.table, buf, compression="zstd")
        data = buf.getvalue().to_pybytes()

        with filesystem.open(parquet_path, "wb") as f:
            f.write(data)

    # Build and write metadata.json last (commit marker)
    metadata = FeedMetadata.from_archive(
        archive=archive,
        schedule_url=schedule_url,
        date_retrieved=date_retrieved,
        source_sha256=source_sha256,
    )

    metadata_path = f"{version_dir}/metadata.json"
    with filesystem.open(metadata_path, "wb") as f:
        f.write(metadata.to_bytes())

    return metadata


def read_metadata(
    base_path: str,
    fingerprint: str,
    filesystem: fsspec.AbstractFileSystem | None = None,
) -> FeedMetadata:
    """Read metadata.json for a specific version.

    Args:
        base_path: Root path for the feed.
        fingerprint: The versioned fingerprint string (e.g. "v1:abc...").
        filesystem: fsspec filesystem. If None, inferred from base_path.
    """
    if filesystem is None:
        filesystem, base_path = _resolve_fs(base_path)

    metadata_path = f"{base_path}/_fingerprint={fingerprint}/metadata.json"
    with filesystem.open(metadata_path, "rb") as f:
        return FeedMetadata.from_json(f.read())


def version_exists(
    base_path: str,
    fingerprint: str,
    filesystem: fsspec.AbstractFileSystem | None = None,
) -> bool:
    """Check if a version has been fully ingested (metadata.json exists).

    Args:
        base_path: Root path for the feed.
        fingerprint: The versioned fingerprint string.
        filesystem: fsspec filesystem. If None, inferred from base_path.
    """
    if filesystem is None:
        filesystem, base_path = _resolve_fs(base_path)

    metadata_path = f"{base_path}/_fingerprint={fingerprint}/metadata.json"
    return filesystem.exists(metadata_path)


def list_versions(
    base_path: str,
    filesystem: fsspec.AbstractFileSystem | None = None,
) -> list[str]:
    """List all complete versions (those with metadata.json) for a feed.

    Returns a list of fingerprint strings, sorted.
    """
    if filesystem is None:
        filesystem, base_path = _resolve_fs(base_path)

    versions = []
    try:
        entries = filesystem.ls(base_path, detail=False)
    except FileNotFoundError:
        return []

    for entry in entries:
        # Extract fingerprint from path like .../base64url=x/_fingerprint=v1:abc
        basename = entry.rstrip("/").rsplit("/", 1)[-1]
        if basename.startswith("_fingerprint="):
            fp = basename[len("_fingerprint="):]
            metadata_path = f"{entry}/metadata.json"
            if filesystem.exists(metadata_path):
                versions.append(fp)

    return sorted(versions)


def read_table(
    base_path: str,
    fingerprint: str,
    table_name: str,
    filesystem: fsspec.AbstractFileSystem | None = None,
) -> pa.Table:
    """Read a single parquet table from a version directory.

    Args:
        base_path: Root path for the feed.
        fingerprint: The versioned fingerprint string.
        table_name: Table name without extension (e.g. "stops", "stop_times").
        filesystem: fsspec filesystem. If None, inferred from base_path.
    """
    if filesystem is None:
        filesystem, base_path = _resolve_fs(base_path)

    parquet_path = f"{base_path}/_fingerprint={fingerprint}/{table_name}.parquet"
    with filesystem.open(parquet_path, "rb") as f:
        return pq.read_table(f)


def _resolve_fs(path: str) -> tuple[fsspec.AbstractFileSystem, str]:
    """Resolve an fsspec filesystem from a path string."""
    fs, resolved_path = fsspec.core.url_to_fs(path)
    return fs, resolved_path
