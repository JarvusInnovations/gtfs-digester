"""Tests for exploded parquet storage and metadata."""

import io
import json
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gtfs_digester import (
    GTFSArchive,
    FeedMetadata,
    write_exploded,
    read_metadata,
    read_table,
    list_versions,
    version_exists,
)


def _make_zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


SAMPLE_ZIP = _make_zip({
    "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Main St,40.0,-75.0\nS2,Elm Ave,40.1,-75.1",
    "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\nA1,Test Agency,http://test.com,America/New_York",
    "routes.txt": "route_id,agency_id,route_short_name,route_long_name,route_type\nR1,A1,1,Route One,3",
    "feed_info.txt": "feed_publisher_name,feed_publisher_url,feed_lang,feed_start_date,feed_end_date\nTest,http://test.com,en,20260101,20260630",
})


class TestFeedMetadata:
    def test_from_archive(self):
        archive = GTFSArchive.from_zip(SAMPLE_ZIP)
        meta = FeedMetadata.from_archive(
            archive,
            schedule_url="https://example.com/gtfs.zip",
            date_retrieved="2026-04-05T12:00:00Z",
            source_sha256="abc123",
        )
        assert meta._feed_digest == archive.fingerprint.root_hash
        assert meta.schedule_url == "https://example.com/gtfs.zip"
        assert meta.date_retrieved == "2026-04-05T12:00:00Z"
        assert meta.feed_start_date == "20260101"
        assert meta.feed_end_date == "20260630"
        assert meta.source_sha256 == "abc123"
        assert "stops.txt" in meta.file_hashes
        assert meta.file_row_counts["stops.txt"] == 2
        assert meta.file_row_counts["agency.txt"] == 1

    def test_json_roundtrip(self):
        archive = GTFSArchive.from_zip(SAMPLE_ZIP)
        meta = FeedMetadata.from_archive(
            archive,
            schedule_url="https://example.com/gtfs.zip",
            date_retrieved="2026-04-05T12:00:00Z",
        )
        json_str = meta.to_json()
        parsed = json.loads(json_str)
        assert parsed["_feed_digest"] == meta._feed_digest
        assert parsed["feed_start_date"] == "20260101"

        meta2 = FeedMetadata.from_json(json_str)
        assert meta2._feed_digest == meta._feed_digest
        assert meta2.file_row_counts == meta.file_row_counts

    def test_no_feed_info(self):
        """Archives without feed_info.txt should have null dates."""
        zip_bytes = _make_zip({
            "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Main,40.0,-75.0",
        })
        archive = GTFSArchive.from_zip(zip_bytes)
        meta = FeedMetadata.from_archive(archive, schedule_url="http://x.com/g.zip")
        assert meta.feed_start_date is None
        assert meta.feed_end_date is None


class TestExplodedStorage:
    def test_write_and_read(self, tmp_path):
        archive = GTFSArchive.from_zip(SAMPLE_ZIP)
        base = str(tmp_path / "feed")

        meta = write_exploded(
            archive,
            base_path=base,
            schedule_url="https://example.com/gtfs.zip",
            date_retrieved="2026-04-05T12:00:00Z",
        )

        fp = archive.fingerprint.root_hash
        version_dir = tmp_path / "feed" / f"_feed_digest={fp}"

        # Check files exist
        assert (version_dir / "stops.parquet").exists()
        assert (version_dir / "agency.parquet").exists()
        assert (version_dir / "routes.parquet").exists()
        assert (version_dir / "feed_info.parquet").exists()
        assert (version_dir / "metadata.json").exists()

        # Read back metadata
        meta2 = read_metadata(base, fp)
        assert meta2._feed_digest == meta._feed_digest
        assert meta2.schedule_url == "https://example.com/gtfs.zip"

        # Read back a table
        stops = read_table(base, fp, "stops")
        assert stops.num_rows == 2
        assert "stop_id" in stops.column_names

    def test_version_exists(self, tmp_path):
        archive = GTFSArchive.from_zip(SAMPLE_ZIP)
        base = str(tmp_path / "feed")
        fp = archive.fingerprint.root_hash

        assert not version_exists(base, fp)

        write_exploded(archive, base, schedule_url="http://x.com/g.zip")

        assert version_exists(base, fp)
        assert not version_exists(base, "v1:nonexistent")

    def test_list_versions(self, tmp_path):
        base = str(tmp_path / "feed")

        # No versions yet
        assert list_versions(base) == []

        # Write first version
        a1 = GTFSArchive.from_zip(SAMPLE_ZIP)
        write_exploded(a1, base, schedule_url="http://x.com/g.zip")

        # Write second version (different content)
        zip2 = _make_zip({
            "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Changed,40.0,-75.0",
            "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\nA1,Test,http://test.com,America/New_York",
        })
        a2 = GTFSArchive.from_zip(zip2)
        write_exploded(a2, base, schedule_url="http://x.com/g.zip")

        versions = list_versions(base)
        assert len(versions) == 2
        assert a1.fingerprint.root_hash in versions
        assert a2.fingerprint.root_hash in versions

    def test_idempotent_write(self, tmp_path):
        """Writing the same version twice should be safe (content-addressed)."""
        archive = GTFSArchive.from_zip(SAMPLE_ZIP)
        base = str(tmp_path / "feed")

        meta1 = write_exploded(archive, base, schedule_url="http://x.com/g.zip", date_retrieved="2026-01-01T00:00:00Z")
        meta2 = write_exploded(archive, base, schedule_url="http://x.com/g.zip", date_retrieved="2026-01-02T00:00:00Z")

        # Same fingerprint
        assert meta1._feed_digest == meta2._feed_digest

        # Only one version directory
        assert len(list_versions(base)) == 1

        # metadata.json reflects the latest write
        meta_read = read_metadata(base, archive.fingerprint.root_hash)
        assert meta_read.date_retrieved == "2026-01-02T00:00:00Z"

    def test_parquet_readable_with_pyarrow(self, tmp_path):
        """Verify written parquet files are valid and queryable."""
        archive = GTFSArchive.from_zip(SAMPLE_ZIP)
        base = str(tmp_path / "feed")
        write_exploded(archive, base, schedule_url="http://x.com/g.zip")

        fp = archive.fingerprint.root_hash
        version_dir = tmp_path / "feed" / f"_feed_digest={fp}"

        # Read directly with PyArrow
        stops = pq.read_table(version_dir / "stops.parquet")
        assert stops.num_rows == 2
        assert stops.column("stop_id")[0].as_py() == "S1"

        # All data columns should be string type (hive partition columns may differ)
        for col_name in stops.column_names:
            if not col_name.startswith("_feed_digest"):
                assert stops.schema.field(col_name).type == pa.string()

    def test_unknown_files_written(self, tmp_path):
        """Non-standard GTFS files should be written as parquet too."""
        zip_bytes = _make_zip({
            "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Main,40.0,-75.0",
            "custom_data.txt": "col_a,col_b\nfoo,bar",
        })
        archive = GTFSArchive.from_zip(zip_bytes)
        base = str(tmp_path / "feed")
        write_exploded(archive, base, schedule_url="http://x.com/g.zip")

        fp = archive.fingerprint.root_hash
        version_dir = tmp_path / "feed" / f"_feed_digest={fp}"

        assert (version_dir / "custom_data.parquet").exists()
        custom = pq.read_table(version_dir / "custom_data.parquet")
        assert custom.num_rows == 1
        assert custom.column("col_a")[0].as_py() == "foo"


class TestExplodedStorageRealFeed:
    def test_write_septa(self, septa_zip, tmp_path):
        """Write a real SEPTA feed as exploded parquet."""
        archive = GTFSArchive.from_zip(septa_zip)
        base = str(tmp_path / "septa")

        meta = write_exploded(
            archive,
            base,
            schedule_url="https://www3.septa.org/developer/google_bus.zip",
            date_retrieved="2025-12-20T01:21:22Z",
        )

        fp = archive.fingerprint.root_hash
        version_dir = tmp_path / "septa" / f"_feed_digest={fp}"

        # Core files exist as parquet
        assert (version_dir / "stops.parquet").exists()
        assert (version_dir / "routes.parquet").exists()
        assert (version_dir / "trips.parquet").exists()
        assert (version_dir / "stop_times.parquet").exists()
        assert (version_dir / "metadata.json").exists()

        # Metadata has correct provenance
        assert meta.schedule_url == "https://www3.septa.org/developer/google_bus.zip"
        assert meta.date_retrieved == "2025-12-20T01:21:22Z"
        assert meta.file_row_counts["stops.txt"] > 0
        assert meta.file_row_counts["stop_times.txt"] > 100000  # SEPTA has millions

        # Read back stops
        stops = read_table(base, fp, "stops")
        assert stops.num_rows == archive["stops.txt"].row_count
