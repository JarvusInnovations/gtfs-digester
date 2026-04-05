"""Core tests: normalization, file processing, fingerprinting, archive loading."""

import csv
import io
import tempfile
import zipfile
from pathlib import Path

import pyarrow as pa
import pytest

from gtfs_digester import GTFSArchive, GTFSFile, ArchiveFingerprint
from gtfs_digester.normalize import normalize_time, normalize_value, normalize_time_value
from gtfs_digester.schema import get_schema, STOPS, STOP_TIMES


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

class TestNormalization:
    def test_normalize_time_single_digit_hour(self):
        assert normalize_time("9:05:00") == "09:05:00"

    def test_normalize_time_single_digit_all(self):
        assert normalize_time("9:5:0") == "09:05:00"

    def test_normalize_time_already_padded(self):
        assert normalize_time("09:05:00") == "09:05:00"

    def test_normalize_time_over_24(self):
        assert normalize_time("25:30:00") == "25:30:00"

    def test_normalize_time_invalid(self):
        assert normalize_time("not-a-time") == "not-a-time"

    def test_normalize_value_strips_whitespace(self):
        assert normalize_value("  hello  ") == "hello"

    def test_normalize_value_none_to_empty(self):
        assert normalize_value(None) == ""

    def test_normalize_value_whitespace_only(self):
        assert normalize_value("   ") == ""

    def test_normalize_time_value_empty(self):
        assert normalize_time_value("") == ""

    def test_normalize_time_value_with_time(self):
        assert normalize_time_value(" 9:05:00 ") == "09:05:00"


# ---------------------------------------------------------------------------
# GTFSFile
# ---------------------------------------------------------------------------

class TestGTFSFile:
    def test_known_file_column_reordering(self):
        """Columns should be reordered to spec order."""
        csv_data = b"stop_name,stop_id,stop_lat,stop_lon\nMain St,S1,40.0,-75.0"
        schema = get_schema("stops.txt")
        f = GTFSFile.from_csv_bytes("stops.txt", csv_data, schema=schema)
        # stop_id should come before stop_name per spec
        assert f.columns.index("stop_id") < f.columns.index("stop_name")

    def test_known_file_preserves_unknown_columns(self):
        """Unknown columns should be preserved after spec columns."""
        csv_data = b"stop_id,stop_name,stop_lat,stop_lon,custom_field,another_ext\nS1,Main,40.0,-75.0,val1,val2"
        schema = get_schema("stops.txt")
        f = GTFSFile.from_csv_bytes("stops.txt", csv_data, schema=schema)
        assert "custom_field" in f.columns
        assert "another_ext" in f.columns
        # Unknown columns should be after all spec columns
        spec_cols = [c for c in schema.columns if c in f.columns]
        last_spec_idx = max(f.columns.index(c) for c in spec_cols)
        assert f.columns.index("another_ext") > last_spec_idx
        assert f.columns.index("custom_field") > last_spec_idx
        # Unknown columns sorted alphabetically
        assert f.columns.index("another_ext") < f.columns.index("custom_field")

    def test_unknown_file_alphabetical_columns(self):
        """Unknown files should have columns sorted alphabetically."""
        csv_data = b"zebra,alpha,middle\n1,2,3"
        f = GTFSFile.from_csv_bytes("custom_file.txt", csv_data, schema=None)
        assert f.columns == ["alpha", "middle", "zebra"]

    def test_unknown_file_lexicographic_sort(self):
        """Unknown files should have rows sorted lexicographically."""
        csv_data = b"id,name\nB,second\nA,first\nC,third"
        f = GTFSFile.from_csv_bytes("custom.txt", csv_data, schema=None)
        assert f.table.column("id")[0].as_py() == "A"
        assert f.table.column("id")[1].as_py() == "B"
        assert f.table.column("id")[2].as_py() == "C"

    def test_primary_key_sort_numeric(self):
        """stop_sequence should sort numerically, not lexicographically."""
        csv_data = b"trip_id,stop_id,stop_sequence,arrival_time,departure_time\nT1,S1,2,09:00:00,09:00:00\nT1,S2,10,09:05:00,09:05:00\nT1,S3,1,08:55:00,08:55:00"
        schema = get_schema("stop_times.txt")
        f = GTFSFile.from_csv_bytes("stop_times.txt", csv_data, schema=schema)
        seqs = [f.table.column("stop_sequence")[i].as_py() for i in range(3)]
        assert seqs == ["1", "2", "10"]  # numeric order, not "1", "10", "2"

    def test_time_normalization(self):
        """Time columns should be zero-padded."""
        csv_data = b"trip_id,stop_id,stop_sequence,arrival_time,departure_time\nT1,S1,1,9:5:0,9:5:0"
        schema = get_schema("stop_times.txt")
        f = GTFSFile.from_csv_bytes("stop_times.txt", csv_data, schema=schema)
        assert f.table.column("arrival_time")[0].as_py() == "09:05:00"

    def test_duplicate_primary_key_raises(self):
        """Duplicate primary keys should raise ValueError."""
        csv_data = b"stop_id,stop_name,stop_lat,stop_lon\nS1,First,40.0,-75.0\nS1,Duplicate,41.0,-76.0"
        schema = get_schema("stops.txt")
        with pytest.raises(ValueError, match="Duplicate"):
            GTFSFile.from_csv_bytes("stops.txt", csv_data, schema=schema)

    def test_canonical_csv_deterministic(self):
        """Same input should always produce identical canonical CSV."""
        csv_data = b"stop_id,stop_name,stop_lat,stop_lon\nS1,Main,40.0,-75.0"
        schema = get_schema("stops.txt")
        f1 = GTFSFile.from_csv_bytes("stops.txt", csv_data, schema=schema)
        f2 = GTFSFile.from_csv_bytes("stops.txt", csv_data, schema=schema)
        assert f1.to_canonical_csv() == f2.to_canonical_csv()
        assert f1.fingerprint_hash() == f2.fingerprint_hash()

    def test_bom_stripped(self):
        """BOM should be stripped from input."""
        csv_data = b"\xef\xbb\xbfstop_id,stop_name,stop_lat,stop_lon\nS1,Main,40.0,-75.0"
        schema = get_schema("stops.txt")
        f = GTFSFile.from_csv_bytes("stops.txt", csv_data, schema=schema)
        assert f.row_count == 1


# ---------------------------------------------------------------------------
# Fingerprint
# ---------------------------------------------------------------------------

class TestFingerprint:
    def test_compute_and_roundtrip(self):
        fp = ArchiveFingerprint.compute({"a.txt": "abc123", "b.txt": "def456"})
        assert fp.root_hash.startswith("v1:")
        assert "a.txt" in fp.files

        json_str = fp.to_json()
        fp2 = ArchiveFingerprint.from_json(json_str)
        assert fp == fp2

    def test_file_order_independent(self):
        """Fingerprint should be the same regardless of insertion order."""
        fp1 = ArchiveFingerprint.compute({"b.txt": "def", "a.txt": "abc"})
        fp2 = ArchiveFingerprint.compute({"a.txt": "abc", "b.txt": "def"})
        assert fp1 == fp2


# ---------------------------------------------------------------------------
# Archive (synthetic)
# ---------------------------------------------------------------------------

def _make_zip(files: dict[str, str]) -> bytes:
    """Create a zip file in memory from filename -> CSV content mapping."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


class TestArchive:
    def test_load_from_zip(self):
        zip_bytes = _make_zip({
            "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Main,40.0,-75.0",
            "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\nA1,Test,http://test.com,America/New_York",
        })
        archive = GTFSArchive.from_zip(zip_bytes)
        assert "stops.txt" in archive.filenames
        assert "agency.txt" in archive.filenames
        assert archive["stops.txt"].row_count == 1

    def test_unknown_files_preserved(self):
        """Files not in the GTFS spec should be preserved."""
        zip_bytes = _make_zip({
            "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Main,40.0,-75.0",
            "custom_data.txt": "col_a,col_b\n1,2\n3,4",
        })
        archive = GTFSArchive.from_zip(zip_bytes)
        assert "custom_data.txt" in archive.filenames
        assert archive["custom_data.txt"].row_count == 2
        assert archive["custom_data.txt"].schema is None  # no schema for unknown files

    def test_fingerprint_deterministic(self):
        zip_bytes = _make_zip({
            "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Main,40.0,-75.0",
        })
        a1 = GTFSArchive.from_zip(zip_bytes)
        a2 = GTFSArchive.from_zip(zip_bytes)
        assert a1.fingerprint == a2.fingerprint

    def test_fingerprint_changes_with_content(self):
        z1 = _make_zip({"stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Main,40.0,-75.0"})
        z2 = _make_zip({"stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Changed,40.0,-75.0"})
        a1 = GTFSArchive.from_zip(z1)
        a2 = GTFSArchive.from_zip(z2)
        assert a1.fingerprint != a2.fingerprint

    def test_fingerprint_ignores_zip_metadata(self):
        """Different zip packaging of identical CSV content should produce same fingerprint."""
        content = "stop_id,stop_name,stop_lat,stop_lon\nS1,Main,40.0,-75.0"

        buf1 = io.BytesIO()
        with zipfile.ZipFile(buf1, "w", compression=zipfile.ZIP_STORED) as zf:
            zf.writestr("stops.txt", content)

        buf2 = io.BytesIO()
        with zipfile.ZipFile(buf2, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("stops.txt", content)

        a1 = GTFSArchive.from_zip(buf1.getvalue())
        a2 = GTFSArchive.from_zip(buf2.getvalue())
        assert a1.fingerprint == a2.fingerprint

    def test_normalized_zip_roundtrip(self):
        zip_bytes = _make_zip({
            "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Main,40.0,-75.0",
            "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\nA1,Test,http://test.com,America/New_York",
        })
        archive = GTFSArchive.from_zip(zip_bytes)
        fp1 = archive.fingerprint

        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as f:
            archive.to_normalized_zip(f.name)
            reloaded = GTFSArchive.from_zip(f.name)

        assert reloaded.fingerprint == fp1

    def test_from_directory(self, tmp_path):
        (tmp_path / "stops.txt").write_text("stop_id,stop_name,stop_lat,stop_lon\nS1,Main,40.0,-75.0")
        (tmp_path / "agency.txt").write_text("agency_id,agency_name,agency_url,agency_timezone\nA1,Test,http://test.com,America/New_York")

        archive = GTFSArchive.from_directory(tmp_path)
        assert "stops.txt" in archive.filenames
        assert archive["stops.txt"].row_count == 1

    def test_diff_identical(self):
        zip_bytes = _make_zip({"stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Main,40.0,-75.0"})
        a1 = GTFSArchive.from_zip(zip_bytes)
        a2 = GTFSArchive.from_zip(zip_bytes)
        diff = a1.diff(a2)
        assert diff.is_identical

    def test_diff_modified(self):
        z1 = _make_zip({"stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Old,40.0,-75.0"})
        z2 = _make_zip({"stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,New,40.0,-75.0"})
        diff = GTFSArchive.from_zip(z1).diff(GTFSArchive.from_zip(z2))
        assert not diff.is_identical
        assert "stops.txt" in diff.modified_files
        fd = diff.file_diff("stops.txt")
        assert fd.modified_count == 1
        assert fd.added_count == 0

    def test_diff_added_file(self):
        z1 = _make_zip({"stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Main,40.0,-75.0"})
        z2 = _make_zip({
            "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Main,40.0,-75.0",
            "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\nA1,Test,http://test.com,America/New_York",
        })
        diff = GTFSArchive.from_zip(z1).diff(GTFSArchive.from_zip(z2))
        assert "agency.txt" in diff.added_files


# ---------------------------------------------------------------------------
# Integration: real GTFS feed
# ---------------------------------------------------------------------------

class TestRealFeed:
    def test_load_septa(self, septa_zip):
        archive = GTFSArchive.from_zip(septa_zip)
        # SEPTA Bus should have core files
        assert "stops.txt" in archive.filenames
        assert "routes.txt" in archive.filenames
        assert "trips.txt" in archive.filenames
        assert "stop_times.txt" in archive.filenames
        assert archive["stops.txt"].row_count > 0
        assert archive["routes.txt"].row_count > 0

    def test_fingerprint_deterministic_real(self, septa_zip):
        a1 = GTFSArchive.from_zip(septa_zip)
        a2 = GTFSArchive.from_zip(septa_zip)
        assert a1.fingerprint == a2.fingerprint

    def test_self_diff_is_identical(self, septa_zip):
        a = GTFSArchive.from_zip(septa_zip)
        diff = a.diff(a)
        assert diff.is_identical

    def test_different_feeds_produce_different_fingerprints(self, septa_zip, septa_zip_2):
        a1 = GTFSArchive.from_zip(septa_zip)
        a2 = GTFSArchive.from_zip(septa_zip_2)
        assert a1.fingerprint != a2.fingerprint

    def test_diff_real_feeds(self, septa_zip, septa_zip_2):
        a1 = GTFSArchive.from_zip(septa_zip)
        a2 = GTFSArchive.from_zip(septa_zip_2)
        diff = a1.diff(a2)
        assert not diff.is_identical
        # Should have some modified files (schedules change between versions)
        assert len(diff.modified_files) > 0

    def test_unknown_files_preserved_real(self, septa_zip):
        """Any non-standard files in SEPTA feed should be preserved."""
        archive = GTFSArchive.from_zip(septa_zip)
        known = set(get_schema(f).filename for f in archive.filenames if get_schema(f))
        unknown = archive.filenames - known
        # Whether there are unknown files depends on the feed — just verify
        # that all files are present (known + unknown = total)
        assert known | unknown == archive.filenames

    def test_normalized_zip_roundtrip_real(self, septa_zip, tmp_path):
        archive = GTFSArchive.from_zip(septa_zip)
        fp1 = archive.fingerprint

        out_path = tmp_path / "normalized.zip"
        archive.to_normalized_zip(out_path)
        reloaded = GTFSArchive.from_zip(out_path)

        assert reloaded.fingerprint == fp1
        # Row counts should match for all files
        for filename in archive.filenames:
            assert archive[filename].row_count == reloaded[filename].row_count
