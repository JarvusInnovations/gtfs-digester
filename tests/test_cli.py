"""CLI tests using click's CliRunner."""

import io
import json
import tempfile
import zipfile
from pathlib import Path

import pytest
from click.testing import CliRunner

from gtfs_digester.cli import main


def _make_zip(files: dict[str, str]) -> bytes:
    """Create a zip file in memory from filename -> CSV content mapping."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


SAMPLE_FEED = {
    "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\nA1,Test,http://test.com,America/New_York",
    "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Main,40.0,-75.0\nS2,Elm,40.1,-75.1",
    "routes.txt": "route_id,agency_id,route_short_name,route_long_name,route_type\nR1,A1,1,Route One,3",
}


@pytest.fixture
def sample_zip(tmp_path: Path) -> Path:
    p = tmp_path / "feed.zip"
    p.write_bytes(_make_zip(SAMPLE_FEED))
    return p


@pytest.fixture
def sample_dir(tmp_path: Path) -> Path:
    d = tmp_path / "feed"
    d.mkdir()
    for name, content in SAMPLE_FEED.items():
        (d / name).write_text(content)
    return d


class TestDigest:
    def test_basic(self, sample_zip: Path):
        result = CliRunner().invoke(main, ["digest", str(sample_zip)])
        assert result.exit_code == 0
        assert "Fingerprint: v1:" in result.output
        assert "Files: 3" in result.output
        assert "agency.txt" in result.output
        assert "stops.txt" in result.output

    def test_directory(self, sample_dir: Path):
        result = CliRunner().invoke(main, ["digest", str(sample_dir)])
        assert result.exit_code == 0
        assert "Fingerprint: v1:" in result.output

    def test_quiet(self, sample_zip: Path):
        result = CliRunner().invoke(main, ["digest", "--quiet", str(sample_zip)])
        assert result.exit_code == 0
        assert result.output.startswith("v1:")
        # Only the fingerprint, no other output
        assert "\n" == result.output[result.output.index("\n"):]

    def test_json(self, sample_zip: Path):
        result = CliRunner().invoke(main, ["digest", "--json", str(sample_zip)])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["fingerprint"].startswith("v1:")
        assert "file_hashes" in data
        assert "file_row_counts" in data
        assert data["file_row_counts"]["stops.txt"] == 2

    def test_unknown_file_marked(self, tmp_path: Path):
        files = {**SAMPLE_FEED, "custom.txt": "col_a,col_b\n1,2"}
        p = tmp_path / "feed.zip"
        p.write_bytes(_make_zip(files))
        result = CliRunner().invoke(main, ["digest", str(p)])
        assert "(unknown)" in result.output

    def test_nonexistent_path(self):
        result = CliRunner().invoke(main, ["digest", "/nonexistent/path"])
        assert result.exit_code != 0


class TestDiff:
    def test_identical(self, sample_zip: Path):
        result = CliRunner().invoke(main, ["diff", str(sample_zip), str(sample_zip)])
        assert result.exit_code == 0
        assert "identical" in result.output

    def test_different(self, sample_zip: Path, tmp_path: Path):
        other_feed = {**SAMPLE_FEED, "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS1,Main,40.0,-75.0\nS3,Oak,40.2,-75.2"}
        other = tmp_path / "feed2.zip"
        other.write_bytes(_make_zip(other_feed))
        result = CliRunner().invoke(main, ["diff", str(sample_zip), str(other)])
        assert result.exit_code == 1
        assert "differ" in result.output
        assert "stops.txt" in result.output

    def test_quiet_identical(self, sample_zip: Path):
        result = CliRunner().invoke(main, ["diff", "--quiet", str(sample_zip), str(sample_zip)])
        assert result.exit_code == 0

    def test_quiet_different(self, sample_zip: Path, tmp_path: Path):
        other_feed = {**SAMPLE_FEED, "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS3,Oak,40.2,-75.2"}
        other = tmp_path / "feed2.zip"
        other.write_bytes(_make_zip(other_feed))
        result = CliRunner().invoke(main, ["diff", "--quiet", str(sample_zip), str(other)])
        assert result.exit_code == 1

    def test_json_identical(self, sample_zip: Path):
        result = CliRunner().invoke(main, ["diff", "--json", str(sample_zip), str(sample_zip)])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["identical"] is True

    def test_json_different(self, sample_zip: Path, tmp_path: Path):
        other_feed = {**SAMPLE_FEED, "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS3,Oak,40.2,-75.2"}
        other = tmp_path / "feed2.zip"
        other.write_bytes(_make_zip(other_feed))
        result = CliRunner().invoke(main, ["diff", "--json", str(sample_zip), str(other)])
        assert result.exit_code == 1
        data = json.loads(result.output)
        assert data["identical"] is False
        assert "stops.txt" in data["modified_files"]

    def test_added_file(self, sample_zip: Path, tmp_path: Path):
        other_feed = {**SAMPLE_FEED, "custom.txt": "a,b\n1,2"}
        other = tmp_path / "feed2.zip"
        other.write_bytes(_make_zip(other_feed))
        result = CliRunner().invoke(main, ["diff", str(sample_zip), str(other)])
        assert result.exit_code == 1
        assert "custom.txt" in result.output


class TestWrite:
    def test_basic(self, sample_zip: Path, tmp_path: Path):
        output = tmp_path / "output"
        result = CliRunner().invoke(main, ["write", str(sample_zip), str(output)])
        assert result.exit_code == 0
        assert "Wrote 3 files" in result.output
        # Check that parquet files were written
        digest_dirs = list(output.glob("_feed_digest=*"))
        assert len(digest_dirs) == 1
        parquets = list(digest_dirs[0].glob("*.parquet"))
        assert len(parquets) == 3
        # Check metadata.json exists
        assert (digest_dirs[0] / "metadata.json").exists()

    def test_with_schedule_url(self, sample_zip: Path, tmp_path: Path):
        output = tmp_path / "output"
        result = CliRunner().invoke(main, [
            "write", str(sample_zip), str(output),
            "--schedule-url", "https://example.com/gtfs.zip",
        ])
        assert result.exit_code == 0


class TestNormalize:
    def test_basic(self, sample_zip: Path, tmp_path: Path):
        output = tmp_path / "clean.zip"
        result = CliRunner().invoke(main, ["normalize", str(sample_zip), str(output)])
        assert result.exit_code == 0
        assert "Normalized 3 files" in result.output
        assert "fingerprint:" in result.output
        assert output.exists()
        # Verify the output is a valid zip with the expected files
        with zipfile.ZipFile(str(output)) as zf:
            names = zf.namelist()
            assert "agency.txt" in names
            assert "stops.txt" in names
            assert ".gtfs_digester.json" in names
