"""The streamed canonical CSV writer must reproduce the reference bytes exactly.

The reference is Python's csv.writer (file._reference_canonical_csv), which
defined the v1 canonical bytes. These tests compare the two on generated tables
that hit every quoting rule and slice boundary, check the Arrow time padding
against the per-value function, and check that fingerprinting a large table
creates no Python value per cell.
"""

from __future__ import annotations

import io
import random
import tracemalloc
import warnings
import zipfile

import polars as pl
import pyarrow as pa
import pyarrow.csv
import pytest

from gtfs_digester import GTFSArchive, GTFSFile, file as file_mod
from gtfs_digester.normalize import _normalize_time_column, normalize_time_value

from golden_cases import EXPECTED_FEED_DIGESTS, FEED_SOURCES, FEEDS

# Pieces that exercise each quoting rule: separator, quote, CR, LF, CRLF,
# whitespace (never quoted), non-ASCII, and characters csv.writer leaves alone.
PIECES = ["", ",", '"', "\r", "\n", "\r\n", " ", "\t", "a", "Z9", "é", "昭島", "#", "'", ";", "\\"]


def _random_value(rng: random.Random) -> str | None:
    roll = rng.random()
    if roll < 0.15:
        return None
    if roll < 0.3:
        return ""
    return "".join(rng.choice(PIECES) for _ in range(rng.randint(1, 4)))


# Column names, including the ones polars would rename or reject: empty,
# polars' own default names, quoting triggers, and repeats.
NAMES = ["", "", "column_0", "column_1", "c0", "a,b", 'q"', "n\nl", " sp ", "é", "stop_id"]


def _random_table(rng: random.Random, string_type: pa.DataType) -> pa.Table:
    ncols = rng.randint(1, 4)
    nrows = rng.randint(0, 40)
    names = [rng.choice(NAMES) if rng.random() < 0.5 else f"c{i}" for i in range(ncols)]
    arrays = [pa.array([_random_value(rng) for _ in range(nrows)], type=string_type) for _ in names]
    return pa.Table.from_arrays(arrays, names=names)


@pytest.fixture
def small_slices(monkeypatch):
    """Slice every 7 rows so most generated tables cross slice boundaries."""
    monkeypatch.setattr(file_mod, "CANONICAL_CSV_CHUNK_ROWS", 7)


class TestStreamedMatchesReference:
    @pytest.mark.parametrize("string_type", [pa.string(), pa.large_string(), pa.string_view()])
    def test_generated_tables(self, small_slices, string_type):
        rng = random.Random(1760)
        for _ in range(150):
            table = _random_table(rng, string_type)
            streamed = GTFSFile("x.txt", table).to_canonical_csv()
            assert streamed == file_mod._reference_canonical_csv(table), table.to_pylist()

    @pytest.mark.parametrize(
        "columns",
        [
            {"only": [""]},
            {"only": [None]},
            {"only": ["", "", ""]},
            {"only": ["a", "", None, "b"]},
            {"a": [""], "b": [""]},
            {"a": [None], "b": [""]},
            {"a": ["x\r", "\ry"], "b": ["\r\n", "\n"]},
            {"a": pa.array([], type=pa.string()), "b": pa.array([], type=pa.string())},
            {"only": pa.array([], type=pa.string())},
        ],
    )
    def test_edge_tables(self, small_slices, columns):
        table = pa.table(columns)
        assert GTFSFile("x.txt", table).to_canonical_csv() == file_mod._reference_canonical_csv(table)

    @pytest.mark.parametrize(
        "names",
        [["stop_id", "stop_name", ""], ["", "a", "b"], ["agency_id", "", "agency_name"], [""], ["", ""], ["x", "x"]],
    )
    def test_empty_and_repeated_column_names(self, small_slices, names):
        arrays = [pa.array(["v", "", None, "w,z"] * 4, type=pa.string()) for _ in names]
        table = pa.Table.from_arrays(arrays, names=names)
        assert GTFSFile("x.txt", table).to_canonical_csv() == file_mod._reference_canonical_csv(table)

    def test_chunked_string_view_table(self, small_slices):
        chunks = [["a", "", None, "b,c"], ["d\re", "", "f"], []]
        table = pa.table(
            {
                "a": pa.chunked_array(chunks, type=pa.string_view()),
                "b": pa.chunked_array([["1"] * 4, ["2"] * 3, []], type=pa.string_view()),
            }
        )
        single = pa.table({"only": pa.chunked_array(chunks, type=pa.string_view())})
        for t in (table, single):
            assert GTFSFile("x.txt", t).to_canonical_csv() == file_mod._reference_canonical_csv(t)

    def test_zero_columns(self):
        assert GTFSFile("x.txt", pa.table({})).to_canonical_csv() == b""

    def test_exact_slice_multiple(self, small_slices):
        table = pa.table({"a": [str(i) for i in range(14)], "b": [""] * 14})
        assert GTFSFile("x.txt", table).to_canonical_csv() == file_mod._reference_canonical_csv(table)

    def test_non_string_table_uses_reference(self):
        table = pa.table({"a": [1, None, 3], "b": ["x", "", None]})
        assert GTFSFile("x.txt", table).to_canonical_csv() == b"a,b\n1,x\n,\n3,"

    def test_fingerprint_hash_streams_same_bytes(self, small_slices):
        from gtfs_digester.fingerprint import hash_bytes

        table = pa.table({"a": [f"v{i}" for i in range(30)], "b": ["", "q,\"r"] * 15})
        f = GTFSFile("x.txt", table)
        assert f.fingerprint_hash() == hash_bytes(file_mod._reference_canonical_csv(table))


def _list_based_time_column(column: pa.ChunkedArray) -> list[str]:
    """The 0.4.0 time padding, verbatim apart from returning a list: the oracle."""
    import pyarrow.compute as pc

    from gtfs_digester.normalize import normalize_time

    stripped = pc.if_else(pc.is_null(column), "", pc.utf8_trim(column, " \t\r\n"))
    needs_padding = pc.match_substring_regex(stripped, r"^\d{1,3}:\d{1,2}:\d{1,2}$")
    already_padded = pc.match_substring_regex(stripped, r"^\d{2,3}:\d{2}:\d{2}$")
    needs_fix = pc.and_(needs_padding, pc.invert(already_padded))
    result = stripped.to_pylist()
    for i, fix in enumerate(needs_fix.to_pylist()):
        if fix:
            result[i] = normalize_time(result[i])
    return result


class TestArrowTimePadding:
    def test_matches_list_based_padding(self):
        rng = random.Random(48)
        values = ["", " ", "abc", "1:2", "24:00:00", "007:05:00", " 7:00:00 ", None]
        for _ in range(5000):
            parts = [
                "".join(rng.choice("0123456789") for _ in range(rng.randint(0, 4))) for _ in range(3)
            ]
            value = ":".join(parts)
            if rng.random() < 0.2:
                value = rng.choice([" ", "\t"]) + value + rng.choice(["", " ", "\r\n"])
            values.append(value)
        column = pa.chunked_array([values[:2000], values[2000:]], type=pa.string())
        assert _normalize_time_column(column).to_pylist() == _list_based_time_column(column)

    def test_padded_values_match_per_value_function(self):
        values = ["5:07:00", "012:3:4", " 7:0:0 ", "24:5:00", "000:1:1", "1:2:3", "100:0:0"]
        column = pa.chunked_array([values], type=pa.string())
        assert _normalize_time_column(column).to_pylist() == [normalize_time_value(v) for v in values]


def _large_stop_times(rows: int) -> pa.Table:
    i = pl.int_range(rows)
    df = pl.select(
        trip_id=(i // 40).cast(pl.String),
        arrival_time=pl.format("{}:{}:00", (i // 60) % 30, i % 60),
        departure_time=pl.format("{}:{}:30", (i // 60) % 30, i % 60),
        stop_id=(i % 5000).cast(pl.String),
        stop_sequence=(i % 40).cast(pl.String),
        stop_headsign=pl.when(i % 3 == 0).then(pl.lit("")).otherwise(pl.lit("Downtown, via 5th")),
    )
    return df.to_arrow()


class TestLargeTableMemory:
    ROWS = 500_000
    # A Python str per cell for 3 M cells is about 200 MiB of Python heap.
    # Arrow and polars buffers are not traced, so the bound only covers Python objects.
    PYTHON_HEAP_BOUND = 64 * 2**20

    def test_canonicalize_and_fingerprint_create_no_python_values(self, monkeypatch):
        table = _large_stop_times(self.ROWS)

        def no_reference(_table):
            raise AssertionError("streamed path fell back to the csv.writer reference serializer")

        monkeypatch.setattr(file_mod, "_reference_canonical_csv", no_reference)

        tracemalloc.start()
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                f = GTFSFile.from_arrow_table("stop_times.txt", table, _schema("stop_times.txt"))
            digest = f.fingerprint_hash()
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()

        assert len(digest) == 64
        assert f.table.column("arrival_time")[1].as_py() == "00:01:00"
        assert peak < self.PYTHON_HEAP_BOUND, f"Python heap peak {peak / 2**20:.1f} MiB"


def _schema(name: str):
    from gtfs_digester import get_schema

    return get_schema(name)


def _pyarrow_tables(zip_path) -> dict[str, pa.Table]:
    """Parse each CSV member the way GTFSFile.from_csv_bytes does, skipping what it skips."""
    tables = {}
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if not name.endswith(".txt"):
                continue
            data = zf.read(name).removeprefix(b"\xef\xbb\xbf")
            headers = file_mod._get_csv_headers(data)
            try:
                if b"\n" not in data.strip():
                    table = pa.table({h: pa.array([], type=pa.string()) for h in headers})
                else:
                    table = pyarrow.csv.read_csv(
                        io.BytesIO(data),
                        convert_options=pyarrow.csv.ConvertOptions(
                            strings_can_be_null=False,
                            column_types={h: pa.string() for h in headers},
                        ),
                    )
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    GTFSFile.from_arrow_table(name, table, _schema(name))
            except Exception:
                continue
            tables[name] = table
    return tables


# Feeds whose CSV files polars parses to the same strings as pyarrow. The others
# have blank lines (polars keeps them as all-null rows) or short rows (polars
# fills them; pyarrow rejects the file), which changes the tables, not the writer.
POLARS_CLEAN_FEEDS = ["akishima-abus", "los-reginas", "mi-teleferico", "mv-shuttle", "sf-bay-ferry"]


class TestFromTables:
    @pytest.mark.parametrize("name", sorted(FEED_SOURCES))
    def test_arrow_tables_match_zip_digests(self, name):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            archive = GTFSArchive.from_tables(_pyarrow_tables(FEEDS / f"{name}.zip"))
        expected = EXPECTED_FEED_DIGESTS[name]
        assert archive.fingerprint.files == expected["files"]
        assert archive.fingerprint.root_hash == expected["root"]

    @pytest.mark.parametrize("name", POLARS_CLEAN_FEEDS)
    def test_polars_frames_match_zip_digests(self, name):
        with zipfile.ZipFile(FEEDS / f"{name}.zip") as zf:
            frames = {
                n: pl.read_csv(zf.read(n), infer_schema=False) for n in zf.namelist() if n.endswith(".txt")
            }
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            archive = GTFSArchive.from_tables(frames)
        assert archive.fingerprint.root_hash == EXPECTED_FEED_DIGESTS[name]["root"]

    def test_rejects_non_string_columns(self):
        with pytest.raises(TypeError):
            GTFSArchive.from_tables({"stops.txt": pa.table({"stop_id": [1, 2]})})
