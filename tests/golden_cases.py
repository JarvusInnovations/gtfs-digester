"""Fixture definitions shared by the golden-digest test and its recorder.

The golden digests in ``fixtures/golden_digests.json`` were recorded from the
Python ``csv.writer`` serializer (gtfs-digester 0.4.0) before the streamed
serializer replaced it. A digest that moves is a breaking change to every
stored ``v1:`` fingerprint, so the test fails on any moved byte.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import pyarrow as pa

from gtfs_digester import GTFSArchive, GTFSFile, get_schema

FIXTURES = Path(__file__).parent / "fixtures"
FEEDS = FIXTURES / "feeds"
EDGE = FIXTURES / "edge"
GOLDEN = FIXTURES / "golden_digests.json"

# Real public feeds committed under fixtures/feeds. Each entry records where the
# zip came from and its license; the zips are committed unmodified.
FEED_SOURCES: dict[str, dict[str, str]] = {
    "google-sample": {
        "source": "https://developers.google.com/static/transit/gtfs/examples/sample-feed.zip",
        "license": "CC-BY-4.0 (Google Developers documentation)",
    },
    "akishima-abus": {
        "source": "https://api.gtfs-data.jp/v2/organizations/akishimacity/feeds/Abus/files/feed.zip?rid=current",
        "license": "CC0-1.0",
    },
    "los-reginas": {
        "source": "https://github.com/juanmacuevas/ferrynorte-gtfs/releases/download/los-reginas/los-reginas_gtfs.zip",
        "license": "CC-BY-4.0",
    },
    "mi-teleferico": {
        "source": "https://datosbolivia.github.io/gtfs/mi-teleferico.zip",
        "license": "CC-BY-4.0",
    },
    "mv-shuttle": {
        "source": "https://gtfs.mvcommunityshuttle.com/gtfs.zip",
        "license": "ODC-By-1.0",
    },
    "sf-bay-ferry": {
        "source": "https://gtfs.sanfranciscobayferry.com/gtfs.zip",
        "license": "ODC-By-1.0",
    },
    "zyrardow": {
        "source": "https://github.com/minkowskimaciej/zyrardow/raw/main/zyrardow.zip",
        "license": "CC0-1.0",
    },
}
FEEDS_RETRIEVED = "2026-11-26"

# Hand-made edge cases under fixtures/edge, one directory per case.
EDGE_CASES = [
    "single-column-empty",
    "embedded-carriage-return",
    "empty-vs-null",
    "time-padding",
]


def _known(filename: str, table: pa.Table) -> GTFSFile:
    return GTFSFile.from_arrow_table(filename, table, get_schema(filename))


def _arrow_empty_vs_null() -> GTFSArchive:
    """Nulls and empty strings mixed in Arrow input: both canonicalize to ""."""
    trips = pa.table(
        {
            "route_id": ["R1", "R1", "R1", "R2"],
            "service_id": ["WK", "WK", "WK", None],
            "trip_id": ["T1", "T2", "T3", "T4"],
            "trip_headsign": [None, "", "Downtown", None],
            "trip_short_name": ["", None, None, ""],
            "direction_id": ["0", "1", None, ""],
            "block_id": [None, "", "B1", None],
            "shape_id": ["", None, "SH1", None],
        }
    )
    notes = pa.table({"note": ["alpha", None, "", "beta"]})
    return GTFSArchive({"trips.txt": _known("trips.txt", trips), "notes.txt": _known("notes.txt", notes)})


def _arrow_embedded_carriage_return() -> GTFSArchive:
    """Quoting triggers in Arrow input: CR, CRLF, LF, quotes, commas, unicode."""
    stops = pa.table(
        {
            "stop_id": ["S1", "S2", "S3", "S4", "S5"],
            "stop_name": ["Main St", "Oak, Elm", 'Say "hi"', "Żyrardów", "昭島駅"],
            "stop_desc": ["first line\rsecond line", "crlf\r\ninside", "lf\ninside", "  padded  ", "\r"],
            "stop_lat": ["40.0", "40.1", "40.2", "40.3", "40.4"],
            "stop_lon": ["-75.0", "-75.1", "-75.2", "-75.3", "-75.4"],
        }
    )
    return GTFSArchive({"stops.txt": _known("stops.txt", stops)})


def _raw_constructor_nulls() -> GTFSArchive:
    """GTFSFile built directly (no canonicalization): the serializer writes null as ""."""
    multi = pa.table({"a": ["x", None, "", None], "b": [None, "", "y", None]})
    single = pa.table({"only": ["x", None, "", "z"]})
    return GTFSArchive({"multi.txt": GTFSFile("multi.txt", multi), "single.txt": GTFSFile("single.txt", single)})


ARROW_CASES = {
    "arrow-empty-vs-null": _arrow_empty_vs_null,
    "arrow-embedded-carriage-return": _arrow_embedded_carriage_return,
    "raw-constructor-nulls": _raw_constructor_nulls,
}


def load_feed(name: str) -> GTFSArchive:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return GTFSArchive.from_zip(FEEDS / f"{name}.zip")


def load_edge(name: str) -> GTFSArchive:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return GTFSArchive.from_directory(EDGE / name)


def load_arrow_case(name: str) -> GTFSArchive:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return ARROW_CASES[name]()


def digests(archive: GTFSArchive) -> dict:
    fp = archive.fingerprint
    return {"root": fp.root_hash, "files": dict(sorted(fp.files.items()))}
