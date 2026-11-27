"""Golden-digest test: fingerprints of the committed fixtures must never move.

The expected digests were recorded before the streamed serializer replaced the
Python csv.writer one (see golden_cases.py). Any change to canonical CSV bytes,
normalization, column order or row order fails here.
"""

from __future__ import annotations

import json

import pytest
from golden_cases import (
    ARROW_CASES,
    EDGE_CASES,
    FEED_SOURCES,
    GOLDEN,
    digests,
    load_arrow_case,
    load_edge,
    load_feed,
)

EXPECTED = json.loads(GOLDEN.read_text())


def _assert_same(actual: dict, expected: dict) -> None:
    assert actual["files"] == expected["files"]
    assert actual["root"] == expected["root"]


class TestGoldenDigests:
    def test_every_fixture_has_a_golden_entry(self):
        assert sorted(EXPECTED["feeds"]) == sorted(FEED_SOURCES)
        assert sorted(EXPECTED["edge"]) == sorted(EDGE_CASES)
        assert sorted(EXPECTED["arrow"]) == sorted(ARROW_CASES)

    @pytest.mark.parametrize("name", sorted(FEED_SOURCES))
    def test_public_feed(self, name):
        _assert_same(digests(load_feed(name)), EXPECTED["feeds"][name])

    @pytest.mark.parametrize("name", EDGE_CASES)
    def test_edge_case(self, name):
        _assert_same(digests(load_edge(name)), EXPECTED["edge"][name])

    @pytest.mark.parametrize("name", sorted(ARROW_CASES))
    def test_arrow_case(self, name):
        _assert_same(digests(load_arrow_case(name)), EXPECTED["arrow"][name])

    def test_null_and_empty_string_digest_alike(self):
        """trips.txt with nulls (Arrow input) matches trips.txt with empty fields (CSV)."""
        assert (
            EXPECTED["arrow"]["arrow-empty-vs-null"]["files"]["trips.txt"]
            == EXPECTED["edge"]["empty-vs-null"]["files"]["trips.txt"]
        )
