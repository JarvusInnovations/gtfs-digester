"""Record fixtures/golden_digests.json from the current serializer.

Run only to add a new fixture, and only on code whose digests already match the
committed file: the golden digests are the compatibility contract for every
stored v1 fingerprint, and re-recording them after a serializer change hides
exactly the break the test exists to catch.

    uv run python tests/record_golden.py
"""

from __future__ import annotations

import json

from golden_cases import (
    ARROW_CASES,
    EDGE_CASES,
    FEED_SOURCES,
    FEEDS_RETRIEVED,
    GOLDEN,
    digests,
    load_arrow_case,
    load_edge,
    load_feed,
)


def main() -> None:
    out = {
        "fingerprint_algorithm": "v1",
        "recorded_with": "gtfs-digester 0.4.0 (Python csv.writer serializer)",
        "feeds_retrieved": FEEDS_RETRIEVED,
        "feeds": {name: {**meta, **digests(load_feed(name))} for name, meta in FEED_SOURCES.items()},
        "edge": {name: digests(load_edge(name)) for name in EDGE_CASES},
        "arrow": {name: digests(load_arrow_case(name)) for name in ARROW_CASES},
    }
    GOLDEN.write_text(json.dumps(out, indent=2, ensure_ascii=False) + "\n")
    print(f"wrote {GOLDEN}")


if __name__ == "__main__":
    main()
