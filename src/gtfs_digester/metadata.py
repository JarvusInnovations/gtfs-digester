"""Feed version metadata: provenance, validity dates, digester info.

Generates and parses the `metadata.json` file that lives inside each
version directory as the commit marker.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime


@dataclass
class FeedMetadata:
    """Metadata for a single GTFS feed version."""

    _feed_digest: str
    schedule_url: str
    date_retrieved: str  # ISO 8601 UTC
    digester_version: str
    file_hashes: dict[str, str]  # filename -> versioned hash
    file_row_counts: dict[str, int]  # filename -> row count
    feed_start_date: str | None = None  # YYYY-MM-DD from feed_info.txt
    feed_end_date: str | None = None  # YYYY-MM-DD from feed_info.txt
    source_sha256: str | None = None  # SHA256 of original zip bytes

    def to_json(self, indent: int = 2) -> str:
        """Serialize to JSON string."""
        return json.dumps(
            {
                "_feed_digest": self._feed_digest,
                "schedule_url": self.schedule_url,
                "date_retrieved": self.date_retrieved,
                "feed_start_date": self.feed_start_date,
                "feed_end_date": self.feed_end_date,
                "source_sha256": self.source_sha256,
                "digester_version": self.digester_version,
                "file_hashes": self.file_hashes,
                "file_row_counts": self.file_row_counts,
            },
            indent=indent,
        )

    def to_bytes(self) -> bytes:
        """Serialize to UTF-8 JSON bytes."""
        return self.to_json().encode("utf-8")

    @staticmethod
    def from_json(data: str | bytes) -> FeedMetadata:
        """Deserialize from JSON."""
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        parsed = json.loads(data)
        return FeedMetadata(
            _feed_digest=parsed["_feed_digest"],
            schedule_url=parsed["schedule_url"],
            date_retrieved=parsed["date_retrieved"],
            feed_start_date=parsed.get("feed_start_date"),
            feed_end_date=parsed.get("feed_end_date"),
            source_sha256=parsed.get("source_sha256"),
            digester_version=parsed["digester_version"],
            file_hashes=parsed["file_hashes"],
            file_row_counts=parsed["file_row_counts"],
        )

    @staticmethod
    def from_archive(
        archive: "GTFSArchive",
        schedule_url: str,
        date_retrieved: str | datetime | None = None,
        source_sha256: str | None = None,
    ) -> FeedMetadata:
        """Build metadata from an archive and provenance info.

        Args:
            archive: The digested archive.
            schedule_url: Source URL the feed was downloaded from.
            date_retrieved: When the feed was downloaded. Defaults to now (UTC).
            source_sha256: SHA256 of the original zip bytes.
        """
        from .archive import GTFSArchive
        from .fingerprint import FINGERPRINT_VERSION

        if date_retrieved is None:
            date_retrieved = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        elif isinstance(date_retrieved, datetime):
            date_retrieved = date_retrieved.strftime("%Y-%m-%dT%H:%M:%SZ")

        fp = archive.fingerprint

        # Extract feed_start_date and feed_end_date from feed_info.txt if present
        feed_start_date = None
        feed_end_date = None
        if "feed_info.txt" in archive:
            fi = archive["feed_info.txt"].table
            if fi.num_rows > 0:
                if "feed_start_date" in fi.column_names:
                    val = fi.column("feed_start_date")[0].as_py()
                    if val:
                        feed_start_date = val
                if "feed_end_date" in fi.column_names:
                    val = fi.column("feed_end_date")[0].as_py()
                    if val:
                        feed_end_date = val

        return FeedMetadata(
            _feed_digest=fp.root_hash,
            schedule_url=schedule_url,
            date_retrieved=date_retrieved,
            feed_start_date=feed_start_date,
            feed_end_date=feed_end_date,
            source_sha256=source_sha256,
            digester_version=FINGERPRINT_VERSION,
            file_hashes=fp.files,
            file_row_counts={
                filename: archive[filename].row_count
                for filename in sorted(archive.filenames)
            },
        )
