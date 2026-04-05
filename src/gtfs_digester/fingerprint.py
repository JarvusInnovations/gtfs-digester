"""ArchiveFingerprint: merkle-tree structure for GTFS archive fingerprinting.

- Each file gets a BLAKE3 hash of its canonical CSV serialization.
- The archive fingerprint is BLAKE3(sorted concatenation of "filename:hash" pairs).
- All hashes are prefixed with "v1:" for versioning.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import blake3 as _blake3


FINGERPRINT_VERSION = "v1"


def hash_bytes(data: bytes) -> str:
    """Compute BLAKE3 hash of bytes, returning hex string (no version prefix)."""
    return _blake3.blake3(data).hexdigest()


@dataclass(frozen=True)
class ArchiveFingerprint:
    """Merkle-tree fingerprint for a GTFS archive.

    Attributes:
        files: Mapping of filename -> versioned hex hash (e.g. "v1:abc123...")
        root_hash: The versioned archive-level hash.
    """

    files: dict[str, str]
    root_hash: str

    @staticmethod
    def compute(file_hashes: dict[str, str]) -> ArchiveFingerprint:
        """Compute an archive fingerprint from per-file hashes.

        Args:
            file_hashes: Mapping of filename -> raw hex hash (without version prefix).
        """
        versioned = {fn: f"{FINGERPRINT_VERSION}:{h}" for fn, h in file_hashes.items()}

        parts = []
        for fn in sorted(versioned.keys()):
            parts.append(f"{fn}:{versioned[fn]}")
        combined = "\n".join(parts).encode("utf-8")

        root_raw = hash_bytes(combined)
        root = f"{FINGERPRINT_VERSION}:{root_raw}"

        return ArchiveFingerprint(files=versioned, root_hash=root)

    def hex(self) -> str:
        """Return the versioned archive-level fingerprint string."""
        return self.root_hash

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ArchiveFingerprint):
            return NotImplemented
        return self.root_hash == other.root_hash

    def __hash__(self) -> int:
        return hash(self.root_hash)

    def __repr__(self) -> str:
        return f"ArchiveFingerprint({self.root_hash})"

    def to_json(self) -> str:
        """Serialize the fingerprint to JSON."""
        return json.dumps(
            {
                "fingerprint_algorithm": FINGERPRINT_VERSION,
                "archive_fingerprint": self.root_hash,
                "file_fingerprints": self.files,
            },
            indent=2,
        )

    @staticmethod
    def from_json(data: str) -> ArchiveFingerprint:
        """Deserialize from JSON."""
        parsed = json.loads(data)
        return ArchiveFingerprint(
            files=parsed["file_fingerprints"],
            root_hash=parsed["archive_fingerprint"],
        )
