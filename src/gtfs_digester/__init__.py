"""gtfs-digester: Canonicalization, fingerprinting, and change detection for GTFS Schedule feeds."""

from .archive import GTFSArchive
from .diff import ArchiveDiff, FileDiff
from .file import GTFSFile
from .fingerprint import ArchiveFingerprint
from .metadata import FeedMetadata
from .schema import GTFS_SCHEMAS, FileSchema, get_schema
from .storage import list_versions, read_metadata, read_table, version_exists, write_exploded

__all__ = [
    "GTFSArchive",
    "GTFSFile",
    "ArchiveFingerprint",
    "ArchiveDiff",
    "FileDiff",
    "FeedMetadata",
    "FileSchema",
    "get_schema",
    "GTFS_SCHEMAS",
    "write_exploded",
    "read_metadata",
    "read_table",
    "list_versions",
    "version_exists",
]
