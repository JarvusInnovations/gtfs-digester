"""GTFSArchive: load from zip or directory, access Arrow tables, fingerprint, diff."""

from __future__ import annotations

import io
import warnings
import zipfile
from pathlib import Path

import pyarrow as pa

from .diff import ArchiveDiff, FileDiff, compute_file_diff
from .file import GTFSFile
from .fingerprint import ArchiveFingerprint
from .schema import get_schema


class GTFSArchive:
    """Represents a complete GTFS feed.

    Provides access to individual files, computes fingerprints,
    and writes normalized output. Archives are immutable.

    All .txt files are preserved — both known (with schema-aware normalization)
    and unknown (with lexicographic sorting).
    """

    def __init__(self, files: dict[str, GTFSFile]) -> None:
        self._files = files
        self._fingerprint: ArchiveFingerprint | None = None

    @staticmethod
    def from_zip(
        source: str | bytes | io.BytesIO | Path,
    ) -> GTFSArchive:
        """Load a GTFS archive from a zip file.

        Args:
            source: Path to zip file, raw bytes, or BytesIO.
        """
        if isinstance(source, (str, Path)):
            with open(source, "rb") as f:
                zip_bytes = f.read()
        elif isinstance(source, bytes):
            zip_bytes = source
        else:
            zip_bytes = source.read()

        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
        files: dict[str, GTFSFile] = {}

        for name in sorted(zf.namelist()):
            if name.endswith("/") or name.startswith(".") or name.startswith("__"):
                continue

            # Strip directory prefix (some zips nest files in a subdirectory)
            basename = name.split("/")[-1]

            if not basename.endswith(".txt"):
                continue

            schema = get_schema(basename)
            data = zf.read(name)
            try:
                gtfs_file = GTFSFile.from_csv_bytes(
                    filename=basename,
                    data=data,
                    schema=schema,
                )
                files[basename] = gtfs_file
            except Exception as e:
                warnings.warn(f"Skipping {basename}: {e}")
                continue

        return GTFSArchive(files=files)

    @staticmethod
    def from_directory(
        path: str | Path,
    ) -> GTFSArchive:
        """Load a GTFS archive from a directory of .txt files.

        Args:
            path: Path to directory containing GTFS .txt files.
        """
        dirpath = Path(path)
        if not dirpath.is_dir():
            raise ValueError(f"Not a directory: {dirpath}")

        files: dict[str, GTFSFile] = {}

        for txt_file in sorted(dirpath.glob("*.txt")):
            basename = txt_file.name
            if basename.startswith(".") or basename.startswith("__"):
                continue

            schema = get_schema(basename)
            data = txt_file.read_bytes()
            try:
                gtfs_file = GTFSFile.from_csv_bytes(
                    filename=basename,
                    data=data,
                    schema=schema,
                )
                files[basename] = gtfs_file
            except Exception as e:
                warnings.warn(f"Skipping {basename}: {e}")
                continue

        return GTFSArchive(files=files)

    @property
    def filenames(self) -> set[str]:
        """Set of filenames in this archive."""
        return set(self._files.keys())

    def __contains__(self, filename: str) -> bool:
        return filename in self._files

    def __getitem__(self, filename: str) -> GTFSFile:
        return self._files[filename]

    def arrow_table(self, filename: str) -> pa.Table:
        """Get the canonical Arrow table for a file."""
        return self[filename].table

    @property
    def fingerprint(self) -> ArchiveFingerprint:
        """Compute (or return cached) archive fingerprint."""
        if self._fingerprint is None:
            file_hashes: dict[str, str] = {}
            for filename, gtfs_file in self._files.items():
                file_hashes[filename] = gtfs_file.fingerprint_hash()
            self._fingerprint = ArchiveFingerprint.compute(file_hashes)
        return self._fingerprint

    def to_normalized_zip(self, path: str | Path) -> None:
        """Write the archive as a normalized GTFS zip.

        Contains canonical CSV files and a .gtfs_digester.json metadata file.
        """
        fp = self.fingerprint

        with zipfile.ZipFile(str(path), "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for filename in sorted(self._files.keys()):
                gtfs_file = self._files[filename]
                csv_bytes = gtfs_file.to_canonical_csv()
                zf.writestr(filename, csv_bytes)

            zf.writestr(".gtfs_digester.json", fp.to_json())

    def diff(self, other: GTFSArchive) -> ArchiveDiff:
        """Compare this archive with another, returning an ArchiveDiff.

        This archive is treated as "old", other as "new".
        """
        old_files = self.filenames
        new_files = other.filenames

        added = new_files - old_files
        removed = old_files - new_files
        common = old_files & new_files

        modified: set[str] = set()
        unchanged: set[str] = set()
        file_diffs: dict[str, FileDiff] = {}

        for filename in common:
            old_table = self._files[filename].table
            new_table = other._files[filename].table

            # Fast equality check via Arrow's native comparison — avoids the
            # expensive CSV-serialize-and-hash path used by fingerprint_hash().
            if old_table.equals(new_table):
                unchanged.add(filename)
                continue

            modified.add(filename)
            old_schema = self._files[filename].schema
            pk = old_schema.primary_key if old_schema else []
            file_diffs[filename] = compute_file_diff(
                filename, old_table, new_table, pk
            )

        return ArchiveDiff(
            added_files=added,
            removed_files=removed,
            modified_files=modified,
            unchanged_files=unchanged,
            _file_diffs=file_diffs,
        )
