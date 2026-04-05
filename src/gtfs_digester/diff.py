"""ArchiveDiff and FileDiff: archive and file-level diffing by primary key.

Diffing is hierarchical:
1. Compare archive fingerprints — if equal, done.
2. Compare file fingerprints — identify added/removed/modified files.
3. For modified files, compare rows by primary key.
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa


@dataclass
class FileDiff:
    """Row-level diff for a single modified GTFS file."""

    filename: str
    added: pa.Table
    removed: pa.Table
    modified: pa.Table

    @property
    def added_count(self) -> int:
        return self.added.num_rows

    @property
    def removed_count(self) -> int:
        return self.removed.num_rows

    @property
    def modified_count(self) -> int:
        return self.modified.num_rows

    def summary(self) -> str:
        return (
            f"+{self.added_count} added, "
            f"-{self.removed_count} removed, "
            f"~{self.modified_count} modified"
        )


@dataclass
class ArchiveDiff:
    """Diff between two GTFS archives."""

    added_files: set[str]
    removed_files: set[str]
    modified_files: set[str]
    unchanged_files: set[str]
    _file_diffs: dict[str, FileDiff]

    def file_diff(self, filename: str) -> FileDiff:
        """Get the row-level diff for a modified file."""
        if filename not in self._file_diffs:
            raise KeyError(
                f"No file diff for '{filename}'. "
                f"Modified files: {sorted(self.modified_files)}"
            )
        return self._file_diffs[filename]

    @property
    def is_identical(self) -> bool:
        return (
            not self.added_files
            and not self.removed_files
            and not self.modified_files
        )


def compute_file_diff(
    filename: str,
    old_table: pa.Table,
    new_table: pa.Table,
    primary_key: list[str],
) -> FileDiff:
    """Compute row-level diff between two tables using primary key."""
    if not primary_key:
        return _diff_no_pk(filename, old_table, new_table)

    old_keys: dict[tuple[str, ...], int] = {}
    for i in range(old_table.num_rows):
        key = tuple(old_table.column(col)[i].as_py() for col in primary_key)
        old_keys[key] = i

    new_keys: dict[tuple[str, ...], int] = {}
    for i in range(new_table.num_rows):
        key = tuple(new_table.column(col)[i].as_py() for col in primary_key)
        new_keys[key] = i

    old_key_set = set(old_keys.keys())
    new_key_set = set(new_keys.keys())

    added_keys = new_key_set - old_key_set
    removed_keys = old_key_set - new_key_set
    common_keys = old_key_set & new_key_set

    modified_indices_new = []
    all_columns = new_table.column_names

    for key in common_keys:
        old_idx = old_keys[key]
        new_idx = new_keys[key]
        for col_name in all_columns:
            if col_name in old_table.column_names:
                old_val = old_table.column(col_name)[old_idx].as_py()
                new_val = new_table.column(col_name)[new_idx].as_py()
                if old_val != new_val:
                    modified_indices_new.append(new_idx)
                    break

    added_indices = sorted(new_keys[k] for k in added_keys)
    removed_indices = sorted(old_keys[k] for k in removed_keys)

    added_table = new_table.take(added_indices) if added_indices else new_table.slice(0, 0)
    removed_table = old_table.take(removed_indices) if removed_indices else old_table.slice(0, 0)
    modified_table = (
        new_table.take(sorted(modified_indices_new))
        if modified_indices_new
        else new_table.slice(0, 0)
    )

    return FileDiff(
        filename=filename,
        added=added_table,
        removed=removed_table,
        modified=modified_table,
    )


def _diff_no_pk(filename: str, old_table: pa.Table, new_table: pa.Table) -> FileDiff:
    """Diff tables with no primary key by comparing full row content."""
    def _row_tuple(table: pa.Table, idx: int) -> tuple[str, ...]:
        return tuple(table.column(c)[idx].as_py() for c in table.column_names)

    old_rows = {_row_tuple(old_table, i) for i in range(old_table.num_rows)}
    new_rows = {_row_tuple(new_table, i) for i in range(new_table.num_rows)}

    added_rows = new_rows - old_rows
    removed_rows = old_rows - new_rows

    cols = new_table.column_names
    added_data = {c: [] for c in cols}
    for row in sorted(added_rows):
        for c, v in zip(cols, row):
            added_data[c].append(v)

    removed_data = {c: [] for c in cols}
    for row in sorted(removed_rows):
        for c, v in zip(cols, row):
            removed_data[c].append(v)

    return FileDiff(
        filename=filename,
        added=pa.table({c: pa.array(v, type=pa.string()) for c, v in added_data.items()}),
        removed=pa.table({c: pa.array(v, type=pa.string()) for c, v in removed_data.items()}),
        modified=new_table.slice(0, 0),
    )
