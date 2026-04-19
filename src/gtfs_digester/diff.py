"""ArchiveDiff and FileDiff: archive and file-level diffing by primary key.

Diffing is hierarchical:
1. Compare archive fingerprints — if equal, done.
2. Compare file fingerprints — identify added/removed/modified files.
3. For modified files, compare rows by primary key using polars (vectorized).
"""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl
import pyarrow as pa


@dataclass
class FileDiff:
    """Row-level diff for a single modified GTFS file.

    ``added_columns`` and ``removed_columns`` surface schema-level drift —
    columns present on exactly one side. They can't participate in per-row
    comparison (no opposite-side value), so their appearance here is the only
    signal a caller gets that a file's column set changed.
    """

    filename: str
    added: pa.Table
    removed: pa.Table
    modified: pa.Table
    added_columns: list[str] = field(default_factory=list)
    removed_columns: list[str] = field(default_factory=list)

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
        base = (
            f"+{self.added_count} added, "
            f"-{self.removed_count} removed, "
            f"~{self.modified_count} modified"
        )
        if not self.added_columns and not self.removed_columns:
            return base
        drift_parts = []
        if self.added_columns:
            drift_parts.append(f"+{', +'.join(self.added_columns)}")
        if self.removed_columns:
            drift_parts.append(f"-{', -'.join(self.removed_columns)}")
        return f"{base}; schema: {' '.join(drift_parts)}"


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


# NUL is guaranteed not to appear in GTFS CSV values (canonicalized strings
# never contain NULs), so it's safe as a composite-key separator.
_KEY_SEP = "\x00"


def compute_file_diff(
    filename: str,
    old_table: pa.Table,
    new_table: pa.Table,
    primary_key: list[str],
) -> FileDiff:
    """Compute row-level diff between two tables using primary key.

    Implementation is polars-native: PK columns are concatenated into a
    composite key column, anti-joins identify added/removed, and non-PK
    columns are compared via a concatenated row-hash for modified rows.
    """
    if not primary_key:
        return _diff_no_pk(filename, old_table, new_table)

    old_df = pl.from_arrow(old_table)
    new_df = pl.from_arrow(new_table)

    # Schema-level drift (preserves relative new/old column order)
    old_cols_set = set(old_df.columns)
    new_cols_set = set(new_df.columns)
    added_columns = [c for c in new_df.columns if c not in old_cols_set]
    removed_columns = [c for c in old_df.columns if c not in new_cols_set]

    # Only use PK columns that actually exist in both tables
    pk_cols = [c for c in primary_key if c in old_df.columns and c in new_df.columns]
    if not pk_cols:
        return _diff_no_pk(filename, old_table, new_table)

    pk_expr = pl.concat_str(pk_cols, separator=_KEY_SEP).alias("__pk__")
    old_keyed = old_df.with_columns(pk_expr)
    new_keyed = new_df.with_columns(pk_expr)

    # Anti-joins for added / removed (vectorized, runs entirely in Rust)
    added = new_keyed.join(old_keyed.select("__pk__"), on="__pk__", how="anti")
    removed = old_keyed.join(new_keyed.select("__pk__"), on="__pk__", how="anti")

    # Modified: inner join on PK, compare non-PK columns via concatenated hash.
    # Keep all original columns (including PK) on the new side; the old side
    # only contributes the non-PK columns we need to compare against.
    # Only compare columns present on both sides — columns added or removed
    # at the schema level can't be pairwise-compared and would otherwise crash
    # the select from old_keyed.
    non_pk_cols = [
        c for c in new_keyed.columns
        if c not in pk_cols and c != "__pk__" and c in old_keyed.columns
    ]

    if non_pk_cols:
        old_renamed = old_keyed.select(
            [pl.col("__pk__")]
            + [pl.col(c).alias(f"{c}__old__") for c in non_pk_cols]
        )
        joined = new_keyed.join(old_renamed, on="__pk__", how="inner")
        old_hash = pl.concat_str(
            [pl.col(f"{c}__old__") for c in non_pk_cols], separator=_KEY_SEP, ignore_nulls=False,
        )
        new_hash = pl.concat_str(
            [pl.col(c) for c in non_pk_cols], separator=_KEY_SEP, ignore_nulls=False,
        )
        modified_frame = joined.filter(old_hash != new_hash).select(list(new_keyed.columns))
    else:
        # No non-PK columns → nothing can differ beyond presence. No modifieds.
        modified_frame = new_keyed.slice(0, 0)

    # Strip the composite key column before returning; preserve original column
    # order from the new table for UX consistency with the old implementation.
    original_cols = list(new_df.columns)
    added = added.select(original_cols)
    removed = removed.select([c for c in original_cols if c in removed.columns])
    modified = modified_frame.select(
        [c for c in original_cols if c in modified_frame.columns]
    )

    return FileDiff(
        filename=filename,
        added=added.to_arrow(),
        removed=removed.to_arrow(),
        modified=modified.to_arrow(),
        added_columns=added_columns,
        removed_columns=removed_columns,
    )


def _diff_no_pk(filename: str, old_table: pa.Table, new_table: pa.Table) -> FileDiff:
    """Diff tables with no primary key by comparing full row content."""
    old_df = pl.from_arrow(old_table)
    new_df = pl.from_arrow(new_table)

    old_cols_set = set(old_df.columns)
    new_cols_set = set(new_df.columns)
    added_columns = [c for c in new_df.columns if c not in old_cols_set]
    removed_columns = [c for c in old_df.columns if c not in new_cols_set]

    # Row comparison only works on the shared column set; columns present on
    # only one side are surfaced separately via added_columns/removed_columns.
    cols = [c for c in new_df.columns if c in old_cols_set]

    if not cols:
        # No overlap at all — nothing to hash for presence. Treat as empty
        # row-level diff; schema drift fields still communicate what changed.
        empty = new_table.slice(0, 0)
        return FileDiff(
            filename=filename,
            added=empty,
            removed=old_table.slice(0, 0),
            modified=empty,
            added_columns=added_columns,
            removed_columns=removed_columns,
        )

    old_shared = old_df.select(cols)
    new_shared = new_df.select(cols)

    # Full-row composite key covering every shared column
    row_key = pl.concat_str(cols, separator=_KEY_SEP).alias("__row__")
    old_keyed = old_shared.with_columns(row_key)
    new_keyed = new_shared.with_columns(row_key)

    added = (
        new_keyed.join(old_keyed.select("__row__"), on="__row__", how="anti")
        .select(cols)
        .sort(cols)
    )
    removed = (
        old_keyed.join(new_keyed.select("__row__"), on="__row__", how="anti")
        .select(cols)
        .sort(cols)
    )

    return FileDiff(
        filename=filename,
        added=added.to_arrow(),
        removed=removed.to_arrow(),
        modified=new_table.slice(0, 0),
        added_columns=added_columns,
        removed_columns=removed_columns,
    )
