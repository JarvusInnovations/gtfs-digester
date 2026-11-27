"""Value normalization for GTFS data.

Rules:
- Strip leading/trailing whitespace from all values
- Zero-pad times: H:MM:SS -> HH:MM:SS, H:M:S -> HH:MM:SS
- Normalize empty/whitespace-only values to empty string
- Do NOT reformat numeric values (preserve "007", "1.0", "1.000000", etc.)

Uses PyArrow compute functions for vectorized operations on large tables.
"""

from __future__ import annotations

import re

import pyarrow as pa
import pyarrow.compute as pc

from .schema import TIME_COLUMNS

_TIME_RE = re.compile(r"^(\d{1,3}):(\d{1,2}):(\d{1,2})$")


def normalize_time(value: str) -> str:
    """Zero-pad a GTFS time value to HH:MM:SS (or HHH:MM:SS for >99h)."""
    match = _TIME_RE.match(value)
    if match is None:
        return value
    h, m, s = match.groups()
    return f"{int(h):02d}:{int(m):02d}:{int(s):02d}"


def normalize_value(value: str | None) -> str:
    """Normalize a single GTFS value: strip whitespace, empty -> ""."""
    if value is None:
        return ""
    return value.strip()


def normalize_time_value(value: str | None) -> str:
    """Normalize a time value: strip, empty-normalize, then zero-pad."""
    base = normalize_value(value)
    if not base:
        return base
    return normalize_time(base)


def _normalize_column_vectorized(column: pa.Array) -> pa.Array:
    """Vectorized whitespace normalization using PyArrow compute."""
    # utf8_trim strips leading/trailing whitespace
    stripped = pc.utf8_trim(column, " \t\r\n")
    # Replace nulls with empty string
    return pc.if_else(pc.is_null(stripped), "", stripped)


def _normalize_time_column(column: pa.Array | pa.ChunkedArray) -> pa.Array | pa.ChunkedArray:
    """Normalize a time column: strip whitespace then zero-pad times.

    Same result as applying normalize_time_value to every value, computed with
    Arrow kernels only (no Python values). A value is padded when it matches
    H:M:S with a 1-3 digit hour and 1-2 digit minutes and seconds and is not
    already HH:MM:SS / HHH:MM:SS. Padding formats each part the way
    ``f"{int(part):02d}"`` does: minutes and seconds are left-padded to two
    digits, and the hour drops one leading zero from three digits ("012" ->
    "12", "000" -> "00") before being left-padded to two.
    """
    stripped = pc.if_else(pc.is_null(column), "", pc.utf8_trim(column, " \t\r\n"))

    needs_padding = pc.match_substring_regex(stripped, r"^\d{1,3}:\d{1,2}:\d{1,2}$")
    already_padded = pc.match_substring_regex(stripped, r"^\d{2,3}:\d{2}:\d{2}$")
    needs_fix = pc.and_(needs_padding, pc.invert(already_padded))

    if not pc.any(needs_fix).as_py():
        return stripped

    parts = pc.extract_regex(stripped, r"^(?P<h>\d{1,3}):(?P<m>\d{1,2}):(?P<s>\d{1,2})$")
    hours = pc.replace_substring_regex(pc.struct_field(parts, "h"), r"^0(\d\d)$", r"\1")
    padded = pc.binary_join_element_wise(
        pc.utf8_lpad(hours, width=2, padding="0"),
        pc.utf8_lpad(pc.struct_field(parts, "m"), width=2, padding="0"),
        pc.utf8_lpad(pc.struct_field(parts, "s"), width=2, padding="0"),
        ":",
    )
    return pc.if_else(needs_fix, padded, stripped)


def normalize_column(column: pa.Array, column_name: str) -> pa.Array:
    """Normalize all values in a PyArrow string column."""
    if column_name in TIME_COLUMNS:
        return _normalize_time_column(column)
    return _normalize_column_vectorized(column)


def normalize_table(table: pa.Table, time_columns: frozenset[str] | None = None) -> pa.Table:
    """Normalize all values in a PyArrow table."""
    time_cols = time_columns if time_columns is not None else TIME_COLUMNS
    new_columns = {}
    for name in table.column_names:
        col = table.column(name)
        if name in time_cols:
            new_columns[name] = _normalize_time_column(col)
        else:
            new_columns[name] = _normalize_column_vectorized(col)
    return pa.table(new_columns)
