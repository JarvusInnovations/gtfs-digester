"""GTFSFile: holds a canonical Arrow table and computes per-file fingerprint.

All data stays as strings in Arrow. Numeric casting is only done temporarily for sorting.
Supports both known files (with schema) and unknown files (preserved verbatim with
lexicographic sorting).
"""

from __future__ import annotations

import csv
import io
import warnings

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv

from .fingerprint import hash_bytes
from .normalize import normalize_table
from .schema import FileSchema


class GTFSFile:
    """Represents a single canonicalized GTFS file.

    For known files (with a FileSchema), columns are reordered per spec,
    unknown columns are preserved after spec columns in alphabetical order,
    and rows are sorted by primary key.

    For unknown files (no schema), all columns are preserved in alphabetical
    order and rows are sorted lexicographically.
    """

    def __init__(self, filename: str, table: pa.Table, schema: FileSchema | None = None) -> None:
        self._filename = filename
        self._schema = schema
        self._table = table
        self._hash: str | None = None

    @property
    def schema(self) -> FileSchema | None:
        return self._schema

    @property
    def table(self) -> pa.Table:
        return self._table

    @property
    def filename(self) -> str:
        return self._filename

    @property
    def row_count(self) -> int:
        return self._table.num_rows

    @property
    def columns(self) -> list[str]:
        return self._table.column_names

    @staticmethod
    def from_csv_bytes(
        filename: str,
        data: bytes,
        schema: FileSchema | None = None,
    ) -> GTFSFile:
        """Parse CSV bytes into a canonical GTFSFile.

        If schema is provided (known file):
        1. Reorder columns: spec order first, then unknown columns alphabetically
        2. Normalize values
        3. Sort by primary key
        4. Validate primary key uniqueness

        If schema is None (unknown file):
        1. Sort columns alphabetically
        2. Normalize values (whitespace only, no time zero-padding)
        3. Sort rows lexicographically
        """
        # Strip BOM
        if data.startswith(b"\xef\xbb\xbf"):
            data = data[3:]

        # Handle empty files (header-only or completely empty)
        stripped = data.strip()
        if not stripped or b"\n" not in stripped:
            # Empty or header-only CSV — return empty table
            if stripped:
                headers = _get_csv_headers(data)
                empty_table = pa.table({h: pa.array([], type=pa.string()) for h in headers})
            else:
                empty_table = pa.table({})
            return GTFSFile(filename=filename, table=empty_table, schema=schema)

        # Parse CSV — all values as strings
        headers = _get_csv_headers(data)
        table = pyarrow.csv.read_csv(
            io.BytesIO(data),
            convert_options=pyarrow.csv.ConvertOptions(
                strings_can_be_null=False,
                column_types={col: pa.string() for col in headers},
            ),
        )

        # Strip column name whitespace
        clean_names = [name.strip() for name in table.column_names]
        if clean_names != table.column_names:
            arrays = [table.column(i) for i in range(table.num_columns)]
            table = pa.table(dict(zip(clean_names, arrays)))

        if schema is not None:
            table = _process_known_file(table, schema)
        else:
            table = _process_unknown_file(table)

        return GTFSFile(filename=filename, table=table, schema=schema)

    def fingerprint_hash(self) -> str:
        """Compute the BLAKE3 hash of this file's canonical CSV serialization."""
        if self._hash is None:
            csv_bytes = self.to_canonical_csv()
            self._hash = hash_bytes(csv_bytes)
        return self._hash

    def to_canonical_csv(self) -> bytes:
        """Serialize to canonical CSV: QUOTE_MINIMAL, LF line endings, no trailing newline."""
        output = io.StringIO()
        writer = csv.writer(output, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)

        writer.writerow(self._table.column_names)

        # Convert columns to Python lists for fast row iteration
        col_lists = [self._table.column(name).to_pylist() for name in self._table.column_names]
        for i in range(self._table.num_rows):
            writer.writerow(val if val is not None else "" for val in (cols[i] for cols in col_lists))

        result = output.getvalue()
        if result.endswith("\n"):
            result = result[:-1]

        return result.encode("utf-8")


def _get_csv_headers(data: bytes) -> list[str]:
    """Extract CSV header names from raw bytes."""
    first_line = data.split(b"\n", 1)[0].rstrip(b"\r")
    reader = csv.reader([first_line.decode("utf-8")])
    return [h.strip() for h in next(reader)]


def _process_known_file(table: pa.Table, schema: FileSchema) -> pa.Table:
    """Process a file with a known schema: reorder, preserve unknowns, normalize, sort."""
    input_cols = set(table.column_names)
    known_cols = set(schema.columns)

    # Spec columns in spec order (only those present in input)
    ordered = [c for c in schema.columns if c in input_cols]

    # Unknown columns alphabetically after spec columns
    unknown = sorted(input_cols - known_cols)
    if unknown:
        warnings.warn(
            f"Preserving unknown columns in {schema.filename}: {unknown}"
        )

    table = table.select(ordered + unknown)

    # Replace nulls with empty string
    new_arrays = {}
    for name in table.column_names:
        col = table.column(name)
        if col.null_count > 0:
            col = pc.if_else(pc.is_null(col), "", col)
        new_arrays[name] = col
    table = pa.table(new_arrays)

    # Normalize values
    table = normalize_table(table)

    # Sort by primary key
    table = _sort_by_primary_key(table, schema)

    # Validate uniqueness
    if schema.primary_key:
        _validate_unique_keys(table, schema)

    return table


def _process_unknown_file(table: pa.Table) -> pa.Table:
    """Process a file with no known schema: alphabetical columns, lexicographic sort."""
    # Sort columns alphabetically
    table = table.select(sorted(table.column_names))

    # Replace nulls with empty string
    new_arrays = {}
    for name in table.column_names:
        col = table.column(name)
        if col.null_count > 0:
            col = pc.if_else(pc.is_null(col), "", col)
        new_arrays[name] = col
    table = pa.table(new_arrays)

    # Normalize (whitespace only — no time zero-padding for unknown files)
    table = normalize_table(table, time_columns=frozenset())

    # Sort all rows lexicographically by all columns
    if table.num_rows > 0 and table.num_columns > 0:
        sort_keys = [(col, "ascending") for col in table.column_names]
        table = table.sort_by(sort_keys)

    return table


def _sort_by_primary_key(table: pa.Table, schema: FileSchema) -> pa.Table:
    """Sort table by primary key, using numeric comparison for numeric sort columns."""
    if not schema.primary_key:
        return table

    # Add temporary sort keys for numeric columns
    temp_cols = []
    for col_name, col_type in schema.numeric_sort_columns.items():
        if col_name in table.column_names:
            sort_key_name = f"_sort_{col_name}"
            sort_key = pc.cast(table[col_name], col_type)
            table = table.append_column(sort_key_name, sort_key)
            temp_cols.append(sort_key_name)

    # Build sort specification: PK columns first (only those present), then remaining
    sort_keys = []
    present_pk = [col for col in schema.primary_key if col in table.column_names]
    for col in present_pk:
        if col in schema.numeric_sort_columns and f"_sort_{col}" in table.column_names:
            sort_keys.append((f"_sort_{col}", "ascending"))
        else:
            sort_keys.append((col, "ascending"))

    all_cols = [c for c in table.column_names if not c.startswith("_sort_")]
    for col in all_cols:
        if col not in present_pk:
            if col in schema.numeric_sort_columns and f"_sort_{col}" in table.column_names:
                sort_keys.append((f"_sort_{col}", "ascending"))
            else:
                sort_keys.append((col, "ascending"))

    table = table.sort_by(sort_keys)

    # Drop temporary columns
    if temp_cols:
        table = table.drop(temp_cols)

    return table


def _validate_unique_keys(table: pa.Table, schema: FileSchema) -> None:
    """Raise ValueError if duplicate primary keys exist."""
    present_pk = [col for col in schema.primary_key if col in table.column_names]
    if not present_pk:
        return

    if len(present_pk) == 1:
        col = table.column(present_pk[0])
        n_unique = pc.count_distinct(col).as_py()
        if n_unique < table.num_rows:
            raise ValueError(
                f"Duplicate primary keys in {schema.filename} "
                f"(column: {present_pk[0]}): "
                f"{table.num_rows} rows but only {n_unique} unique keys"
            )
    else:
        seen: set[tuple[str, ...]] = set()
        for i in range(table.num_rows):
            key = tuple(
                table.column(col)[i].as_py() for col in present_pk
            )
            if key in seen:
                raise ValueError(
                    f"Duplicate primary key in {schema.filename}: "
                    f"{dict(zip(present_pk, key))}"
                )
            seen.add(key)
