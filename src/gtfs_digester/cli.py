"""CLI entry point for gtfs-digester."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click

from .archive import GTFSArchive
from .schema import get_schema


def _load_archive(path: str) -> GTFSArchive:
    """Load a GTFSArchive from a zip file or directory."""
    p = Path(path)
    if p.is_dir():
        return GTFSArchive.from_directory(p)
    elif p.is_file():
        return GTFSArchive.from_zip(p)
    else:
        raise click.BadParameter(f"Path does not exist: {path}")


@click.group()
def main() -> None:
    """Canonicalize, fingerprint, and diff GTFS Schedule feeds."""


@main.command()
@click.argument("path")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.option("--quiet", is_flag=True, help="Only print the fingerprint hash.")
def digest(path: str, as_json: bool, quiet: bool) -> None:
    """Digest a GTFS zip or directory. Print fingerprint and file summary."""
    archive = _load_archive(path)
    fp = archive.fingerprint

    if quiet:
        click.echo(fp.root_hash)
        return

    if as_json:
        data = {
            "fingerprint": fp.root_hash,
            "file_hashes": fp.files,
            "file_row_counts": {
                filename: archive[filename].row_count
                for filename in sorted(archive.filenames)
            },
        }
        click.echo(json.dumps(data, indent=2))
        return

    click.echo(f"Fingerprint: {fp.root_hash}")
    click.echo(f"Files: {len(archive.filenames)}")

    for filename in sorted(archive.filenames):
        gtfs_file = archive[filename]
        row_count = gtfs_file.row_count
        unknown = "" if get_schema(filename) is not None else "  (unknown)"
        click.echo(f"  {filename:<25s} {row_count:>7d} rows{unknown}")


@main.command()
@click.argument("old")
@click.argument("new")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON.")
@click.option("--quiet", is_flag=True, help="Exit code only (0=identical, 1=different).")
def diff(old: str, new: str, as_json: bool, quiet: bool) -> None:
    """Compare two GTFS feeds (zips or directories)."""
    old_archive = _load_archive(old)
    new_archive = _load_archive(new)
    d = old_archive.diff(new_archive)

    if quiet:
        sys.exit(0 if d.is_identical else 1)

    if as_json:
        data: dict = {
            "identical": d.is_identical,
            "added_files": sorted(d.added_files),
            "removed_files": sorted(d.removed_files),
            "modified_files": sorted(d.modified_files),
            "unchanged_files": sorted(d.unchanged_files),
            "file_diffs": {},
        }
        for filename in sorted(d.modified_files):
            fd = d.file_diff(filename)
            data["file_diffs"][filename] = {
                "added": fd.added_count,
                "removed": fd.removed_count,
                "modified": fd.modified_count,
            }
        click.echo(json.dumps(data, indent=2))
        if not d.is_identical:
            sys.exit(1)
        return

    if d.is_identical:
        click.echo("Archives are identical.")
        return

    click.echo("Archives differ.")
    click.echo(f"  Modified files: {', '.join(sorted(d.modified_files)) or '(none)'}")
    click.echo(f"  Added files: {', '.join(sorted(d.added_files)) or '(none)'}")
    click.echo(f"  Removed files: {', '.join(sorted(d.removed_files)) or '(none)'}")
    click.echo(f"  Unchanged files: {', '.join(sorted(d.unchanged_files)) or '(none)'}")

    if d.modified_files:
        click.echo()
        for filename in sorted(d.modified_files):
            fd = d.file_diff(filename)
            click.echo(f"{filename}: {fd.summary()}")

    sys.exit(1)


@main.command("write")
@click.argument("path")
@click.argument("output")
@click.option("--schedule-url", default="", help="Source URL for metadata.json provenance.")
@click.option("--date-retrieved", default=None, help="Override date_retrieved (ISO 8601, default: now).")
def write_cmd(path: str, output: str, schedule_url: str, date_retrieved: str | None) -> None:
    """Write a GTFS feed as exploded parquet."""
    from .storage import write_exploded

    archive = _load_archive(path)
    metadata = write_exploded(
        archive,
        base_path=output,
        schedule_url=schedule_url,
        date_retrieved=date_retrieved,
    )
    fp = archive.fingerprint
    click.echo(
        f"Wrote {len(archive.filenames)} files to {output}/_feed_digest={fp.root_hash}/"
    )


@main.command()
@click.argument("path")
@click.argument("output")
def normalize(path: str, output: str) -> None:
    """Write a normalized GTFS zip (canonical CSVs + .gtfs_digester.json)."""
    archive = _load_archive(path)
    archive.to_normalized_zip(output)
    fp = archive.fingerprint
    click.echo(
        f"Normalized {len(archive.filenames)} files -> {output} (fingerprint: {fp.root_hash})"
    )
