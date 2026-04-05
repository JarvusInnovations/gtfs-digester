"""Shared fixtures for gtfs-digester tests."""

from pathlib import Path

import pytest

# Use a real SEPTA GTFS feed for integration tests
SEPTA_FEED = Path("/home/chris/transit-lake/.scratch/gtfs-schedules-historical/mdb-502/mdb-502-202512200121.zip")
SEPTA_FEED_2 = Path("/home/chris/transit-lake/.scratch/gtfs-schedules-historical/mdb-502/mdb-502-202603280031.zip")


@pytest.fixture
def septa_zip() -> Path:
    """Path to a real SEPTA GTFS zip for integration tests."""
    if not SEPTA_FEED.exists():
        pytest.skip("SEPTA GTFS feed not available")
    return SEPTA_FEED


@pytest.fixture
def septa_zip_2() -> Path:
    """Path to a different SEPTA GTFS zip for diff tests."""
    if not SEPTA_FEED_2.exists():
        pytest.skip("SEPTA GTFS feed 2 not available")
    return SEPTA_FEED_2
