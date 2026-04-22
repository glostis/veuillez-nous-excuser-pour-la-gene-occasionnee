"""
Pytest tests for the GTFS static data ingestion module.
Tests use the test data in tests/data/gtfs_static/ to verify the ingestion logic.
"""

import os
import tempfile
from unittest.mock import patch

import duckdb
import pytest

from gene_occasionnee import TABLE
from gene_occasionnee.back import GTFS_COMPIEGNE_STOP_ID, GTFS_PARIS_NORD_STOP_ID
from gene_occasionnee.back.ingest_gtfs_static import (
    find_trips_through_both_stations,
    get_trip_info,
    load_route_info,
    load_service_dates,
    parse_gtfs_time,
    store_in_duckdb,
)

# Test data directory
TEST_DATA_DIR = "tests/data/gtfs_static"


def get_test_data_path(filename):
    """Get the full path to a test data file."""
    return os.path.join(TEST_DATA_DIR, filename)


def test_load_service_dates():
    """Test loading service dates from calendar_dates.txt."""
    # Mock today's date to be 2024-01-01
    with patch("gene_occasionnee.back.ingest_gtfs_static.datetime") as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2024-01-01"
        mock_datetime.now.return_value.strftime.return_value = "2024-01-01"

        service_dates = load_service_dates(TEST_DATA_DIR)

        # Should find 7 services running on 2024-01-01
        assert len(service_dates) == 7
        assert "000001" in service_dates
        assert "000002" in service_dates
        assert "000003" in service_dates
        assert "000004" in service_dates
        assert "000005" in service_dates
        assert "000006" in service_dates
        assert "000007" in service_dates


def test_load_route_info():
    """Test loading route information from routes.txt."""
    route_info = load_route_info(TEST_DATA_DIR)

    # Should find 3 routes
    assert len(route_info) == 3
    assert route_info["route_K01"] == "K01"
    assert route_info["route_K02"] == "K02"
    assert route_info["route_K03"] == "K03"


def test_get_trip_info():
    """Test getting trip information from trips.txt."""
    # Test trip_01
    route_id, service_id, trip_headsign = get_trip_info(TEST_DATA_DIR, "trip_01")
    assert route_id == "route_K01"
    assert service_id == "000001"
    assert trip_headsign == "01"

    # Test trip_04
    route_id, service_id, trip_headsign = get_trip_info(TEST_DATA_DIR, "trip_04")
    assert route_id == "route_K04"
    assert service_id == "000004"
    assert trip_headsign == "04"


def test_find_trips_through_both_stations():
    """Test finding trips that go through both Paris Nord and Compiègne."""
    # Mock today's date to be 2024-01-01
    with patch("gene_occasionnee.back.ingest_gtfs_static.datetime") as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2024-01-01"

        service_dates = load_service_dates(TEST_DATA_DIR)
        relevant_trips = find_trips_through_both_stations(TEST_DATA_DIR, service_dates)

        # Should find 7 trips that go through both stations and run today
        assert len(relevant_trips) == 7

        # Check that all trips have both stations
        for trip in relevant_trips:
            trip_id = trip["trip_id"]
            stops = trip["stops"]

            # Find if this trip has both stations
            has_paris = any(s["stop_id"] == GTFS_PARIS_NORD_STOP_ID for s in stops)
            has_compiegne = any(s["stop_id"] == GTFS_COMPIEGNE_STOP_ID for s in stops)

            assert has_paris, f"Trip {trip_id} should have Paris Nord"
            assert has_compiegne, f"Trip {trip_id} should have Compiègne"


def test_parse_gtfs_time():
    """Test parsing GTFS time format."""
    # Test valid time
    result = parse_gtfs_time("2024-01-01", "08:00:00")
    assert result == "2024-01-01 08:00:00"

    # Test empty time
    result = parse_gtfs_time("2024-01-01", "")
    assert result is None

    # Test None time
    result = parse_gtfs_time("2024-01-01", None)
    assert result is None


def test_store_in_duckdb():
    """Test storing data in DuckDB database."""
    # Create a temporary database for testing
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".duckdb")
    temp_db.close()

    try:
        # Remove the file if it exists (it might be an empty file from NamedTemporaryFile)
        if os.path.exists(temp_db.name):
            os.unlink(temp_db.name)

        # Mock today's date to be 2024-01-01
        with patch("gene_occasionnee.back.ingest_gtfs_static.datetime") as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "2024-01-01"

            # Get service dates and relevant trips
            service_dates = load_service_dates(TEST_DATA_DIR)
            relevant_trips = find_trips_through_both_stations(TEST_DATA_DIR, service_dates)

            # Store in temporary database
            with patch("gene_occasionnee.back.ingest_gtfs_static.DB_PATH", temp_db.name):
                inserted_count = store_in_duckdb(relevant_trips, TEST_DATA_DIR, service_dates)

            # Verify that data was inserted
            assert inserted_count == 7

            # Connect to the temporary database and verify the data
            conn = duckdb.connect(temp_db.name)
            result = conn.execute(f"SELECT COUNT(*) as count FROM {TABLE}").fetchone()
            assert result[0] == 7

            # Verify specific trip data
            result = conn.execute(f"SELECT trip_id, route_short_name FROM {TABLE} WHERE trip_id = 'trip_01'").fetchone()
            assert result[0] == "trip_01"
            assert result[1] == "K01"

            # Verify departure and arrival stations
            result = conn.execute(
                f"SELECT departure_station_name, arrival_station_name FROM {TABLE} WHERE trip_id = 'trip_01'"
            ).fetchone()
            assert result[0] == "Paris Nord"
            assert result[1] == "Compiègne"

            # Verify times
            result = conn.execute(
                f"SELECT departure_time_scheduled, arrival_time_scheduled FROM {TABLE} WHERE trip_id = 'trip_01'"
            ).fetchone()
            assert "08:00:00" in str(result[0])
            assert "09:00:00" in str(result[1])

            conn.close()

    finally:
        # Clean up temporary database
        if os.path.exists(temp_db.name):
            os.unlink(temp_db.name)


def test_trip_direction_paris_to_compiegne():
    """Test that trip direction is correctly identified (Paris -> Compiègne)."""
    # Mock today's date to be 2024-01-01
    with patch("gene_occasionnee.back.ingest_gtfs_static.datetime") as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2024-01-01"

        service_dates = load_service_dates(TEST_DATA_DIR)
        relevant_trips = find_trips_through_both_stations(TEST_DATA_DIR, service_dates)

        # Find trip_01 (Paris -> Compiègne)
        trip_01 = next(trip for trip in relevant_trips if trip["trip_id"] == "trip_01")
        stops = trip_01["stops"]

        # Find Paris and Compiègne indices
        paris_index = next(i for i, s in enumerate(stops) if s["stop_id"] == GTFS_PARIS_NORD_STOP_ID)
        compiegne_index = next(i for i, s in enumerate(stops) if s["stop_id"] == GTFS_COMPIEGNE_STOP_ID)

        # Paris should come before Compiègne
        assert paris_index < compiegne_index


def test_trip_direction_compiegne_to_paris():
    """Test that trip direction is correctly identified (Compiègne -> Paris)."""
    # Mock today's date to be 2024-01-01
    with patch("gene_occasionnee.back.ingest_gtfs_static.datetime") as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2024-01-01"

        service_dates = load_service_dates(TEST_DATA_DIR)
        relevant_trips = find_trips_through_both_stations(TEST_DATA_DIR, service_dates)

        # Find trip_04 (Compiègne -> Paris)
        trip_04 = next(trip for trip in relevant_trips if trip["trip_id"] == "trip_04")
        stops = trip_04["stops"]

        # Find Paris and Compiègne indices
        paris_index = next(i for i, s in enumerate(stops) if s["stop_id"] == GTFS_PARIS_NORD_STOP_ID)
        compiegne_index = next(i for i, s in enumerate(stops) if s["stop_id"] == GTFS_COMPIEGNE_STOP_ID)

        # Compiègne should come before Paris
        assert compiegne_index < paris_index


def test_no_duplicate_data_insertion():
    """Test that duplicate data insertion is prevented."""
    # Create a temporary database for testing
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".duckdb")
    temp_db.close()

    try:
        # Remove the file if it exists
        if os.path.exists(temp_db.name):
            os.unlink(temp_db.name)

        # Mock today's date to be 2024-01-01
        with patch("gene_occasionnee.back.ingest_gtfs_static.datetime") as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "2024-01-01"

            # Get service dates and relevant trips
            service_dates = load_service_dates(TEST_DATA_DIR)
            relevant_trips = find_trips_through_both_stations(TEST_DATA_DIR, service_dates)

            # Store in temporary database (first time)
            with patch("gene_occasionnee.back.ingest_gtfs_static.DB_PATH", temp_db.name):
                inserted_count = store_in_duckdb(relevant_trips, TEST_DATA_DIR, service_dates)

            # Verify that data was inserted
            assert inserted_count == 7

            # Try to insert again (should fail)
            with patch("gene_occasionnee.back.ingest_gtfs_static.DB_PATH", temp_db.name):
                with pytest.raises(ValueError, match="Data already exists for 2024-01-01"):
                    store_in_duckdb(relevant_trips, TEST_DATA_DIR, service_dates)

    finally:
        # Clean up temporary database
        if os.path.exists(temp_db.name):
            os.unlink(temp_db.name)


def test_service_filtering():
    """Test that only trips running today are included."""
    # Mock today's date to be 2024-01-02 (only some services run on this date)
    with patch("gene_occasionnee.back.ingest_gtfs_static.datetime") as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2024-01-02"

        service_dates = load_service_dates(TEST_DATA_DIR)

        # Should only find 3 services running on 2024-01-02
        assert len(service_dates) == 3
        assert "000001" in service_dates
        assert "000002" in service_dates
        assert "000006" in service_dates

        # Find trips running today
        relevant_trips = find_trips_through_both_stations(TEST_DATA_DIR, service_dates)

        # Should find fewer trips (only those with services 000001, 000002, 000006)
        # trip_01, trip_02, trip_06 should be running
        assert len(relevant_trips) == 3
        trip_ids = [trip["trip_id"] for trip in relevant_trips]
        assert "trip_01" in trip_ids
        assert "trip_02" in trip_ids
        assert "trip_06" in trip_ids
