"""
Pytest tests for the GTFS real-time data ingestion module.
Tests use mocked GTFS-RT data to verify the ingestion logic.
"""

import os
import tempfile
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from google.transit import gtfs_realtime_pb2

from gene_occasionnee import TABLE
from gene_occasionnee.back import COMPIEGNE_STOP_ID, PARIS_NORD_STOP_ID
from gene_occasionnee.back.ingest_gtfs_rt import (
    clean_stop_id,
    fetch_and_decode_gtfs_rt,
    get_trip_ids_from_duckdb,
    parse_gtfs_rt_timestamp,
    process_gtfs_rt_data,
    update_real_times_in_duckdb,
)


def create_mock_gtfs_rt_feed():
    """Create a mock GTFS-RT feed with test data."""
    feed = gtfs_realtime_pb2.FeedMessage()

    # Add required header
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = int(datetime.now().timestamp())

    # Create a trip update entity
    entity = feed.entity.add()
    entity.id = "test_entity_1"

    # Create trip update
    trip_update = entity.trip_update
    trip = trip_update.trip
    trip.trip_id = "trip_01"
    trip.route_id = "route_K01"

    # Add stop time updates for Paris Nord (departure)
    stu_paris = trip_update.stop_time_update.add()
    stu_paris.stop_id = PARIS_NORD_STOP_ID

    # Set departure time (current time + 1 hour)
    departure_time = int((datetime.now().replace(microsecond=0) + timedelta(hours=1)).timestamp())
    stu_paris.departure.time = departure_time
    stu_paris.departure.delay = 300  # 5 minutes delay

    # Add stop time updates for Compiègne (arrival)
    stu_compiegne = trip_update.stop_time_update.add()
    stu_compiegne.stop_id = COMPIEGNE_STOP_ID

    # Set arrival time (current time + 2 hours)
    arrival_time = int((datetime.now().replace(microsecond=0) + timedelta(hours=2)).timestamp())
    stu_compiegne.arrival.time = arrival_time
    stu_compiegne.arrival.delay = 600  # 10 minutes delay

    return feed


def create_mock_gtfs_rt_feed_with_schedule_relationships():
    """Create a mock GTFS-RT feed with schedule relationships."""
    feed = gtfs_realtime_pb2.FeedMessage()

    # Add required header
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = int(datetime.now().timestamp())

    # Create a trip update entity with CANCELED trip
    entity = feed.entity.add()
    entity.id = "test_entity_canceled"

    trip_update = entity.trip_update
    trip = trip_update.trip
    trip.trip_id = "trip_canceled"
    trip.route_id = "route_K01"
    trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.CANCELED

    # Add stop time updates for Paris Nord (departure) - SKIPPED
    stu_paris = trip_update.stop_time_update.add()
    stu_paris.stop_id = PARIS_NORD_STOP_ID
    stu_paris.schedule_relationship = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED

    # Add stop time updates for Compiègne (arrival) - SKIPPED
    stu_compiegne = trip_update.stop_time_update.add()
    stu_compiegne.stop_id = COMPIEGNE_STOP_ID
    stu_compiegne.schedule_relationship = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED

    return feed


def test_parse_gtfs_rt_timestamp():
    """Test parsing GTFS-RT timestamp (Unix epoch) to datetime."""
    # Test with current timestamp
    now = datetime.now()
    timestamp = int(now.timestamp())
    result = parse_gtfs_rt_timestamp(timestamp)

    # Should return formatted datetime string
    assert result is not None
    assert "-" in result and ":" in result

    # Test with None
    result = parse_gtfs_rt_timestamp(None)
    assert result is None


def test_clean_stop_id():
    """Test cleaning stop IDs to extract numeric ID."""
    # Test with standard format
    result = clean_stop_id("stop_12345678")
    assert result == "12345678"

    # Test with just numeric
    result = clean_stop_id("12345678")
    assert result == "12345678"

    # Test with None
    result = clean_stop_id(None)
    assert result == ""

    # Test with empty string
    result = clean_stop_id("")
    assert result == ""


def test_get_trip_ids_from_duckdb():
    """Test getting trip IDs from DuckDB for today's trips."""
    # Create a temporary database for testing
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".duckdb")
    temp_db.close()

    try:
        # Remove the file if it exists (it might be an empty file from NamedTemporaryFile)
        if os.path.exists(temp_db.name):
            os.unlink(temp_db.name)

        # Create test data
        conn = duckdb.connect(temp_db.name)
        conn.execute(f"""
            CREATE TABLE {TABLE} (
                trip_id STRING,
                departure_time_scheduled TIMESTAMP,
                arrival_time_scheduled TIMESTAMP,
                departure_schedule_relationship STRING,
                arrival_schedule_relationship STRING,
                trip_schedule_relationship STRING
            )
        """)

        # Insert test trips for today
        today = datetime.now().strftime("%Y-%m-%d")
        conn.execute(f"""
            INSERT INTO {TABLE} VALUES
                ('trip_01', '{today} 08:00:00', '{today} 09:00:00', NULL, NULL, NULL),
                ('trip_02', '{today} 10:00:00', '{today} 11:00:00', NULL, NULL, NULL),
                ('trip_03', '{today} 12:00:00', '{today} 13:00:00', NULL, NULL, NULL)
        """)
        conn.close()

        # Test the function
        with patch("gene_occasionnee.back.ingest_gtfs_rt.DB_PATH", temp_db.name):
            trip_ids = get_trip_ids_from_duckdb()

        # Should find 3 trip IDs
        assert len(trip_ids) == 3
        assert "trip_01" in trip_ids
        assert "trip_02" in trip_ids
        assert "trip_03" in trip_ids

    finally:
        # Clean up temporary database
        if os.path.exists(temp_db.name):
            os.unlink(temp_db.name)


def test_update_real_times_in_duckdb():
    """Test updating real departure and arrival times in DuckDB."""
    # Create a temporary database for testing
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".duckdb")
    temp_db.close()

    try:
        # Remove the file if it exists (it might be an empty file from NamedTemporaryFile)
        if os.path.exists(temp_db.name):
            os.unlink(temp_db.name)

        # Create test data
        conn = duckdb.connect(temp_db.name)
        conn.execute(f"""
            CREATE TABLE {TABLE} (
                trip_id STRING,
                departure_time_scheduled TIMESTAMP,
                arrival_time_scheduled TIMESTAMP,
                departure_time_real TIMESTAMP,
                arrival_time_real TIMESTAMP,
                departure_gtfs_delay INTEGER,
                arrival_gtfs_delay INTEGER,
                departure_schedule_relationship STRING,
                arrival_schedule_relationship STRING,
                trip_schedule_relationship STRING,
                updated_at TIMESTAMP
            )
        """)

        # Insert test trips for today
        today = datetime.now().strftime("%Y-%m-%d")
        conn.execute(f"""
            INSERT INTO {TABLE} VALUES
                ('trip_01', '{today} 08:00:00', '{today} 09:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
                ('trip_02', '{today} 10:00:00', '{today} 11:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """)
        conn.close()

        # Prepare test updates
        today = datetime.now().strftime("%Y-%m-%d")
        trip_updates = [
            (
                "trip_01",
                {"time": f"{today} 08:05:00", "delay": 300},  # 5 minutes delay
                {"time": f"{today} 09:10:00", "delay": 600},  # 10 minutes delay
                None,  # trip_schedule_relationship
                None,  # departure_schedule_relationship
                None,  # arrival_schedule_relationship
            ),
            (
                "trip_02",
                {"time": f"{today} 10:03:00", "delay": 180},  # 3 minutes delay
                None,
                None,  # trip_schedule_relationship
                None,  # departure_schedule_relationship
                None,  # arrival_schedule_relationship
            ),
        ]

        # Test the function
        with patch("gene_occasionnee.back.ingest_gtfs_rt.DB_PATH", temp_db.name):
            updated_count = update_real_times_in_duckdb(trip_updates)

        # Should update 2 trips
        assert updated_count == 2

        # Verify the updates
        conn = duckdb.connect(temp_db.name)

        # Check trip_01
        result = conn.execute(f"""
            SELECT departure_time_real, arrival_time_real,
                   departure_gtfs_delay, arrival_gtfs_delay
            FROM {TABLE} WHERE trip_id = 'trip_01'
        """).fetchone()
        # Convert datetime to string for comparison
        assert str(result[0]) == f"{today} 08:05:00"
        assert str(result[1]) == f"{today} 09:10:00"
        assert result[2] == 300
        assert result[3] == 600

        # Check trip_02
        result = conn.execute(f"""
            SELECT departure_time_real, arrival_time_real,
                   departure_gtfs_delay, arrival_gtfs_delay
            FROM {TABLE} WHERE trip_id = 'trip_02'
        """).fetchone()
        # Convert datetime to string for comparison
        assert str(result[0]) == f"{today} 10:03:00"
        assert result[1] is None  # Should remain NULL
        assert result[2] == 180
        assert result[3] is None  # Should remain NULL

        conn.close()

    finally:
        # Clean up temporary database
        if os.path.exists(temp_db.name):
            os.unlink(temp_db.name)


def test_process_gtfs_rt_data():
    """Test processing GTFS-RT data and finding matching trips."""
    # Create a temporary database for testing
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".duckdb")
    temp_db.close()

    try:
        # Remove the file if it exists (it might be an empty file from NamedTemporaryFile)
        if os.path.exists(temp_db.name):
            os.unlink(temp_db.name)

        # Create test data
        conn = duckdb.connect(temp_db.name)
        conn.execute(f"""
            CREATE TABLE {TABLE} (
                trip_id STRING,
                departure_station_name STRING,
                arrival_station_name STRING,
                departure_time_scheduled TIMESTAMP,
                arrival_time_scheduled TIMESTAMP,
                departure_schedule_relationship STRING,
                arrival_schedule_relationship STRING,
                trip_schedule_relationship STRING,
                updated_at TIMESTAMP
            )
        """)

        # Insert test trips for today
        today = datetime.now().strftime("%Y-%m-%d")
        conn.execute(f"""
            INSERT INTO {TABLE} VALUES
                ('trip_01', 'Paris Nord', 'Compiègne', '{today} 08:00:00', '{today} 09:00:00', NULL, NULL, NULL, NULL),
                ('trip_02', 'Compiègne', 'Paris Nord', '{today} 10:00:00', '{today} 11:00:00', NULL, NULL, NULL, NULL),
                ('trip_03', 'Paris Nord', 'Compiègne', '{today} 12:00:00', '{today} 13:00:00', NULL, NULL, NULL, NULL)
        """)
        conn.close()

        # Create mock GTFS-RT feed
        feed = create_mock_gtfs_rt_feed()

        # Test the function
        with patch("gene_occasionnee.back.ingest_gtfs_rt.DB_PATH", temp_db.name):
            trip_updates = process_gtfs_rt_data(feed, ["trip_01", "trip_02", "trip_03"])

        # Should find updates for trip_01
        assert len(trip_updates) == 1
        assert trip_updates[0][0] == "trip_01"

        # Verify the updates contain departure and arrival data
        trip_id, departure, arrival, trip_sch_rel, dep_sch_rel, arr_sch_rel = trip_updates[0]
        assert departure is not None
        assert arrival is not None
        assert "time" in departure
        assert "delay" in departure
        assert "time" in arrival
        assert "delay" in arrival
        # Schedule relationships should be None for this test
        assert trip_sch_rel is None
        assert dep_sch_rel is None
        assert arr_sch_rel is None

    finally:
        # Clean up temporary database
        if os.path.exists(temp_db.name):
            os.unlink(temp_db.name)


def test_process_gtfs_rt_data_direction_compiegne_to_paris():
    """Test processing GTFS-RT data for Compiègne -> Paris direction."""
    # Create a temporary database for testing
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".duckdb")
    temp_db.close()

    try:
        # Remove the file if it exists (it might be an empty file from NamedTemporaryFile)
        if os.path.exists(temp_db.name):
            os.unlink(temp_db.name)

        # Create test data
        conn = duckdb.connect(temp_db.name)
        conn.execute(f"""
            CREATE TABLE {TABLE} (
                trip_id STRING,
                departure_station_name STRING,
                arrival_station_name STRING,
                departure_time_scheduled TIMESTAMP,
                arrival_time_scheduled TIMESTAMP,
                departure_schedule_relationship STRING,
                arrival_schedule_relationship STRING,
                trip_schedule_relationship STRING,
                updated_at TIMESTAMP
            )
        """)

        # Insert test trip for Compiègne -> Paris
        today = datetime.now().strftime("%Y-%m-%d")
        conn.execute(f"""
            INSERT INTO {TABLE} VALUES
                ('trip_02', 'Compiègne', 'Paris Nord', '{today} 10:00:00', '{today} 11:00:00', NULL, NULL, NULL, NULL)
        """)
        conn.close()

        # Create mock GTFS-RT feed with data for both stations
        feed = gtfs_realtime_pb2.FeedMessage()
        entity = feed.entity.add()
        entity.id = "test_entity_2"

        trip_update = entity.trip_update
        trip = trip_update.trip
        trip.trip_id = "trip_02"

        # Add Compiègne departure
        stu_compiegne = trip_update.stop_time_update.add()
        stu_compiegne.stop_id = COMPIEGNE_STOP_ID
        departure_time = int((datetime.now().replace(microsecond=0) + timedelta(hours=1)).timestamp())
        stu_compiegne.departure.time = departure_time
        stu_compiegne.departure.delay = 180  # 3 minutes delay

        # Add Paris Nord arrival
        stu_paris = trip_update.stop_time_update.add()
        stu_paris.stop_id = PARIS_NORD_STOP_ID
        arrival_time = int((datetime.now().replace(microsecond=0) + timedelta(hours=2)).timestamp())
        stu_paris.arrival.time = arrival_time
        stu_paris.arrival.delay = 300  # 5 minutes delay

        # Test the function
        with patch("gene_occasionnee.back.ingest_gtfs_rt.DB_PATH", temp_db.name):
            trip_updates = process_gtfs_rt_data(feed, ["trip_02"])

        # Should find updates for trip_02
        assert len(trip_updates) == 1
        assert trip_updates[0][0] == "trip_02"

        # For Compiègne -> Paris direction, departure should be from Compiègne
        # and arrival should be at Paris Nord
        trip_id, departure, arrival, trip_sch_rel, dep_sch_rel, arr_sch_rel = trip_updates[0]
        assert departure is not None  # Compiègne departure
        assert arrival is not None  # Paris Nord arrival
        # Schedule relationships should be None for this test
        assert trip_sch_rel is None
        assert dep_sch_rel is None
        assert arr_sch_rel is None

    finally:
        # Clean up temporary database
        if os.path.exists(temp_db.name):
            os.unlink(temp_db.name)


def test_process_gtfs_rt_data_no_matching_trips():
    """Test processing GTFS-RT data when no trips match."""
    # Create mock GTFS-RT feed with different trip ID
    feed = gtfs_realtime_pb2.FeedMessage()
    entity = feed.entity.add()
    entity.id = "test_entity_3"

    trip_update = entity.trip_update
    trip = trip_update.trip
    trip.trip_id = "trip_999"  # Not in our database

    # Test the function with empty trip_ids list
    trip_updates = process_gtfs_rt_data(feed, [])
    assert len(trip_updates) == 0

    # Test with non-matching trip IDs
    trip_updates = process_gtfs_rt_data(feed, ["trip_01", "trip_02"])
    assert len(trip_updates) == 0


def test_fetch_and_decode_gtfs_rt_mocked():
    """Test fetching and decoding GTFS-RT data with mocked response."""
    # Create mock response
    mock_feed = create_mock_gtfs_rt_feed()
    mock_response = MagicMock()
    mock_response.content = mock_feed.SerializeToString()
    mock_response.raise_for_status = MagicMock()

    # Mock requests.get
    with patch("gene_occasionnee.back.ingest_gtfs_rt.requests.get", return_value=mock_response):
        feed = fetch_and_decode_gtfs_rt()

        # Should successfully decode the feed
        assert len(feed.entity) == 1
        assert feed.entity[0].trip_update.trip.trip_id == "trip_01"


def test_process_gtfs_rt_data_with_schedule_relationships():
    """Test processing GTFS-RT data with schedule relationships (CANCELED, SKIPPED)."""
    # Create a temporary database for testing
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".duckdb")
    temp_db.close()

    try:
        # Remove the file if it exists (it might be an empty file from NamedTemporaryFile)
        if os.path.exists(temp_db.name):
            os.unlink(temp_db.name)

        # Create test data
        conn = duckdb.connect(temp_db.name)
        conn.execute(f"""
            CREATE TABLE {TABLE} (
                trip_id STRING,
                departure_station_name STRING,
                arrival_station_name STRING,
                departure_time_scheduled TIMESTAMP,
                arrival_time_scheduled TIMESTAMP,
                departure_schedule_relationship STRING,
                arrival_schedule_relationship STRING,
                trip_schedule_relationship STRING,
                updated_at TIMESTAMP
            )
        """)

        # Insert test trip for today
        today = datetime.now().strftime("%Y-%m-%d")
        conn.execute(f"""
            INSERT INTO {TABLE} VALUES
                ('trip_canceled', 'Paris Nord', 'Compiègne', '{today} 08:00:00', '{today} 09:00:00', NULL, NULL, NULL, NULL)
        """)
        conn.close()

        # Create mock GTFS-RT feed with schedule relationships
        feed = create_mock_gtfs_rt_feed_with_schedule_relationships()

        # Test the function
        with patch("gene_occasionnee.back.ingest_gtfs_rt.DB_PATH", temp_db.name):
            trip_updates = process_gtfs_rt_data(feed, ["trip_canceled"])

        # Should find updates for trip_canceled (even without time updates, because of schedule relationships)
        assert len(trip_updates) == 1
        assert trip_updates[0][0] == "trip_canceled"

        # Verify the updates contain schedule relationship data
        trip_id, departure, arrival, trip_sch_rel, dep_sch_rel, arr_sch_rel = trip_updates[0]
        assert trip_id == "trip_canceled"
        assert trip_sch_rel == "CANCELED"  # Trip-level schedule relationship
        assert dep_sch_rel == "SKIPPED"  # Paris Nord departure skipped
        assert arr_sch_rel == "SKIPPED"  # Compiègne arrival skipped
        assert departure is None  # No time updates for skipped stations
        assert arrival is None  # No time updates for skipped stations

        # Apply the updates to the database
        with patch("gene_occasionnee.back.ingest_gtfs_rt.DB_PATH", temp_db.name):
            update_real_times_in_duckdb(trip_updates)

        # Verify the updates are applied to the database
        conn = duckdb.connect(temp_db.name)
        result = conn.execute(f"""
            SELECT
                trip_schedule_relationship,
                departure_schedule_relationship,
                arrival_schedule_relationship
            FROM {TABLE} WHERE trip_id = 'trip_canceled'
        """).fetchone()

        assert result[0] == "CANCELED"
        assert result[1] == "SKIPPED"
        assert result[2] == "SKIPPED"

        conn.close()

    finally:
        # Clean up temporary database
        if os.path.exists(temp_db.name):
            os.unlink(temp_db.name)


def test_update_real_times_in_duckdb_with_schedule_relationships():
    """Test updating schedule relationships in DuckDB."""
    # Create a temporary database for testing
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".duckdb")
    temp_db.close()

    try:
        # Remove the file if it exists (it might be an empty file from NamedTemporaryFile)
        if os.path.exists(temp_db.name):
            os.unlink(temp_db.name)

        # Create test data
        conn = duckdb.connect(temp_db.name)
        conn.execute(f"""
            CREATE TABLE {TABLE} (
                trip_id STRING,
                departure_time_scheduled TIMESTAMP,
                arrival_time_scheduled TIMESTAMP,
                departure_time_real TIMESTAMP,
                arrival_time_real TIMESTAMP,
                departure_gtfs_delay INTEGER,
                arrival_gtfs_delay INTEGER,
                departure_schedule_relationship STRING,
                arrival_schedule_relationship STRING,
                trip_schedule_relationship STRING,
                updated_at TIMESTAMP
            )
        """)

        # Insert test trip for today
        today = datetime.now().strftime("%Y-%m-%d")
        conn.execute(f"""
            INSERT INTO {TABLE} VALUES
                ('trip_01', '{today} 08:00:00', '{today} 09:00:00', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """)
        conn.close()

        # Prepare test updates with schedule relationships
        trip_updates = [
            (
                "trip_01",
                None,  # No departure time update
                None,  # No arrival time update
                "CANCELED",  # Trip canceled
                "SKIPPED",  # Departure skipped
                "SKIPPED",  # Arrival skipped
            ),
        ]

        # Test the function
        with patch("gene_occasionnee.back.ingest_gtfs_rt.DB_PATH", temp_db.name):
            updated_count = update_real_times_in_duckdb(trip_updates)

        # Should update 1 trip
        assert updated_count == 1

        # Verify the schedule relationships were updated
        conn = duckdb.connect(temp_db.name)
        result = conn.execute(f"""
            SELECT
                trip_schedule_relationship,
                departure_schedule_relationship,
                arrival_schedule_relationship
            FROM {TABLE} WHERE trip_id = 'trip_01'
        """).fetchone()

        assert result[0] == "CANCELED"
        assert result[1] == "SKIPPED"
        assert result[2] == "SKIPPED"

        conn.close()

    finally:
        # Clean up temporary database
        if os.path.exists(temp_db.name):
            os.unlink(temp_db.name)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
