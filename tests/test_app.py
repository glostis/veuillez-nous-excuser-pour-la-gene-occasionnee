"""
Pytest tests for the Flask API routes.
Tests use a synthetic database with ~10 rows containing trips from 3 lines with various delays.
"""

import os
from datetime import datetime, timedelta

import duckdb
import pytest

from gene_occasionnee import TABLE
from gene_occasionnee.front.app import app

# Test database path template
TEST_DB_PATH_TEMPLATE = "test_gtfs_{test_name}.duckdb"


@pytest.fixture(scope="module")
def test_client():
    """Create a test client for the Flask app."""
    app.config["TESTING"] = True

    with app.test_client() as client:
        yield client


@pytest.fixture(scope="function", autouse=True)
def setup_test_database(request):
    """Create a test database with synthetic data using a unique name per test."""
    # Generate unique database path based on test function name
    test_name = request.node.name.replace("[", "_").replace("]", "_").replace("/", "_")
    test_db_path = TEST_DB_PATH_TEMPLATE.format(test_name=test_name)

    # Create test database
    conn = duckdb.connect(test_db_path)

    # Create table with new GTFS schema
    conn.execute(f"""
        CREATE TABLE {TABLE} (
            trip_id VARCHAR,
            route_id VARCHAR,
            route_short_name VARCHAR,
            trip_headsign VARCHAR,
            departure_station_name VARCHAR,
            departure_time_scheduled TIMESTAMP WITH TIME ZONE,
            departure_time_real TIMESTAMP WITH TIME ZONE,
            departure_gtfs_delay INTEGER,
            arrival_station_name VARCHAR,
            arrival_time_scheduled TIMESTAMP WITH TIME ZONE,
            arrival_time_real TIMESTAMP WITH TIME ZONE,
            arrival_gtfs_delay INTEGER,
            created_at TIMESTAMP WITH TIME ZONE,
            updated_at TIMESTAMP WITH TIME ZONE,
            departure_schedule_relationship VARCHAR,
            arrival_schedule_relationship VARCHAR,
            trip_schedule_relationship VARCHAR
        )
    """)

    # Insert synthetic data - 10 rows with:
    # - 3 different lines names (K01-03)
    # - 7 different train numbers (01-07)
    # - various delays
    base_date = datetime(2024, 1, 1, 0, 0, 0)

    data = [
        # Line K01 - Paris Nord → Compiègne
        (
            "trip_01",
            "route_K01",
            "K01",
            "01",
            "Paris Nord",
            base_date + timedelta(hours=8),
            base_date + timedelta(hours=8, minutes=5),
            5,
            "Compiègne",
            base_date + timedelta(hours=9),
            base_date + timedelta(hours=9, minutes=10),
            10,
        ),  # train 1: 10 min delay
        (
            "trip_02",
            "route_K01",
            "K01",
            "02",
            "Paris Nord",
            base_date + timedelta(hours=10),
            base_date + timedelta(hours=10, minutes=2),
            2,
            "Compiègne",
            base_date + timedelta(hours=11),
            base_date + timedelta(hours=11, minutes=3),
            3,
        ),  # train 2: 3 min delay
        (
            "trip_03",
            "route_K01",
            "K01",
            "03",
            "Paris Nord",
            base_date + timedelta(hours=12),
            base_date + timedelta(hours=12),
            0,
            "Compiègne",
            base_date + timedelta(hours=13),
            base_date + timedelta(hours=13),
            0,
        ),  # train 3: 0 min delay (on time)
        # Line K02 - Compiègne → Paris Nord
        (
            "trip_04",
            "route_K02",
            "K02",
            "04",
            "Compiègne",
            base_date + timedelta(hours=14),
            base_date + timedelta(hours=14, minutes=15),
            15,
            "Paris Nord",
            base_date + timedelta(hours=15),
            base_date + timedelta(hours=15, minutes=30),
            30,
        ),  # train 4: 30 min delay
        (
            "trip_05",
            "route_K02",
            "K02",
            "05",
            "Compiègne",
            base_date + timedelta(hours=16),
            base_date + timedelta(hours=16, minutes=5),
            5,
            "Paris Nord",
            base_date + timedelta(hours=17),
            base_date + timedelta(hours=17, minutes=20),
            20,
        ),  # train 5: 20 min delay
        # Line K03 - Paris Nord → Compiègne (earlier than K02 to test sorting)
        (
            "trip_06",
            "route_K03",
            "K03",
            "06",
            "Paris Nord",
            base_date + timedelta(hours=13),  # 13:00 - earlier than K02's 14:00
            base_date + timedelta(hours=13, minutes=10),
            10,
            "Compiègne",
            base_date + timedelta(hours=14),
            base_date + timedelta(hours=14, minutes=45),
            45,
        ),  # train 6: 46 min delay
        (
            "trip_07",
            "route_K03",
            "K03",
            "07",
            "Paris Nord",
            base_date + timedelta(hours=15),  # 15:00 - between K02's 14:00 and 16:00
            base_date + timedelta(hours=15, minutes=5),
            5,
            "Compiègne",
            base_date + timedelta(hours=16),
            base_date + timedelta(hours=17),
            60,
        ),  # train 7: 60 min delay (over 45)
        # Additional data for different dates
        (
            "trip_01",
            "route_K01",
            "K01",
            "01",
            "Paris Nord",
            base_date + timedelta(days=1, hours=8),
            base_date + timedelta(days=1, hours=8, minutes=2),
            2,
            "Compiègne",
            base_date + timedelta(days=1, hours=9),
            base_date + timedelta(days=1, hours=9, minutes=5),
            5,
        ),  # train 8 (same as train 1 but different day): 5 min delay on day 2
        (
            "trip_02",
            "route_K01",
            "K01",
            "02",
            "Compiègne",
            base_date + timedelta(days=1, hours=10),
            base_date + timedelta(days=1, hours=10, minutes=8),
            8,
            "Paris Nord",
            base_date + timedelta(days=1, hours=11),
            base_date + timedelta(days=1, hours=11, minutes=15),
            15,
        ),  # train 9 (same as train 2 but different day): 15 min delay on day 2
        (
            "trip_06",
            "route_K03",
            "K03",
            "06",
            "Paris Nord",
            base_date + timedelta(days=1, hours=13),
            base_date + timedelta(days=1, hours=13, minutes=3),
            3,
            "Compiègne",
            base_date + timedelta(days=1, hours=14),
            None,
            25,
        ),  # train 10 (same as train 6 but different day): no arrival_time_real yet for day 2
    ]

    for row in data:
        conn.execute(
            f"""
            INSERT INTO {TABLE} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{base_date}', '{base_date}', NULL, NULL, NULL)
        """,
            row,
        )

    conn.close()

    # Set the database path in app config for this test
    app.config["DB_PATH"] = test_db_path

    yield

    # Cleanup - remove test database
    if os.path.exists(test_db_path):
        os.remove(test_db_path)


def test_get_stats_no_split(test_client):
    """Test /api/stats without split_by_line parameter."""
    response = test_client.get("/api/stats")
    assert response.status_code == 200

    data = response.get_json()

    # Expected values based on our synthetic data
    # Total: 10 trains
    # On time: 1 (train 3)
    # 0-5 min: 2 (trains 2, 8)
    # 5-15 min: 2 (trains 1, 9)
    # 15-45 min: 3 (trains 4, 5, 6)
    # Over 45 min: 1 (train 7)
    # Unknown: 1 (train 10)

    assert data["total_trains"] == 10
    assert data["on_time"] == 1
    assert data["on_time_percentage"] == 10.0
    assert data["delay_5min"] == 2
    assert data["delay_5min_percentage"] == 20.0
    assert data["delay_15min"] == 2
    assert data["delay_15min_percentage"] == 20.0
    assert data["delay_45min"] == 3
    assert data["delay_45min_percentage"] == 30.0
    assert data["delay_over_45min"] == 1
    assert data["delay_over_45min_percentage"] == 10.0
    assert data["delay_unknown"] == 1
    assert data["delay_unknown_percentage"] == 10.0


def test_get_stats_with_split(test_client):
    """Test /api/stats with split_by_line parameter."""
    response = test_client.get("/api/stats?split_by_line=true")
    assert response.status_code == 200

    data = response.get_json()

    # Should have 7 lines: K01 01, K01 02, K01 03, K02 04, K02 05, K03 06, K03 07
    assert len(data) == 7

    # Verify results are sorted by departure time (not alphabetically by line name)
    departure_times = [item["departure_time_scheduled"] for item in data]
    assert departure_times == sorted(departure_times), "Results should be sorted by departure time"

    # Find each line and verify its statistics
    lines = {item["line"]: item for item in data}

    assert lines.keys() == {"K01 01", "K01 02", "K01 03", "K03 06", "K02 04", "K03 07", "K02 05"}

    # Line K01 01: trains 1, 8
    k01_01 = lines["K01 01"]
    assert k01_01["total_trains"] == 2
    assert k01_01["on_time"] == 0  # both trains have delays
    assert k01_01["delay_5min"] == 1  # train 8 (5 min)
    assert k01_01["delay_15min"] == 1  # train 1 (10 min)
    assert k01_01["delay_45min"] == 0
    assert k01_01["delay_over_45min"] == 0
    assert k01_01["delay_unknown"] == 0
    assert k01_01["average_delay_minutes"] == pytest.approx(7.5)  # (10 + 5) / 2 = 7.5

    # Line K01 02: trains 2, 9
    k01_02 = lines["K01 02"]
    assert k01_02["total_trains"] == 2
    assert k01_02["on_time"] == 0
    assert k01_02["delay_5min"] == 1  # train 2 (3 min)
    assert k01_02["delay_15min"] == 1  # train 9 (15 min)
    assert k01_02["delay_45min"] == 0
    assert k01_02["delay_over_45min"] == 0
    assert k01_02["delay_unknown"] == 0
    assert k01_02["average_delay_minutes"] == pytest.approx(9.0)  # (3 + 15) / 2 = 9.0

    # Line K01 03: train 3
    k01_03 = lines["K01 03"]
    assert k01_03["total_trains"] == 1
    assert k01_03["on_time"] == 1  # train 3
    assert k01_03["delay_5min"] == 0
    assert k01_03["delay_15min"] == 0
    assert k01_03["delay_45min"] == 0
    assert k01_03["delay_over_45min"] == 0
    assert k01_03["delay_unknown"] == 0
    assert k01_03["average_delay_minutes"] == 0.0

    # Line K02 04: train 4
    k02_04 = lines["K02 04"]
    assert k02_04["total_trains"] == 1
    assert k02_04["on_time"] == 0
    assert k02_04["delay_5min"] == 0
    assert k02_04["delay_15min"] == 0
    assert k02_04["delay_45min"] == 1  # train 4 (30 min)
    assert k02_04["delay_over_45min"] == 0
    assert k02_04["delay_unknown"] == 0
    assert k02_04["average_delay_minutes"] == 30.0

    # Line K02 05: train 5
    k02_05 = lines["K02 05"]
    assert k02_05["total_trains"] == 1
    assert k02_05["on_time"] == 0
    assert k02_05["delay_5min"] == 0
    assert k02_05["delay_15min"] == 0
    assert k02_05["delay_45min"] == 1  # train 5 (20 min)
    assert k02_05["delay_over_45min"] == 0
    assert k02_05["delay_unknown"] == 0
    assert k02_05["average_delay_minutes"] == 20.0

    # Line K03 06: trains 6 and 10
    k03_06 = lines["K03 06"]
    assert k03_06["total_trains"] == 2
    assert k03_06["on_time"] == 0
    assert k03_06["delay_5min"] == 0
    assert k03_06["delay_15min"] == 0
    assert k03_06["delay_45min"] == 1  # train 6 (45 min)
    assert k03_06["delay_over_45min"] == 0
    assert k03_06["delay_unknown"] == 1  # train 10 (no arrival_time_real)
    assert k03_06["average_delay_minutes"] == pytest.approx(45.0)

    # Line K03 07: train 7
    k03_07 = lines["K03 07"]
    assert k03_07["total_trains"] == 1
    assert k03_07["on_time"] == 0
    assert k03_07["delay_5min"] == 0
    assert k03_07["delay_15min"] == 0
    assert k03_07["delay_45min"] == 0
    assert k03_07["delay_over_45min"] == 1  # train 7 (60 min)
    assert k03_07["delay_unknown"] == 0
    assert k03_07["average_delay_minutes"] == 60.0


def test_get_stats_with_date_range(test_client):
    """Test /api/stats with date range filtering."""
    # Test with only the first day (2024-01-01)
    response = test_client.get("/api/stats?start_date=2024-01-02&end_date=2024-01-02")
    assert response.status_code == 200

    data = response.get_json()

    # Should only include 3 trains from day 2 (trains 1-7)
    assert data["total_trains"] == 3
    assert data["on_time"] == 0
    assert data["delay_5min"] == 1  # train 8
    assert data["delay_15min"] == 1  # train 9
    assert data["delay_45min"] == 0
    assert data["delay_over_45min"] == 0
    assert data["delay_unknown"] == 1  # train 10


def test_get_timeline(test_client):
    """Test /api/timeline endpoint."""
    response = test_client.get("/api/timeline")
    assert response.status_code == 200

    data = response.get_json()

    # Should have 2 dates: 2024-01-01 and 2024-01-02
    assert len(data) == 2

    # Find each date and verify statistics
    dates = {item["date"]: item for item in data}

    # Day 1: 7 trains
    day1 = dates["2024-01-01"]
    assert day1["total_trains"] == 7
    assert day1["on_time"] == 1  # train 3
    assert day1["delay_5min"] == 1  # train 2
    assert day1["delay_15min"] == 1  # train 1
    assert day1["delay_45min"] == 3  # trains 4, 5, 6
    assert day1["delay_over_45min"] == 1  # train 7
    assert day1["delay_unknown"] == 0

    # Day 2: 3 trains
    day2 = dates["2024-01-02"]
    assert day2["total_trains"] == 3
    assert day2["on_time"] == 0
    assert day2["delay_5min"] == 1  # train 8
    assert day2["delay_15min"] == 1  # train 9
    assert day2["delay_45min"] == 0
    assert day2["delay_over_45min"] == 0
    assert day2["delay_unknown"] == 1  # train 10


def test_get_timeline_with_date_range(test_client):
    """Test /api/timeline with date range filtering."""
    # Test with only the second day
    response = test_client.get("/api/timeline?start_date=2024-01-02&end_date=2024-01-02")
    assert response.status_code == 200

    data = response.get_json()

    # Should only have 1 date
    assert len(data) == 1
    assert data[0]["date"] == "2024-01-02"
    assert data[0]["total_trains"] == 3


def test_get_date_range(test_client):
    """Test /api/date-range endpoint."""
    response = test_client.get("/api/date-range")
    assert response.status_code == 200

    data = response.get_json()

    # Our data spans from 2024-01-01 to 2024-01-02
    assert data["min_date"] == "2024-01-01"
    assert data["max_date"] == "2024-01-02"


def test_get_latest_timestamp(test_client):
    """Test /api/latest-timestamp endpoint."""
    response = test_client.get("/api/latest-timestamp")
    assert response.status_code == 200

    data = response.get_json()

    # Should return the latest updated_at timestamp (2024-01-03 from our data)
    assert data["formatted_timestamp"] == "01/01/2024 à 00h00 (heure de Compiègne)"
    assert data["is_outdated"]


def test_error_handling_no_data(test_client):
    """Test error handling when no data is found."""
    # Use a date range with no data
    response = test_client.get("/api/stats?start_date=2025-01-01&end_date=2025-01-02")
    assert response.status_code == 404

    data = response.get_json()
    assert "error" in data
    assert data["error"] == "No data found"
