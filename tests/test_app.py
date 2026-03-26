#!/usr/bin/env python3
"""
Pytest tests for the Flask API routes.
Tests use a synthetic database with ~10 rows containing trips from 3 lines with various delays.
"""

import os
from datetime import datetime, timedelta

import duckdb
import pytest

from app import TABLE_NAME, app

# Test database path
TEST_DB_PATH = "test_train_journeys.duckdb"


@pytest.fixture(scope="module")
def test_client():
    """Create a test client for the Flask app."""
    app.config["TESTING"] = True
    app.config["DB_PATH"] = TEST_DB_PATH

    with app.test_client() as client:
        yield client


@pytest.fixture(scope="function", autouse=True)
def setup_test_database():
    """Create a test database with synthetic data."""
    # Create test database
    conn = duckdb.connect(TEST_DB_PATH)

    # Create table
    conn.execute(f"""
        CREATE TABLE {TABLE_NAME} (
            id VARCHAR PRIMARY KEY,
            train_number VARCHAR,
            train_line_name VARCHAR,
            departure_station_name VARCHAR,
            scheduled_departure_time TIMESTAMP WITH TIME ZONE,
            real_departure_time TIMESTAMP WITH TIME ZONE,
            arrival_station_name VARCHAR,
            scheduled_arrival_time TIMESTAMP WITH TIME ZONE,
            real_arrival_time TIMESTAMP WITH TIME ZONE,
            fetch_timestamp TIMESTAMP WITH TIME ZONE
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
            "1",
            "01",
            "K01",
            "Paris Nord",
            base_date + timedelta(hours=8),
            base_date + timedelta(hours=8, minutes=5),
            "Compiègne",
            base_date + timedelta(hours=9),
            base_date + timedelta(hours=9, minutes=10),
            base_date + timedelta(days=1),
        ),  # 10 min delay
        (
            "2",
            "02",
            "K01",
            "Paris Nord",
            base_date + timedelta(hours=10),
            base_date + timedelta(hours=10, minutes=2),
            "Compiègne",
            base_date + timedelta(hours=11),
            base_date + timedelta(hours=11, minutes=3),
            base_date + timedelta(days=1),
        ),  # 3 min delay
        (
            "3",
            "03",
            "K01",
            "Paris Nord",
            base_date + timedelta(hours=12),
            base_date + timedelta(hours=12),
            "Compiègne",
            base_date + timedelta(hours=13),
            base_date + timedelta(hours=13),
            base_date + timedelta(days=1),
        ),  # 0 min delay (on time)
        # Line K02 - Compiègne → Paris Nord
        (
            "4",
            "04",
            "K02",
            "Compiègne",
            base_date + timedelta(hours=14),
            base_date + timedelta(hours=14, minutes=15),
            "Paris Nord",
            base_date + timedelta(hours=15),
            base_date + timedelta(hours=15, minutes=30),
            base_date + timedelta(days=1),
        ),  # 30 min delay
        (
            "5",
            "05",
            "K02",
            "Compiègne",
            base_date + timedelta(hours=16),
            base_date + timedelta(hours=16, minutes=5),
            "Paris Nord",
            base_date + timedelta(hours=17),
            base_date + timedelta(hours=17, minutes=20),
            base_date + timedelta(days=1),
        ),  # 20 min delay
        # Line K03 - Paris Nord → Compiègne (earlier than K02 to test sorting)
        (
            "6",
            "06",
            "K03",
            "Paris Nord",
            base_date + timedelta(hours=13),  # 13:00 - earlier than K02's 14:00
            base_date + timedelta(hours=13, minutes=10),
            "Compiègne",
            base_date + timedelta(hours=14),
            base_date + timedelta(hours=14, minutes=45),
            base_date + timedelta(days=1),
        ),  # 45 min delay
        (
            "7",
            "07",
            "K03",
            "Paris Nord",
            base_date + timedelta(hours=15),  # 15:00 - between K02's 14:00 and 16:00
            base_date + timedelta(hours=15, minutes=5),
            "Compiègne",
            base_date + timedelta(hours=16),
            base_date + timedelta(hours=17),
            base_date + timedelta(days=1),
        ),  # 60 min delay (over 45)
        # Additional data for different dates
        (
            "8",
            "01",  # Same train number as data 1, but different date
            "K01",
            "Paris Nord",
            base_date + timedelta(days=1, hours=8),
            base_date + timedelta(days=1, hours=8, minutes=2),
            "Compiègne",
            base_date + timedelta(days=1, hours=9),
            base_date + timedelta(days=1, hours=9, minutes=5),
            base_date + timedelta(days=2),
        ),  # 5 min delay on day 2
        (
            "9",
            "02",
            "K01",
            "Compiègne",
            base_date + timedelta(days=1, hours=10),
            base_date + timedelta(days=1, hours=10, minutes=8),
            "Paris Nord",
            base_date + timedelta(days=1, hours=11),
            base_date + timedelta(days=1, hours=11, minutes=15),
            base_date + timedelta(days=2),
        ),  # 15 min delay on day 2
        (
            "10",
            "06",
            "K03",
            "Paris Nord",
            base_date + timedelta(days=1, hours=13),
            base_date + timedelta(days=1, hours=13, minutes=3),
            "Compiègne",
            base_date + timedelta(days=1, hours=14),
            base_date + timedelta(days=1, hours=14, minutes=25),
            base_date + timedelta(days=2),
        ),  # 25 min delay on day 2
    ]

    for row in data:
        conn.execute(
            f"""
            INSERT INTO {TABLE_NAME} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            row,
        )

    conn.close()

    yield

    # Cleanup - remove test database
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)


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
    # 15-45 min: 4 (trains 4, 5, 6, 10)
    # Over 45 min: 1 (train 7)

    assert data["total_trains"] == 10
    assert data["on_time"] == 1
    assert data["on_time_percentage"] == 10.0
    assert data["delay_5min"] == 2
    assert data["delay_5min_percentage"] == 20.0
    assert data["delay_15min"] == 2
    assert data["delay_15min_percentage"] == 20.0
    assert data["delay_45min"] == 4
    assert data["delay_45min_percentage"] == 40.0
    assert data["delay_over_45min"] == 1
    assert data["delay_over_45min_percentage"] == 10.0


def test_get_stats_with_split(test_client):
    """Test /api/stats with split_by_line parameter."""
    response = test_client.get("/api/stats?split_by_line=true")
    assert response.status_code == 200

    data = response.get_json()

    # Should have 7 lines: K01 01, K01 02, K01 03, K02 04, K02 05, K03 06, K03 07
    assert len(data) == 7

    # Verify results are sorted by departure time (not alphabetically by line name)
    departure_times = [item["departure_time"] for item in data]
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
    assert k01_01["average_delay_minutes"] == pytest.approx(7.5)  # (10 + 5) / 2 = 7.5

    # Line K01 02: trains 2, 9
    k01_02 = lines["K01 02"]
    assert k01_02["total_trains"] == 2
    assert k01_02["on_time"] == 0
    assert k01_02["delay_5min"] == 1  # train 2 (3 min)
    assert k01_02["delay_15min"] == 1  # train 9 (15 min)
    assert k01_02["delay_45min"] == 0
    assert k01_02["delay_over_45min"] == 0
    assert k01_02["average_delay_minutes"] == pytest.approx(9.0)  # (3 + 15) / 2 = 9.0

    # Line K01 03: train 3
    k01_03 = lines["K01 03"]
    assert k01_03["total_trains"] == 1
    assert k01_03["on_time"] == 1  # train 3
    assert k01_03["delay_5min"] == 0
    assert k01_03["delay_15min"] == 0
    assert k01_03["delay_45min"] == 0
    assert k01_03["delay_over_45min"] == 0
    assert k01_03["average_delay_minutes"] == 0.0

    # Line K02 04: train 4
    k02_04 = lines["K02 04"]
    assert k02_04["total_trains"] == 1
    assert k02_04["on_time"] == 0
    assert k02_04["delay_5min"] == 0
    assert k02_04["delay_15min"] == 0
    assert k02_04["delay_45min"] == 1  # train 4 (30 min)
    assert k02_04["delay_over_45min"] == 0
    assert k02_04["average_delay_minutes"] == 30.0

    # Line K02 05: train 5
    k02_05 = lines["K02 05"]
    assert k02_05["total_trains"] == 1
    assert k02_05["on_time"] == 0
    assert k02_05["delay_5min"] == 0
    assert k02_05["delay_15min"] == 0
    assert k02_05["delay_45min"] == 1  # train 5 (20 min)
    assert k02_05["delay_over_45min"] == 0
    assert k02_05["average_delay_minutes"] == 20.0

    # Line K03 06: trains 6, 10
    k03_06 = lines["K03 06"]
    assert k03_06["total_trains"] == 2
    assert k03_06["on_time"] == 0
    assert k03_06["delay_5min"] == 0
    assert k03_06["delay_15min"] == 0
    assert k03_06["delay_45min"] == 2  # train 6 (45 min), train 10 (25 min)
    assert k03_06["delay_over_45min"] == 0
    assert k03_06["average_delay_minutes"] == pytest.approx(35.0)  # (45 + 25) / 2 = 35.0

    # Line K03 07: train 7
    k03_07 = lines["K03 07"]
    assert k03_07["total_trains"] == 1
    assert k03_07["on_time"] == 0
    assert k03_07["delay_5min"] == 0
    assert k03_07["delay_15min"] == 0
    assert k03_07["delay_45min"] == 0
    assert k03_07["delay_over_45min"] == 1  # train 7 (60 min)
    assert k03_07["average_delay_minutes"] == 60.0


def test_get_stats_with_date_range(test_client):
    """Test /api/stats with date range filtering."""
    # Test with only the first day (2024-01-01)
    response = test_client.get("/api/stats?start_date=2024-01-01&end_date=2024-01-01")
    assert response.status_code == 200

    data = response.get_json()

    # Should only include 7 trains from day 1 (trains 1-7)
    assert data["total_trains"] == 7
    assert data["on_time"] == 1  # train 3
    assert data["delay_5min"] == 1  # train 2
    assert data["delay_15min"] == 1  # train 1
    assert data["delay_45min"] == 3  # trains 4, 5, 6
    assert data["delay_over_45min"] == 1  # train 7


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

    # Day 2: 3 trains
    day2 = dates["2024-01-02"]
    assert day2["total_trains"] == 3
    assert day2["on_time"] == 0
    assert day2["delay_5min"] == 1  # train 8
    assert day2["delay_15min"] == 1  # train 9
    assert day2["delay_45min"] == 1  # train 10
    assert day2["delay_over_45min"] == 0


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

    # Should return the latest fetch timestamp (2024-01-03 from our data)
    assert "fetch_timestamp" in data
    assert "row_count" in data
    assert "data_date" in data
    assert "is_outdated" in data

    # The latest fetch timestamp should be for 3 rows (trains 8, 9, 10 from day 2)
    assert data["row_count"] == "3"


def test_error_handling_no_data(test_client):
    """Test error handling when no data is found."""
    # Use a date range with no data
    response = test_client.get("/api/stats?start_date=2025-01-01&end_date=2025-01-02")
    assert response.status_code == 404

    data = response.get_json()
    assert "error" in data
    assert data["error"] == "No data found"
