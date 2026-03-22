#!/usr/bin/env python3
"""
Script to fetch route schedules for trains between Compiègne and Paris Nord using the SNCF API
and store the data in DuckDB.

This script focuses on:
1. Fetching route schedules from the API
2. Filtering to keep only routes passing through Paris and Compiègne
3. Storing raw data in DuckDB with one row per train trip
"""

import os
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

SNCF_API_KEY = os.getenv("SNCF_API_KEY")

if not SNCF_API_KEY:
    raise ValueError("SNCF_API_KEY not found in environment variables.")

BASE_URL = "https://api.sncf.com/v1/coverage/sncf"

COMPIEGNE_STATION_ID = "stop_area:SNCF:87276691"
PARIS_GARE_DU_NORD_STATION_ID = "stop_area:SNCF:87271007"

# Database configuration
DB_PATH = "data/train_journeys.duckdb"
TABLE_NAME = "route_schedules"


def get_yesterday_date() -> str:
    """Get yesterday's date in YYYYMMDD format."""
    return datetime.now().strftime("%Y%m%d")


def datetime_to_unix_timestamp(dt_str: str) -> int:
    """Convert datetime string to Unix timestamp."""
    dt = datetime.strptime(dt_str, "%Y%m%dT%H%M%S")
    return int(dt.timestamp())


def datetime_to_timestamp(dt_str: str) -> str:
    """Convert datetime string to ISO format for TIMESTAMP WITH TIME ZONE.

    The SNCF API returns datetimes in Europe/Paris timezone, so we need to
    properly handle the timezone conversion.
    """
    # Parse as naive datetime (SNCF API format)
    dt = datetime.strptime(dt_str, "%Y%m%dT%H%M%S")

    # Localize to Europe/Paris timezone
    paris_tz = ZoneInfo("Europe/Paris")
    dt_paris = dt.replace(tzinfo=paris_tz)

    # Format as ISO 8601 with timezone offset
    return dt_paris.isoformat()


def fetch_lines_between_stations(station_1: str, station_2: str) -> list[dict[str, Any]]:
    """
    Fetch all lines that run between two stations.

    Returns:
        A list of lines that run between both stations.
    """
    # First, get all lines from station 1
    url = f"{BASE_URL}/stop_areas/{station_1}/lines"
    headers = {"Authorization": SNCF_API_KEY}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch lines. Status code: {response.status_code}, Response: {response.text}")

    station_1_lines = response.json().get("lines", [])

    # Then, get all lines from station 2
    url = f"{BASE_URL}/stop_areas/{station_2}/lines"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch lines. Status code: {response.status_code}, Response: {response.text}")

    station_2_lines = response.json().get("lines", [])

    # Find common lines (by line ID)
    line_1_ids = {line["id"] for line in station_1_lines}
    line_2_ids = {line["id"] for line in station_2_lines}
    common_line_ids = line_1_ids.intersection(line_2_ids)

    return [line for line in station_1_lines if line["id"] in common_line_ids]


def fetch_route_schedules_for_line(line_id: str, date: str) -> dict[str, Any]:
    """
    Fetch route schedules for a specific line on a given date.

    See API reference: https://doc.navitia.io/#route-schedules

    Args:
        line_id: The ID of the line.
        date: The date for which to fetch schedules (format: YYYYMMDD).

    Returns:
        A dictionary containing route schedules with realtime information.
    """
    url = f"{BASE_URL}/lines/{line_id}/route_schedules"

    params = {
        "from_datetime": f"{date}T000000",
        "duration": 86400,  # 24 hours
        "data_freshness": "realtime",
        "disable_geojson": True,
    }
    headers = {"Authorization": SNCF_API_KEY}
    response = requests.get(url, params=params, headers=headers)

    if response.status_code != 200:
        raise Exception(
            f"Failed to fetch route schedules. Status code: {response.status_code}, Response: {response.text}"
        )

    return response.json()


def extract_relevant_schedules(
    route_schedules_data: dict[str, Any], station1_id: str, station2_id: str
) -> list[dict[str, Any]]:
    """
    Extract schedules that include both stations.

    The data returned by the `/route_schedules` API route is a weird table
    with headers and rows. This function extracts the relevant schedules from this data:
    - It finds rows that contain both stations
    - It checks each vehicle journey to see if it serves both stations
    - It assigns departure and arrival times correctly

    Args:
        route_schedules_data: The route schedules data from the API.
        station1_id: ID of the `stop_area` of the first station
        station2_id: ID of the `stop_area` of the second station

    Returns:
        A list of relevant route schedules with correct departure/arrival assignment.
    """
    relevant_schedules = []

    for route_schedule in route_schedules_data.get("route_schedules", []):
        table = route_schedule.get("table", {})
        headers = table.get("headers", [])
        rows = table.get("rows", [])

        # Find rows that contain our stations
        station1_rows = []
        station2_rows = []

        for row_idx, row in enumerate(rows):
            stop_area_id = row.get("stop_point", {}).get("stop_area", {}).get("id", "")

            if stop_area_id == station1_id:
                station1_rows.append(row_idx)
            elif stop_area_id == station2_id:
                station2_rows.append(row_idx)

        # If we found both stations in this schedule, check each vehicle journey
        if station1_rows and station2_rows:
            # Iterate through each vehicle journey (header)
            for header_idx, header in enumerate(headers):
                header_links = header.get("links", [])
                vehicle_journey_id = None

                for link in header_links:
                    if link.get("type") == "vehicle_journey":
                        vehicle_journey_id = link.get("id")
                        break

                # Check if this vehicle journey serves both stations
                serves_station1 = False
                serves_station2 = False

                # Check each station1_row to see if this vehicle journey has a stop there
                for station1_row_idx in station1_rows:
                    station1_row = rows[station1_row_idx]
                    date_times = station1_row.get("date_times", [])

                    # Check if this vehicle journey has a date_time entry in this row
                    if header_idx < len(date_times):
                        date_time_entry = date_times[header_idx]
                        if date_time_entry and date_time_entry.get("date_time"):
                            serves_station1 = True
                            break

                # Check each station2_row to see if this vehicle journey has a stop there
                for station2_row_idx in station2_rows:
                    station2_row = rows[station2_row_idx]
                    date_times = station2_row.get("date_times", [])

                    # Check if this vehicle journey has a date_time entry in this row
                    if header_idx < len(date_times):
                        date_time_entry = date_times[header_idx]
                        if date_time_entry and date_time_entry.get("date_time"):
                            serves_station2 = True
                            break

                # If this vehicle journey serves both stations, add it
                if serves_station1 and serves_station2:
                    # Find the specific rows for this vehicle journey
                    station1_row_obj = None
                    station2_row_obj = None

                    for station1_row_idx in station1_rows:
                        station1_row = rows[station1_row_idx]
                        date_times = station1_row.get("date_times", [])
                        if header_idx < len(date_times) and date_times[header_idx].get("date_time"):
                            station1_row_obj = station1_row
                            break

                    for station2_row_idx in station2_rows:
                        station2_row = rows[station2_row_idx]
                        date_times = station2_row.get("date_times", [])
                        if header_idx < len(date_times) and date_times[header_idx].get("date_time"):
                            station2_row_obj = station2_row
                            break

                    if station1_row_obj and station2_row_obj:
                        # Determine which station is departure and which is arrival based on times
                        station1_time = station1_row_obj.get("date_times", [{}])[header_idx].get("date_time", "")
                        station2_time = station2_row_obj.get("date_times", [{}])[header_idx].get("date_time", "")

                        # Parse times to compare
                        if station1_time and station2_time:
                            station1_dt = datetime.strptime(station1_time, "%Y%m%dT%H%M%S")
                            station2_dt = datetime.strptime(station2_time, "%Y%m%dT%H%M%S")

                            # Station with earlier time is departure, later time is arrival
                            if station1_dt < station2_dt:
                                departure_row = station1_row_obj
                                arrival_row = station2_row_obj
                            else:
                                departure_row = station2_row_obj
                                arrival_row = station1_row_obj
                        else:
                            # If we can't determine times, use original order
                            departure_row = station1_row_obj
                            arrival_row = station2_row_obj

                        relevant_schedules.append(
                            {
                                "route_schedule": route_schedule,
                                "train_number": header.get("display_informations", {}).get("trip_short_name", ""),
                                "from_row": departure_row,
                                "to_row": arrival_row,
                                "vehicle_journey_index": header_idx,
                                "vehicle_journey_id": vehicle_journey_id,
                            }
                        )

    return relevant_schedules


def store_in_duckdb(schedules: list[dict[str, Any]]):
    """
    Store the schedules in DuckDB with the specified structure.

    Args:
        schedules: list of relevant route schedules to store.
    """
    conn = duckdb.connect(DB_PATH)

    # Create table with TIMESTAMP WITH TIME ZONE columns
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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

    # Prepare data for insertion
    data_to_insert = []
    current_timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

    for schedule_data in schedules:
        route_schedule = schedule_data["route_schedule"]
        from_row = schedule_data["from_row"]
        to_row = schedule_data["to_row"]
        vehicle_journey_index = schedule_data["vehicle_journey_index"]
        vehicle_journey_id = schedule_data["vehicle_journey_id"]

        # Extract line information
        display_info = route_schedule.get("display_informations", {})
        train_line_name = display_info.get("label", "Unknown")
        train_number = schedule_data["train_number"]

        # Extract departure information
        from_stop_point = from_row.get("stop_point", {})
        departure_station_name = from_stop_point.get("name", "Unknown")

        from_date_times = from_row.get("date_times", [])
        scheduled_departure_time = "1970-01-01T00:00:00Z"
        real_departure_time = "1970-01-01T00:00:00Z"

        if vehicle_journey_index < len(from_date_times):
            dt = from_date_times[vehicle_journey_index]
            base_date_time = dt.get("base_date_time", "")
            date_time = dt.get("date_time", "")

            scheduled_departure_time = datetime_to_timestamp(base_date_time)
            real_departure_time = datetime_to_timestamp(date_time)

        # Extract arrival information
        to_stop_point = to_row.get("stop_point", {})
        arrival_station_name = to_stop_point.get("name", "Unknown")

        to_date_times = to_row.get("date_times", [])
        scheduled_arrival_time = "1970-01-01T00:00:00Z"
        real_arrival_time = "1970-01-01T00:00:00Z"

        if vehicle_journey_index < len(to_date_times):
            dt = to_date_times[vehicle_journey_index]
            base_date_time = dt.get("base_date_time", "")
            date_time = dt.get("date_time", "")

            scheduled_arrival_time = datetime_to_timestamp(base_date_time)
            real_arrival_time = datetime_to_timestamp(date_time)

        data_to_insert.append(
            {
                "id": vehicle_journey_id,
                "train_number": train_number,
                "train_line_name": train_line_name,
                "departure_station_name": departure_station_name,
                "scheduled_departure_time": scheduled_departure_time,
                "real_departure_time": real_departure_time,
                "arrival_station_name": arrival_station_name,
                "scheduled_arrival_time": scheduled_arrival_time,
                "real_arrival_time": real_arrival_time,
                "fetch_timestamp": current_timestamp,
            }
        )

    # Insert data using upsert to avoid duplicates
    if data_to_insert:
        # Create a temporary table with the correct schema
        conn.execute(f"""
        CREATE TEMP TABLE temp_{TABLE_NAME} (
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

        # Insert new data into temp table
        for row in data_to_insert:
            conn.execute(f"""
            INSERT INTO temp_{TABLE_NAME} VALUES (
                '{row["id"]}',
                '{row["train_number"]}',
                '{row["train_line_name"]}',
                '{row["departure_station_name"]}',
                TIMESTAMP '{row["scheduled_departure_time"]}',
                TIMESTAMP '{row["real_departure_time"]}',
                '{row["arrival_station_name"]}',
                TIMESTAMP '{row["scheduled_arrival_time"]}',
                TIMESTAMP '{row["real_arrival_time"]}',
                TIMESTAMP '{row["fetch_timestamp"]}'
            )
            """)

        # Upsert: insert new records, update existing ones
        conn.execute(f"""
        INSERT INTO {TABLE_NAME}
        SELECT * FROM temp_{TABLE_NAME}
        ON CONFLICT(id) DO NOTHING
        """)

        print(f"Stored {len(data_to_insert)} train trips in DuckDB table '{TABLE_NAME}'")

    conn.close()


def main(debug: bool = True):
    # Step 1: Identify lines that run between both stations
    lines = fetch_lines_between_stations(COMPIEGNE_STATION_ID, PARIS_GARE_DU_NORD_STATION_ID)

    if not lines:
        if debug:
            print("No common train lines found between Compiègne and Paris Nord.")
        return

    if debug:
        print(f"Found {len(lines)} common lines between the stations:")
        for line in lines:
            print(f"  - {line.get('name', 'Unknown')} (ID: {line.get('id', 'Unknown')})")
        print()

    # Step 2: Fetch route schedules for yesterday with realtime data
    # (realtime = real departure and arrival times, not the scheduled ones — see https://doc.navitia.io/#realtime)
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    if debug:
        print(f"Fetching route schedules for {yesterday}...")

    all_relevant_schedules = []

    for line in lines:
        line_id = line["id"]
        line_name = line.get("name", "Unknown")

        if debug:
            print(f"  Fetching schedules for line: {line_name}")

        route_schedules_data = fetch_route_schedules_for_line(line_id, yesterday)

        # Extract relevant schedules that include both stations
        relevant_schedules = extract_relevant_schedules(
            route_schedules_data, COMPIEGNE_STATION_ID, PARIS_GARE_DU_NORD_STATION_ID
        )
        if debug:
            print(f"    Found {len(relevant_schedules)} relevant schedules between the stations")

        all_relevant_schedules.extend(relevant_schedules)

    # Step 3: Store the results in DuckDB
    if all_relevant_schedules:
        if debug:
            print(f"\nStoring {len(all_relevant_schedules)} train trips in DuckDB...")
        store_in_duckdb(all_relevant_schedules)
    else:
        if debug:
            print("\nNo relevant schedules found to store.")


if __name__ == "__main__":
    main()
