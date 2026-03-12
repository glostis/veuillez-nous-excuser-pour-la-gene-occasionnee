#!/usr/bin/env python3
"""
Script to fetch route schedules for trains between Compiègne and Paris Nord using the SNCF API.

This script uses the /route_schedules endpoint to fetch realtime schedules for all trains
that run between both stations, displaying scheduled and actual departure and arrival times.
"""

import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve the SNCF API key from environment variables
SNCF_API_KEY = os.getenv("SNCF_API_KEY")

if not SNCF_API_KEY:
    raise ValueError("SNCF_API_KEY not found in environment variables.")

# Base URL for the SNCF API
BASE_URL = "https://api.sncf.com/v1/coverage/sncf"

# Station IDs for Compiègne and Paris Gare du Nord
COMPIEGNE_STATION_ID = "stop_area:SNCF:87276691"
PARIS_GARE_DU_NORD_STATION_ID = "stop_area:SNCF:87271007"


def get_yesterday_date() -> str:
    """Get yesterday's date in YYYYMMDD format."""
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y%m%d")


def fetch_lines_between_stations(from_station: str, to_station: str) -> List[Dict[str, Any]]:
    """
    Fetch all lines that run between two stations.

    Args:
        from_station: The ID of the departure station.
        to_station: The ID of the arrival station.

    Returns:
        A list of lines that run between both stations.
    """
    # First, get all lines from the departure station
    url = f"{BASE_URL}/stop_areas/{from_station}/lines"
    headers = {"Authorization": SNCF_API_KEY}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch lines. Status code: {response.status_code}, Response: {response.text}")

    from_station_lines = response.json().get("lines", [])

    # Now get all lines from the arrival station
    url = f"{BASE_URL}/stop_areas/{to_station}/lines"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch lines. Status code: {response.status_code}, Response: {response.text}")

    to_station_lines = response.json().get("lines", [])

    # Find common lines (by line ID)
    from_line_ids = {line["id"] for line in from_station_lines}
    to_line_ids = {line["id"] for line in to_station_lines}
    common_line_ids = from_line_ids.intersection(to_line_ids)

    # Return the full line objects for common lines
    common_lines = []
    for line in from_station_lines:
        if line["id"] in common_line_ids:
            common_lines.append(line)

    return common_lines


def fetch_route_schedules_for_line(line_id: str, date: str) -> Dict[str, Any]:
    """
    Fetch route schedules for a specific line on a given date.

    Args:
        line_id: The ID of the line.
        date: The date for which to fetch schedules (format: YYYYMMDD).

    Returns:
        A dictionary containing route schedules with realtime information.
    """
    # Construct the URL for route schedules
    url = f"{BASE_URL}/lines/{line_id}/route_schedules"

    # Calculate datetime range (all day yesterday)
    from_datetime = f"{date}T000000"
    until_datetime = f"{date}T235959"

    params = {
        "from_datetime": from_datetime,
        "duration": 86400,  # 24 hours
        "data_freshness": "realtime",
        "disable_geojson": True,
        # "depth": 2,
    }

    headers = {"Authorization": SNCF_API_KEY}

    response = requests.get(url, params=params, headers=headers)

    if response.status_code != 200:
        raise Exception(
            f"Failed to fetch route schedules. Status code: {response.status_code}, Response: {response.text}"
        )

    return response.json()


def extract_relevant_schedules(
    route_schedules_data: Dict[str, Any], from_station_name: str, to_station_name: str
) -> List[Dict[str, Any]]:
    """
    Extract schedules that include both from and to stations.

    Args:
        route_schedules_data: The route schedules data from the API.
        from_station_name: Name of the departure station.
        to_station_name: Name of the arrival station.

    Returns:
        A list of relevant route schedules.
    """
    relevant_schedules = []

    for route_schedule in route_schedules_data.get("route_schedules", []):
        table = route_schedule.get("table", {})
        headers = table.get("headers", [])
        rows = table.get("rows", [])

        # Find rows that contain our stations
        from_rows = []
        to_rows = []

        for row_idx, row in enumerate(rows):
            stop_point = row.get("stop_point", {})
            stop_point_name = stop_point.get("name", "")

            if from_station_name.lower() in stop_point_name.lower():
                from_rows.append(row_idx)
            elif to_station_name.lower() in stop_point_name.lower():
                to_rows.append(row_idx)

        # If we found both stations in this schedule, check each vehicle journey
        if from_rows and to_rows:
            # Iterate through each vehicle journey (header)
            for header_idx, header in enumerate(headers):
                header_links = header.get("links", [])
                vehicle_journey_id = None

                for link in header_links:
                    if link.get("type") == "vehicle_journey":
                        vehicle_journey_id = link.get("id")
                        break

                # Check if this vehicle journey serves both stations
                serves_from_station = False
                serves_to_station = False

                # Check each from_row to see if this vehicle journey has a stop there
                for from_row_idx in from_rows:
                    from_row = rows[from_row_idx]
                    date_times = from_row.get("date_times", [])

                    # Check if this vehicle journey has a date_time entry in this row
                    if header_idx < len(date_times):
                        date_time_entry = date_times[header_idx]
                        if date_time_entry and date_time_entry.get("date_time"):
                            serves_from_station = True
                            break

                # Check each to_row to see if this vehicle journey has a stop there
                for to_row_idx in to_rows:
                    to_row = rows[to_row_idx]
                    date_times = to_row.get("date_times", [])

                    # Check if this vehicle journey has a date_time entry in this row
                    if header_idx < len(date_times):
                        date_time_entry = date_times[header_idx]
                        if date_time_entry and date_time_entry.get("date_time"):
                            serves_to_station = True
                            break

                # If this vehicle journey serves both stations, add it
                if serves_from_station and serves_to_station:
                    # Find the specific rows for this vehicle journey
                    actual_from_row = None
                    actual_to_row = None

                    for from_row_idx in from_rows:
                        from_row = rows[from_row_idx]
                        date_times = from_row.get("date_times", [])
                        if header_idx < len(date_times) and date_times[header_idx].get("date_time"):
                            actual_from_row = from_row
                            break

                    for to_row_idx in to_rows:
                        to_row = rows[to_row_idx]
                        date_times = to_row.get("date_times", [])
                        if header_idx < len(date_times) and date_times[header_idx].get("date_time"):
                            actual_to_row = to_row
                            break

                    if actual_from_row and actual_to_row:
                        relevant_schedules.append(
                            {
                                "route_schedule": route_schedule,
                                "from_row": actual_from_row,
                                "to_row": actual_to_row,
                                "vehicle_journey_index": header_idx,
                                "vehicle_journey_id": vehicle_journey_id,
                            }
                        )

    return relevant_schedules


def calculate_delay_minutes(actual_time: str, scheduled_time: str) -> int:
    """
    Calculate delay in minutes between actual and scheduled time.

    Args:
        actual_time: Actual time in YYYYMMDDTHHMMSS format
        scheduled_time: Scheduled time in YYYYMMDDTHHMMSS format

    Returns:
        Delay in minutes (positive if late, negative if early)
    """
    try:
        # Parse the times (format: YYYYMMDDTHHMMSS)
        actual_dt = datetime.strptime(actual_time, "%Y%m%dT%H%M%S")
        scheduled_dt = datetime.strptime(scheduled_time, "%Y%m%dT%H%M%S")

        delay = actual_dt - scheduled_dt
        return int(delay.total_seconds() / 60)
    except:
        return 0


def display_schedules_with_delays(schedules: List[Dict[str, Any]]):
    """
    Display the schedules with scheduled and actual times.

    Args:
        schedules: List of relevant route schedules.
    """
    if not schedules:
        print("No relevant schedules found.")
        return

    # Collect delay statistics
    total_delays = []

    print(f"Found {len(schedules)} route schedules between Compiègne and Paris Nord:")
    print()

    for i, schedule_data in enumerate(schedules, 1):
        route_schedule = schedule_data["route_schedule"]
        from_row = schedule_data["from_row"]
        to_row = schedule_data["to_row"]
        vehicle_journey_index = schedule_data.get("vehicle_journey_index", 0)
        vehicle_journey_id = schedule_data.get("vehicle_journey_id", "Unknown")

        display_info = route_schedule.get("display_informations", {})
        route_name = display_info.get("label", "Unknown")
        direction = display_info.get("direction", "Unknown")

        print(f"Schedule {i}:")
        print(f"  Route: {route_name}")
        print(f"  Direction: {direction}")
        print(f"  Vehicle Journey ID: {vehicle_journey_id}")

        # Extract departure times from Compiègne (only for this specific vehicle journey)
        from_stop_point = from_row.get("stop_point", {}).get("name", "Unknown")
        from_date_times = from_row.get("date_times", [])

        print(f"  Departure from {from_stop_point}:")
        has_delays = False

        # Only show the date_time for this specific vehicle journey
        if vehicle_journey_index < len(from_date_times):
            dt = from_date_times[vehicle_journey_index]
            date_time = dt.get("date_time", "")
            base_date_time = dt.get("base_date_time", "")
            data_freshness = dt.get("data_freshness", "base_schedule")

            if date_time and base_date_time and date_time != base_date_time:
                delay_minutes = calculate_delay_minutes(date_time, base_date_time)
                print(
                    f"    🚆 {date_time} (scheduled: {base_date_time}) - {delay_minutes} min delay - {data_freshness}"
                )
                total_delays.append(delay_minutes)
                has_delays = True
            elif date_time and data_freshness == "realtime":
                print(f"    🚆 {date_time} - {data_freshness}")
            elif date_time:
                print(f"    🚆 {date_time} - {data_freshness}")

        if not has_delays and not date_time:
            print("    No realtime delays found for departures")

        # Extract arrival times at Paris Nord (only for this specific vehicle journey)
        to_stop_point = to_row.get("stop_point", {}).get("name", "Unknown")
        to_date_times = to_row.get("date_times", [])

        print(f"  Arrival at {to_stop_point}:")
        has_delays = False

        # Only show the date_time for this specific vehicle journey
        if vehicle_journey_index < len(to_date_times):
            dt = to_date_times[vehicle_journey_index]
            date_time = dt.get("date_time", "")
            base_date_time = dt.get("base_date_time", "")
            data_freshness = dt.get("data_freshness", "base_schedule")

            if date_time and base_date_time and date_time != base_date_time:
                delay_minutes = calculate_delay_minutes(date_time, base_date_time)
                print(
                    f"    🚆 {date_time} (scheduled: {base_date_time}) - {delay_minutes} min delay - {data_freshness}"
                )
                total_delays.append(delay_minutes)
                has_delays = True
            elif date_time and data_freshness == "realtime":
                print(f"    🚆 {date_time} - {data_freshness}")
            elif date_time:
                print(f"    🚆 {date_time} - {data_freshness}")

        if not has_delays and not date_time:
            print("    No realtime delays found for arrivals")

        print()

    # Display summary statistics
    if total_delays:
        print("=" * 80)
        print("DELAY SUMMARY")
        print("=" * 80)
        print(f"Total delays found: {len(total_delays)}")
        print(f"Average delay: {sum(total_delays) / len(total_delays):.1f} minutes")
        print(f"Maximum delay: {max(total_delays)} minutes")
        print(f"Minimum delay: {min(total_delays)} minutes")

        # Count delays by range
        on_time = sum(1 for d in total_delays if abs(d) <= 5)
        minor_delays = sum(1 for d in total_delays if 5 < d <= 15)
        significant_delays = sum(1 for d in total_delays if d > 15)
        early_arrivals = sum(1 for d in total_delays if d < -5)

        print("\nDelay distribution:")
        print(f"  On time (±5 min): {on_time} ({on_time / len(total_delays) * 100:.1f}%)")
        print(f"  Minor delays (5-15 min): {minor_delays} ({minor_delays / len(total_delays) * 100:.1f}%)")
        print(
            f"  Significant delays (>15 min): {significant_delays} ({significant_delays / len(total_delays) * 100:.1f}%)"
        )
        print(f"  Early arrivals (<-5 min): {early_arrivals} ({early_arrivals / len(total_delays) * 100:.1f}%)")


def main():
    """Main function to fetch and display route schedules."""
    print("Fetching train lines between Compiègne and Paris Nord...")

    try:
        # Step 1: Identify lines that run between both stations
        common_lines = fetch_lines_between_stations(COMPIEGNE_STATION_ID, PARIS_GARE_DU_NORD_STATION_ID)

        if not common_lines:
            print("No common lines found between Compiègne and Paris Nord.")
            return

        print(f"Found {len(common_lines)} common lines between the stations:")
        for line in common_lines:
            print(f"  - {line.get('name', 'Unknown')} (ID: {line.get('id', 'Unknown')})")
        print()

        # Step 2: Fetch route schedules for yesterday with realtime data
        yesterday = get_yesterday_date()
        print(f"Fetching route schedules for {yesterday}...")

        all_relevant_schedules = []

        for line in common_lines:
            line_id = line["id"]
            line_name = line.get("name", "Unknown")

            print(f"  Fetching schedules for line: {line_name}")

            try:
                route_schedules_data = fetch_route_schedules_for_line(line_id, yesterday)

                # Extract relevant schedules that include both stations
                relevant_schedules = extract_relevant_schedules(route_schedules_data, "Compiègne", "Paris Nord")
                print(f"    Found {len(relevant_schedules)} relevant schedules between the stations")

                all_relevant_schedules.extend(relevant_schedules)

            except ValueError as e:
                print(f"    Error fetching schedules for line {line_name}: {e}")

        # Step 3: Display the results
        print("\n" + "=" * 80)
        print("TRAIN SCHEDULES WITH REALTIME INFORMATION")
        print("=" * 80)

        display_schedules_with_delays(all_relevant_schedules)

    except ValueError as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
