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
    yesterday = datetime.now() - timedelta(days=0)
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
    }

    headers = {"Authorization": SNCF_API_KEY}

    response = requests.get(url, params=params, headers=headers)

    if response.status_code != 200:
        raise Exception(
            f"Failed to fetch route schedules. Status code: {response.status_code}, Response: {response.text}"
        )

    return response.json()


def extract_relevant_schedules(
    route_schedules_data: Dict[str, Any], station1_name: str, station2_name: str
) -> List[Dict[str, Any]]:
    """
    Extract schedules that include both stations.

    Args:
        route_schedules_data: The route schedules data from the API.
        station1_name: Name of the first station.
        station2_name: Name of the second station.

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
            stop_point = row.get("stop_point", {})
            stop_point_name = stop_point.get("name", "")

            if station1_name.lower() in stop_point_name.lower():
                station1_rows.append(row_idx)
            elif station2_name.lower() in stop_point_name.lower():
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
                        try:
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
                        except:
                            # If time parsing fails, use original order
                            departure_row = station1_row_obj
                            arrival_row = station2_row_obj

                        relevant_schedules.append(
                            {
                                "route_schedule": route_schedule,
                                "from_row": departure_row,
                                "to_row": arrival_row,
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
    Display the schedules with scheduled and actual times in a compact table format.
    Splits results into two tables: trips to Paris and trips to Compiègne.

    Args:
        schedules: List of relevant route schedules.
    """
    if not schedules:
        print("No relevant schedules found.")
        return

    # Separate schedules by direction
    to_paris_schedules = []
    to_compiegne_schedules = []

    for schedule_data in schedules:
        from_row = schedule_data["from_row"]
        to_row = schedule_data["to_row"]

        # Extract station names
        from_stop_point = from_row.get("stop_point", {})
        dep_station = from_stop_point.get("name", "Unknown")

        to_stop_point = to_row.get("stop_point", {})
        arr_station = to_stop_point.get("name", "Unknown")

        # Classify by arrival station
        if "Paris" in arr_station:
            to_paris_schedules.append(schedule_data)
        elif "Compiègne" in arr_station:
            to_compiegne_schedules.append(schedule_data)

    # Function to display a single table
    def display_table(schedules_list, title):
        if not schedules_list:
            return 0

        # Collect delay statistics
        total_delays = []

        # First pass: collect data to determine column widths
        route_names = []
        dep_stations = []
        arr_stations = []
        dep_times = []
        arr_times = []
        delays = []

        for schedule_data in schedules_list:
            route_schedule = schedule_data["route_schedule"]
            from_row = schedule_data["from_row"]
            to_row = schedule_data["to_row"]
            vehicle_journey_index = schedule_data.get("vehicle_journey_index", 0)

            display_info = route_schedule.get("display_informations", {})
            route_name = display_info.get("label", "Unknown")

            # Extract departure station name
            from_stop_point = from_row.get("stop_point", {})
            dep_station = from_stop_point.get("name", "Unknown")

            # Extract arrival station name
            to_stop_point = to_row.get("stop_point", {})
            arr_station = to_stop_point.get("name", "Unknown")

            # Extract departure times
            from_date_times = from_row.get("date_times", [])

            dep_time_str = "N/A"

            if vehicle_journey_index < len(from_date_times):
                dt = from_date_times[vehicle_journey_index]
                date_time = dt.get("date_time", "")

                if date_time:
                    # Extract time only (HH:MM) with proper formatting
                    dep_time_str = f"{date_time[9:11]}:{date_time[11:13]}"

            # Extract arrival times
            to_date_times = to_row.get("date_times", [])

            arr_time_str = "N/A"
            arr_delay = None

            if vehicle_journey_index < len(to_date_times):
                dt = to_date_times[vehicle_journey_index]
                date_time = dt.get("date_time", "")
                base_date_time = dt.get("base_date_time", "")

                if date_time:
                    # Extract time only (HH:MM) with proper formatting
                    arr_time_str = f"{date_time[9:11]}:{date_time[11:13]}"

                    if base_date_time and date_time != base_date_time:
                        arr_delay = calculate_delay_minutes(date_time, base_date_time)
                        total_delays.append(arr_delay)

            # Store data for column width calculation
            route_names.append(route_name)
            dep_stations.append(dep_station)
            arr_stations.append(arr_station)
            dep_times.append(dep_time_str)
            arr_times.append(arr_time_str)
            delays.append(f"{arr_delay} min" if arr_delay is not None else "")

        # Calculate column widths based on content
        route_width = max(len("Route"), max(len(name) for name in route_names)) + 2
        dep_width = max(len("Departure"), max(len(time) for time in dep_times)) + 2
        arr_width = max(len("Arrival"), max(len(time) for time in arr_times)) + 2
        delay_width = max(len("Delay"), max(len(delay) for delay in delays)) + 2

        # Create table header with dynamic widths
        header_format = f"{{:<{route_width}}} {{:<{dep_width}}} {{:<{arr_width}}} {{:<{delay_width}}}"

        print(f"\n{title}")
        print("=" * len(title))
        print(header_format.format("Route", "Departure", "Arrival", "Delay"))
        print("-" * (route_width + dep_width + arr_width + delay_width + 10))

        # Second pass: display the table
        for i, schedule_data in enumerate(schedules_list):
            route_name = route_names[i]
            dep_time_str = dep_times[i]
            arr_time_str = arr_times[i]
            delay_str = delays[i]

            # Print table row
            print(header_format.format(route_name, dep_time_str, arr_time_str, delay_str))

        return len(total_delays) if total_delays else 0

    # Display both tables
    print(f"Found {len(schedules)} route schedules between Compiègne and Paris Nord:")

    paris_delays_count = display_table(to_paris_schedules, f"Trips to Paris ({len(to_paris_schedules)} schedules)")
    compiegne_delays_count = display_table(
        to_compiegne_schedules, f"Trips to Compiègne ({len(to_compiegne_schedules)} schedules)"
    )

    # Collect all delays for summary statistics
    all_delays = []
    for schedule_data in schedules:
        to_row = schedule_data["to_row"]
        vehicle_journey_index = schedule_data.get("vehicle_journey_index", 0)

        to_date_times = to_row.get("date_times", [])
        if vehicle_journey_index < len(to_date_times):
            dt = to_date_times[vehicle_journey_index]
            date_time = dt.get("date_time", "")
            base_date_time = dt.get("base_date_time", "")

            if base_date_time and date_time and date_time != base_date_time:
                arr_delay = calculate_delay_minutes(date_time, base_date_time)
                all_delays.append(arr_delay)

    # Display summary statistics if we have any delays
    if all_delays:
        print("\n" + "=" * 80)
        print("DELAY SUMMARY")
        print("=" * 80)
        print(f"Total delays found: {len(all_delays)}")
        print(f"Average delay: {sum(all_delays) / len(all_delays):.1f} minutes")
        print(f"Maximum delay: {max(all_delays)} minutes")
        print(f"Minimum delay: {min(all_delays)} minutes")

        # Count delays by range
        on_time = sum(1 for d in all_delays if abs(d) <= 5)
        minor_delays = sum(1 for d in all_delays if 5 < d <= 15)
        significant_delays = sum(1 for d in all_delays if d > 15)
        early_arrivals = sum(1 for d in all_delays if d < -5)

        print("\nDelay distribution:")
        print(f"  On time (±5 min): {on_time} ({on_time / len(all_delays) * 100:.1f}%)")
        print(f"  Minor delays (5-15 min): {minor_delays} ({minor_delays / len(all_delays) * 100:.1f}%)")
        print(
            f"  Significant delays (>15 min): {significant_delays} ({significant_delays / len(all_delays) * 100:.1f}%)"
        )
        print(f"  Early arrivals (<-5 min): {early_arrivals} ({early_arrivals / len(all_delays) * 100:.1f}%)")

    print()


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
