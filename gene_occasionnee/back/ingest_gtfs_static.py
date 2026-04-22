"""
GTFS Static Data Ingestion Script

This script processes GTFS static data to extract scheduled trips between
Paris Gare du Nord and Compiègne, then stores the trips for the current day in a DuckDB database.

Usage: python -m gene_occasionnee.back.ingest_gtfs_static
"""

import csv
import os
import tempfile
import zipfile
from datetime import datetime

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from gene_occasionnee import DB_PATH, TABLE, duckdb_connect
from gene_occasionnee.back import GTFS_COMPIEGNE_STOP_ID, GTFS_PARIS_NORD_STOP_ID, GTFS_STATIC_URL

debug = False


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(Exception),
)
def download_and_extract_gtfs():
    """Download and extract GTFS static data to temporary files."""
    if debug:
        print("📦 Downloading and extracting GTFS static data to temporary files...")

    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()

    # Download GTFS static data to temporary file
    zip_path = os.path.join(temp_dir, "gtfs_static.zip")
    if debug:
        print("📥 Downloading GTFS static data...")
    response = requests.get(GTFS_STATIC_URL, timeout=30)
    response.raise_for_status()
    with open(zip_path, "wb") as f:
        f.write(response.content)
    if debug:
        print(f"💾 Downloaded {len(response.content)} bytes")

    # Extract the ZIP file to temporary directory
    extract_dir = os.path.join(temp_dir, "gtfs_static")
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_dir)
    if debug:
        print("📄 GTFS static data extracted.")

    return extract_dir, temp_dir


def find_trips_through_both_stations(extract_dir, service_dates_today):
    """Find trips that go through both Paris Nord and Compiègne and run today."""
    if debug:
        print("🔍 Finding trips through both stations that run today...")

    # Read stop_times.txt to find trips that include both stations
    stop_times_path = f"{extract_dir}/stop_times.txt"

    # Build a mapping of trip_id to its stop times
    trip_stop_times = {}

    with open(stop_times_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            trip_id = row["trip_id"]
            stop_id = row["stop_id"]
            arrival_time = row["arrival_time"]
            departure_time = row["departure_time"]

            if trip_id not in trip_stop_times:
                trip_stop_times[trip_id] = []

            trip_stop_times[trip_id].append(
                {"stop_id": stop_id, "arrival_time": arrival_time, "departure_time": departure_time}
            )

    if debug:
        print(f"📊 Processed {len(trip_stop_times)} trips")

    # Read trips.txt to get service_id for each trip
    trips_path = f"{extract_dir}/trips.txt"
    trip_service_mapping = {}

    with open(trips_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            trip_id = row["trip_id"]
            service_id = row["service_id"]
            trip_service_mapping[trip_id] = service_id

    # Find trips that include both stations AND run today
    relevant_trips = []

    for trip_id, stops in trip_stop_times.items():
        # Skip trips that don't run today
        service_id = trip_service_mapping.get(trip_id)
        if service_id is None or service_id not in service_dates_today:
            continue

        has_paris_nord = any(s["stop_id"] == GTFS_PARIS_NORD_STOP_ID for s in stops)
        has_compiegne = any(s["stop_id"] == GTFS_COMPIEGNE_STOP_ID for s in stops)

        if has_paris_nord and has_compiegne:
            relevant_trips.append({"trip_id": trip_id, "stops": stops})

    if debug:
        print(f"🎯 Found {len(relevant_trips)} trips that go through both stations and run today")

    return relevant_trips


def get_trip_info(extract_dir, trip_id):
    """Get route_id, service_id, and trip_headsign for a given trip_id."""
    trips_path = f"{extract_dir}/trips.txt"

    with open(trips_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["trip_id"] == trip_id:
                return row["route_id"], row["service_id"], row.get("trip_headsign", "")

    return None, None, None


def load_route_info(extract_dir):
    """Load route information from routes.txt."""
    routes_path = f"{extract_dir}/routes.txt"
    route_info = {}

    with open(routes_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            route_id = row["route_id"]
            route_short_name = row.get("route_short_name", "")
            route_info[route_id] = route_short_name

    return route_info


def load_service_dates(extract_dir):
    """Load service dates from calendar_dates.txt, filtered for today's date."""
    calendar_dates_path = f"{extract_dir}/calendar_dates.txt"

    # Get today's date in YYYY-MM-DD format
    today_date = datetime.now().strftime("%Y-%m-%d")
    today_yyyymmdd = today_date.replace("-", "")  # Convert to YYYYMMDD format for comparison

    # Build mapping of service_id to list of dates (exception_type=1 means service is added)
    # Only include services that run today
    service_dates_today = {}

    with open(calendar_dates_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            service_id = row["service_id"]
            date_str = row["date"]  # Format: YYYYMMDD
            exception_type = row["exception_type"]

            # Only include dates where service is added (exception_type=1) and matches today
            if exception_type == "1" and date_str == today_yyyymmdd:
                service_dates_today[service_id] = today_date

    if debug:
        print(f"📅 Loaded service dates for {len(service_dates_today)} services running today")
    return service_dates_today


def parse_gtfs_time(date_str, time_str):
    """Parse GTFS time (HH:MM:SS) with given date."""
    if not time_str or time_str == "" or not date_str:
        return None

    return f"{date_str} {time_str}"


def store_in_duckdb(relevant_trips, extract_dir, service_dates_today):
    """Store the relevant trips in DuckDB."""
    if debug:
        print("💾 Storing data in DuckDB...")

    conn = duckdb_connect(DB_PATH)

    # Load route information
    route_info = load_route_info(extract_dir)

    # Create table if it doesn't exist
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            trip_id VARCHAR,
            route_id VARCHAR,
            route_short_name VARCHAR,
            trip_headsign VARCHAR,
            departure_station_name VARCHAR,
            departure_time_scheduled TIMESTAMP WITH TIME ZONE,
            siri_departure_time_real TIMESTAMP WITH TIME ZONE,
            siri_departure_status VARCHAR,
            arrival_station_name VARCHAR,
            arrival_time_scheduled TIMESTAMP WITH TIME ZONE,
            siri_arrival_time_real TIMESTAMP WITH TIME ZONE,
            siri_arrival_status VARCHAR,
            siri_trip_status VARCHAR,
            created_at TIMESTAMP WITH TIME ZONE,
            siri_updated_at TIMESTAMP WITH TIME ZONE
        )
    """)

    # Check if data already exists for today
    today_date = datetime.now().strftime("%Y-%m-%d")
    result = conn.execute(
        f"SELECT COUNT(*) as count FROM {TABLE} WHERE DATE(departure_time_scheduled) = ?", [today_date]
    ).fetchone()

    if result[0] > 0:
        raise ValueError(f"Data already exists for {today_date}. Aborting to prevent duplicate data.")

    # Insert new data
    total_rows_inserted = 0
    for trip_info in relevant_trips:
        trip_id = trip_info["trip_id"]
        stops = trip_info["stops"]

        # Get route_id, service_id, and trip_headsign
        route_id, service_id, trip_headsign = get_trip_info(extract_dir, trip_id)

        # Find Paris Nord and Compiègne stops
        paris_nord_stop = None
        compiegne_stop = None

        for stop in stops:
            if stop["stop_id"] == GTFS_PARIS_NORD_STOP_ID:
                paris_nord_stop = stop
            elif stop["stop_id"] == GTFS_COMPIEGNE_STOP_ID:
                compiegne_stop = stop

        if paris_nord_stop and compiegne_stop:
            # Determine direction
            # If Paris Nord comes before Compiègne in the sequence, it's Paris -> Compiègne
            # Otherwise it's Compiègne -> Paris
            paris_index = next(i for i, s in enumerate(stops) if s["stop_id"] == GTFS_PARIS_NORD_STOP_ID)
            compiegne_index = next(i for i, s in enumerate(stops) if s["stop_id"] == GTFS_COMPIEGNE_STOP_ID)

            if paris_index < compiegne_index:
                # Paris -> Compiègne
                departure_stop = paris_nord_stop
                arrival_stop = compiegne_stop
                departure_station = "Paris Nord"
                arrival_station = "Compiègne"
            else:
                # Compiègne -> Paris
                departure_stop = compiegne_stop
                arrival_stop = paris_nord_stop
                departure_station = "Compiègne"
                arrival_station = "Paris Nord"

            # Parse times with today's date
            departure_time = parse_gtfs_time(today_date, departure_stop["departure_time"])
            arrival_time = parse_gtfs_time(today_date, arrival_stop["arrival_time"])

            # Get route short name
            route_short_name = route_info.get(route_id, "")

            # Get current timestamp for created_at
            created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Insert into database
            conn.execute(
                f"""
                INSERT INTO {TABLE} (
                    trip_id, route_id, route_short_name, trip_headsign,
                    departure_station_name, departure_time_scheduled,
                    arrival_station_name, arrival_time_scheduled,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    trip_id,
                    route_id,
                    route_short_name,
                    trip_headsign,
                    departure_station,
                    departure_time,
                    arrival_station,
                    arrival_time,
                    created_at,
                ],
            )
            total_rows_inserted += 1

    # Commit and close
    conn.commit()
    conn.close()

    if debug:
        print(f"✅ Stored {total_rows_inserted} trip instances in DuckDB")
    return total_rows_inserted


def ingest_static():
    import shutil

    if debug:
        print("🚆 Starting GTFS Static Data Ingestion")
        print("=" * 60)

    # Step 1: Download and extract GTFS data
    extract_dir, temp_dir = download_and_extract_gtfs()

    try:
        # Step 2: Load service dates (filtered for today)
        service_dates_today = load_service_dates(extract_dir)

        # Step 3: Find trips through both stations that run today
        relevant_trips = find_trips_through_both_stations(extract_dir, service_dates_today)

        # Step 4: Store in DuckDB
        inserted_count = store_in_duckdb(relevant_trips, extract_dir, service_dates_today)

        print(f"GTFS-STATIC: Fetched {len(relevant_trips)} trips, inserted {inserted_count} trips in DB")

        if debug:
            print("=" * 60)
            print("✅ GTFS Static Data Ingestion Completed!")
            print("📊 Found and stored trips between Paris Nord and Compiègne")

    finally:
        # Clean up temporary files
        if debug:
            print("🧹 Cleaning up temporary files...")
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        if debug:
            print("🗑️  Temporary files deleted")


def main():
    import time

    max_retries = 3
    retry_delay = 60  # seconds

    for attempt in range(1, max_retries + 1):
        try:
            ingest_static()
            break  # Success, exit the retry loop
        except Exception as e:
            if attempt < max_retries:
                print(f"❌ Attempt {attempt} failed: {e}")
                print(f"🔄 Retrying in {retry_delay} seconds... (attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                print(f"❌ All {max_retries} attempts failed: {e}")
                raise  # Re-raise the exception after final attempt


if __name__ == "__main__":
    main()
