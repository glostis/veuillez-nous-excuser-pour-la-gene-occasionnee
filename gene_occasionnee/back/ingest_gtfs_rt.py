"""
GTFS Real-Time Data Ingestion Script

This script processes GTFS real-time data to update actual departure and arrival times
for trips between Paris Gare du Nord and Compiègne in the DuckDB database.

Usage: python -m gene_occasionnee.back.ingest_gtfs_rt [--debug]

Options:
  --debug    Enable debug output for detailed logging
"""

import argparse
import re
from datetime import datetime

import duckdb
import requests
from google.transit import gtfs_realtime_pb2

from gene_occasionnee import DB_PATH, TABLE
from gene_occasionnee.back import COMPIEGNE_STOP_ID, GTFS_RT_TU_URL, PARIS_NORD_STOP_ID

# Debug flag
debug = False


def fetch_and_decode_gtfs_rt():
    """Fetch and decode GTFS-RT data."""
    if debug:
        print("🔄 Fetching GTFS-RT data...")

    response = requests.get(GTFS_RT_TU_URL, timeout=20)
    response.raise_for_status()

    if debug:
        print(f"📥 Downloaded {len(response.content)} bytes")

    # Decode the protocol buffer
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    if debug:
        print(f"🔍 Decoded {len(feed.entity)} entities")

    return feed


def get_trip_ids_from_duckdb():
    """Get trip_ids from DuckDB for today's trips."""
    if debug:
        print("🔍 Getting trip IDs from DuckDB for today's trips...")

    # Connect to DuckDB
    conn = duckdb.connect(DB_PATH, read_only=True)

    # Get today's date
    today_date = datetime.now().strftime("%Y-%m-%d")

    # Query trip_ids for today
    result = conn.execute(
        f"SELECT trip_id FROM {TABLE} WHERE DATE(departure_time_scheduled) = ?", [today_date]
    ).fetchall()

    # Close connection
    conn.close()

    # Extract trip_ids from result
    trip_ids = [row[0] for row in result]
    if debug:
        print(f"📊 Found {len(trip_ids)} trip IDs for today")

    return trip_ids


def update_real_times_in_duckdb(trip_updates):
    """Update real departure and arrival times in DuckDB."""
    if debug:
        print("💾 Updating real times in DuckDB...")

    # Connect to DuckDB
    conn = duckdb.connect(DB_PATH, read_only=False)

    # Get current timestamp for updated_at
    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Update each trip
    updated_count = 0
    for trip_id, departure_time_scheduled, departure, arrival in trip_updates:
        if departure:
            conn.execute(
                f"UPDATE {TABLE} SET departure_time_real = ?, departure_gtfs_delay = ?, updated_at = ? WHERE trip_id = ? AND departure_time_scheduled = ?",
                [departure["time"], departure["delay"], updated_at, trip_id, departure_time_scheduled],
            )

        if arrival:
            conn.execute(
                f"UPDATE {TABLE} SET arrival_time_real = ?, arrival_gtfs_delay = ?, updated_at = ? WHERE trip_id = ? AND departure_time_scheduled = ?",
                [arrival["time"], arrival["delay"], updated_at, trip_id, departure_time_scheduled],
            )

        updated_count += 1

    # Commit and close
    conn.commit()
    conn.close()

    if debug:
        print(f"✅ Updated real times for {updated_count} trips")
    return updated_count


def parse_gtfs_rt_timestamp(timestamp):
    """Parse GTFS-RT timestamp (Unix epoch) to datetime."""
    if timestamp is None:
        return None
    return datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d %H:%M:%S")


def clean_stop_id(stop_id: str) -> str:
    """Extract numeric ID from stop ID string."""
    match = re.search(r"(\d{8})", stop_id or "")
    return match.group(1) if match else (stop_id or "").strip()


def process_gtfs_rt_data(feed, trip_ids):
    """Process GTFS real-time data and find matching trips."""
    if debug:
        print("🔍 Processing GTFS real-time data...")
        print(f"📋 Looking for {len(trip_ids)} trip IDs from database")

    # Map for trip schedule relationships
    TripSchRel = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship
    StopSchRel = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship

    TRIP_SCHEDULE_RELATIONSHIP_NAME = {
        TripSchRel.SCHEDULED: "SCHEDULED",
        TripSchRel.ADDED: "ADDED",
        TripSchRel.UNSCHEDULED: "UNSCHEDULED",
        TripSchRel.CANCELED: "CANCELED",
    }

    STOP_SCHEDULE_RELATIONSHIP_NAME = {
        StopSchRel.SCHEDULED: "SCHEDULED",
        StopSchRel.SKIPPED: "SKIPPED",
        StopSchRel.NO_DATA: "NO_DATA",
        StopSchRel.UNSCHEDULED: "UNSCHEDULED",
    }

    # Process the GTFS-RT data
    trip_updates = []
    total_trips_checked = 0
    trips_with_matching_stations = 0

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip_update = entity.trip_update
        trip = trip_update.trip

        trip_id = trip.trip_id or ""
        total_trips_checked += 1

        # Check if this trip is in our database
        if trip_id not in trip_ids:
            continue

        # Process stop time updates - collect all real-time data for our stations
        paris_nord_times = {}
        compiegne_times = {}
        found_relevant_station = False

        for stu in trip_update.stop_time_update:
            # Check if this is one of our target stations (using cleaned IDs)
            if stu.stop_id in {PARIS_NORD_STOP_ID, COMPIEGNE_STOP_ID}:
                found_relevant_station = True

                # Get absolute predicted times (these are already real-time predictions)
                arrival_time = stu.arrival.time if (stu.HasField("arrival") and stu.arrival.HasField("time")) else None
                departure_time = (
                    stu.departure.time if (stu.HasField("departure") and stu.departure.HasField("time")) else None
                )

                # Also get delay for informational purposes
                arrival_delay = (
                    stu.arrival.delay if (stu.HasField("arrival") and stu.arrival.HasField("delay")) else None
                )
                departure_delay = (
                    stu.departure.delay if (stu.HasField("departure") and stu.departure.HasField("delay")) else None
                )

                if stu.stop_id == PARIS_NORD_STOP_ID:
                    if departure_time:
                        paris_nord_times["departure"] = {
                            "time": parse_gtfs_rt_timestamp(departure_time),
                            "delay": departure_delay,
                        }
                    if arrival_time:
                        paris_nord_times["arrival"] = {
                            "time": parse_gtfs_rt_timestamp(arrival_time),
                            "delay": arrival_delay,
                        }

                elif stu.stop_id == COMPIEGNE_STOP_ID:
                    if departure_time:
                        compiegne_times["departure"] = {
                            "time": parse_gtfs_rt_timestamp(departure_time),
                            "delay": departure_delay,
                        }
                    if arrival_time:
                        compiegne_times["arrival"] = {
                            "time": parse_gtfs_rt_timestamp(arrival_time),
                            "delay": arrival_delay,
                        }

        if found_relevant_station:
            trips_with_matching_stations += 1

        # Determine direction and update accordingly
        # Get scheduled times from database to determine direction
        conn = duckdb.connect(DB_PATH, read_only=True)
        db_result = conn.execute(
            f"SELECT departure_station_name, arrival_station_name, departure_time_scheduled FROM {TABLE} WHERE trip_id = ?",
            [trip_id],
        ).fetchone()
        conn.close()

        if db_result:
            departure_station = db_result[0]
            arrival_station = db_result[1]
            departure_time_scheduled = db_result[2]

            # Determine what to update based on direction
            if departure_station == "Paris Nord" and arrival_station == "Compiègne":
                departure = paris_nord_times.get("departure")
                arrival = compiegne_times.get("arrival")
            elif departure_station == "Compiègne" and arrival_station == "Paris Nord":
                departure = compiegne_times.get("departure")
                arrival = paris_nord_times.get("arrival")
            else:
                raise ValueError(
                    f"Unknown direction for trip {trip_id}: departure {departure_station}, arrival {arrival_station}"
                )

            # Add to updates if we found relevant times
            if departure or arrival:
                trip_updates.append((trip_id, departure_time_scheduled, departure, arrival))

    if debug:
        print(f"📊 Processed {total_trips_checked} trips from GTFS-RT feed")
        print(f"📊 Found {trips_with_matching_stations} trips with our target stations")
        print(f"📊 Found {len(trip_updates)} trips with real-time updates")
    return trip_updates


def main():
    global debug

    # Parse command line arguments
    parser = argparse.ArgumentParser(description="GTFS Real-Time Data Ingestion")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    args = parser.parse_args()
    debug = args.debug

    if debug:
        print("🚆 Starting GTFS Real-Time Data Ingestion")
        print("=" * 60)

    try:
        # Step 1: Get trip IDs from DuckDB for today
        trip_ids = get_trip_ids_from_duckdb()

        if not trip_ids:
            if debug:
                print("⚠️ No trip IDs found for today in DuckDB. Nothing to update.")
            return

        # Step 2: Fetch and decode GTFS real-time data
        feed = fetch_and_decode_gtfs_rt()

        # Step 3: Process GTFS-RT data and find matching trips
        trip_updates = process_gtfs_rt_data(feed, trip_ids)

        # Step 4: Update real times in DuckDB
        if trip_updates:
            updated_count = update_real_times_in_duckdb(trip_updates)
            print(
                f"GTFS-RT: Fetched {len(trip_ids)} trips in DB,",
                f"decoded {len(feed.entity)} entities from feed,",
                f"updated {updated_count} trips in DB",
            )
        else:
            if debug:
                print("⚠️ No real-time updates found for today's trips")
            print(f"GTFS-RT: Fetched {len(trip_ids)} trips in DB, no updates needed")

        if debug:
            print("=" * 60)
            print("✅ GTFS Real-Time Data Ingestion Completed!")
            print("📊 Updated real departure/arrival times for trips")

    except Exception as e:
        print(f"❌ Error during GTFS real-time data ingestion: {e}")
        raise


if __name__ == "__main__":
    main()
