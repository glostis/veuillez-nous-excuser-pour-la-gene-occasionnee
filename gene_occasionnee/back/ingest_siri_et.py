"""
SIRI ET Lite Data Ingestion Script

This script processes SIRI ET Lite data to update actual departure and arrival times
for trips between Paris Gare du Nord and Compiègne in the DuckDB database.

Usage: uv run python -m gene_occasionnee.back.ingest_siri_et
"""

import xml.etree.ElementTree as ET
from datetime import datetime

import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from gene_occasionnee import DB_PATH, TABLE, duckdb_connect
from gene_occasionnee.back import SIRI_COMPIEGNE_STOP_ID, SIRI_ET_LITE_URL, SIRI_PARIS_NORD_STOP_ID
from gene_occasionnee.back.ingest_gtfs_static import main as static_main

debug = True

SIRI_NS = {"siri": "http://www.siri.org.uk/siri"}


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(Exception),
)
def fetch_and_parse_siri_et_lite():
    """Fetch and parse SIRI ET Lite XML data."""
    if debug:
        print("🔄 Fetching SIRI ET Lite data...")

    response = requests.get(SIRI_ET_LITE_URL, timeout=60)
    response.raise_for_status()

    if debug:
        print(f"📥 Downloaded {len(response.content)} bytes")

    # Parse the XML
    try:
        xml_data = response.content.decode("utf-8")
        root = ET.fromstring(xml_data)

        if debug:
            print("🔍 Parsed SIRI ET Lite XML")

        return root
    except ET.ParseError as e:
        raise ValueError(f"Failed to parse SIRI ET Lite XML: {e}")


def today():
    return datetime.now().strftime("%Y-%m-%d")


def get_trips_from_duckdb():
    """Get trips from DuckDB for today's trips."""
    if debug:
        print("🔍 Getting today's trips from DuckDB...")

    conn = duckdb_connect(DB_PATH, read_only=True)

    # Query trips for today
    result = conn.execute(
        f"""
        SELECT
            trip_id,
            departure_station_name,
            arrival_station_name,
            departure_time_scheduled,
            arrival_time_scheduled,
            trip_headsign as train_number
        FROM {TABLE}
        WHERE DATE(departure_time_scheduled) = ?
    """,
        [today()],
    ).fetchall()

    # Close connection
    conn.close()

    trips = []
    for row in result:
        trips.append(
            {
                "trip_id": row[0],
                "departure_station": row[1],
                "arrival_station": row[2],
                "departure_time_scheduled": row[3],
                "arrival_time_scheduled": row[4],
                "train_number": row[5],
            }
        )

    if debug:
        print(f"📊 Found {len(trips)} trips for today")

    return trips


def update_siri_times_in_duckdb(trip_updates):
    """Update SIRI real departure and arrival times in DuckDB."""
    if debug:
        print("💾 Updating SIRI times in DuckDB...")

    conn = duckdb_connect(DB_PATH, read_only=False)

    # Get current timestamp for updated_at
    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Update each trip
    updated_count = 0
    for trip_id, departure_time_real, arrival_time_real, departure_status, arrival_status, trip_status in trip_updates:
        update_fields = []
        update_values = []

        if departure_time_real:
            update_fields.append("siri_departure_time_real = ?")
            update_values.append(departure_time_real)

        if arrival_time_real:
            update_fields.append("siri_arrival_time_real = ?")
            update_values.append(arrival_time_real)

        if departure_status:
            update_fields.append("siri_departure_status = ?")
            update_values.append(departure_status)

        if arrival_status:
            update_fields.append("siri_arrival_status = ?")
            update_values.append(arrival_status)

        if trip_status:
            update_fields.append("siri_trip_status = ?")
            update_values.append(trip_status)

        update_fields.append("siri_updated_at = ?")
        update_values.append(updated_at)

        if update_fields:
            update_sql = f"UPDATE {TABLE} SET {', '.join(update_fields)} WHERE trip_id = ? AND DATE(departure_time_scheduled) = ?"
            update_values.extend([trip_id, today()])
            conn.execute(update_sql, update_values)
            updated_count += 1

    # Commit and close
    conn.commit()
    conn.close()

    if debug:
        print(f"✅ Updated SIRI times for {updated_count} trips")
    return updated_count


def parse_siri_timestamp(timestamp_str):
    """Parse SIRI timestamp (ISO 8601) to datetime."""
    if not timestamp_str:
        return None

    # SIRI timestamps are in format like "2026-04-22T07:04:00+02:00"
    # Convert to database format "YYYY-MM-DD HH:MM:SS"
    try:
        # Parse the ISO format and convert to naive datetime for storage
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def get_siri_trip_status(estimated_vehicle_journey):
    """Determine trip status from SIRI data."""
    # Check if trip is monitored
    monitored = estimated_vehicle_journey.find(".//{http://www.siri.org.uk/siri}Monitored")
    if monitored is not None and monitored.text == "false":
        return "UNMONITORED"

    # Check if prediction is inaccurate
    prediction_inaccurate = estimated_vehicle_journey.find(".//{http://www.siri.org.uk/siri}PredictionInaccurate")
    if prediction_inaccurate is not None and prediction_inaccurate.text == "true":
        return "PREDICTION_INACCURATE"

    return "NORMAL"


def get_siri_call_status(estimated_call):
    """Determine call status from SIRI EstimatedCall."""
    # Check if this call is cancelled
    cancellation = estimated_call.find(".//{http://www.siri.org.uk/siri}Cancellation")
    if cancellation is not None:
        return "CANCELLED"

    # Check if prediction is inaccurate
    prediction_inaccurate = estimated_call.find(".//{http://www.siri.org.uk/siri}PredictionInaccurate")
    if prediction_inaccurate is not None and prediction_inaccurate.text == "true":
        return "PREDICTION_INACCURATE"

    return "NORMAL"


def process_siri_et_lite_data(root, db_trips):
    """Process SIRI ET Lite data and find matching trips using train number and date."""
    if debug:
        print("🔍 Processing SIRI ET Lite data...")
        print(f"📋 Looking for {len(db_trips)} trips from database")

    # Process the SIRI ET Lite data
    trip_updates = []
    total_journeys_checked = 0
    journeys_with_matching_stations = 0
    journeys_with_matching_trains = 0

    # Find all EstimatedVehicleJourney elements
    estimated_vehicle_journeys = root.findall(".//siri:EstimatedVehicleJourney", SIRI_NS)

    for journey in estimated_vehicle_journeys:
        total_journeys_checked += 1

        # Extract train number from SIRI data
        train_number_ref = journey.find(".//siri:TrainNumberRef", SIRI_NS)
        siri_train_number = train_number_ref.text if train_number_ref is not None else None

        if not siri_train_number:
            continue

        # Extract date from DataFrameRef
        data_frame_ref = journey.find(".//siri:DataFrameRef", SIRI_NS)
        journey_date = data_frame_ref.text if data_frame_ref is not None else None

        if not journey_date:
            continue

        # Find matching trips in our database by train number and date
        matching_db_trips = []
        for db_trip in db_trips:
            if db_trip["train_number"] == siri_train_number:
                # Check if the date matches (journey_date should be YYYY-MM-DD)
                scheduled_datetime = db_trip["departure_time_scheduled"]
                if scheduled_datetime:
                    # Convert datetime to string if needed
                    if hasattr(scheduled_datetime, "strftime"):
                        scheduled_date = scheduled_datetime.strftime("%Y-%m-%d")
                    else:
                        scheduled_date = str(scheduled_datetime).split(" ")[0]
                    if scheduled_date == journey_date:
                        matching_db_trips.append(db_trip)

        if not matching_db_trips:
            continue

        journeys_with_matching_trains += 1

        # Get trip status
        trip_status = get_siri_trip_status(journey)

        # Process estimated calls - collect all real-time data for our stations
        paris_nord_departure = None
        paris_nord_arrival = None
        compiegne_departure = None
        compiegne_arrival = None
        paris_nord_status = None
        compiegne_status = None
        found_relevant_station = False

        estimated_calls = journey.findall(".//siri:EstimatedCalls/siri:EstimatedCall", SIRI_NS)
        for call in estimated_calls:
            stop_point_ref = call.find("siri:StopPointRef", SIRI_NS)
            if stop_point_ref is None:
                continue

            stop_id = stop_point_ref.text

            # Check if this is one of our target stations
            if stop_id in {SIRI_PARIS_NORD_STOP_ID, SIRI_COMPIEGNE_STOP_ID}:
                found_relevant_station = True
                call_status = get_siri_call_status(call)

                # Get expected times (these are the real-time predictions)
                expected_departure_time = None
                expected_arrival_time = None

                expected_departure_elem = call.find("siri:ExpectedDepartureTime", SIRI_NS)
                if expected_departure_elem is not None:
                    expected_departure_time = parse_siri_timestamp(expected_departure_elem.text)

                expected_arrival_elem = call.find("siri:ExpectedArrivalTime", SIRI_NS)
                if expected_arrival_elem is not None:
                    expected_arrival_time = parse_siri_timestamp(expected_arrival_elem.text)

                if stop_id == SIRI_PARIS_NORD_STOP_ID:
                    paris_nord_departure = expected_departure_time
                    paris_nord_arrival = expected_arrival_time
                    paris_nord_status = call_status
                elif stop_id == SIRI_COMPIEGNE_STOP_ID:
                    compiegne_departure = expected_departure_time
                    compiegne_arrival = expected_arrival_time
                    compiegne_status = call_status

        if found_relevant_station:
            journeys_with_matching_stations += 1

        # For each matching database trip, determine direction and update accordingly
        for db_trip in matching_db_trips:
            trip_id = db_trip["trip_id"]
            departure_station = db_trip["departure_station"]
            arrival_station = db_trip["arrival_station"]

            # Determine what to update based on direction
            if departure_station == "Paris Nord" and arrival_station == "Compiègne":
                departure_time_real = paris_nord_departure
                arrival_time_real = compiegne_arrival
                departure_status = paris_nord_status
                arrival_status = compiegne_status
            elif departure_station == "Compiègne" and arrival_station == "Paris Nord":
                departure_time_real = compiegne_departure
                arrival_time_real = paris_nord_arrival
                departure_status = compiegne_status
                arrival_status = paris_nord_status
            else:
                if debug:
                    print(
                        f"⚠️ Unknown direction for trip {trip_id}: departure {departure_station}, arrival {arrival_station}"
                    )
                continue

            # Add to updates if we found relevant data or status updates
            # We want to track status even if no time updates are available
            if departure_time_real or arrival_time_real or departure_status or arrival_status or trip_status:
                trip_updates.append(
                    (trip_id, departure_time_real, arrival_time_real, departure_status, arrival_status, trip_status)
                )

    if debug:
        print(f"📊 Processed {total_journeys_checked} journeys from SIRI ET Lite feed")
        print(f"📊 Found {journeys_with_matching_stations} journeys with our target stations")
        print(f"📊 Found {journeys_with_matching_trains} journeys with matching train numbers")
        print(f"📊 Found {len(trip_updates)} trips with real-time updates")
    return trip_updates


def main():
    if debug:
        print("🚆 Starting SIRI ET Lite Data Ingestion")
        print("=" * 60)

    try:
        # Step 1: Get trips from DuckDB for today
        db_trips = get_trips_from_duckdb()

        if not db_trips:
            if debug:
                print("⚠️ No trips found for today in DuckDB. Running GTFS static data ingestion first...")
            static_main()
            db_trips = get_trips_from_duckdb()
            if not db_trips:
                print("⚠️ No trips found for today in DuckDB. Exiting.")
                return

        # Step 2: Fetch and parse SIRI ET Lite data
        root = fetch_and_parse_siri_et_lite()

        nb_siri_journeys = len(root.findall(".//siri:EstimatedVehicleJourney", SIRI_NS))

        # Step 3: Process SIRI ET Lite data and find matching trips
        trip_updates = process_siri_et_lite_data(root, db_trips)

        # Step 4: Update SIRI times in DuckDB
        if trip_updates:
            updated_count = update_siri_times_in_duckdb(trip_updates)
            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]",
                f"SIRI-ET-LITE: Fetched {len(db_trips)} trips in DB,",
                f"processed SIRI {nb_siri_journeys} journeys,",
                f"updated {updated_count} trips in DB",
            )
        else:
            if debug:
                print("⚠️ No real-time updates found for today's trips")
            print(
                f"SIRI-ET-LITE: Fetched {len(db_trips)} trips in DB, processed SIRI {nb_siri_journeys} journeys,",
                " no updates needed",
            )

        if debug:
            print("=" * 60)
            print("✅ SIRI ET Lite Data Ingestion Completed!")
            print("📊 Updated SIRI real departure/arrival times for trips")

    except Exception as e:
        print(f"❌ Error during SIRI ET Lite data ingestion: {e}")
        raise


if __name__ == "__main__":
    main()
