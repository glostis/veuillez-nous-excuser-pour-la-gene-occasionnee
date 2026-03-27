#!/usr/bin/env python3
"""
Complete GTFS-RT Analysis for TER Trains between Paris Nord and Compiègne

This script performs a full analysis of realtime train delays using GTFS-RT data:
1. Downloads GTFS static data to identify stations and trips
2. Fetches realtime GTFS-RT data from SNCF Open Data
3. Decodes protocol buffers to extract delay information
4. Analyzes delays for trains serving Paris Nord and Compiègne
5. Generates detailed reports and statistics

Usage: python gtfs_rt_analysis.py
"""

import csv
import json
import os
import re
import zipfile
from datetime import datetime

import requests
from google.transit import gtfs_realtime_pb2

# Endpoints
GTFS_STATIC_URL = "https://eu.ftp.opendatasoft.com/sncf/plandata/Export_OpenData_SNCF_GTFS_NewTripId.zip"
GTFS_RT_TU_URL = "https://proxy.transport.data.gouv.fr/resource/sncf-gtfs-rt-trip-updates"


# Create gtfs_data directory if it doesn't exist
os.makedirs("gtfs_data", exist_ok=True)


def extract_gtfs_static():
    """Extract GTFS static data and build station mapping."""
    print("📦 Downloading and extracting GTFS static data...")

    # Download GTFS static data
    if not os.path.exists("gtfs_data/gtfs_static.zip"):
        print("📥 Downloading GTFS static data...")
        response = requests.get(GTFS_STATIC_URL, timeout=30)
        response.raise_for_status()
        with open("gtfs_data/gtfs_static.zip", "wb") as f:
            f.write(response.content)
        print(f"💾 Downloaded {len(response.content)} bytes")

    # Extract the ZIP file
    with zipfile.ZipFile("gtfs_data/gtfs_static.zip", "r") as zip_ref:
        zip_ref.extractall("gtfs_data/gtfs_static")

    print("📄 GTFS static data extracted.")

    # Parse stops.txt to build station ID mapping
    stops_file = "gtfs_data/gtfs_static/stops.txt"
    stop_id_to_name = {}

    with open(stops_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stop_id_raw = (row.get("stop_id") or "").strip()
            stop_name = (row.get("stop_name") or "").strip()

            # Extract numeric ID (8 digits) from stop_id
            match = re.search(r"(\d{8})", stop_id_raw)
            if match:
                id_num = match.group(1)
                stop_id_to_name[id_num] = stop_name or stop_id_raw

    print(f"📍 Found {len(stop_id_to_name)} stations in GTFS data")

    # Find our specific stations
    paris_nord_id = None
    compiegne_id = None

    for stop_id, stop_name in stop_id_to_name.items():
        if "Paris Gare du Nord" in stop_name:
            paris_nord_id = stop_id
        elif "Compiègne" in stop_name:
            compiegne_id = stop_id

    if not paris_nord_id or not compiegne_id:
        print("❌ Could not find station IDs in GTFS data")
        return None, None, None

    print(f"🎯 Paris Nord ID: {paris_nord_id}")
    print(f"🎯 Compiègne ID: {compiegne_id}")

    return stop_id_to_name, paris_nord_id, compiegne_id


def clean_stop_id(stop_id: str) -> str:
    """Extract numeric ID from stop ID string."""
    match = re.search(r"(\d{8})", stop_id or "")
    return match.group(1) if match else (stop_id or "").strip()


def fetch_and_decode_gtfs_rt():
    """Fetch and decode GTFS-RT data."""
    print("🔄 Fetching GTFS-RT data...")

    response = requests.get(GTFS_RT_TU_URL, timeout=20)
    response.raise_for_status()

    print(f"📥 Downloaded {len(response.content)} bytes")

    # Decode the protocol buffer
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    print(f"🔍 Decoded {len(feed.entity)} entities")

    return feed


def analyze_delays(stop_id_to_name, paris_nord_id, compiegne_id):
    """Analyze realtime delays for our specific stations."""
    print("🚆 Analyzing realtime train delays...")

    # Fetch and decode GTFS-RT data
    feed = fetch_and_decode_gtfs_rt()

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

    # Storage for trains serving our stations
    relevant_trains = []

    # Process each trip update
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip_update = entity.trip_update
        trip = trip_update.trip

        trip_id = trip.trip_id or ""

        # Extract train number (5 digits)
        match_num = re.search(r"(\d{5})", trip_id)
        train_number = match_num.group(1) if match_num else "?????"

        # Trip-level schedule relationship
        trip_sr_val = trip.schedule_relationship
        trip_sr_name = TRIP_SCHEDULE_RELATIONSHIP_NAME.get(trip_sr_val, "SCHEDULED")

        # Check if this train serves our stations
        serves_our_stations = False
        train_delays = []

        for stu in trip_update.stop_time_update:
            stop_id_clean = clean_stop_id(stu.stop_id)
            stop_name = stop_id_to_name.get(stop_id_clean, stu.stop_id or stop_id_clean)

            # Check if this is one of our target stations
            if stop_id_clean in [paris_nord_id, compiegne_id]:
                serves_our_stations = True

                # Get delay information
                arrival_delay = (
                    stu.arrival.delay if (stu.HasField("arrival") and stu.arrival.HasField("delay")) else None
                )
                departure_delay = (
                    stu.departure.delay if (stu.HasField("departure") and stu.departure.HasField("delay")) else None
                )

                # Get timestamps
                arrival_time = stu.arrival.time if (stu.HasField("arrival") and stu.arrival.HasField("time")) else None
                departure_time = (
                    stu.departure.time if (stu.HasField("departure") and stu.departure.HasField("time")) else None
                )

                # Stop-level schedule relationship
                stu_sr_val = stu.schedule_relationship
                stu_sr_name = STOP_SCHEDULE_RELATIONSHIP_NAME.get(stu_sr_val, "SCHEDULED")

                delay_info = {
                    "trip_id": trip_id,
                    "train_number": train_number,
                    "stop_id": stop_id_clean,
                    "stop_name": stop_name,
                    "station_type": "Paris Nord" if stop_id_clean == paris_nord_id else "Compiègne",
                    "arrival_time": arrival_time,
                    "departure_time": departure_time,
                    "arrival_delay_seconds": arrival_delay,
                    "departure_delay_seconds": departure_delay,
                    "arrival_delay_minutes": round(arrival_delay / 60, 1) if arrival_delay else None,
                    "departure_delay_minutes": round(departure_delay / 60, 1) if departure_delay else None,
                    "schedule_relationship": stu_sr_name,
                    "is_skipped": (stu_sr_name == "SKIPPED"),
                    "is_delayed": (arrival_delay is not None and arrival_delay > 300)
                    or (departure_delay is not None and departure_delay > 300),
                }

                train_delays.append(delay_info)

        if serves_our_stations and train_delays:
            # Determine overall status
            if trip_sr_name == "CANCELED":
                status = "CANCELED"
            elif any(d["is_skipped"] for d in train_delays):
                status = "PARTIAL_CANCELLATION"
            elif trip_sr_name in ("ADDED", "MODIFIED"):
                status = trip_sr_name
            elif any(d["is_delayed"] for d in train_delays):
                status = "DELAYED"
            else:
                status = "SCHEDULED"

            train_data = {
                "trip_id": trip_id,
                "train_number": train_number,
                "trip_schedule_relationship": trip_sr_name,
                "status": status,
                "stops": train_delays,
            }

            relevant_trains.append(train_data)

    print(f"🚄 Found {len(relevant_trains)} trains serving Paris Nord/Compiègne")

    return relevant_trains


def save_results(trains):
    """Save analysis results."""
    if not trains:
        print("⚠️ No train data to save")
        return None

    # Save detailed results
    with open("gtfs_data/gtfs_rt_delays.json", "w", encoding="utf-8") as f:
        json.dump(trains, f, ensure_ascii=False, indent=2)

    print("💾 Saved detailed results to gtfs_data/gtfs_rt_delays.json")

    # Calculate summary statistics
    total_trains = len(trains)
    canceled = sum(1 for t in trains if t["status"] == "CANCELED")
    partial_cancel = sum(1 for t in trains if t["status"] == "PARTIAL_CANCELLATION")
    delayed = sum(1 for t in trains if t["status"] == "DELAYED")
    scheduled = sum(1 for t in trains if t["status"] == "SCHEDULED")

    # Calculate delay statistics
    all_delays = []
    for train in trains:
        for stop in train["stops"]:
            if stop["arrival_delay_minutes"] is not None:
                all_delays.append(stop["arrival_delay_minutes"])
            if stop["departure_delay_minutes"] is not None:
                all_delays.append(stop["departure_delay_minutes"])

    avg_delay = sum(all_delays) / len(all_delays) if all_delays else 0
    max_delay = max(all_delays) if all_delays else 0
    min_delay = min(all_delays) if all_delays else 0

    summary = {
        "timestamp": datetime.now().isoformat(),
        "total_trains": total_trains,
        "canceled": canceled,
        "partial_cancellations": partial_cancel,
        "delayed": delayed,
        "on_time": scheduled,
        "average_delay_minutes": round(avg_delay, 1),
        "max_delay_minutes": round(max_delay, 1),
        "min_delay_minutes": round(min_delay, 1),
        "delay_count": len(all_delays),
    }

    with open("gtfs_data/gtfs_rt_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    print("📊 Saved summary statistics to gtfs_data/gtfs_rt_summary.json")

    return summary


def main():
    """Main execution function."""
    print("🚆 Starting GTFS-RT Delay Analysis")
    print("=" * 60)

    # Step 1: Extract GTFS static data
    stop_id_to_name, paris_nord_id, compiegne_id = extract_gtfs_static()

    if not paris_nord_id or not compiegne_id:
        print("❌ Could not find station IDs in GTFS data")
        return

    print("\n" + "=" * 60)

    # Step 2: Fetch and analyze realtime data
    trains = analyze_delays(stop_id_to_name, paris_nord_id, compiegne_id)

    print("\n" + "=" * 60)

    # Step 3: Save results
    summary = save_results(trains)

    if summary:
        # Print summary
        print("\n📈 Analysis Summary:")
        print(f"   Total trains analyzed: {summary['total_trains']}")
        print(f"   🚫 Canceled: {summary['canceled']}")
        print(f"   ⚠️ Partial cancellations: {summary['partial_cancellations']}")
        print(f"   ⏰ Delayed (>5 min): {summary['delayed']}")
        print(f"   ✅ On time: {summary['on_time']}")
        print(f"   📊 Average delay: {summary['average_delay_minutes']} minutes")
        print(f"   📈 Maximum delay: {summary['max_delay_minutes']} minutes")
        print(f"   📉 Minimum delay: {summary['min_delay_minutes']} minutes")

        # Print sample results
        if trains:
            print("\n📋 Sample Train Delays:")
            for i, train in enumerate(trains[:5]):
                print(f"   {i + 1}. Train {train['train_number']} ({train['status']})")
                for stop in train["stops"]:
                    if stop["is_delayed"]:
                        delay_min = stop["arrival_delay_minutes"] or stop["departure_delay_minutes"]
                        print(f"      - {stop['station_type']}: {delay_min} min delayed")
                    elif stop["is_skipped"]:
                        print(f"      - {stop['station_type']}: SKIPPED")
                    else:
                        print(f"      - {stop['station_type']}: On time")

    print("\n" + "=" * 60)
    print("✅ GTFS-RT Delay Analysis Completed!")
    print("\nGenerated files:")
    print("   - gtfs_data/gtfs_rt_delays.json: Detailed delay information for all trains")
    print("   - gtfs_data/gtfs_rt_summary.json: Summary statistics")
    print("\nThese files contain realtime delay data that can be used to:")
    print("   • Replace the current inaccurate SNCF API")
    print("   • Store in DuckDB for historical analysis")
    print("   • Display on your web dashboard")
    print("   • Monitor train punctuality trends")


if __name__ == "__main__":
    main()
