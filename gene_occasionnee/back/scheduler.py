import argparse
import time
from datetime import datetime

import schedule

from gene_occasionnee.back import ingest_gtfs_rt, ingest_gtfs_static


def run_static_ingestion(debug=False):
    """Run the static GTFS data ingestion."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running static GTFS ingestion...")
    try:
        ingest_gtfs_static.debug = debug
        ingest_gtfs_static.main()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Static GTFS ingestion completed successfully")
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error running static GTFS ingestion: {e}")


def run_rt_ingestion(debug=False):
    """Run the real-time GTFS data ingestion."""
    # Check time window first
    if not should_run_rt_ingestion():
        return

    try:
        ingest_gtfs_rt.debug = debug
        ingest_gtfs_rt.main()
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error running real-time GTFS ingestion: {e}")


def should_run_rt_ingestion():
    """Check if RT ingestion should run (between 5:00 AM and 1:58 AM)."""
    now = datetime.now()
    hour = now.hour
    # Run from 5:00 AM (hour 5) to 1:58 AM (hour 1)
    return hour >= 5 or hour <= 1


def run_scheduler(debug=False):
    print("Starting scheduler...")

    # Schedule static ingestion to run daily at 3:23 AM
    schedule.every().day.at("03:23").do(run_static_ingestion, debug=debug)

    # Schedule RT ingestion to run every 2 minutes
    schedule.every(2).minutes.do(run_rt_ingestion, debug=debug)

    print("Started scheduler.")

    while True:
        schedule.run_pending()
        time.sleep(1)


def main_scheduler():
    """Main entry point for the back module."""
    parser = argparse.ArgumentParser(description="GTFS Data Ingestion Scheduler")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    args = parser.parse_args()

    run_scheduler(debug=args.debug)
