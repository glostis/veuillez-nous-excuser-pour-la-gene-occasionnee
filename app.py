#!/usr/bin/env python3
"""
Flask app to display analytics on train delays between Compiègne and Paris.
"""

from datetime import datetime

import duckdb
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

# Database configuration
DB_PATH = "data/train_journeys.duckdb"
TABLE_NAME = "route_schedules"


def get_db_connection():
    """Create a new database connection."""
    return duckdb.connect(DB_PATH)


def calculate_delay_minutes(scheduled_time, real_time):
    """Calculate delay in minutes between scheduled and real time."""
    if scheduled_time and real_time:
        try:
            # The timestamps are already in ISO format with timezone
            # e.g., "2026-03-14 08:48:00+01:00"
            scheduled = datetime.fromisoformat(str(scheduled_time))
            real = datetime.fromisoformat(str(real_time))
            delay = real - scheduled
            return delay.total_seconds() / 60
        except Exception as e:
            print(f"Error calculating delay: {e}")
            print(f"Scheduled: {scheduled_time} ({type(scheduled_time)}), Real: {real_time} ({type(real_time)})")
    return None


@app.route("/")
def index():
    """Main dashboard page."""
    return render_template("index.html")


def calculate_delay_statistics(delays):
    """Calculate delay statistics from a list of delays."""
    total = len(delays)
    if total == 0:
        return {
            "total_trains": 0,
            "on_time": 0,
            "on_time_percentage": 0,
            "delay_5min": 0,
            "delay_5min_percentage": 0,
            "delay_15min": 0,
            "delay_15min_percentage": 0,
            "delay_45min": 0,
            "delay_45min_percentage": 0,
            "delay_over_45min": 0,
            "delay_over_45min_percentage": 0,
        }

    on_time = sum(1 for d in delays if d <= 0)
    delay_5min = sum(1 for d in delays if 0 < d <= 5)
    delay_15min = sum(1 for d in delays if 5 < d <= 15)
    delay_45min = sum(1 for d in delays if 15 < d <= 45)
    delay_over_45min = sum(1 for d in delays if d > 45)

    return {
        "total_trains": total,
        "on_time": on_time,
        "on_time_percentage": (on_time / total * 100) if total > 0 else 0,
        "delay_5min": delay_5min,
        "delay_5min_percentage": (delay_5min / total * 100) if total > 0 else 0,
        "delay_15min": delay_15min,
        "delay_15min_percentage": (delay_15min / total * 100) if total > 0 else 0,
        "delay_45min": delay_45min,
        "delay_45min_percentage": (delay_45min / total * 100) if total > 0 else 0,
        "delay_over_45min": delay_over_45min,
        "delay_over_45min_percentage": (delay_over_45min / total * 100) if total > 0 else 0,
    }


@app.route("/api/stats")
def get_stats():
    """Get delay statistics. Can be split by line if split_by_line parameter is True."""
    split_by_line = request.args.get("split_by_line", "false").lower() == "true"
    conn = get_db_connection()

    try:
        if split_by_line:
            # Get all records with direction information
            query = f"""
            SELECT
                train_line_name,
                train_number,
                departure_station_name,
                arrival_station_name,
                scheduled_departure_time,
                real_departure_time,
                scheduled_arrival_time,
                real_arrival_time
            FROM {TABLE_NAME}
            ORDER BY scheduled_departure_time ASC
            """

            results = conn.execute(query).fetchdf()

            if len(results) == 0:
                return jsonify({"error": "No data found"}), 404

            # Group by train line and number with direction
            line_stats = {}

            for _, row in results.iterrows():
                # Create a key combining line name and departure time
                dep_time = row["scheduled_departure_time"]
                dep_time_str = str(dep_time)
                time_part = dep_time_str.split(" ")[1].split(":")[:2]  # Get HH:MM

                # Determine direction
                departure = row["departure_station_name"]
                arrival = row["arrival_station_name"]
                direction = f"{departure} → {arrival}"

                line_key = f"{row['train_line_name']} {row['train_number']} ({':'.join(time_part)}) - {direction}"

                # Calculate arrival delay
                delay = calculate_delay_minutes(row["scheduled_arrival_time"], row["real_arrival_time"])

                if delay is not None:
                    if line_key not in line_stats:
                        line_stats[line_key] = {
                            "delays": [],
                            "direction": direction,
                            "departure_time": ":".join(time_part),
                        }
                    line_stats[line_key]["delays"].append(delay)

            # Calculate statistics for each line
            stats_by_line = []
            for line_key, data in line_stats.items():
                delays = data["delays"]
                stats = calculate_delay_statistics(delays)
                avg_delay = sum(delays) / len(delays) if delays else 0

                stats_by_line.append(
                    {
                        "line": line_key,
                        "direction": data["direction"],
                        "departure_time": data["departure_time"],
                        "average_delay_minutes": avg_delay,
                        **stats,
                    }
                )

            # Sort by departure time (ascending)
            stats_by_line.sort(key=lambda x: x["departure_time"])

            return jsonify(stats_by_line)
        else:
            # Get all records
            query = f"""
            SELECT
                train_line_name,
                train_number,
                scheduled_departure_time,
                real_departure_time,
                scheduled_arrival_time,
                real_arrival_time
            FROM {TABLE_NAME}
            """

            results = conn.execute(query).fetchdf()

            if len(results) == 0:
                return jsonify({"error": "No data found"}), 404

            # Calculate delays
            delays = []
            for _, row in results.iterrows():
                delay = calculate_delay_minutes(row["scheduled_arrival_time"], row["real_arrival_time"])
                if delay is not None:
                    delays.append(delay)

            # Calculate statistics
            stats = calculate_delay_statistics(delays)

            return jsonify(stats)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()


@app.route("/api/latest-timestamp")
def get_latest_timestamp():
    """Get the latest fetch timestamp and corresponding number of train schedules."""
    conn = get_db_connection()

    try:
        # Get the record with the most recent fetch_timestamp
        query = f"""
            SELECT
                fetch_timestamp,
                COUNT(*) OVER (PARTITION BY fetch_timestamp) AS row_count
            FROM {TABLE_NAME}
            ORDER BY fetch_timestamp DESC
            LIMIT 1;
        """

        result = conn.execute(query).fetchdf()

        if len(result) == 0:
            return jsonify({"error": "No data found"}), 404

        latest_data = result.iloc[0]
        fetch_timestamp = latest_data["fetch_timestamp"]
        row_count = latest_data["row_count"]

        # Check if data is outdated (older than 24 hours)
        from datetime import timedelta

        current_time = datetime.now(fetch_timestamp.tzinfo) if fetch_timestamp.tzinfo else datetime.now()
        time_diff = current_time - fetch_timestamp
        is_outdated = time_diff > timedelta(hours=24)

        # Format the timestamp for display
        formatted_timestamp = fetch_timestamp.strftime("%d/%m/%Y à %Hh%M (heure de Compiègne)")

        # Get the date for which data was collected (subtract one day from fetch timestamp)
        data_date = fetch_timestamp.date() - timedelta(days=1)
        formatted_data_date = data_date.strftime("%d/%m/%Y")

        timestamps = {
            "fetch_timestamp": formatted_timestamp,
            "row_count": str(row_count),
            "data_date": formatted_data_date,
            "is_outdated": is_outdated,
        }

        return jsonify(timestamps)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
