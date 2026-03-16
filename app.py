#!/usr/bin/env python3
"""
Flask app to display analytics on train delays between Compiègne and Paris.
"""

import os
from datetime import datetime

import duckdb
from flask import Flask, jsonify, render_template

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


@app.route("/api/stats")
def get_stats():
    """Get overall delay statistics."""
    conn = get_db_connection()

    try:
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
            # Calculate arrival delay
            delay = calculate_delay_minutes(row["scheduled_arrival_time"], row["real_arrival_time"])

            if delay is not None:
                delays.append(
                    {"train_line": row["train_line_name"], "train_number": row["train_number"], "delay_minutes": delay}
                )

        # Calculate statistics
        total_trains = len(delays)
        on_time = sum(1 for d in delays if d["delay_minutes"] <= 0)
        delay_5min = sum(1 for d in delays if 0 < d["delay_minutes"] <= 5)
        delay_15min = sum(1 for d in delays if 5 < d["delay_minutes"] <= 15)
        delay_45min = sum(1 for d in delays if 15 < d["delay_minutes"] <= 45)
        delay_over_45min = sum(1 for d in delays if d["delay_minutes"] > 45)

        stats = {
            "total_trains": total_trains,
            "on_time": on_time,
            "on_time_percentage": (on_time / total_trains * 100) if total_trains > 0 else 0,
            "delay_5min": delay_5min,
            "delay_5min_percentage": (delay_5min / total_trains * 100) if total_trains > 0 else 0,
            "delay_15min": delay_15min,
            "delay_15min_percentage": (delay_15min / total_trains * 100) if total_trains > 0 else 0,
            "delay_45min": delay_45min,
            "delay_45min_percentage": (delay_45min / total_trains * 100) if total_trains > 0 else 0,
            "delay_over_45min": delay_over_45min,
            "delay_over_45min_percentage": (delay_over_45min / total_trains * 100) if total_trains > 0 else 0,
        }

        return jsonify(stats)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()


@app.route("/api/stats-by-line")
def get_stats_by_line():
    """Get delay statistics by train line, split by direction."""
    conn = get_db_connection()

    try:
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

        # Group by train line and number with direction (e.g., "K13 08:02")
        line_stats = {}

        for _, row in results.iterrows():
            # Create a key combining line name and departure time
            dep_time = row["scheduled_departure_time"]
            # Convert timestamp to string if needed
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
                        'delays': [],
                        'direction': direction,
                        'departure_time': ':'.join(time_part)
                    }
                line_stats[line_key]['delays'].append(delay)

        # Calculate statistics for each line
        stats_by_line = []
        for line_key, data in line_stats.items():
            delays = data['delays']
            total = len(delays)
            on_time = sum(1 for d in delays if d <= 0)
            delay_5min = sum(1 for d in delays if 0 < d <= 5)
            delay_15min = sum(1 for d in delays if 5 < d <= 15)
            delay_45min = sum(1 for d in delays if 15 < d <= 45)
            delay_over_45min = sum(1 for d in delays if d > 45)

            avg_delay = sum(delays) / len(delays) if delays else 0

            stats_by_line.append(
                {
                    "line": line_key,
                    "direction": data['direction'],
                    "departure_time": data['departure_time'],
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
                    "average_delay_minutes": avg_delay,
                }
            )

        # Sort by departure time (ascending)
        stats_by_line.sort(key=lambda x: x["departure_time"])

        return jsonify(stats_by_line)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)

