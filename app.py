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
    # Use test database if in testing mode
    if app.config.get('TESTING'):
        db_path = app.config.get('DB_PATH', DB_PATH)
    else:
        db_path = DB_PATH
    return duckdb.connect(db_path)


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
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    conn = get_db_connection()

    try:
        # Build date filter condition
        date_filter = ""
        if start_date and end_date:
            date_filter = f"WHERE DATE(scheduled_departure_time) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"WHERE DATE(scheduled_departure_time) >= '{start_date}'"
        elif end_date:
            date_filter = f"WHERE DATE(scheduled_departure_time) <= '{end_date}'"

        if split_by_line:
            # Use SQL aggregation to get statistics by line
            query = f"""
            SELECT
                train_line_name || ' ' || train_number AS line,
                MIN(departure_station_name || ' → ' || arrival_station_name) AS direction,
                MIN(STRFTIME(scheduled_departure_time, '%H:%M')) AS departure_time,
                MIN(STRFTIME(scheduled_arrival_time, '%H:%M')) AS arrival_time,
                MIN(EXTRACT(MINUTE FROM (scheduled_arrival_time - scheduled_departure_time)) +
                EXTRACT(HOUR FROM (scheduled_arrival_time - scheduled_departure_time)) * 60) AS scheduled_duration,
                AVG(EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60) AS average_delay_minutes,
                COUNT(*) AS total_trains,
                SUM(CASE WHEN EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 <= 0 THEN 1 ELSE 0 END) AS on_time,
                SUM(CASE WHEN EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 > 0 AND
                         EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 <= 5 THEN 1 ELSE 0 END) AS delay_5min,
                SUM(CASE WHEN EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 > 5 AND
                         EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 <= 15 THEN 1 ELSE 0 END) AS delay_15min,
                SUM(CASE WHEN EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 > 15 AND
                         EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 <= 45 THEN 1 ELSE 0 END) AS delay_45min,
                SUM(CASE WHEN EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 > 45 THEN 1 ELSE 0 END) AS delay_over_45min
            FROM {TABLE_NAME}
            {date_filter}
            GROUP BY line
            ORDER BY line ASC
            """

            results = conn.execute(query).fetchdf()

            if len(results) == 0:
                return jsonify({"error": "No data found"}), 404

            # Calculate percentages and format response
            stats_by_line = []
            for _, row in results.iterrows():
                total = row["total_trains"]
                stats_by_line.append({
                    "line": row["line"],
                    "direction": row["direction"],
                    "departure_time": row["departure_time"],
                    "arrival_time": row["arrival_time"],
                    "scheduled_duration": int(row["scheduled_duration"]) if row["scheduled_duration"] is not None else 0,
                    "average_delay_minutes": float(row["average_delay_minutes"]) if row["average_delay_minutes"] is not None else 0,
                    "total_trains": int(row["total_trains"]),
                    "on_time": int(row["on_time"]),
                    "on_time_percentage": (row["on_time"] / total * 100) if total > 0 else 0,
                    "delay_5min": int(row["delay_5min"]),
                    "delay_5min_percentage": (row["delay_5min"] / total * 100) if total > 0 else 0,
                    "delay_15min": int(row["delay_15min"]),
                    "delay_15min_percentage": (row["delay_15min"] / total * 100) if total > 0 else 0,
                    "delay_45min": int(row["delay_45min"]),
                    "delay_45min_percentage": (row["delay_45min"] / total * 100) if total > 0 else 0,
                    "delay_over_45min": int(row["delay_over_45min"]),
                    "delay_over_45min_percentage": (row["delay_over_45min"] / total * 100) if total > 0 else 0,
                })

            return jsonify(stats_by_line)
        else:
            # First check if there's any data
            count_query = f"""
            SELECT COUNT(*) AS count FROM {TABLE_NAME} {date_filter}
            """
            count_result = conn.execute(count_query).fetchdf()
            total_count = count_result.iloc[0]["count"]

            if total_count == 0:
                return jsonify({"error": "No data found"}), 404

            # Use SQL aggregation to get overall statistics
            query = f"""
            SELECT
                COUNT(*) AS total_trains,
                SUM(CASE WHEN EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 <= 0 THEN 1 ELSE 0 END) AS on_time,
                SUM(CASE WHEN EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 > 0 AND
                         EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 <= 5 THEN 1 ELSE 0 END) AS delay_5min,
                SUM(CASE WHEN EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 > 5 AND
                         EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 <= 15 THEN 1 ELSE 0 END) AS delay_15min,
                SUM(CASE WHEN EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 > 15 AND
                         EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 <= 45 THEN 1 ELSE 0 END) AS delay_45min,
                SUM(CASE WHEN EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 > 45 THEN 1 ELSE 0 END) AS delay_over_45min
            FROM {TABLE_NAME}
            {date_filter}
            """

            results = conn.execute(query).fetchdf()

            row = results.iloc[0]
            total = row["total_trains"]

            stats = {
                "total_trains": int(row["total_trains"]),
                "on_time": int(row["on_time"]),
                "on_time_percentage": (row["on_time"] / total * 100) if total > 0 else 0,
                "delay_5min": int(row["delay_5min"]),
                "delay_5min_percentage": (row["delay_5min"] / total * 100) if total > 0 else 0,
                "delay_15min": int(row["delay_15min"]),
                "delay_15min_percentage": (row["delay_15min"] / total * 100) if total > 0 else 0,
                "delay_45min": int(row["delay_45min"]),
                "delay_45min_percentage": (row["delay_45min"] / total * 100) if total > 0 else 0,
                "delay_over_45min": int(row["delay_over_45min"]),
                "delay_over_45min_percentage": (row["delay_over_45min"] / total * 100) if total > 0 else 0,
            }

            return jsonify(stats)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()


@app.route("/api/timeline")
def get_timeline():
    """Get timeline data for delay distribution over time."""
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    conn = get_db_connection()

    try:
        # Build date filter condition
        date_filter = ""
        if start_date and end_date:
            date_filter = f"WHERE DATE(scheduled_departure_time) BETWEEN '{start_date}' AND '{end_date}'"
        elif start_date:
            date_filter = f"WHERE DATE(scheduled_departure_time) >= '{start_date}'"
        elif end_date:
            date_filter = f"WHERE DATE(scheduled_departure_time) <= '{end_date}'"

        # Use SQL aggregation to get statistics by date
        query = f"""
        SELECT
            STRFTIME(scheduled_departure_time, '%Y-%m-%d') AS date,
            COUNT(*) AS total_trains,
            SUM(CASE WHEN EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 <= 0 THEN 1 ELSE 0 END) AS on_time,
            SUM(CASE WHEN EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 > 0 AND
                     EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 <= 5 THEN 1 ELSE 0 END) AS delay_5min,
            SUM(CASE WHEN EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 > 5 AND
                     EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 <= 15 THEN 1 ELSE 0 END) AS delay_15min,
            SUM(CASE WHEN EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 > 15 AND
                     EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 <= 45 THEN 1 ELSE 0 END) AS delay_45min,
            SUM(CASE WHEN EXTRACT(EPOCH FROM (real_arrival_time - scheduled_arrival_time)) / 60 > 45 THEN 1 ELSE 0 END) AS delay_over_45min
        FROM {TABLE_NAME}
        {date_filter}
        GROUP BY date
        ORDER BY date ASC
        """

        results = conn.execute(query).fetchdf()

        if len(results) == 0:
            return jsonify({"error": "No data found"}), 404

        # Format the results
        timeline_stats = []
        for _, row in results.iterrows():
            timeline_stats.append({
                "date": row["date"],
                "total_trains": int(row["total_trains"]),
                "on_time": int(row["on_time"]),
                "delay_5min": int(row["delay_5min"]),
                "delay_15min": int(row["delay_15min"]),
                "delay_45min": int(row["delay_45min"]),
                "delay_over_45min": int(row["delay_over_45min"])
            })

        return jsonify(timeline_stats)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        conn.close()


@app.route("/api/date-range")
def get_date_range():
    """Get the minimum and maximum dates available in the database."""
    conn = get_db_connection()

    try:
        query = f"""
            SELECT
                MIN(DATE(scheduled_departure_time)) AS min_date,
                MAX(DATE(scheduled_departure_time)) AS max_date
            FROM {TABLE_NAME}
        """

        result = conn.execute(query).fetchdf()

        if len(result) == 0:
            return jsonify({"error": "No data found"}), 404

        date_range = result.iloc[0]

        # Format dates as YYYY-MM-DD (remove time portion if present)
        min_date_str = str(date_range["min_date"])
        max_date_str = str(date_range["max_date"])

        # Remove timestamp if present (format: YYYY-MM-DD HH:MM:SS)
        min_date_clean = min_date_str.split(" ")[0] if " " in min_date_str else min_date_str
        max_date_clean = max_date_str.split(" ")[0] if " " in max_date_str else max_date_str

        return jsonify({
            "min_date": min_date_clean,
            "max_date": max_date_clean
        })

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
