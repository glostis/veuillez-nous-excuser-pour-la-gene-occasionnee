#!/usr/bin/env python3
"""
Generate synthetic train data for testing the timeline chart.
This script creates data spanning several months with realistic delay patterns.
"""

import random
from datetime import datetime, timedelta

import duckdb
import numpy as np

# Connect to the database
DB_PATH = "data/train_journeys.duckdb"
TABLE_NAME = "route_schedules"

conn = duckdb.connect(DB_PATH)

# Get the current table structure
result = conn.execute(f"DESCRIBE {TABLE_NAME}").fetchdf()
print("Current table structure:")
print(result)

conn.execute(f"DELETE FROM {TABLE_NAME}")  # Clear existing data

# Generate data for a reasonable time period (e.g., 3 months)
end_date = datetime.today().date()
start_date = end_date - timedelta(days=90)

# Step 1: Generate 60 realistic train lines with fixed schedules
# Realistic line names and numbers
train_lines = []
line_names = ["C13", "C14", "K13", "K14"]

for i in range(60):
    line_name = random.choice(line_names)
    train_number = f"{random.randint(1000, 9999):04d}"

    # Random direction
    direction = random.choice([("Compiègne", "Paris Nord"), ("Paris Nord", "Compiègne")])

    # Random departure time between 5:00 and 21:00
    hour = random.randint(5, 21)
    minute = random.choice([0, 15, 30, 45])

    # Random duration (50-80 minutes for this route)
    duration_minutes = random.randint(50, 80)

    train_lines.append(
        {
            "line_name": line_name,
            "train_number": train_number,
            "direction": direction,
            "departure_hour": hour,
            "departure_minute": minute,
            "duration_minutes": duration_minutes,
        }
    )

print(f"Generated {len(train_lines)} train lines")

# Step 2: Generate daily trips for each line
all_data = []
current_date = start_date
day_id = 1

while current_date < end_date:
    date_str = current_date.strftime("%Y-%m-%d")

    for line in train_lines:
        departure_station, arrival_station = line["direction"]
        hour = line["departure_hour"]
        minute = line["departure_minute"]
        duration_minutes = line["duration_minutes"]

        # Create scheduled times
        scheduled_departure = datetime.combine(current_date, datetime.min.time()).replace(hour=hour, minute=minute)
        scheduled_arrival = scheduled_departure + timedelta(minutes=duration_minutes)

        # Generate realistic delays based on time of day and day of week
        departure_hour = scheduled_departure.hour

        # Morning/evening rush hours have more delays
        rush_hour_penalty = (
            1.5
            if (departure_hour >= 6 and departure_hour <= 9) or (departure_hour >= 16 and departure_hour <= 19)
            else 1.0
        )

        # Weekends have fewer delays
        weekend_bonus = 0.7 if current_date.weekday() >= 5 else 1.0  # Saturday/Sunday

        # Base delay probability and magnitude
        delay_probability = 0.3 * rush_hour_penalty * weekend_bonus

        if random.random() < delay_probability:
            # Generate delay (1-60 minutes, with decreasing probability)
            delay_minutes = int(np.random.exponential(15))  # Average 15 minutes
            delay_minutes = max(1, min(90, delay_minutes))  # Clamp to 1-90 minutes

            real_departure = scheduled_departure + timedelta(minutes=delay_minutes)
            real_arrival = scheduled_arrival + timedelta(minutes=delay_minutes)
        else:
            # On time or slightly early
            early_minutes = random.randint(0, 2)
            real_departure = scheduled_departure - timedelta(minutes=early_minutes)
            real_arrival = scheduled_arrival - timedelta(minutes=early_minutes)

        # Create record
        record = {
            "id": f"{line['line_name']}-{line['train_number']}-{current_date.strftime('%Y%m%d')}",
            "train_number": line["train_number"],
            "train_line_name": line["line_name"],
            "departure_station_name": departure_station,
            "scheduled_departure_time": scheduled_departure.isoformat(),
            "real_departure_time": real_departure.isoformat(),
            "arrival_station_name": arrival_station,
            "scheduled_arrival_time": scheduled_arrival.isoformat(),
            "real_arrival_time": real_arrival.isoformat(),
            "fetch_timestamp": (
                datetime.combine(current_date, datetime.min.time()) + timedelta(days=1, hours=3)
            ).isoformat(),
        }

        all_data.append(record)

    current_date += timedelta(days=1)
    day_id += 1

    if day_id % 10 == 0:  # Progress feedback
        print(f"Processed {day_id} days...")

print(f"Generated {len(all_data)} synthetic records")

# Insert data in batches
batch_size = 1000
for i in range(0, len(all_data), batch_size):
    batch = all_data[i : i + batch_size]

    # Create a temporary table and insert batch
    temp_table = f"temp_batch_{i // batch_size}"

    # Create temporary table with same structure
    conn.execute(f"""
    CREATE TEMP TABLE {temp_table} AS
    SELECT * FROM {TABLE_NAME} WHERE 1=0
    """)

    # Insert batch data
    columns = list(batch[0].keys())
    values_clauses = []

    for record in batch:
        # Escape single quotes
        escaped_values = []
        for col in columns:
            value = record[col]
            if isinstance(value, str):
                value = value.replace("'", "''")
                escaped_values.append(f"'{value}'")
            else:
                escaped_values.append(str(value))

        values_clauses.append(f"({', '.join(escaped_values)})")

    insert_sql = f"""
    INSERT INTO {temp_table} ({", ".join(columns)})
    VALUES {", ".join(values_clauses)}
    """

    try:
        conn.execute(insert_sql)

        # Insert from temp table to main table
        conn.execute(f"""
        INSERT INTO {TABLE_NAME}
        SELECT * FROM {temp_table}
        """)

        print(f"Inserted batch {i // batch_size + 1}: {len(batch)} records")

    except Exception as e:
        print(f"Error inserting batch {i // batch_size + 1}: {e}")
        print("Problematic SQL:", insert_sql[:500] + "..." if len(insert_sql) > 500 else insert_sql)

print("Synthetic data generation complete!")

conn.close()
