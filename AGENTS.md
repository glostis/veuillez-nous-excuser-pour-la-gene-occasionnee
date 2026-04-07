# AGENTS.md

This document describes how to run the Python scripts and analyze the data stored in DuckDB for the project.

## Project Structure

```
.
├── AGENTS.md                                    # Project documentation
├── data/                                        # Data files
│   └── gtfs.duckdb                              # DuckDB database
├── docker-compose.yml                           # Docker Compose configuration
├── docker-compose.local.yml                     # Docker Compose overlay for local dev (port mapping and volume mount for hot reloading)
├── Dockerfile                                   # Docker configuration
├── gene_occasionnee/                            # Main application package
│   ├── __init__.py
│   ├── back/                                    # Backend data processing
│   │   ├── __init__.py                          # Backend constants and config
│   │   ├── ingest_gtfs_static.py                # GTFS static data ingestion
│   │   ├── ingest_gtfs_rt.py                    # GTFS real-time data ingestion
│   │   └── scheduler.py                         # Scheduled ingestion runner
│   └── front/                                   # Frontend web application
│       ├── __init__.py
│       ├── app.py                               # Flask web application
│       ├── static/                              # Static files
│       │   ├── script.js                        # Frontend JavaScript
│       │   └── ter.png
│       └── templates/
│           └── index.html
├── generate_synthetic_data.py                   # Synthetic data generator
├── pyproject.toml                               # Python dependencies (uv)
├── tests/
│   ├── __init__.py
│   ├── test_app.py                              # pytest module that tests the API routes
│   └── test_fetch_and_store_route_schedules.py  # pytest module that tests the data fetching script
└── .env                                         # Environment variables
```

## Agent instructions

- Do not summarize your changes at each end of your turn, unless you are explicitly asked to.
- The tests in `test_app.py` only test the Flask API routes, there is no need to run them unless you are modifying the Flask API.
- The tests in `test_fetch_and_store_route_schedules.py` test the data fetching scripts, you should run them if you are modifying the data fetching scripts.

## Project Overview

The project aggregates and analyzes punctuality data for TER trains between Paris and Compiègne. It consists of:

1. **GTFS Static Ingestion**: Fetches scheduled trip data from SNCF GTFS static feed
2. **GTFS Real-Time Ingestion**: Updates with real-time delays from SNCF GTFS-RT feed
3. **DuckDB Database**: Stores all processed trip data at `data/gtfs.duckdb`
4. **Flask Web Application**: Visualizes analytics and statistics

## Language

The code of this project is written in English, but all user-facing elements (the website) are in French.

## Python Virtual Environment

The project uses a Python virtual environment managed by `uv`.
Prefix any commands by `uv run`. For example: `uv run pytest tests/`.

### Running Python Scripts

Once the environment is activated, you can run the Python scripts directly:

```bash
# Run GTFS static ingestion
uv run python -m gene_occasionnee.back.ingest_gtfs_static

# Run GTFS real-time ingestion
uv run python -m gene_occasionnee.back.ingest_gtfs_rt

# Run the scheduler (both ingestion processes scheduled at given intervals)
uv run python -m gene_occasionnee.back

# Run the Flask application
uv run python -m gene_occasionnee.front
```

## Data Collection

The system uses the SNCF GTFS feeds. There are two main ingestion scripts:

### GTFS Static Ingestion

The `ingest_gtfs_static.py` script:

1. Downloads and extracts GTFS static data from SNCF
2. Filters trips that pass through both Paris Nord and Compiègne
3. Stores scheduled trip data in DuckDB database

**Usage:**
```bash
uv run python -m gene_occasionnee.back.ingest_gtfs_static
```

### GTFS Real-Time Ingestion

The `ingest_gtfs_rt.py` script:

1. Fetches real-time updates from SNCF GTFS-RT feed
2. Updates actual departure/arrival times and delays
3. Updates the DuckDB database with real-time data

**Usage:**
```bash
uv run python -m gene_occasionnee.back.ingest_gtfs_rt
```

### Scheduler

The `scheduler.py` runs both ingestion processes automatically:
- Static ingestion: Daily at 3:23 AM
- Real-time ingestion: Every 2 minutes (5:00 AM - 1:58 AM)

**Usage:**
```bash
uv run python -m gene_occasionnee.back
```

## DuckDB Database

The project uses DuckDB for data storage. The database file is located at `data/gtfs.duckdb`.

### Database Schema

The main table is `gtfs`, which stores information about train journeys including:

- `trip_id`: Unique trip identifier
- `route_id`, `route_short_name`: Route information
- `departure_station_name`, `arrival_station_name`: Station names
- `departure_time_scheduled`, `arrival_time_scheduled`: Scheduled times
- `departure_time_real`, `arrival_time_real`: Actual times (from GTFS-RT)
- `departure_gtfs_delay`, `arrival_gtfs_delay`: Delay in seconds
- `created_at`, `updated_at`: Timestamps

### Analyzing the Data

You can analyze the data directly using DuckDB's CLI or Python API:

#### Using DuckDB CLI

```bash
# Start the DuckDB CLI
duckdb data/gtfs.duckdb

# Example queries:
-- Get basic statistics
SELECT
    COUNT(*) as total_trips,
    AVG(arrival_gtfs_delay / 60) as avg_delay_minutes,
    MIN(arrival_gtfs_delay / 60) as min_delay_minutes,
    MAX(arrival_gtfs_delay / 60) as max_delay_minutes
FROM gtfs;

-- Get delay distribution
SELECT
    CASE
        WHEN arrival_gtfs_delay IS NULL THEN 'On time or no data'
        WHEN arrival_gtfs_delay < 300 THEN '0-5 minutes'
        WHEN arrival_gtfs_delay < 900 THEN '5-15 minutes'
        WHEN arrival_gtfs_delay < 1800 THEN '15-30 minutes'
        ELSE '30+ minutes'
    END as delay_category,
    COUNT(*) as count
FROM gtfs
GROUP BY delay_category
ORDER BY delay_category;
```

#### Using Python API

```python
import duckdb

# Connect to the database
conn = duckdb.connect('data/gtfs.duckdb')

# Execute a query
result = conn.execute("""
    SELECT
        COUNT(*) as total_trips,
        AVG(arrival_gtfs_delay / 60) as avg_delay_minutes
    FROM gtfs
""").fetchall()

print(result)

# Close the connection
conn.close()
```

## Flask Application

The Flask application provides a web interface to visualize the analytics. To run it:

```bash
uv run python -m gene_occasionnee.front
```

The application will be available at `http://localhost:5000` by default.

### API Endpoints

- `GET /`: Main dashboard page
- `GET /api/stats`: Get delay statistics (can split by line with `split_by_line=true`)
- `GET /api/timeline`: Get timeline data for delay distribution over time
- `GET /api/date-range`: Get available date range
- `GET /api/latest-timestamp`: Get latest data update timestamp

## Docker Support

The project includes Docker support for deployment:

```bash
docker-compose up --build
```

## Cron Job

The data collection script is designed to run daily. A sample cron job configuration is provided in the `cronjob` file.

## Data Files

- `data/gtfs.duckdb`: Main DuckDB database with GTFS data

## Dependencies

The project dependencies are managed using `uv`. See `pyproject.toml` for the complete list of dependencies.
