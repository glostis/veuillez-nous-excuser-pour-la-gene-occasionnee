# AGENTS.md

This document describes how to run the Python scripts and analyze the data stored in DuckDB for the project.

## Project Structure

```
.
├── AGENTS.md                           # Project documentation
├── app.py                              # Flask web application
├── cronjob                             # CRON entry for the data collection script
├── data/                               # Data files
│   └── train_journeys.duckdb           # DuckDB database
├── docker-compose.yml                  # Docker Compose configuration
├── Dockerfile                          # Docker configuration
├── fetch_and_store_route_schedules.py  # Data collection script
├── logs/                               # Log files
├── requirements.txt                    # Python dependencies
├── static/                             # Static files (CSS, JS)
├── templates/                          # HTML templates
│   └── index.html                      # HTML template
├── test_app.py                         # pytest module that tests the API routes
└── .env                                # Environment variables
```

## Agent instructions

- Do not summarize your changes at each end of your turn, unless you are explicitly asked to.
- When touching the Flask API in `app.py`, run the tests with `pytest -v test_app.py` to ensure no regressions are made.

## Project Overview

The project aggregates and analyzes punctuality data for TER trains between Paris and Compiègne. It consists of:

1. A data collection script that fetches real-time information from the SNCF API
2. A DuckDB database for storing the collected data
3. A Flask web application for visualizing the analytics

## Language

The code of this project is written in English, but all user-facing elements (the website) are in French.

## Python Virtual Environment

The project uses a Python virtual environment managed by `direnv`.
The environment is configured in `.direnv/` and uses a Nix flake for dependency management.

### Activating the Environment

To activate the Python virtual environment:

```bash
direnv allow
```

This will automatically load the environment when you enter the project directory.

### Running Python Scripts

Once the environment is activated, you can run the Python scripts directly:

```bash
# Run the data collection script
python fetch_and_store_route_schedules.py

# Run the Flask application
python app.py
```

## Data Collection

The main data collection script is `fetch_and_store_route_schedules.py`. It:

1. Fetches route schedules from the SNCF API
2. Filters to keep only routes passing through Paris Nord and Compiègne
3. Stores the data in a DuckDB database

### Requirements

- An `.env` file with a valid `SNCF_API_KEY`
- Python dependencies listed in `requirements.txt`

### Running the Script

```bash
python fetch_and_store_route_schedules.py
```

The script is designed to be run daily via a cron job to collect data from the previous day.

## DuckDB Database

The project uses DuckDB for data storage. The database file is located at `data/train_journeys.duckdb`.

### Database Schema

The main table is `route_schedules`, which stores information about train journeys including:

- Train identifiers
- Scheduled and actual departure/arrival times
- Station information
- Delay calculations

### Analyzing the Data

You can analyze the data directly using DuckDB's CLI or Python API:

#### Using DuckDB CLI

```bash
# Start the DuckDB CLI
duckdb data/train_journeys.duckdb

# Example queries:
-- Get basic statistics
SELECT
    COUNT(*) as total_trips,
    AVG(delay_minutes) as avg_delay,
    MIN(delay_minutes) as min_delay,
    MAX(delay_minutes) as max_delay
FROM route_schedules;

-- Get delay distribution
SELECT
    CASE
        WHEN delay_minutes IS NULL THEN 'On time or no data'
        WHEN delay_minutes < 5 THEN '0-5 minutes'
        WHEN delay_minutes < 15 THEN '5-15 minutes'
        WHEN delay_minutes < 30 THEN '15-30 minutes'
        ELSE '30+ minutes'
    END as delay_category,
    COUNT(*) as count
FROM route_schedules
GROUP BY delay_category
ORDER BY delay_category;
```

#### Using Python API

```python
import duckdb

# Connect to the database
conn = duckdb.connect('data/train_journeys.duckdb')

# Execute a query
result = conn.execute("""
    SELECT
        COUNT(*) as total_trips,
        AVG(delay_minutes) as avg_delay
    FROM route_schedules
""").fetchall()

print(result)

# Close the connection
conn.close()
```

## Flask Application

The Flask application provides a web interface to visualize the analytics. To run it:

```bash
python app.py
```

The application will be available at `http://localhost:5000` by default.

## Docker Support

The project includes Docker support for deployment:

```bash
# Build the Docker image
docker-compose build

# Run the application
docker-compose up
```

## Cron Job

The data collection script is designed to run daily. A sample cron job configuration is provided in the `cronjob` file.

## Data Files

- `data/train_journeys.duckdb`: Main DuckDB database
- `data/*.json`: Raw data files from the SNCF API

## Environment Variables

- `SNCF_API_KEY`: Required for accessing the SNCF API

## Dependencies

The project dependencies are listed in `requirements.txt`.
