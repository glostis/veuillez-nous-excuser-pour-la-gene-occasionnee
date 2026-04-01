# Veuillez nous excuser pour la gêne occasionnée

This repo hosts the code that powers the website [ter.glost.is](https://ter.glost.is), which aggregates information about the punctuality of TER trains between Paris and Compiègne.

## Data source

The data is sourced from the SNCF GTFS feeds[^1]
- **GTFS Static**: Provides scheduled trip information (scheduled departure/arrival times, stops, etc.)
- **GTFS Real-Time Trip Updates**: Provides real-time updates and delays (actual departure/arrival times, delays, etc.)

[^1]: GTFS stands for [General Transit Feed Specification](https://en.wikipedia.org/wiki/GTFS). More info about the SNCF feeds [here](https://data.sncf.com/explore/dataset/horaires-sncf/information/).

The system consists of:
1. **Static ingestion**: Runs daily at night to fetch scheduled trips for the day from the GTFS static feed
2. **Real-time ingestion**: Runs every 2 minutes to update the static trips fetched above with real-time data from the GTFS-RT feed
3. **DuckDB database**: Stores all processed trip data

## Data analysis

The data is presented in an aggregated form on a website using a Flask app.
