# Veuillez nous excuser pour la gêne occasionnée

This repo hosts the code that powers the website [ter-compiegne.glost.is](https://ter-compiegne.glost.is), which aggregates information about the punctuality of TER trains between Paris and Compiègne.

## Data source

The data is sourced from the SNCF open-data feeds:
- **GTFS Static**[^1]: Provides scheduled trip information (scheduled departure/arrival times, stops, etc.)
- **SIRI ET Lite**[^2]: Provides real-time updates and delays (actual departure/arrival times, platforms, etc.)
More info about the SNCF feeds [here](https://data.sncf.com/explore/dataset/horaires-sncf/information/).

[^1]: GTFS stands for [General Transit Feed Specification](https://en.wikipedia.org/wiki/GTFS).
[^2]: SIRI ET stands for [Service Interface for Real Time Information](https://en.wikipedia.org/wiki/Service_Interface_for_Real_Time_Information) Estimated Timetable.

The system consists of:
1. **Static ingestion**: Runs daily at night to fetch scheduled trips for the day from the GTFS static feed
2. **Real-time ingestion**: Runs every 2 minutes to update the static trips fetched above with real-time data from the SIRI ET Lite feed
3. **DuckDB database**: Stores all processed trip data

## Data analysis

The data is presented in an aggregated form on a website using a Flask app.
