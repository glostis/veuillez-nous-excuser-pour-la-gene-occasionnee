# Veuillez nous excuser pour la gêne occasionnée

This repo hosts the code that powers the website [ter.glost.is](https://ter.glost.is), which aggregates information about the punctuality of TER trains between Paris and Compiègne.

## Data Collection

Data collection is done using the [SNCF API](https://www.digital.sncf.com/startup/api) which publishes real-time information about train schedules and delays.

A python script is run daily to fetch the data from the previous day and store it in a DuckDB database.

This script is launched in a Docker container and is scheduled to run daily using a cron job.

A `.env` file containing an SNCF API key is required.

## Data Analysis

The data is presented in an aggregated form on a website using a Flask app.
