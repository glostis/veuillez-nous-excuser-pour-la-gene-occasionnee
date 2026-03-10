# Implementation Plan for TER Hauts-de-France

## Overview
This plan outlines the steps to implement the TER Hauts-de-France project, which fetches real-time train data from the SNCF API and analyzes delays on the Compiègne to Paris Gare du Nord line.

## Phase 1: Setup and Configuration
1. **Environment Setup**:
   - Create a `.env` file to store the SNCF API key.
   - Set up `uv` for dependency management.

## Phase 2: Core Functionality

The SNCF API uses the Navitia platform, whose documentation is at https://doc.navitia.io/

Use this documentation to understand the API endpoints and data structure, especially the parts talking about "real-time" data.

1. **API Client**:
   - Create a module to interact with the SNCF API.
   - The station IDs for Compiègne and Paris Gare du Nord can be fetched from the API.
   - Implement functions to fetch train schedules and real-time data.

2. **Data Fetching**:
   - Fetch train journeys between Compiègne and Paris Gare du Nord.
   - Parse the API response to extract relevant data (departure/arrival times, delays, etc.).

3. **Data Storage**:
   - Store fetched data in a structured format in a local DuckDB database.
   - Ensure data includes timestamps, train line name and train number, etc. for historical analysis.

## Phase 3: Data Analysis
1. **Delay Calculation**:
   - Compare scheduled vs. actual departure/arrival times.
   - Calculate delays for each train journey.

2. **Statistics**:
   - Compute average delays, punctuality rates, and other relevant metrics.
   - Generate summaries (e.g., daily, weekly, monthly).

3. **Visualization**:
   - Create plots/charts to visualize delay patterns.
   - Use libraries like `matplotlib` or `seaborn`.

## Phase 4: Automation
1. **Scheduling**:
   - Set up a scheduler to fetch data at regular intervals (e.g., hourly or daily).
   - Use `cron` or a Python-based scheduler like `schedule`.

2. **Logging**:
   - Implement logging to track API calls and data processing.

## Phase 5: Testing and Deployment
1. **Testing**:
   - Write unit tests for API interactions and data processing.
   - Validate delay calculations and statistics.

2. **Deployment**:
   - Deploy the project as a CLI tool or a web service.
   - Document usage and configuration.

## Timeline
- **Week 1**: Setup, API client, and data fetching.
- **Week 2**: Data storage, delay calculation, and statistics.
- **Week 3**: Visualization, automation, and testing.
- **Week 4**: Deployment and documentation.

## Dependencies
- `requests`: For API interactions.
- `python-dotenv`: For environment variable management.
- `pandas`: For data manipulation and analysis.
- `matplotlib`/`seaborn`: For visualization (optional).
- `schedule`: For task scheduling (optional).

## Next Steps
1. Create the `.env` file with the API key.
2. Implement the API client module.
3. Test the API client with the correct station IDs.
