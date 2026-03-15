# TER HDF - Train Schedule Fetcher

## Docker Deployment

This project is set up to run in a Docker container with a daily cron job.

### Prerequisites
- Docker
- Docker Compose

### Setup

1. **Build and start the container:**
   ```bash
   docker-compose up -d --build
   ```

2. **Check logs:**
   ```bash
   docker-compose logs -f fetch-schedules
   ```

3. **View cron logs:**
   ```bash
   tail -f logs/cron.log
   ```

### Configuration

- The script runs daily at 5:05 AM (Paris time)
- Data is stored in the `data/` directory on your host machine
- Logs are available in the `logs/` directory
- The timezone is set to Europe/Paris

### Environment Variables

The `.env` file contains the SNCF API key. Make sure to keep this file secure.

### Updating

To update the container after making changes:
```bash
docker-compose down
docker-compose up -d --build
```

### Manual Execution

To run the script manually:
```bash
docker-compose exec fetch-schedules python fetch_and_store_route_schedules.py
```

## Original README

This script fetches route schedules for trains between Compiègne and Paris Nord using the SNCF API and stores the data in DuckDB.

The script focuses on:
1. Fetching route schedules from the API
2. Filtering to keep only routes passing through Paris and Compiègne
3. Storing raw data in DuckDB with one row per train trip
