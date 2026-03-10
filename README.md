# TER Hauts-de-France

A Python project to scrape real-time train departure and arrival times from Compiègne to Paris Gare du Nord using the SNCF API, and analyze delays on this line.

## Features
- Fetch real-time train schedules using the SNCF API
- Calculate delays between scheduled and actual times
- Generate statistics on train punctuality
- Visualize delay patterns

## Setup

### Prerequisites
- Python 3.8+
- `uv` for dependency management

### Installation
1. Clone the repository
2. Install dependencies:
   ```bash
   uv sync
   ```

## Usage

```bash
# Fetch and analyze train data
python -m ter_hdf fetch

# Generate statistics
python -m ter_hdf stats
```

## Configuration

Create a `.env` file with your SNCF API credentials:

```env
SNCF_API_KEY=your_api_key_here
```

## License

MIT
