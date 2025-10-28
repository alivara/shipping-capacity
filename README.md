# Shipping Capacity API

A FastAPI-based service for calculating shipping capacity and analyzing sailing data.

## Overview

This application provides RESTful APIs to:
- Query shipping capacity across routes and time periods
- Analyze sailing schedules and port utilization
- Calculate aggregated capacity metrics (TEU - Twenty-foot Equivalent Units)

## Architecture

### External ETL Pattern

This application follows:

- **Application**: Stateless FastAPI service that queries pre-loaded data
- **ETL**: Standalone scripts for data loading, separate from application lifecycle
- **Database**: PostgreSQL with persistent data between application restarts

## Quick Start
Make sure you have installed Docker on your system, and then you can use `./deploy.sh`.

```bash
# To run the tests
./deploy.sh test

# To run the app, it automatically build and run the steps
./deploy.sh run
```

## Manual Start

### 1. Install Dependencies (For Local Development)

```bash
# Using Poetry (recommended)
poetry install

# Or using pip
pip install -r requirements.txt
```

### 2. Create ENV

Create `.env` from the example:

```bash
cp .env.Example .env
```

Edit `.env` with your credentials:

### 3. Configure Database

Create `database.env` from the example:

```bash
cp database.env.Example database.env
```

Edit `database.env` with your database credentials:

```env
DATABASE_POSTGRESQL_HOST=localhost
DATABASE_POSTGRESQL_USER=your_user
DATABASE_POSTGRESQL_PASSWORD=your_password
DATABASE_POSTGRESQL_DB=shipping_capacity
DATABASE_CSV_FILE_PATH=data/sailing_level_raw.csv
```

### 4. Run Database Migrations

```bash
# Create database schema
alembic upgrade head
```

### 5. Load Data

**Important**: Data is loaded externally, not by the application.

```bash
# Check database status
python scripts/etl_manager.py status

# Load data from CSV
python scripts/etl_manager.py load --csv-path data/sailing_level_raw.csv

# Or refresh (clear + load)
python scripts/etl_manager.py refresh
```

### 6. Start Application

```bash
# Development mode
uvicorn app.main:app --reload

# Or using the provided script
./run_app.sh
```

The API will be available at:
- **API**: http://localhost:80
- **Docs**: http://localhost:80/docs (Swagger UI)
- **ReDoc**: http://localhost:80/redoc

## Data Management

### ETL Commands

```bash
# View status
python scripts/etl_manager.py status

# Load data
python scripts/etl_manager.py load

# Clear data
python scripts/etl_manager.py clear

# Refresh (clear + load)
python scripts/etl_manager.py refresh

# Help
python scripts/etl_manager.py --help
```

## API Endpoints

### Health Check

```bash
GET /health
```

### Application Info

```bash
GET /info
```

### Capacity Queries

```bash
# Get capacity with filters
GET /api/v1/capacity?origin=Shanghai&destination=Rotterdam&start_date=2024-01-01&end_date=2024-12-31
```

Query parameters:
- `origin`: Filter by origin location
- `destination`: Filter by destination location
- `origin_port_code`: Filter by origin port code
- `destination_port_code`: Filter by destination port code
- `start_date`: Start date (YYYY-MM-DD)
- `end_date`: End date (YYYY-MM-DD)
- `aggregation_level`: Group by time period (day, week, month, year)

## Development

### Project Structure

```
shipping-capacity/
├── app/
│   ├── api/              # API routes
│   ├── capacity/         # Business logic for capacity calculations
│   ├── database/         # Database models and utilities
│   │   └── utils/        # ETL pipeline, data cleaning
│   ├── migrations/       # Alembic migrations
│   ├── security/         # Security middleware
│   ├── tests/            # Test suite
│   ├── config.py         # Application configuration
│   └── main.py           # FastAPI application
├── scripts/              # Standalone ETL scripts
│   ├── etl_manager.py    # CLI for data management
│   └── README.md         # ETL documentation
├── data/                 # CSV data files (not in repo)
├── pyproject.toml        # Poetry dependencies
└── README.md             # This file
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app

# Run specific test file
pytest app/tests/test_capacity_calculation.py
```

### Environment Variables

Set these in production:

```bash
# Application
APP_ENV=local
APP_DEBUG=false

# Database
DATABASE_POSTGRESQL_HOST=your-db-host
DATABASE_POSTGRESQL_USER=your-user
DATABASE_POSTGRESQL_PASSWORD=your-password
DATABASE_POSTGRESQL_DB=shipping_capacity
DATABASE_CSV_FILE_PATH=/data/sailing_level_raw.csv
```

## Database Schema

### Sailings Table

Stores sailing-level raw data:

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key (auto-generated) |
| origin | VARCHAR | Origin location name |
| destination | VARCHAR | Destination location name |
| origin_port_code | VARCHAR | Origin port code |
| destination_port_code | VARCHAR | Destination port code |
| service_version_and_roundtrip_identfiers | VARCHAR | Service identifiers |
| origin_service_version_and_master | VARCHAR | Origin service info |
| destination_service_version_and_master | VARCHAR | Destination service info |
| origin_at_utc | TIMESTAMP | Departure time (UTC) |
| offered_capacity_teu | INTEGER | Offered capacity in TEU |
| created_at | TIMESTAMP | Record creation time |
| updated_at | TIMESTAMP | Record update time |

Indexes:
- `origin_port_code` + `destination_port_code` (for route queries)
- `origin_at_utc` (for time-range queries)

### Logs

Application logs include:
- ETL operations (load/clear)
- API requests
- Query performance
- Errors and warnings

Configure log levels in `app/logging_config.py`.

## Troubleshooting

### Application starts but returns empty results

**Cause**: Database is empty (no data loaded).

**Solution**:
```bash
# Check status
python scripts/etl_manager.py status

# Load data if empty
python scripts/etl_manager.py load
```

### "CSV file not found" error

**Cause**: CSV path is incorrect.

**Solution**:
```bash
# Use absolute path
python scripts/etl_manager.py load --csv-path /full/path/to/file.csv

# Or update DATABASE_CSV_FILE_PATH in database.env
```
