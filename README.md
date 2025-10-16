# FastAPI User Management API

A FastAPI-based REST API with SQLAlchemy ORM and SQLite database.

## Prerequisites

Install [uv](https://docs.astral.sh/uv/) package manager:

**macOS/Linux:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Windows:**
```bash
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

## Running Locally

Start the development server:

```bash
uv run uvicorn app.main:app --reload
```

The API will be available at `http://localhost:8000`

Interactive API documentation: `http://localhost:8000/docs`

### Stopping the Server and Resetting the Database

Stop the development server and remove the database:

```bash
# Kill all uvicorn processes
lsof -ti:8000 | xargs kill -9 2>/dev/null

# Remove the database file
rm -f test.db
```

Or combine both commands:

```bash
lsof -ti:8000 | xargs kill -9 2>/dev/null && rm -f test.db && echo "Server stopped and database removed"
```

**Note:** This will completely remove all data. You'll need to re-seed the database with GP practices after restarting the server.

### Database Connection

The application uses SQLite as the database. Once the server starts:
- Database file: `test.db` (created automatically in the project root)
- No credentials required (file-based database)
- Connection string: `sqlite:///./test.db`

**Connect with Azure Data Studio:**
1. Install the SQLite extension
2. Create new connection with path: `/path/to/project/test.db`
3. No username/password needed

**Connect with SQLite CLI:**
```bash
sqlite3 test.db
.tables                    # View all tables
SELECT * FROM users;       # Query users table
SELECT * FROM cohort_update LIMIT 10;  # Query cohort data
.exit                      # Exit
```

### Seeding Test Data

The application requires GP practice reference data for validation. Seed the database with test GP practices:

```bash
uv run python -m scripts.seed_gp_practices
```

This populates the `gp_practice` table with 35 GP practice codes that match the test data, including:
- GP001, GP002 (orchestration tests)
- GP0001-GP0020 (CSV test file)
- A12345, B23456, C34567 (validation tests)
- And additional test codes

**Note:** This is required before running the orchestration pipeline with test data, as validation checks ensure GP practices exist.

## Running Tests

Run all tests:

```bash
uv run pytest
```

Run specific test file:

```bash
uv run pytest tests/api/v1/test_user.py
```

Run with verbose output:

```bash
uv run pytest -v
```

## Running with Docker

Build and run with Docker Compose:

```bash
docker-compose up --build
```

Or build and run with Docker directly:

```bash
docker build -t fastapi-app .
docker run -p 8000:80 fastapi-app
```

The API will be available at `http://localhost:8000`

## API Endpoints

### Cohort Data Loading

Load CSV or Parquet files containing cohort data into the database.

**Endpoint:** `POST /api/v1/cohort/load-file`

**Request Body:**
```json
{
  "file_path": "/path/to/your/file.csv",
  "file_type": "csv"
}
```

**Example - Load CSV file:**
```bash
curl -X POST "http://localhost:8000/api/v1/cohort/load-file" \
     -H "Content-Type: application/json" \
     -d '{
       "file_path": "./tests/uploads/test_cohort_data_20_records.csv",
       "file_type": "csv"
     }'
```

**Example - Load Parquet file:**
```bash
curl -X POST "http://localhost:8000/api/v1/cohort/load-file" \
     -H "Content-Type: application/json" \
     -d '{
       "file_path": "./tests/uploads/test_cohort_data_20_records.parquet",
       "file_type": "parquet"
     }'
```

**Response:**
```json
{
  "file_id": 1,
  "records_loaded": 150,
  "message": "Successfully loaded 150 records"
}
```

**Note:** The file must exist on the server where the API is running. The endpoint loads the file into the `cohort_update` table with automatically generated:
- `id` - Primary key (auto-increment)
- `create_date` - Timestamp when record was loaded
- `file_id` - Sequential file identifier

### Orchestration - Full Pipeline Processing

Process a file through the complete 7-stage pipeline automatically. This endpoint orchestrates all stages from data loading through distribution.

**Endpoint:** `POST /api/v1/orchestration/process-file`

**Pipeline Stages:**
1. **Cohort Loading** - Load file into cohort_update table
2. **Demographics Loading** - Map data to participant_demographic table
3. **Participant Management Loading** - Map data to participant_management table
4. **Validation** - Run validation rules on all records
5. **Exception Creation** - Create exception records for validation failures
6. **Transformation** - Apply transformation rules (idempotent)
7. **Distribution** - Load validated records to cohort_distribution table

**Request Body:**
```json
{
  "file_path": "/path/to/your/file.csv",
  "file_type": "csv"
}
```

**Example - Process a CSV file:**
```bash
curl -X POST "http://localhost:8000/api/v1/orchestration/process-file" \
     -H "Content-Type: application/json" \
     -d '{
       "file_path": "./tests/uploads/test_cohort_data_20_records.csv",
       "file_type": "csv"
     }'
```

**Response:**
```json
{
  "file_id": 1,
  "filename": "test_cohort_data_20_records.csv",
  "total_records": 20,
  "records_processed": 20,
  "records_passed": 18,
  "records_failed": 2,
  "stages_completed": [
    "cohort",
    "demographics",
    "participant_management",
    "validation",
    "transformation",
    "distribution"
  ],
  "current_stage": "complete",
  "is_complete": true,
  "has_errors": false
}
```

**Query File Processing Status:**
```bash
curl -X GET "http://localhost:8000/api/v1/orchestration/file-status/1"
```

**Query Individual Record Status:**
```bash
curl -X GET "http://localhost:8000/api/v1/orchestration/record-status/1/1234567890"
```

**Status Tracking:**
- **File-level status** - Track overall progress, stage completion, record counts
- **Record-level status** - Track individual NHS number progress through each stage
- **Exception tracking** - Validation failures are automatically logged to exception_management table
- **Distribution output** - Successfully validated records appear in cohort_distribution table



```bash
curl -X DELETE "http://localhost:8000/api/v1/users/1"
```
