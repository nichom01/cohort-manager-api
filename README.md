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

### User Management

Here's a set of simple curl examples for user management:

### 1️⃣ Create a User

```bash
curl -X POST "http://localhost:8000/api/v1/users" \
     -H "Content-Type: application/json" \
     -d '{"name": "Ada Lovelace"}'
```

### 2️⃣ Get All Users

```bash
curl -X GET "http://localhost:8000/api/v1/users"
```

### 3️⃣ Get a User by ID

(Replace 1 with the actual ID from the create response)

```bash
curl -X GET "http://localhost:8000/api/v1/users/1"
```

### 4️⃣ Update a User

```bash
curl -X PUT "http://localhost:8000/api/v1/users/1" \
     -H "Content-Type: application/json" \
     -d '{"name": "Grace Hopper"}'
```

### 5️⃣ Delete a User

```bash
curl -X DELETE "http://localhost:8000/api/v1/users/1"
```
