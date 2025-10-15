# Feature 001: Load Data - Implementation Complete

## Overview
Created an endpoint to load CSV or Parquet files into the `cohort_update` database table.

## Endpoint
**POST** `/api/v1/cohort/load-file`

### Request Body
```json
{
  "file_path": "/path/to/file.csv",
  "file_type": "csv"  // or "parquet"
}
```

### Response
```json
{
  "file_id": 1,
  "records_loaded": 100,
  "message": "Successfully loaded 100 records"
}
```

## Implementation Details

### Files Created/Modified

1. **app/db/schema.py** - Added `CohortUpdate` ORM model with:
   - System columns: `id` (PK), `create_date`, `file_id`
   - All 34 data fields from specification (record_type, boolean fields, number fields, string fields)

2. **app/models/cohort.py** - Created Pydantic models:
   - `LoadFileRequest` - for request validation
   - `LoadFileResponse` - for response structure

3. **app/services/cohort_service.py** - Service layer with:
   - File loading logic for CSV and Parquet
   - Sequential file ID generation
   - File existence validation
   - Bulk insert for performance

4. **app/api/v1/cohort.py** - API endpoint with:
   - POST `/load-file` endpoint
   - Error handling for file not found, unsupported file types, etc.

5. **app/main.py** - Registered cohort router

6. **tests/api/v1/test_cohort.py** - Comprehensive tests:
   - CSV file loading
   - Parquet file loading
   - File not found error handling
   - Unsupported file type error handling
   - Sequential file ID validation

### Dependencies Added
- `pandas>=2.3.3` - for CSV/Parquet file reading
- `pyarrow>=21.0.0` - for Parquet file support

## Features

✅ Accepts file path (on server) and file type
✅ Supports CSV and Parquet files
✅ Adds 3 system columns: `id` (PK), `create_date`, `file_id`
✅ Sequential file ID generation
✅ Stores all 34 fields as specified:
  - record_type (ADD/AMENDED/DEL)
  - 3 boolean fields
  - 6 number fields
  - 25 string fields
✅ Returns records loaded count
✅ Full test coverage (5 tests, all passing)
✅ Fixed database setup fixture for existing user tests

## Usage Example

```bash
# Create a CSV file with cohort data
# Then call the endpoint:

curl -X POST "http://localhost:8000/api/v1/cohort/load-file" \
  -H "Content-Type: application/json" \
  -d '{
    "file_path": "/path/to/cohort_data.csv",
    "file_type": "csv"
  }'
```

## Notes
- Data is stored "as-is" without validation (as requested)
- File IDs are sequential starting from 1
- create_date is automatically set to current UTC time
- All data fields are nullable to handle missing values gracefully
