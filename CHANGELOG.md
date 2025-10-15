# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added - 2025-10-15

- **Cohort Data File Loading Feature** (Feature 001-loadData)
  - New `POST /api/v1/cohort/load-file` endpoint to load CSV and Parquet files into the database
  - `CohortUpdate` database table with 34 data fields plus 3 system columns:
    - System columns: `id` (PK), `create_date`, `file_id`
    - Data fields: record_type, 3 boolean fields, 6 number fields, 25 string fields
  - Sequential file ID generation starting from 1
  - Support for both CSV and Parquet file formats
  - Comprehensive test suite with 5 tests covering:
    - CSV file loading
    - Parquet file loading
    - Error handling for missing files
    - Error handling for unsupported file types
    - Sequential file ID validation
  - Dependencies: `pandas>=2.3.3` and `pyarrow>=21.0.0` for file processing
  - Service layer (`CohortService`) for business logic
  - API models (`LoadFileRequest`, `LoadFileResponse`) for request/response validation

### Fixed - 2025-10-15

- Database setup fixture in user tests to ensure proper test isolation
- Updated `datetime.utcnow()` to `datetime.now(UTC)` to address deprecation warning
- Corrected repository URL in CLAUDE.md from `python-ui-base.git` to `cohort-manager-api.git`

### Changed - 2025-10-15

- Registered cohort router in main application with prefix `/api/v1/cohort`
- Updated project dependencies in `pyproject.toml` and `uv.lock`

## [0.1.0] - Initial Release

### Added

- FastAPI-based REST API framework
- SQLAlchemy ORM with SQLite database
- User management endpoints (CRUD operations)
- Environment-based configuration with `python-dotenv`
- Pydantic models for request/response validation
- Comprehensive logging setup
- Docker support with docker-compose
- Test suite with pytest
- uv package manager for dependency management
