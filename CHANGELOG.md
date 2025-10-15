# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added - 2025-10-15

- **File Metadata Tracking Feature**
  - New `FileMetadata` database table to track uploaded files
  - Tracks filename, file path, file type, file hash (SHA256), file size, upload timestamp, and records loaded
  - Duplicate file detection using SHA256 hash to prevent reloading the same file
  - Enhanced `LoadFileResponse` to include file metadata (filename, upload_timestamp, file_hash)
  - Updated `CohortService` to create file metadata records on upload
  - Test data files added: `test_cohort_data_20_records.csv` and `test_cohort_data_20_records.parquet` with 20 sample ADD records

- **Participant Management Loading Feature** (Feature 004-loadParticipantManagement)
  - New `ParticipantManagement` database table with comprehensive screening management fields
  - Unique constraint on NHS number to ensure one management record per participant
  - Two new API endpoints:
    - `POST /api/v1/participant-management/load-by-file`: Load participant management from all cohort records with specified file_id
    - `POST /api/v1/participant-management/load-by-record`: Load participant management from single cohort record by ID
  - Upsert functionality: automatically inserts new participants or updates existing ones based on NHS number
  - Traceability: `cohort_update_id` field links each participant record back to its source cohort record
  - Timestamp tracking with `record_insert_datetime` (on creation) and `record_update_datetime` (on updates)
  - Detailed response statistics showing records inserted vs updated
  - Field mapping from cohort data including:
    - Core screening fields (screening_id, eligibility_flag, record_type)
    - Removal information (reason_for_removal, reason_for_removal_from_dt)
    - Status flags (exception_flag, blocked_flag, referral_flag)
    - Test scheduling (next_test_due_date, next_test_due_date_calc_method)
    - Screening status (participant_screening_status, screening_ceased_reason)
    - Higher risk tracking (is_higher_risk, higher_risk_next_test_due_date, higher_risk_referral_reason_id)
    - Additional fields (date_irradiated, gene_code_id, business_rule_version)
  - Full transactional support with automatic rollback on errors
  - New service layer (`ParticipantManagementService`) with upsert logic
  - API models (`LoadParticipantManagementByFileRequest`, `LoadParticipantManagementByRecordRequest`, `LoadParticipantManagementResponse`)

- **Demographic Data Loading Feature** (Features 002-loadDemographics, 003-uniqueDemographic)
  - New `ParticipantDemographic` database table with comprehensive patient demographic fields
  - Unique constraint on NHS number to ensure one demographic record per patient
  - Two new API endpoints:
    - `POST /api/v1/demographic/load-by-file`: Load demographics from all cohort records with specified file_id
    - `POST /api/v1/demographic/load-by-record`: Load demographic from single cohort record by ID
  - Upsert functionality: automatically inserts new demographics or updates existing ones based on NHS number
  - Timestamp tracking with `record_insert_datetime` (on creation) and `record_update_datetime` (on updates)
  - Detailed response statistics showing records inserted vs updated
  - Field mapping from cohort data including:
    - Personal details (names, DOB, gender)
    - Address information (5 address lines, postcode, PAF key)
    - Contact information (home/mobile phone, email)
    - Care provider details (primary care provider, current posting)
    - Language and accessibility (preferred language, interpreter required)
    - Death information (date of death, death status)
  - Full transactional support with automatic rollback on errors
  - New service layer (`DemographicService`) with upsert logic
  - API models (`LoadDemographicsByFileRequest`, `LoadDemographicByRecordRequest`, `LoadDemographicsResponse`)

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
