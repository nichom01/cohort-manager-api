from datetime import datetime

from pydantic import BaseModel


class ProcessFileRequest(BaseModel):
    """Request to process a file through the entire pipeline."""

    file_path: str
    file_type: str  # "csv" or "parquet"


class FileStatusResponse(BaseModel):
    """Response with file processing status."""

    file_id: int
    filename: str
    current_stage: str
    is_complete: bool
    has_errors: bool
    total_records: int
    records_passed: int
    records_failed: int
    cohort_loaded: bool
    demographics_loaded: bool
    participant_management_loaded: bool
    validation_complete: bool
    transformation_complete: bool
    distribution_loaded: bool
    started_at: datetime
    completed_at: datetime | None
    last_updated: datetime


class RecordStatusResponse(BaseModel):
    """Response with individual record processing status."""

    file_id: int
    nhs_number: int
    cohort_update_id: int
    current_stage: str
    is_complete: bool
    demographics_loaded: bool
    participant_management_loaded: bool
    validation_passed: bool
    transformation_applied: bool
    distributed: bool
    has_validation_errors: bool
    has_transformation_errors: bool
    exception_count: int
    created_at: datetime
    updated_at: datetime


class ProcessFileResponse(BaseModel):
    """Response from processing a file."""

    file_id: int
    filename: str
    total_records: int
    records_processed: int
    records_passed: int
    records_failed: int
    stages_completed: list[str]
    current_stage: str
    is_complete: bool
    has_errors: bool
