from fastapi import APIRouter, Depends, HTTPException

from app.db.schema import SessionLocal
from app.models.orchestration import (
    FileStatusResponse,
    ProcessFileRequest,
    ProcessFileResponse,
    RecordStatusResponse,
)
from app.services.orchestration_service import OrchestrationService

router = APIRouter()


def get_orchestration_service() -> OrchestrationService:
    return OrchestrationService(session=SessionLocal())


@router.post("/process-file", response_model=ProcessFileResponse)
def process_file(
    request: ProcessFileRequest,
    service: OrchestrationService = Depends(get_orchestration_service),
):
    """
    Process a file through the entire pipeline.

    This endpoint orchestrates the complete workflow:
    1. Load cohort
    2. Load demographics
    3. Load participant management
    4. Validation
    5. Create exceptions on failures
    6. Transformation
    7. Load results to distribution

    Progress is tracked at both file and record level.

    Args:
        request: ProcessFileRequest with file path and type

    Returns:
        ProcessFileResponse with results and status

    Raises:
        HTTPException: If processing fails
    """
    try:
        result = service.process_file(request.file_path, request.file_type)
        return ProcessFileResponse(**result)

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error processing file: {str(e)}"
        )


@router.get("/file-status/{file_id}", response_model=FileStatusResponse)
def get_file_status(
    file_id: int,
    service: OrchestrationService = Depends(get_orchestration_service),
):
    """
    Get processing status for a file.

    This endpoint returns the current status of a file including:
    - Which stages have been completed
    - Current stage in progress
    - Record counts (total, passed, failed)
    - Timestamps

    Args:
        file_id: ID of the file

    Returns:
        FileStatusResponse with complete status

    Raises:
        HTTPException: If file not found
    """
    file_status = service.get_file_status(file_id)

    if not file_status:
        raise HTTPException(status_code=404, detail=f"File {file_id} not found")

    return FileStatusResponse(
        file_id=file_status.file_id,
        filename=file_status.filename,
        current_stage=file_status.current_stage,
        is_complete=file_status.is_complete,
        has_errors=file_status.has_errors,
        total_records=file_status.total_records,
        records_passed=file_status.records_passed,
        records_failed=file_status.records_failed,
        cohort_loaded=file_status.cohort_loaded,
        demographics_loaded=file_status.demographics_loaded,
        participant_management_loaded=file_status.participant_management_loaded,
        validation_complete=file_status.validation_complete,
        transformation_complete=file_status.transformation_complete,
        distribution_loaded=file_status.distribution_loaded,
        started_at=file_status.started_at,
        completed_at=file_status.completed_at,
        last_updated=file_status.last_updated,
    )


@router.get("/record-status/{file_id}/{nhs_number}", response_model=RecordStatusResponse)
def get_record_status(
    file_id: int,
    nhs_number: int,
    service: OrchestrationService = Depends(get_orchestration_service),
):
    """
    Get processing status for a specific record.

    This endpoint returns the detailed status of an individual NHS number
    record within a file, including:
    - Which stages have been completed for this record
    - Current stage in progress
    - Validation and transformation status
    - Error flags and exception counts

    Args:
        file_id: ID of the file
        nhs_number: NHS number of the record

    Returns:
        RecordStatusResponse with complete record status

    Raises:
        HTTPException: If record not found
    """
    record_status = service.get_record_status(file_id, nhs_number)

    if not record_status:
        raise HTTPException(
            status_code=404,
            detail=f"Record {nhs_number} in file {file_id} not found",
        )

    return RecordStatusResponse(
        file_id=record_status.file_id,
        nhs_number=record_status.nhs_number,
        cohort_update_id=record_status.cohort_update_id,
        current_stage=record_status.current_stage,
        is_complete=record_status.is_complete,
        demographics_loaded=record_status.demographics_loaded,
        participant_management_loaded=record_status.participant_management_loaded,
        validation_passed=record_status.validation_passed,
        transformation_applied=record_status.transformation_applied,
        distributed=record_status.distributed,
        has_validation_errors=record_status.has_validation_errors,
        has_transformation_errors=record_status.has_transformation_errors,
        exception_count=record_status.exception_count,
        created_at=record_status.created_at,
        updated_at=record_status.updated_at,
    )
