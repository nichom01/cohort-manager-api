from fastapi import APIRouter, Depends, HTTPException

from app.db.schema import SessionLocal
from app.models.participant_management import (
    LoadParticipantManagementByFileRequest,
    LoadParticipantManagementByRecordRequest,
    LoadParticipantManagementResponse,
)
from app.services.participant_management_service import ParticipantManagementService

router = APIRouter()


def get_participant_management_service() -> ParticipantManagementService:
    return ParticipantManagementService(session=SessionLocal())


@router.post("/load-by-file", response_model=LoadParticipantManagementResponse)
def load_participant_management_by_file(
    request: LoadParticipantManagementByFileRequest,
    service: ParticipantManagementService = Depends(
        get_participant_management_service
    ),
):
    """
    Load participant management from all cohort records with the specified file_id.

    For each record, if a participant with the same NHS number exists,
    it will be updated; otherwise a new record will be inserted.

    This is a transactional operation - if any record fails to load,
    the entire transaction will be rolled back.

    Args:
        request: LoadParticipantManagementByFileRequest containing file_id

    Returns:
        LoadParticipantManagementResponse with records_loaded, inserted, and updated counts

    Raises:
        HTTPException: If no records found or transaction fails
    """
    try:
        result = service.load_participant_management_by_file_id(request.file_id)
        return LoadParticipantManagementResponse(
            records_loaded=result["records_loaded"],
            records_inserted=result["records_inserted"],
            records_updated=result["records_updated"],
            message=f"Successfully processed {result['records_loaded']} participant management records from file_id {request.file_id} ({result['records_inserted']} inserted, {result['records_updated']} updated)",
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error loading participant management: {str(e)}"
        )


@router.post("/load-by-record", response_model=LoadParticipantManagementResponse)
def load_participant_management_by_record(
    request: LoadParticipantManagementByRecordRequest,
    service: ParticipantManagementService = Depends(
        get_participant_management_service
    ),
):
    """
    Load participant management from a single cohort record by its id.

    If a participant with the same NHS number exists, it will be updated;
    otherwise a new record will be inserted.

    This is a transactional operation - if the record fails to load,
    the transaction will be rolled back.

    Args:
        request: LoadParticipantManagementByRecordRequest containing cohort_update_id

    Returns:
        LoadParticipantManagementResponse with records_loaded and action

    Raises:
        HTTPException: If record not found or transaction fails
    """
    try:
        result = service.load_participant_management_by_record_id(
            request.cohort_update_id
        )
        return LoadParticipantManagementResponse(
            records_loaded=result["records_loaded"],
            action=result["action"],
            message=f"Successfully {result['action']} participant management record from cohort_update_id {request.cohort_update_id}",
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error loading participant management: {str(e)}"
        )
