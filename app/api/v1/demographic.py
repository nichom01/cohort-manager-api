from fastapi import APIRouter, Depends, HTTPException

from app.db.schema import SessionLocal
from app.models.demographic import (
    LoadDemographicByRecordRequest,
    LoadDemographicsByFileRequest,
    LoadDemographicsResponse,
)
from app.services.demographic_service import DemographicService

router = APIRouter()


def get_demographic_service() -> DemographicService:
    return DemographicService(session=SessionLocal())


@router.post("/load-by-file", response_model=LoadDemographicsResponse)
def load_demographics_by_file(
    request: LoadDemographicsByFileRequest,
    service: DemographicService = Depends(get_demographic_service),
):
    """
    Load demographics from all cohort records with the specified file_id.

    This is a transactional operation - if any record fails to load,
    the entire transaction will be rolled back.

    Args:
        request: LoadDemographicsByFileRequest containing file_id

    Returns:
        LoadDemographicsResponse with records_loaded count and message

    Raises:
        HTTPException: If no records found or transaction fails
    """
    try:
        result = service.load_demographics_by_file_id(request.file_id)
        return LoadDemographicsResponse(
            records_loaded=result["records_loaded"],
            records_inserted=result["records_inserted"],
            records_updated=result["records_updated"],
            message=f"Successfully processed {result['records_loaded']} demographic records from file_id {request.file_id} ({result['records_inserted']} inserted, {result['records_updated']} updated)",
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error loading demographics: {str(e)}"
        )


@router.post("/load-by-record", response_model=LoadDemographicsResponse)
def load_demographic_by_record(
    request: LoadDemographicByRecordRequest,
    service: DemographicService = Depends(get_demographic_service),
):
    """
    Load demographic from a single cohort record by its id.

    This is a transactional operation - if the record fails to load,
    the transaction will be rolled back.

    Args:
        request: LoadDemographicByRecordRequest containing cohort_update_id

    Returns:
        LoadDemographicsResponse with records_loaded count (always 1) and message

    Raises:
        HTTPException: If record not found or transaction fails
    """
    try:
        result = service.load_demographic_by_record_id(request.cohort_update_id)
        return LoadDemographicsResponse(
            records_loaded=result["records_loaded"],
            action=result["action"],
            message=f"Successfully {result['action']} demographic record from cohort_update_id {request.cohort_update_id}",
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error loading demographic: {str(e)}"
        )
