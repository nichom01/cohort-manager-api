from fastapi import APIRouter, Depends, HTTPException

from app.db.schema import SessionLocal
from app.models.cohort import LoadFileRequest, LoadFileResponse
from app.services.cohort_service import CohortService

router = APIRouter()


def get_cohort_service() -> CohortService:
    return CohortService(session=SessionLocal())


@router.post("/load-file", response_model=LoadFileResponse)
def load_file(
    request: LoadFileRequest, service: CohortService = Depends(get_cohort_service)
):
    """
    Load a CSV or Parquet file into the cohort_update table.

    Args:
        request: LoadFileRequest containing file_path and file_type

    Returns:
        LoadFileResponse with file_id, records_loaded, and message

    Raises:
        HTTPException: If file not found or file type not supported
    """
    try:
        result = service.load_file(request.file_path, request.file_type)
        return LoadFileResponse(
            file_id=result["file_id"],
            records_loaded=result["records_loaded"],
            message=f"Successfully loaded {result['records_loaded']} records",
        )
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading file: {str(e)}")
