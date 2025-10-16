from fastapi import APIRouter, Depends, HTTPException

from app.db.schema import SessionLocal
from app.models.exception import (
    CreateExceptionsRequest,
    CreateExceptionsResponse,
    ResolveExceptionsRequest,
    ResolveExceptionsResponse,
)
from app.services.exception_service import ExceptionService

router = APIRouter()


def get_exception_service() -> ExceptionService:
    return ExceptionService(session=SessionLocal())


@router.post("/create", response_model=CreateExceptionsResponse)
def create_exceptions(
    request: CreateExceptionsRequest,
    service: ExceptionService = Depends(get_exception_service),
):
    """
    Create one or more exception records.

    This endpoint persists exception records for validation failures,
    transformation errors, or data quality issues. These exceptions
    can be tracked, reported, and resolved later.

    Args:
        request: CreateExceptionsRequest containing list of exception records

    Returns:
        CreateExceptionsResponse with count and IDs of created exceptions

    Raises:
        HTTPException: If creation fails
    """
    try:
        exceptions_created, exception_ids = service.create_exceptions(
            request.exceptions
        )

        return CreateExceptionsResponse(
            exceptions_created=exceptions_created, exception_ids=exception_ids
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error creating exception records: {str(e)}"
        )


@router.post("/resolve", response_model=ResolveExceptionsResponse)
def resolve_exceptions(
    request: ResolveExceptionsRequest,
    service: ExceptionService = Depends(get_exception_service),
):
    """
    Resolve all exceptions for a given NHS number.

    This endpoint finds all unresolved exceptions (date_resolved is NULL)
    for the specified NHS number and marks them as resolved with the
    current timestamp. This is useful when data issues have been corrected
    and the exceptions are no longer relevant.

    Args:
        request: ResolveExceptionsRequest containing NHS number

    Returns:
        ResolveExceptionsResponse with count resolved and resolution timestamp

    Raises:
        HTTPException: If no unresolved exceptions found or resolution fails
    """
    try:
        exceptions_resolved, resolution_date = service.resolve_exceptions(
            request.nhs_number
        )

        return ResolveExceptionsResponse(
            nhs_number=request.nhs_number,
            exceptions_resolved=exceptions_resolved,
            resolution_date=resolution_date,
        )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error resolving exceptions: {str(e)}"
        )
