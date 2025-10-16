from datetime import datetime

from pydantic import BaseModel


class ExceptionRecordCreate(BaseModel):
    """Request model for creating a single exception record."""

    category: int | None = None
    rule_id: int | None = None
    rule_description: str | None = None
    is_fatal: int | None = None
    nhs_number: str | None = None
    file_name: str | None = None
    error_record: str | None = None
    cohort_name: str | None = None
    screening_name: str | None = None
    exception_date: datetime | None = None
    servicenow_id: str | None = None
    servicenow_created_date: datetime | None = None


class ExceptionRecordResponse(ExceptionRecordCreate):
    """Response model for an exception record including system fields."""

    exception_id: int
    date_created: datetime
    date_resolved: datetime | None
    record_updated_date: datetime | None


class CreateExceptionsRequest(BaseModel):
    """Request to create one or more exception records."""

    exceptions: list[ExceptionRecordCreate]


class CreateExceptionsResponse(BaseModel):
    """Response from creating exception records."""

    exceptions_created: int
    exception_ids: list[int]


class ResolveExceptionsRequest(BaseModel):
    """Request to resolve all exceptions for an NHS number."""

    nhs_number: str


class ResolveExceptionsResponse(BaseModel):
    """Response from resolving exceptions."""

    nhs_number: str
    exceptions_resolved: int
    resolution_date: datetime
