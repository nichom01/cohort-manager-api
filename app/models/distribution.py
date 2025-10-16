from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class DistributionRecordCreate(BaseModel):
    """Request model for creating a single distribution record."""

    nhs_number: int
    participant_id: int
    superseded_nhs_number: int | None = None
    primary_care_provider: str | None = None
    primary_care_provider_from_dt: datetime | None = None
    current_posting: str | None = None
    current_posting_from_dt: datetime | None = None
    name_prefix: str | None = None
    given_name: str | None = None
    other_given_name: str | None = None
    family_name: str | None = None
    previous_family_name: str | None = None
    date_of_birth: datetime | None = None
    date_of_death: datetime | None = None
    gender: int
    address_line_1: str | None = None
    address_line_2: str | None = None
    address_line_3: str | None = None
    address_line_4: str | None = None
    address_line_5: str | None = None
    post_code: str | None = None
    usual_address_from_dt: datetime | None = None
    reason_for_removal: str | None = None
    reason_for_removal_from_dt: datetime | None = None
    telephone_number_home: str | None = None
    telephone_number_home_from_dt: datetime | None = None
    telephone_number_mob: str | None = None
    telephone_number_mob_from_dt: datetime | None = None
    email_address_home: str | None = None
    email_address_home_from_dt: datetime | None = None
    preferred_language: str | None = None
    interpreter_required: int


class DistributionRecordResponse(DistributionRecordCreate):
    """Response model for a distribution record including system fields."""

    cohort_distribution_id: int
    is_extracted: int
    request_id: UUID | None
    record_insert_datetime: datetime
    record_update_datetime: datetime | None


class CreateDistributionRequest(BaseModel):
    """Request to create one or more distribution records."""

    records: list[DistributionRecordCreate]


class CreateDistributionResponse(BaseModel):
    """Response from creating distribution records."""

    records_created: int
    distribution_ids: list[int]


class ExtractNewRecordsRequest(BaseModel):
    """Request to extract new unextracted records."""

    limit: int | None = None  # Optional limit on number of records to extract


class ExtractNewRecordsResponse(BaseModel):
    """Response from extracting new records."""

    request_id: UUID
    records_extracted: int
    records: list[DistributionRecordResponse]


class ReplayExtractionRequest(BaseModel):
    """Request to replay a previous extraction by request_id."""

    request_id: UUID


class ReplayExtractionResponse(BaseModel):
    """Response from replaying an extraction."""

    request_id: UUID
    records_found: int
    records: list[DistributionRecordResponse]
