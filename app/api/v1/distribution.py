from fastapi import APIRouter, Depends, HTTPException

from app.db.schema import SessionLocal
from app.models.distribution import (
    CreateDistributionRequest,
    CreateDistributionResponse,
    DistributionRecordResponse,
    ExtractNewRecordsRequest,
    ExtractNewRecordsResponse,
    ReplayExtractionRequest,
    ReplayExtractionResponse,
)
from app.services.distribution_service import DistributionService

router = APIRouter()


def get_distribution_service() -> DistributionService:
    return DistributionService(session=SessionLocal())


@router.post("/create", response_model=CreateDistributionResponse)
def create_distribution_records(
    request: CreateDistributionRequest,
    service: DistributionService = Depends(get_distribution_service),
):
    """
    Create one or more distribution records.

    This endpoint persists distribution data for single or multiple participant records.
    These records are marked as unextracted (is_extracted=0) and ready to be
    collected by downstream systems.

    Args:
        request: CreateDistributionRequest containing list of distribution records

    Returns:
        CreateDistributionResponse with count and IDs of created records

    Raises:
        HTTPException: If creation fails
    """
    try:
        records_created, distribution_ids = service.create_distribution_records(
            request.records
        )

        return CreateDistributionResponse(
            records_created=records_created, distribution_ids=distribution_ids
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error creating distribution records: {str(e)}"
        )


@router.post("/extract-new", response_model=ExtractNewRecordsResponse)
def extract_new_records(
    request: ExtractNewRecordsRequest,
    service: DistributionService = Depends(get_distribution_service),
):
    """
    Extract new unextracted records from the distribution table.

    This endpoint:
    1. Finds all records where is_extracted = 0
    2. Generates a unique request_id for this extraction
    3. Marks records as extracted (is_extracted = 1)
    4. Records who collected the data and when
    5. Returns the extracted records

    On success, the records are marked with the request_id and extraction timestamp.

    Args:
        request: ExtractNewRecordsRequest with optional limit

    Returns:
        ExtractNewRecordsResponse with request_id and extracted records

    Raises:
        HTTPException: If extraction fails
    """
    try:
        request_id, records = service.extract_new_records(limit=request.limit)

        # Convert ORM objects to response models
        record_responses = [
            DistributionRecordResponse(
                cohort_distribution_id=r.cohort_distribution_id,
                nhs_number=r.nhs_number,
                participant_id=r.participant_id,
                superseded_nhs_number=r.superseded_nhs_number,
                primary_care_provider=r.primary_care_provider,
                primary_care_provider_from_dt=r.primary_care_provider_from_dt,
                current_posting=r.current_posting,
                current_posting_from_dt=r.current_posting_from_dt,
                name_prefix=r.name_prefix,
                given_name=r.given_name,
                other_given_name=r.other_given_name,
                family_name=r.family_name,
                previous_family_name=r.previous_family_name,
                date_of_birth=r.date_of_birth,
                date_of_death=r.date_of_death,
                gender=r.gender,
                address_line_1=r.address_line_1,
                address_line_2=r.address_line_2,
                address_line_3=r.address_line_3,
                address_line_4=r.address_line_4,
                address_line_5=r.address_line_5,
                post_code=r.post_code,
                usual_address_from_dt=r.usual_address_from_dt,
                reason_for_removal=r.reason_for_removal,
                reason_for_removal_from_dt=r.reason_for_removal_from_dt,
                telephone_number_home=r.telephone_number_home,
                telephone_number_home_from_dt=r.telephone_number_home_from_dt,
                telephone_number_mob=r.telephone_number_mob,
                telephone_number_mob_from_dt=r.telephone_number_mob_from_dt,
                email_address_home=r.email_address_home,
                email_address_home_from_dt=r.email_address_home_from_dt,
                preferred_language=r.preferred_language,
                interpreter_required=r.interpreter_required,
                is_extracted=r.is_extracted,
                request_id=r.request_id,
                record_insert_datetime=r.record_insert_datetime,
                record_update_datetime=r.record_update_datetime,
            )
            for r in records
        ]

        return ExtractNewRecordsResponse(
            request_id=request_id,
            records_extracted=len(record_responses),
            records=record_responses,
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error extracting records: {str(e)}"
        )


@router.post("/replay", response_model=ReplayExtractionResponse)
def replay_extraction(
    request: ReplayExtractionRequest,
    service: DistributionService = Depends(get_distribution_service),
):
    """
    Replay a previous extraction by request_id.

    This endpoint retrieves all records that were extracted in a previous
    extraction session identified by the request_id. This allows downstream
    systems to re-retrieve data if needed without creating a new extraction.

    Args:
        request: ReplayExtractionRequest containing the request_id

    Returns:
        ReplayExtractionResponse with the original records

    Raises:
        HTTPException: If request_id not found or replay fails
    """
    try:
        records = service.replay_extraction(request.request_id)

        # Convert ORM objects to response models
        record_responses = [
            DistributionRecordResponse(
                cohort_distribution_id=r.cohort_distribution_id,
                nhs_number=r.nhs_number,
                participant_id=r.participant_id,
                superseded_nhs_number=r.superseded_nhs_number,
                primary_care_provider=r.primary_care_provider,
                primary_care_provider_from_dt=r.primary_care_provider_from_dt,
                current_posting=r.current_posting,
                current_posting_from_dt=r.current_posting_from_dt,
                name_prefix=r.name_prefix,
                given_name=r.given_name,
                other_given_name=r.other_given_name,
                family_name=r.family_name,
                previous_family_name=r.previous_family_name,
                date_of_birth=r.date_of_birth,
                date_of_death=r.date_of_death,
                gender=r.gender,
                address_line_1=r.address_line_1,
                address_line_2=r.address_line_2,
                address_line_3=r.address_line_3,
                address_line_4=r.address_line_4,
                address_line_5=r.address_line_5,
                post_code=r.post_code,
                usual_address_from_dt=r.usual_address_from_dt,
                reason_for_removal=r.reason_for_removal,
                reason_for_removal_from_dt=r.reason_for_removal_from_dt,
                telephone_number_home=r.telephone_number_home,
                telephone_number_home_from_dt=r.telephone_number_home_from_dt,
                telephone_number_mob=r.telephone_number_mob,
                telephone_number_mob_from_dt=r.telephone_number_mob_from_dt,
                email_address_home=r.email_address_home,
                email_address_home_from_dt=r.email_address_home_from_dt,
                preferred_language=r.preferred_language,
                interpreter_required=r.interpreter_required,
                is_extracted=r.is_extracted,
                request_id=r.request_id,
                record_insert_datetime=r.record_insert_datetime,
                record_update_datetime=r.record_update_datetime,
            )
            for r in records
        ]

        return ReplayExtractionResponse(
            request_id=request.request_id,
            records_found=len(record_responses),
            records=record_responses,
        )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error replaying extraction: {str(e)}")
