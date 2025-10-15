from fastapi import APIRouter, Depends, HTTPException

from app.db.schema import SessionLocal
from app.models.transformation import (
    ParticipantRecords,
    TransformBatchRequest,
    TransformBatchResponse,
    TransformBatchSummary,
    TransformParticipantRequest,
    TransformParticipantResponse,
    TransformationResultModel,
    TransformationSummary,
)
from app.services.transformation_service import TransformationService

router = APIRouter()


def get_transformation_service() -> TransformationService:
    return TransformationService(session=SessionLocal())


@router.post("/transform-participant", response_model=TransformParticipantResponse)
def transform_participant(
    request: TransformParticipantRequest,
    service: TransformationService = Depends(get_transformation_service),
):
    """
    Apply transformation rules to a single participant's records.

    This endpoint applies both conditional transformation rules (Type 1) and
    character replacement rules (Type 2) to the participant's demographic and
    management records.

    This endpoint is idempotent and does not commit changes to the database.
    It returns both inbound (original) and outbound (transformed) records.

    Conditional rules are executed in parallel for performance, while replacement
    rules are grouped and executed together.

    Args:
        request: TransformParticipantRequest containing NHS number

    Returns:
        TransformParticipantResponse with inbound/outbound records,
        transformation results, and summary

    Raises:
        HTTPException: If participant not found or transformation fails
    """
    try:
        result = service.transform_participant(request.nhs_number)

        # Convert results to response models
        conditional_results = [
            TransformationResultModel(
                rule_name=r.rule_name,
                applied=r.applied,
                changes=r.changes,
                message=r.message,
            )
            for r in result["conditional_results"]
        ]

        replacement_results = [
            TransformationResultModel(
                rule_name=r.rule_name,
                applied=r.applied,
                changes=r.changes,
                message=r.message,
            )
            for r in result["replacement_results"]
        ]

        summary = TransformationSummary(**result["summary"])

        # Create inbound and outbound record containers
        inbound = ParticipantRecords(**result["inbound"])
        outbound = ParticipantRecords(**result["outbound"])

        return TransformParticipantResponse(
            nhs_number=result["nhs_number"],
            inbound=inbound,
            outbound=outbound,
            conditional_results=conditional_results,
            replacement_results=replacement_results,
            summary=summary,
        )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error transforming participant: {str(e)}"
        )


@router.post("/transform-batch", response_model=TransformBatchResponse)
def transform_batch(
    request: TransformBatchRequest,
    service: TransformationService = Depends(get_transformation_service),
):
    """
    Apply transformation rules to multiple participants.

    This endpoint is idempotent and does not commit changes to the database.
    Each participant transformation returns both inbound and outbound records.

    Args:
        request: TransformBatchRequest containing list of NHS numbers

    Returns:
        TransformBatchResponse with results for all participants

    Raises:
        HTTPException: If batch transformation fails
    """
    try:
        batch_result = service.transform_batch(request.nhs_numbers)

        # Convert results to response format
        participant_responses = {}

        for nhs_number, result in batch_result["results"].items():
            if "error" in result:
                # Participant not found or other error
                participant_responses[nhs_number] = result
            else:
                # Successful transformation
                conditional_results = [
                    TransformationResultModel(
                        rule_name=r.rule_name,
                        applied=r.applied,
                        changes=r.changes,
                        message=r.message,
                    )
                    for r in result["conditional_results"]
                ]

                replacement_results = [
                    TransformationResultModel(
                        rule_name=r.rule_name,
                        applied=r.applied,
                        changes=r.changes,
                        message=r.message,
                    )
                    for r in result["replacement_results"]
                ]

                summary = TransformationSummary(**result["summary"])

                # Create inbound and outbound record containers
                inbound = ParticipantRecords(**result["inbound"])
                outbound = ParticipantRecords(**result["outbound"])

                participant_responses[nhs_number] = TransformParticipantResponse(
                    nhs_number=result["nhs_number"],
                    inbound=inbound,
                    outbound=outbound,
                    conditional_results=conditional_results,
                    replacement_results=replacement_results,
                    summary=summary,
                )

        batch_summary = TransformBatchSummary(**batch_result["summary"])

        return TransformBatchResponse(
            results=participant_responses, summary=batch_summary
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error transforming batch: {str(e)}"
        )
