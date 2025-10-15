from fastapi import APIRouter, Depends, HTTPException

from app.db.schema import SessionLocal
from app.models.validation import (
    ValidateBatchRequest,
    ValidateBatchResponse,
    ValidateParticipantRequest,
    ValidateParticipantResponse,
    ValidationResultModel,
)
from app.services.validation_service import ValidationService

router = APIRouter()


def get_validation_service() -> ValidationService:
    return ValidationService(session=SessionLocal())


@router.post("/validate-participant", response_model=ValidateParticipantResponse)
def validate_participant(
    request: ValidateParticipantRequest,
    service: ValidationService = Depends(get_validation_service),
):
    """
    Validate a single participant against all validation rules.

    This endpoint runs all validation rules for the specified participant
    in parallel and returns the results. The validation is idempotent -
    running it multiple times with the same NHS number will produce the
    same results.

    Args:
        request: ValidateParticipantRequest containing NHS number

    Returns:
        ValidateParticipantResponse with validation results and summary statistics

    Raises:
        HTTPException: If participant not found or validation fails
    """
    try:
        results = service.validate_participant(request.nhs_number)

        # Convert results to response models
        validation_results = [
            ValidationResultModel(
                rule_name=r.rule_name,
                passed=r.passed,
                message=r.message,
                severity=r.severity,
            )
            for r in results
        ]

        # Calculate summary statistics
        passed_rules = sum(1 for r in results if r.passed)
        failed_rules = sum(1 for r in results if not r.passed)
        has_errors = any(r.severity == "ERROR" and not r.passed for r in results)
        has_warnings = any(r.severity == "WARNING" and not r.passed for r in results)

        return ValidateParticipantResponse(
            nhs_number=request.nhs_number,
            validation_results=validation_results,
            total_rules=len(results),
            passed_rules=passed_rules,
            failed_rules=failed_rules,
            has_errors=has_errors,
            has_warnings=has_warnings,
        )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error validating participant: {str(e)}"
        )


@router.post("/validate-batch", response_model=ValidateBatchResponse)
def validate_batch(
    request: ValidateBatchRequest,
    service: ValidationService = Depends(get_validation_service),
):
    """
    Validate multiple participants against all validation rules.

    This endpoint runs all validation rules for each specified participant
    in parallel and returns the results. The validation is idempotent -
    running it multiple times with the same NHS numbers will produce the
    same results.

    Args:
        request: ValidateBatchRequest containing list of NHS numbers

    Returns:
        ValidateBatchResponse with validation results for all participants

    Raises:
        HTTPException: If validation fails
    """
    try:
        batch_results = service.validate_batch(request.nhs_numbers)

        # Convert results to response format
        participant_responses = {}
        participants_with_errors = 0
        participants_with_warnings = 0

        for nhs_number, results in batch_results.items():
            validation_results = [
                ValidationResultModel(
                    rule_name=r.rule_name,
                    passed=r.passed,
                    message=r.message,
                    severity=r.severity,
                )
                for r in results
            ]

            passed_rules = sum(1 for r in results if r.passed)
            failed_rules = sum(1 for r in results if not r.passed)
            has_errors = any(r.severity == "ERROR" and not r.passed for r in results)
            has_warnings = any(r.severity == "WARNING" and not r.passed for r in results)

            if has_errors:
                participants_with_errors += 1
            if has_warnings:
                participants_with_warnings += 1

            participant_responses[nhs_number] = ValidateParticipantResponse(
                nhs_number=nhs_number,
                validation_results=validation_results,
                total_rules=len(results),
                passed_rules=passed_rules,
                failed_rules=failed_rules,
                has_errors=has_errors,
                has_warnings=has_warnings,
            )

        return ValidateBatchResponse(
            results=participant_responses,
            total_participants=len(request.nhs_numbers),
            participants_with_errors=participants_with_errors,
            participants_with_warnings=participants_with_warnings,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error validating batch: {str(e)}")
