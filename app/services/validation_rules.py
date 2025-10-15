"""
Validation rules for participant data.

This module defines individual validation rules that can be executed independently
and in parallel. Each rule is a function that takes participant data and reference
datasets, returning a ValidationResult.
"""

from dataclasses import dataclass
from typing import Optional

from app.db.schema import GpPractice, ParticipantDemographic, ParticipantManagement


@dataclass
class ValidationResult:
    """Result of a validation rule execution."""

    rule_name: str
    passed: bool
    message: str
    severity: str = "ERROR"  # ERROR, WARNING, INFO


class ValidationRules:
    """Collection of validation rules for participant data."""

    @staticmethod
    def validate_primary_care_provider_exists(
        demographic: Optional[ParticipantDemographic],
        participant_management: Optional[ParticipantManagement],
        gp_practices: dict[str, GpPractice],
    ) -> ValidationResult:
        """
        Validate that the primary care provider exists in the GP Practice dataset.

        Args:
            demographic: Participant demographic record
            participant_management: Participant management record (unused in this rule)
            gp_practices: Dictionary of GP practice codes to GpPractice objects

        Returns:
            ValidationResult indicating if the rule passed
        """
        rule_name = "primary_care_provider_exists"

        if not demographic:
            return ValidationResult(
                rule_name=rule_name,
                passed=False,
                message="No demographic record found",
                severity="ERROR",
            )

        if not demographic.primary_care_provider:
            return ValidationResult(
                rule_name=rule_name,
                passed=True,
                message="No primary care provider specified",
                severity="INFO",
            )

        if demographic.primary_care_provider in gp_practices:
            return ValidationResult(
                rule_name=rule_name,
                passed=True,
                message=f"Primary care provider {demographic.primary_care_provider} exists in GP Practice dataset",
                severity="INFO",
            )
        else:
            return ValidationResult(
                rule_name=rule_name,
                passed=False,
                message=f"Primary care provider {demographic.primary_care_provider} not found in GP Practice dataset",
                severity="ERROR",
            )

    @staticmethod
    def validate_nhs_number_present(
        demographic: Optional[ParticipantDemographic],
        participant_management: Optional[ParticipantManagement],
        gp_practices: dict[str, GpPractice],
    ) -> ValidationResult:
        """
        Validate that NHS number is present in both demographic and participant management.

        Args:
            demographic: Participant demographic record
            participant_management: Participant management record
            gp_practices: Dictionary of GP practice codes (unused in this rule)

        Returns:
            ValidationResult indicating if the rule passed
        """
        rule_name = "nhs_number_present"

        if not demographic and not participant_management:
            return ValidationResult(
                rule_name=rule_name,
                passed=False,
                message="No demographic or participant management record found",
                severity="ERROR",
            )

        if demographic and not demographic.nhs_number:
            return ValidationResult(
                rule_name=rule_name,
                passed=False,
                message="NHS number missing from demographic record",
                severity="ERROR",
            )

        if participant_management and not participant_management.nhs_number:
            return ValidationResult(
                rule_name=rule_name,
                passed=False,
                message="NHS number missing from participant management record",
                severity="ERROR",
            )

        return ValidationResult(
            rule_name=rule_name,
            passed=True,
            message="NHS number present in all records",
            severity="INFO",
        )

    @staticmethod
    def validate_nhs_number_consistency(
        demographic: Optional[ParticipantDemographic],
        participant_management: Optional[ParticipantManagement],
        gp_practices: dict[str, GpPractice],
    ) -> ValidationResult:
        """
        Validate that NHS number is consistent between demographic and participant management.

        Args:
            demographic: Participant demographic record
            participant_management: Participant management record
            gp_practices: Dictionary of GP practice codes (unused in this rule)

        Returns:
            ValidationResult indicating if the rule passed
        """
        rule_name = "nhs_number_consistency"

        if not demographic or not participant_management:
            return ValidationResult(
                rule_name=rule_name,
                passed=True,
                message="Skipped - not both records present",
                severity="INFO",
            )

        if demographic.nhs_number == participant_management.nhs_number:
            return ValidationResult(
                rule_name=rule_name,
                passed=True,
                message=f"NHS number {demographic.nhs_number} is consistent across records",
                severity="INFO",
            )
        else:
            return ValidationResult(
                rule_name=rule_name,
                passed=False,
                message=f"NHS number mismatch: demographic={demographic.nhs_number}, management={participant_management.nhs_number}",
                severity="ERROR",
            )

    @staticmethod
    def validate_name_present(
        demographic: Optional[ParticipantDemographic],
        participant_management: Optional[ParticipantManagement],
        gp_practices: dict[str, GpPractice],
    ) -> ValidationResult:
        """
        Validate that participant has given name and family name.

        Args:
            demographic: Participant demographic record
            participant_management: Participant management record (unused in this rule)
            gp_practices: Dictionary of GP practice codes (unused in this rule)

        Returns:
            ValidationResult indicating if the rule passed
        """
        rule_name = "name_present"

        if not demographic:
            return ValidationResult(
                rule_name=rule_name,
                passed=False,
                message="No demographic record found",
                severity="ERROR",
            )

        if not demographic.given_name or not demographic.family_name:
            missing = []
            if not demographic.given_name:
                missing.append("given name")
            if not demographic.family_name:
                missing.append("family name")

            return ValidationResult(
                rule_name=rule_name,
                passed=False,
                message=f"Missing required name fields: {', '.join(missing)}",
                severity="ERROR",
            )

        return ValidationResult(
            rule_name=rule_name,
            passed=True,
            message="Given name and family name are present",
            severity="INFO",
        )

    @staticmethod
    def validate_postcode_format(
        demographic: Optional[ParticipantDemographic],
        participant_management: Optional[ParticipantManagement],
        gp_practices: dict[str, GpPractice],
    ) -> ValidationResult:
        """
        Validate that postcode is present (basic validation).

        Args:
            demographic: Participant demographic record
            participant_management: Participant management record (unused in this rule)
            gp_practices: Dictionary of GP practice codes (unused in this rule)

        Returns:
            ValidationResult indicating if the rule passed
        """
        rule_name = "postcode_present"

        if not demographic:
            return ValidationResult(
                rule_name=rule_name,
                passed=False,
                message="No demographic record found",
                severity="ERROR",
            )

        if not demographic.post_code:
            return ValidationResult(
                rule_name=rule_name,
                passed=False,
                message="Postcode is missing",
                severity="WARNING",
            )

        # Basic check for postcode length (UK postcodes are typically 6-8 chars)
        postcode = demographic.post_code.strip()
        if len(postcode) < 5 or len(postcode) > 8:
            return ValidationResult(
                rule_name=rule_name,
                passed=False,
                message=f"Postcode '{postcode}' has unusual length",
                severity="WARNING",
            )

        return ValidationResult(
            rule_name=rule_name,
            passed=True,
            message=f"Postcode '{postcode}' is present",
            severity="INFO",
        )


# List of all validation rules
ALL_VALIDATION_RULES = [
    ValidationRules.validate_primary_care_provider_exists,
    ValidationRules.validate_nhs_number_present,
    ValidationRules.validate_nhs_number_consistency,
    ValidationRules.validate_name_present,
    ValidationRules.validate_postcode_format,
]
