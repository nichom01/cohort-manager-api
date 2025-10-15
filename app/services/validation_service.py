"""
Validation service for running validation rules on participant data.

This service coordinates the execution of validation rules in parallel and
provides an idempotent interface for validating participant data.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

from sqlalchemy.orm import Session

from app.db.schema import GpPractice, ParticipantDemographic, ParticipantManagement
from app.services.validation_rules import ALL_VALIDATION_RULES, ValidationResult


class ValidationService:
    """
    Service for validating participant data against business rules.

    This service runs validation rules in parallel for efficiency and provides
    an idempotent interface - running validation multiple times with the same
    data will produce the same results.
    """

    def __init__(self, session: Session):
        """
        Initialize the validation service.

        Args:
            session: SQLAlchemy database session
        """
        self.session = session

    def _load_gp_practices(self) -> dict[str, GpPractice]:
        """
        Load all GP practices from the database into a dictionary.

        Returns:
            Dictionary mapping GP practice codes to GpPractice objects
        """
        gp_practices = self.session.query(GpPractice).all()
        return {gp.gp_practice_code: gp for gp in gp_practices}

    def _execute_rule(
        self,
        rule: Callable,
        demographic: Optional[ParticipantDemographic],
        participant_management: Optional[ParticipantManagement],
        gp_practices: dict[str, GpPractice],
    ) -> ValidationResult:
        """
        Execute a single validation rule.

        Args:
            rule: Validation rule function to execute
            demographic: Participant demographic record
            participant_management: Participant management record
            gp_practices: Dictionary of GP practices

        Returns:
            ValidationResult from the rule execution
        """
        try:
            return rule(demographic, participant_management, gp_practices)
        except Exception as e:
            # If a rule raises an exception, return a failed validation result
            return ValidationResult(
                rule_name=rule.__name__,
                passed=False,
                message=f"Rule execution failed: {str(e)}",
                severity="ERROR",
            )

    def validate_participant(
        self,
        nhs_number: int,
        rules: Optional[list[Callable]] = None,
    ) -> list[ValidationResult]:
        """
        Validate a participant against all or specified validation rules.

        This method is idempotent - running it multiple times with the same
        NHS number will produce the same results.

        Args:
            nhs_number: NHS number of the participant to validate
            rules: Optional list of specific rules to run. If None, runs all rules.

        Returns:
            List of ValidationResult objects, one per rule executed

        Raises:
            ValueError: If participant not found
        """
        # Load participant data
        demographic = (
            self.session.query(ParticipantDemographic)
            .filter(ParticipantDemographic.nhs_number == nhs_number)
            .first()
        )

        participant_management = (
            self.session.query(ParticipantManagement)
            .filter(ParticipantManagement.nhs_number == nhs_number)
            .first()
        )

        if not demographic and not participant_management:
            raise ValueError(f"No participant found with NHS number {nhs_number}")

        # Load reference data
        gp_practices = self._load_gp_practices()

        # Determine which rules to run
        rules_to_run = rules if rules is not None else ALL_VALIDATION_RULES

        # Execute rules in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(
                    self._execute_rule,
                    rule,
                    demographic,
                    participant_management,
                    gp_practices,
                )
                for rule in rules_to_run
            ]

            # Collect results as they complete
            results = [future.result() for future in futures]

        return results

    async def validate_participant_async(
        self,
        nhs_number: int,
        rules: Optional[list[Callable]] = None,
    ) -> list[ValidationResult]:
        """
        Asynchronous version of validate_participant.

        Runs validation rules in parallel using asyncio. This method is
        idempotent - running it multiple times with the same NHS number
        will produce the same results.

        Args:
            nhs_number: NHS number of the participant to validate
            rules: Optional list of specific rules to run. If None, runs all rules.

        Returns:
            List of ValidationResult objects, one per rule executed

        Raises:
            ValueError: If participant not found
        """
        # Load participant data
        demographic = (
            self.session.query(ParticipantDemographic)
            .filter(ParticipantDemographic.nhs_number == nhs_number)
            .first()
        )

        participant_management = (
            self.session.query(ParticipantManagement)
            .filter(ParticipantManagement.nhs_number == nhs_number)
            .first()
        )

        if not demographic and not participant_management:
            raise ValueError(f"No participant found with NHS number {nhs_number}")

        # Load reference data
        gp_practices = self._load_gp_practices()

        # Determine which rules to run
        rules_to_run = rules if rules is not None else ALL_VALIDATION_RULES

        # Execute rules in parallel using asyncio
        loop = asyncio.get_event_loop()
        tasks = [
            loop.run_in_executor(
                None,
                self._execute_rule,
                rule,
                demographic,
                participant_management,
                gp_practices,
            )
            for rule in rules_to_run
        ]

        results = await asyncio.gather(*tasks)
        return list(results)

    def validate_batch(
        self,
        nhs_numbers: list[int],
        rules: Optional[list[Callable]] = None,
    ) -> dict[int, list[ValidationResult]]:
        """
        Validate multiple participants in parallel.

        This method is idempotent - running it multiple times with the same
        NHS numbers will produce the same results.

        Args:
            nhs_numbers: List of NHS numbers to validate
            rules: Optional list of specific rules to run. If None, runs all rules.

        Returns:
            Dictionary mapping NHS numbers to their validation results
        """
        results = {}

        with ThreadPoolExecutor() as executor:
            futures = {
                nhs_number: executor.submit(self.validate_participant, nhs_number, rules)
                for nhs_number in nhs_numbers
            }

            for nhs_number, future in futures.items():
                try:
                    results[nhs_number] = future.result()
                except ValueError as e:
                    # Participant not found - record as validation failure
                    results[nhs_number] = [
                        ValidationResult(
                            rule_name="participant_exists",
                            passed=False,
                            message=str(e),
                            severity="ERROR",
                        )
                    ]

        return results
