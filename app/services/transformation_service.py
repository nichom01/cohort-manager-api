"""
Transformation service for applying transformation rules to participant data.

This service coordinates the execution of transformation rules, handling
conditional rules in parallel and grouping replacement rules for efficiency.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Optional

from sqlalchemy.orm import Session

from app.db.schema import ParticipantDemographic, ParticipantManagement
from app.services.transformation_rules import (
    ALL_CONDITIONAL_RULES,
    ALL_REPLACEMENT_RULES,
    CharacterReplacementRule,
    ConditionalTransformationRule,
    TransformationResult,
)


class TransformationService:
    """
    Service for applying transformation rules to participant data.

    This service applies two types of rules:
    1. Conditional rules: Executed in parallel
    2. Replacement rules: Grouped and executed together
    """

    def __init__(self, session: Session):
        """
        Initialize the transformation service.

        Args:
            session: SQLAlchemy database session
        """
        self.session = session

    def _apply_conditional_rules(
        self,
        demographic: Optional[ParticipantDemographic],
        participant_management: Optional[ParticipantManagement],
        rules: list[ConditionalTransformationRule],
    ) -> list[TransformationResult]:
        """
        Apply conditional transformation rules in parallel.

        Args:
            demographic: Participant demographic record
            participant_management: Participant management record
            rules: List of conditional rules to apply

        Returns:
            List of TransformationResult objects
        """
        results = []

        # Execute conditional rules in parallel
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(rule.apply, demographic, participant_management)
                for rule in rules
            ]

            results = [future.result() for future in futures]

        return results

    def _apply_replacement_rules(
        self,
        demographic: Optional[ParticipantDemographic],
        participant_management: Optional[ParticipantManagement],
        rules: list[CharacterReplacementRule],
    ) -> list[TransformationResult]:
        """
        Apply character replacement rules.

        These rules are grouped and applied sequentially since they may
        have overlapping field operations.

        Args:
            demographic: Participant demographic record
            participant_management: Participant management record
            rules: List of replacement rules to apply

        Returns:
            List of TransformationResult objects
        """
        results = []

        # Execute replacement rules sequentially
        for rule in rules:
            result = rule.apply(demographic, participant_management)
            results.append(result)

        return results

    def transform_participant(
        self,
        nhs_number: int,
        conditional_rules: Optional[list[ConditionalTransformationRule]] = None,
        replacement_rules: Optional[list[CharacterReplacementRule]] = None,
        commit: bool = True,
    ) -> dict:
        """
        Apply transformation rules to a participant's records.

        Args:
            nhs_number: NHS number of the participant
            conditional_rules: Optional list of conditional rules. If None, uses all default rules
            replacement_rules: Optional list of replacement rules. If None, uses all default rules
            commit: Whether to commit changes to the database (default True)

        Returns:
            Dictionary containing transformation results and summary

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

        # Determine which rules to use
        cond_rules = conditional_rules if conditional_rules is not None else ALL_CONDITIONAL_RULES
        repl_rules = replacement_rules if replacement_rules is not None else ALL_REPLACEMENT_RULES

        # Apply conditional rules in parallel
        conditional_results = self._apply_conditional_rules(
            demographic, participant_management, cond_rules
        )

        # Apply replacement rules sequentially
        replacement_results = self._apply_replacement_rules(
            demographic, participant_management, repl_rules
        )

        # Update timestamps
        if demographic:
            demographic.record_update_datetime = datetime.now(UTC)
        if participant_management:
            participant_management.record_update_datetime = datetime.now(UTC)

        # Commit changes if requested
        if commit:
            self.session.commit()

        # Calculate summary statistics
        all_results = conditional_results + replacement_results
        applied_count = sum(1 for r in all_results if r.applied)
        total_changes = sum(len(r.changes) for r in all_results)

        return {
            "nhs_number": nhs_number,
            "conditional_results": conditional_results,
            "replacement_results": replacement_results,
            "summary": {
                "total_rules": len(all_results),
                "rules_applied": applied_count,
                "total_field_changes": total_changes,
            },
        }

    def transform_batch(
        self,
        nhs_numbers: list[int],
        conditional_rules: Optional[list[ConditionalTransformationRule]] = None,
        replacement_rules: Optional[list[CharacterReplacementRule]] = None,
        commit: bool = True,
    ) -> dict:
        """
        Apply transformation rules to multiple participants.

        Args:
            nhs_numbers: List of NHS numbers to transform
            conditional_rules: Optional list of conditional rules
            replacement_rules: Optional list of replacement rules
            commit: Whether to commit changes to the database (default True)

        Returns:
            Dictionary containing results for all participants
        """
        results = {}
        successful = 0
        failed = 0

        for nhs_number in nhs_numbers:
            try:
                result = self.transform_participant(
                    nhs_number,
                    conditional_rules,
                    replacement_rules,
                    commit=False,  # Commit all at once at the end
                )
                results[nhs_number] = result
                successful += 1
            except ValueError as e:
                results[nhs_number] = {
                    "error": str(e),
                    "nhs_number": nhs_number,
                }
                failed += 1

        # Commit all changes if requested
        if commit:
            try:
                self.session.commit()
            except Exception as e:
                self.session.rollback()
                raise ValueError(f"Failed to commit transformations: {str(e)}")

        return {
            "results": results,
            "summary": {
                "total_participants": len(nhs_numbers),
                "successful": successful,
                "failed": failed,
            },
        }
