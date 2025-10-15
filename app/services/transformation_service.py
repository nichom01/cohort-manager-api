"""
Transformation service for applying transformation rules to participant data.

This service coordinates the execution of transformation rules, handling
conditional rules in parallel and grouping replacement rules for efficiency.

The transformation process is idempotent and does not commit changes to the database.
It returns both inbound (original) and outbound (transformed) records for comparison.
"""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
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

    The transformation is idempotent - it does not persist changes to the database.
    """

    def __init__(self, session: Session):
        """
        Initialize the transformation service.

        Args:
            session: SQLAlchemy database session
        """
        self.session = session

    def _create_record_snapshot(
        self, record: Optional[ParticipantDemographic | ParticipantManagement]
    ) -> Optional[dict]:
        """
        Create a serializable snapshot of a database record.

        Args:
            record: SQLAlchemy ORM object

        Returns:
            Dictionary representation of the record, or None if record is None
        """
        if record is None:
            return None

        # Get all column names from the SQLAlchemy model
        snapshot = {}
        for column in record.__table__.columns:
            value = getattr(record, column.name)
            # Convert datetime objects to ISO format strings for serialization
            if isinstance(value, datetime):
                snapshot[column.name] = value.isoformat()
            else:
                snapshot[column.name] = value

        return snapshot

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
    ) -> dict:
        """
        Apply transformation rules to a participant's records.

        This method is idempotent and does not commit changes to the database.
        It returns both the inbound (original) and outbound (transformed) records.

        Args:
            nhs_number: NHS number of the participant
            conditional_rules: Optional list of conditional rules. If None, uses all default rules
            replacement_rules: Optional list of replacement rules. If None, uses all default rules

        Returns:
            Dictionary containing inbound/outbound records, transformation results, and summary

        Raises:
            ValueError: If participant not found
        """
        # Load participant data from database
        demographic_db = (
            self.session.query(ParticipantDemographic)
            .filter(ParticipantDemographic.nhs_number == nhs_number)
            .first()
        )

        participant_management_db = (
            self.session.query(ParticipantManagement)
            .filter(ParticipantManagement.nhs_number == nhs_number)
            .first()
        )

        if not demographic_db and not participant_management_db:
            raise ValueError(f"No participant found with NHS number {nhs_number}")

        # Create snapshots of inbound (original) records
        inbound_demographic = self._create_record_snapshot(demographic_db)
        inbound_management = self._create_record_snapshot(participant_management_db)

        # Create working copies for transformation (detached from session)
        # We need to use make_transient to detach from session but keep as ORM objects
        from sqlalchemy.orm import make_transient

        demographic_copy = None
        if demographic_db:
            self.session.expunge(demographic_db)  # Detach from session
            demographic_copy = demographic_db

        management_copy = None
        if participant_management_db:
            self.session.expunge(participant_management_db)  # Detach from session
            management_copy = participant_management_db

        # Rollback to prevent any accidental commits
        self.session.rollback()

        # Reload fresh copies from database for working with
        demographic_work = None
        if inbound_demographic:
            demographic_work = ParticipantDemographic(**inbound_demographic)

        management_work = None
        if inbound_management:
            management_work = ParticipantManagement(**inbound_management)

        # Determine which rules to use
        cond_rules = conditional_rules if conditional_rules is not None else ALL_CONDITIONAL_RULES
        repl_rules = replacement_rules if replacement_rules is not None else ALL_REPLACEMENT_RULES

        # Apply conditional rules in parallel
        conditional_results = self._apply_conditional_rules(
            demographic_work, management_work, cond_rules
        )

        # Apply replacement rules sequentially
        replacement_results = self._apply_replacement_rules(
            demographic_work, management_work, repl_rules
        )

        # Update timestamps on transformed records
        transformation_time = datetime.now(UTC)
        if demographic_work:
            demographic_work.record_update_datetime = transformation_time
        if management_work:
            management_work.record_update_datetime = transformation_time

        # Create snapshots of outbound (transformed) records
        outbound_demographic = self._create_record_snapshot(demographic_work)
        outbound_management = self._create_record_snapshot(management_work)

        # Calculate summary statistics
        all_results = conditional_results + replacement_results
        applied_count = sum(1 for r in all_results if r.applied)
        total_changes = sum(len(r.changes) for r in all_results)

        return {
            "nhs_number": nhs_number,
            "inbound": {
                "demographic": inbound_demographic,
                "participant_management": inbound_management,
            },
            "outbound": {
                "demographic": outbound_demographic,
                "participant_management": outbound_management,
            },
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
    ) -> dict:
        """
        Apply transformation rules to multiple participants.

        This method is idempotent and does not commit changes to the database.
        Each participant transformation returns inbound and outbound records.

        Args:
            nhs_numbers: List of NHS numbers to transform
            conditional_rules: Optional list of conditional rules
            replacement_rules: Optional list of replacement rules

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
                )
                results[nhs_number] = result
                successful += 1
            except ValueError as e:
                results[nhs_number] = {
                    "error": str(e),
                    "nhs_number": nhs_number,
                }
                failed += 1

        return {
            "results": results,
            "summary": {
                "total_participants": len(nhs_numbers),
                "successful": successful,
                "failed": failed,
            },
        }
