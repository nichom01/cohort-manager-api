from datetime import UTC, datetime

from sqlalchemy.orm import Session

from app.db.schema import CohortUpdate, ParticipantManagement


class ParticipantManagementService:
    def __init__(self, session: Session):
        self.session = session

    def _update_participant_management_fields(
        self, participant: ParticipantManagement, cohort_record: CohortUpdate
    ) -> None:
        """Update participant management fields from cohort record."""
        # Note: screening_id not in cohort_update, using nhs_number as placeholder
        participant.screening_id = cohort_record.nhs_number or 0
        participant.record_type = cohort_record.record_type or "ADD"
        participant.eligibility_flag = 1 if cohort_record.eligibility else 0
        participant.reason_for_removal = cohort_record.reason_for_removal
        # Parse reason_for_removal_effective_from_date if needed
        participant.reason_for_removal_from_dt = None  # Not directly mapped
        participant.business_rule_version = None  # Not in cohort_update
        participant.exception_flag = 0  # Not in cohort_update
        participant.blocked_flag = 0  # Not in cohort_update
        participant.referral_flag = 0  # Not in cohort_update
        participant.next_test_due_date = None  # Not in cohort_update
        participant.next_test_due_date_calc_method = None  # Not in cohort_update
        participant.participant_screening_status = None  # Not in cohort_update
        participant.screening_ceased_reason = None  # Not in cohort_update
        participant.is_higher_risk = None  # Not in cohort_update
        participant.is_higher_risk_active = None  # Not in cohort_update
        participant.higher_risk_next_test_due_date = None  # Not in cohort_update
        participant.higher_risk_referral_reason_id = None  # Not in cohort_update
        participant.date_irradiated = None  # Not in cohort_update
        participant.gene_code_id = None  # Not in cohort_update
        participant.src_system_processed_datetime = None  # Not in cohort_update
        participant.cohort_update_id = cohort_record.id
        participant.record_update_datetime = datetime.now(UTC)

    def _upsert_participant_management(self, cohort_record: CohortUpdate) -> bool:
        """
        Insert or update a participant management record based on NHS number.

        Returns:
            True if record was inserted (new), False if updated (existing)
        """
        # Check if participant management already exists for this NHS number
        existing = (
            self.session.query(ParticipantManagement)
            .filter(ParticipantManagement.nhs_number == cohort_record.nhs_number)
            .first()
        )

        if existing:
            # Update existing record
            self._update_participant_management_fields(existing, cohort_record)
            return False
        else:
            # Create new record
            new_participant = ParticipantManagement(
                nhs_number=cohort_record.nhs_number or 0
            )
            self._update_participant_management_fields(new_participant, cohort_record)
            self.session.add(new_participant)
            return True

    def load_participant_management_by_file_id(self, file_id: int) -> dict:
        """
        Load participant management from all cohort records with the specified file_id.

        For each record, if a participant with the same NHS number exists,
        it will be updated; otherwise a new record will be inserted.

        This is a transactional operation - if any record fails, the entire
        transaction will be rolled back.

        Args:
            file_id: The file_id to filter cohort records

        Returns:
            dict with records_inserted, records_updated, and total records_loaded

        Raises:
            ValueError: If no records found for the file_id or if transaction fails
        """
        try:
            # Get all cohort records for the file_id
            cohort_records = (
                self.session.query(CohortUpdate)
                .filter(CohortUpdate.file_id == file_id)
                .all()
            )

            if not cohort_records:
                raise ValueError(f"No cohort records found for file_id {file_id}")

            # Upsert all participant management records
            inserted_count = 0
            updated_count = 0

            for record in cohort_records:
                was_inserted = self._upsert_participant_management(record)
                if was_inserted:
                    inserted_count += 1
                else:
                    updated_count += 1

            self.session.commit()

            return {
                "records_loaded": len(cohort_records),
                "records_inserted": inserted_count,
                "records_updated": updated_count,
            }

        except Exception as e:
            self.session.rollback()
            raise ValueError(f"Failed to load participant management: {str(e)}")

    def load_participant_management_by_record_id(self, cohort_update_id: int) -> dict:
        """
        Load participant management from a single cohort record by its id.

        If a participant with the same NHS number exists, it will be updated;
        otherwise a new record will be inserted.

        This is a transactional operation - if the record fails, the transaction
        will be rolled back.

        Args:
            cohort_update_id: The id of the cohort_update record

        Returns:
            dict with records_loaded, action (inserted or updated)

        Raises:
            ValueError: If record not found or if transaction fails
        """
        try:
            # Get the cohort record
            cohort_record = (
                self.session.query(CohortUpdate)
                .filter(CohortUpdate.id == cohort_update_id)
                .first()
            )

            if not cohort_record:
                raise ValueError(
                    f"No cohort record found with id {cohort_update_id}"
                )

            # Upsert participant management
            was_inserted = self._upsert_participant_management(cohort_record)
            self.session.commit()

            return {
                "records_loaded": 1,
                "action": "inserted" if was_inserted else "updated",
            }

        except Exception as e:
            self.session.rollback()
            raise ValueError(f"Failed to load participant management: {str(e)}")
