from datetime import UTC, datetime

from sqlalchemy.orm import Session

from app.db.schema import CohortUpdate, ParticipantDemographic


class DemographicService:
    def __init__(self, session: Session):
        self.session = session

    def _update_demographic_fields(
        self, demographic: ParticipantDemographic, cohort_record: CohortUpdate
    ) -> None:
        """Update demographic fields from cohort record."""
        demographic.superseded_by_nhs_number = cohort_record.superseded_by_nhs_number
        demographic.primary_care_provider = cohort_record.primary_care_provider
        demographic.primary_care_provider_from_dt = cohort_record.primary_care_effective_from_date
        demographic.current_posting = cohort_record.current_posting
        demographic.current_posting_from_dt = cohort_record.current_posting_effective_from_date
        demographic.name_prefix = cohort_record.name_prefix
        demographic.given_name = cohort_record.given_name
        demographic.other_given_name = cohort_record.other_given_name
        demographic.family_name = cohort_record.family_name
        demographic.previous_family_name = cohort_record.previous_family_name
        demographic.date_of_birth = cohort_record.date_of_birth
        demographic.gender = cohort_record.gender
        demographic.address_line_1 = cohort_record.address_line_1
        demographic.address_line_2 = cohort_record.address_line_2
        demographic.address_line_3 = cohort_record.address_line_3
        demographic.address_line_4 = cohort_record.address_line_4
        demographic.address_line_5 = cohort_record.address_line_5
        demographic.post_code = cohort_record.postcode
        demographic.paf_key = cohort_record.paf_key
        demographic.usual_address_from_dt = cohort_record.address_effective_from_date
        demographic.date_of_death = cohort_record.date_of_death
        demographic.death_status = cohort_record.death_status
        demographic.telephone_number_home = cohort_record.home_telephone_number
        demographic.telephone_number_home_from_dt = cohort_record.home_telephone_effective_from_date
        demographic.telephone_number_mob = cohort_record.mobile_telephone_number
        demographic.telephone_number_mob_from_dt = cohort_record.mobile_telephone_effective_from_date
        demographic.email_address_home = cohort_record.email_address
        demographic.email_address_home_from_dt = cohort_record.email_address_effective_from_date
        demographic.preferred_language = cohort_record.preferred_language
        demographic.interpreter_required = 1 if cohort_record.is_interpreter_required else 0
        demographic.invalid_flag = 1 if cohort_record.invalid_flag else 0
        demographic.record_update_datetime = datetime.now(UTC)

    def _upsert_demographic(self, cohort_record: CohortUpdate) -> bool:
        """
        Insert or update a demographic record based on NHS number.

        Returns:
            True if record was inserted (new), False if updated (existing)
        """
        # Check if demographic already exists for this NHS number
        existing = (
            self.session.query(ParticipantDemographic)
            .filter(ParticipantDemographic.nhs_number == cohort_record.nhs_number)
            .first()
        )

        if existing:
            # Update existing record
            self._update_demographic_fields(existing, cohort_record)
            return False
        else:
            # Create new record
            new_demographic = ParticipantDemographic(nhs_number=cohort_record.nhs_number)
            self._update_demographic_fields(new_demographic, cohort_record)
            self.session.add(new_demographic)
            return True

    def load_demographics_by_file_id(self, file_id: int) -> dict:
        """
        Load demographics from all cohort records with the specified file_id.

        For each record, if a demographic with the same NHS number exists,
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

            # Upsert all demographics
            inserted_count = 0
            updated_count = 0

            for record in cohort_records:
                was_inserted = self._upsert_demographic(record)
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
            raise ValueError(f"Failed to load demographics: {str(e)}")

    def load_demographic_by_record_id(self, cohort_update_id: int) -> dict:
        """
        Load demographic from a single cohort record by its id.

        If a demographic with the same NHS number exists, it will be updated;
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

            # Upsert demographic
            was_inserted = self._upsert_demographic(cohort_record)
            self.session.commit()

            return {
                "records_loaded": 1,
                "action": "inserted" if was_inserted else "updated",
            }

        except Exception as e:
            self.session.rollback()
            raise ValueError(f"Failed to load demographic: {str(e)}")
