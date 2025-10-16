"""
Distribution service for managing cohort distribution records.

This service handles:
1. Creating distribution records for downstream systems
2. Extracting new unextracted records and marking them as extracted
3. Replaying previous extractions by request_id
"""

import uuid
from datetime import UTC, datetime

from sqlalchemy.orm import Session

from app.db.schema import CohortDistribution
from app.models.distribution import DistributionRecordCreate


class DistributionService:
    """
    Service for managing distribution records.

    Distribution records represent participant data ready to be sent to
    downstream systems. Records are marked as extracted when retrieved,
    and extractions are tracked by request_id for replay capability.
    """

    def __init__(self, session: Session):
        """
        Initialize the distribution service.

        Args:
            session: SQLAlchemy database session
        """
        self.session = session

    def create_distribution_records(
        self, records: list[DistributionRecordCreate]
    ) -> tuple[int, list[int]]:
        """
        Create one or more distribution records.

        Args:
            records: List of distribution records to create

        Returns:
            Tuple of (count of records created, list of distribution IDs)
        """
        created_ids = []

        for record_data in records:
            # Create new distribution record
            distribution_record = CohortDistribution(
                nhs_number=record_data.nhs_number,
                participant_id=record_data.participant_id,
                superseded_nhs_number=record_data.superseded_nhs_number,
                primary_care_provider=record_data.primary_care_provider,
                primary_care_provider_from_dt=record_data.primary_care_provider_from_dt,
                current_posting=record_data.current_posting,
                current_posting_from_dt=record_data.current_posting_from_dt,
                name_prefix=record_data.name_prefix,
                given_name=record_data.given_name,
                other_given_name=record_data.other_given_name,
                family_name=record_data.family_name,
                previous_family_name=record_data.previous_family_name,
                date_of_birth=record_data.date_of_birth,
                date_of_death=record_data.date_of_death,
                gender=record_data.gender,
                address_line_1=record_data.address_line_1,
                address_line_2=record_data.address_line_2,
                address_line_3=record_data.address_line_3,
                address_line_4=record_data.address_line_4,
                address_line_5=record_data.address_line_5,
                post_code=record_data.post_code,
                usual_address_from_dt=record_data.usual_address_from_dt,
                reason_for_removal=record_data.reason_for_removal,
                reason_for_removal_from_dt=record_data.reason_for_removal_from_dt,
                telephone_number_home=record_data.telephone_number_home,
                telephone_number_home_from_dt=record_data.telephone_number_home_from_dt,
                telephone_number_mob=record_data.telephone_number_mob,
                telephone_number_mob_from_dt=record_data.telephone_number_mob_from_dt,
                email_address_home=record_data.email_address_home,
                email_address_home_from_dt=record_data.email_address_home_from_dt,
                preferred_language=record_data.preferred_language,
                interpreter_required=record_data.interpreter_required,
                is_extracted=0,  # New records are not extracted yet
                request_id=None,  # Will be set when extracted
                record_insert_datetime=datetime.now(UTC),
            )

            self.session.add(distribution_record)
            self.session.flush()  # Flush to get the ID
            created_ids.append(distribution_record.cohort_distribution_id)

        self.session.commit()
        return len(created_ids), created_ids

    def extract_new_records(
        self, limit: int | None = None
    ) -> tuple[uuid.UUID, list[CohortDistribution]]:
        """
        Extract new unextracted records and mark them as extracted.

        This method:
        1. Finds all records where is_extracted = 0
        2. Generates a new request_id
        3. Updates records to set is_extracted = 1 and request_id
        4. Returns the records

        Args:
            limit: Optional limit on number of records to extract

        Returns:
            Tuple of (request_id, list of extracted distribution records)
        """
        # Generate unique request ID for this extraction
        request_id = uuid.uuid4()

        # Query for unextracted records
        query = (
            self.session.query(CohortDistribution)
            .filter(CohortDistribution.is_extracted == 0)
            .order_by(CohortDistribution.cohort_distribution_id)
        )

        if limit:
            query = query.limit(limit)

        records = query.all()

        # Mark records as extracted with this request_id
        for record in records:
            record.is_extracted = 1
            record.request_id = str(request_id)
            record.record_update_datetime = datetime.now(UTC)

        self.session.commit()

        return request_id, records

    def replay_extraction(self, request_id: uuid.UUID) -> list[CohortDistribution]:
        """
        Replay a previous extraction by request_id.

        This method retrieves all records that were extracted in a previous
        extraction identified by the request_id. This allows downstream systems
        to re-retrieve data if needed.

        Args:
            request_id: The UUID of the previous extraction

        Returns:
            List of distribution records from that extraction

        Raises:
            ValueError: If no records found for the request_id
        """
        records = (
            self.session.query(CohortDistribution)
            .filter(CohortDistribution.request_id == str(request_id))
            .order_by(CohortDistribution.cohort_distribution_id)
            .all()
        )

        if not records:
            raise ValueError(f"No records found for request_id {request_id}")

        return records
