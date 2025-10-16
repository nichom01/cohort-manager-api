"""
Orchestration service for coordinating the entire cohort processing pipeline.

This service connects all individual services and manages the workflow:
1. Load cohort
2. Load demographics
3. Load participant management
4. Validation
5. Create exceptions on failures
6. Transformation
7. Load results to distribution
"""

from datetime import UTC, datetime

from sqlalchemy.orm import Session

from app.db.schema import (
    CohortDistribution,
    CohortUpdate,
    FileProcessingStatus,
    RecordProcessingStatus,
)
from app.services.cohort_service import CohortService
from app.services.demographic_service import DemographicService
from app.services.distribution_service import DistributionService
from app.services.exception_service import ExceptionService
from app.services.participant_management_service import ParticipantManagementService
from app.services.transformation_service import TransformationService
from app.services.validation_service import ValidationService


class OrchestrationService:
    """
    Service for orchestrating the complete cohort processing pipeline.

    This service coordinates all stages and maintains status tracking.
    """

    def __init__(self, session: Session):
        """
        Initialize the orchestration service.

        Args:
            session: SQLAlchemy database session
        """
        self.session = session
        self.cohort_service = CohortService(session)
        self.demographic_service = DemographicService(session)
        self.participant_mgmt_service = ParticipantManagementService(session)
        self.validation_service = ValidationService(session)
        self.exception_service = ExceptionService(session)
        self.transformation_service = TransformationService(session)
        self.distribution_service = DistributionService(session)

    def process_file(self, file_path: str, file_type: str) -> dict:
        """
        Process a file through the entire pipeline.

        Args:
            file_path: Path to the file to process
            file_type: Type of file ("csv" or "parquet")

        Returns:
            Dictionary with processing results and status
        """
        # Stage 1: Load cohort
        file_id, records_loaded, filename = self._load_cohort(file_path, file_type)

        # Create file processing status
        file_status = self._init_file_status(file_id, filename, records_loaded)

        # Stage 2: Load demographics
        self._load_demographics(file_id, file_status)

        # Stage 3: Load participant management
        self._load_participant_management(file_id, file_status)

        # Stage 4 & 5: Validation and exception creation
        self._validate_and_create_exceptions(file_id, file_status)

        # Stage 6: Transformation (idempotent, no DB changes)
        self._apply_transformations(file_id, file_status)

        # Stage 7: Load to distribution
        self._load_distribution(file_id, file_status)

        # Mark as complete
        file_status.is_complete = True
        file_status.completed_at = datetime.now(UTC)
        file_status.current_stage = "complete"
        self.session.commit()

        return self._build_response(file_status)

    def _load_cohort(self, file_path: str, file_type: str) -> tuple[int, int, str]:
        """Stage 1: Load cohort file."""
        file_id, records_loaded = self.cohort_service.load_file(file_path, file_type)

        # Get filename
        metadata = self.cohort_service.get_file_metadata(file_id)
        filename = metadata.filename if metadata else file_path

        return file_id, records_loaded, filename

    def _init_file_status(
        self, file_id: int, filename: str, total_records: int
    ) -> FileProcessingStatus:
        """Initialize file processing status."""
        file_status = FileProcessingStatus(
            file_id=file_id,
            filename=filename,
            total_records=total_records,
            cohort_loaded=True,
            current_stage="demographics_loading",
        )
        self.session.add(file_status)
        self.session.commit()
        return file_status

    def _load_demographics(self, file_id: int, file_status: FileProcessingStatus):
        """Stage 2: Load demographics."""
        try:
            self.demographic_service.load_demographics_by_file(file_id)
            file_status.demographics_loaded = True
            file_status.current_stage = "participant_management_loading"
            self.session.commit()
        except Exception as e:
            file_status.has_errors = True
            self.session.commit()
            raise ValueError(f"Demographics loading failed: {str(e)}")

    def _load_participant_management(
        self, file_id: int, file_status: FileProcessingStatus
    ):
        """Stage 3: Load participant management."""
        try:
            self.participant_mgmt_service.load_participant_management_by_file(file_id)
            file_status.participant_management_loaded = True
            file_status.current_stage = "validation"
            self.session.commit()
        except Exception as e:
            file_status.has_errors = True
            self.session.commit()
            raise ValueError(f"Participant management loading failed: {str(e)}")

    def _validate_and_create_exceptions(
        self, file_id: int, file_status: FileProcessingStatus
    ):
        """Stages 4 & 5: Validation and exception creation."""
        # Get all cohort records for this file
        cohort_records = (
            self.session.query(CohortUpdate)
            .filter(CohortUpdate.file_id == file_id)
            .all()
        )

        records_passed = 0
        records_failed = 0
        exceptions_to_create = []

        for record in cohort_records:
            if not record.nhs_number:
                continue

            # Create record status
            record_status = RecordProcessingStatus(
                file_id=file_id,
                nhs_number=record.nhs_number,
                cohort_update_id=record.id,
                demographics_loaded=file_status.demographics_loaded,
                participant_management_loaded=file_status.participant_management_loaded,
                current_stage="validation",
            )
            self.session.add(record_status)

            # Validate
            try:
                validation_results = self.validation_service.validate_participant(
                    record.nhs_number
                )

                # Check if validation passed
                has_errors = any(not r.passed for r in validation_results)

                if has_errors:
                    records_failed += 1
                    record_status.has_validation_errors = True
                    record_status.validation_passed = False

                    # Create exceptions for failures
                    for result in validation_results:
                        if not result.passed:
                            exceptions_to_create.append({
                                "nhs_number": str(record.nhs_number),
                                "rule_description": result.message,
                                "file_name": file_status.filename,
                                "is_fatal": 0 if result.severity == "WARNING" else 1,
                            })
                            record_status.exception_count += 1
                else:
                    records_passed += 1
                    record_status.validation_passed = True

            except Exception:
                records_failed += 1
                record_status.has_validation_errors = True

        # Create exceptions in batch
        if exceptions_to_create:
            from app.models.exception import ExceptionRecordCreate
            exception_records = [ExceptionRecordCreate(**e) for e in exceptions_to_create]
            self.exception_service.create_exceptions(exception_records)

        file_status.validation_complete = True
        file_status.records_passed = records_passed
        file_status.records_failed = records_failed
        file_status.has_errors = records_failed > 0
        file_status.current_stage = "transformation"
        self.session.commit()

    def _apply_transformations(self, file_id: int, file_status: FileProcessingStatus):
        """Stage 6: Apply transformations (idempotent)."""
        # Get all cohort records
        cohort_records = (
            self.session.query(CohortUpdate)
            .filter(CohortUpdate.file_id == file_id)
            .all()
        )

        for record in cohort_records:
            if not record.nhs_number:
                continue

            # Get record status
            record_status = (
                self.session.query(RecordProcessingStatus)
                .filter(
                    RecordProcessingStatus.file_id == file_id,
                    RecordProcessingStatus.nhs_number == record.nhs_number,
                )
                .first()
            )

            if record_status and record_status.validation_passed:
                try:
                    # Apply transformation (idempotent, doesn't modify DB)
                    self.transformation_service.transform_participant(record.nhs_number)
                    record_status.transformation_applied = True
                    record_status.current_stage = "distribution_loading"
                except Exception:
                    record_status.has_transformation_errors = True

        file_status.transformation_complete = True
        file_status.current_stage = "distribution_loading"
        self.session.commit()

    def _load_distribution(self, file_id: int, file_status: FileProcessingStatus):
        """Stage 7: Load to distribution."""
        from app.models.distribution import DistributionRecordCreate

        # Get all records that passed validation
        record_statuses = (
            self.session.query(RecordProcessingStatus)
            .filter(
                RecordProcessingStatus.file_id == file_id,
                RecordProcessingStatus.validation_passed == True,
            )
            .all()
        )

        distribution_records = []
        for record_status in record_statuses:
            # Get demographic and management data
            from app.db.schema import ParticipantDemographic, ParticipantManagement

            demographic = (
                self.session.query(ParticipantDemographic)
                .filter(ParticipantDemographic.nhs_number == record_status.nhs_number)
                .first()
            )

            management = (
                self.session.query(ParticipantManagement)
                .filter(ParticipantManagement.nhs_number == record_status.nhs_number)
                .first()
            )

            if demographic and management:
                dist_record = DistributionRecordCreate(
                    nhs_number=demographic.nhs_number,
                    participant_id=management.participant_id,
                    primary_care_provider=demographic.primary_care_provider,
                    given_name=demographic.given_name,
                    family_name=demographic.family_name,
                    gender=demographic.gender or 0,
                    post_code=demographic.post_code,
                    interpreter_required=demographic.interpreter_required or 0,
                )
                distribution_records.append(dist_record)

        # Create distribution records
        if distribution_records:
            self.distribution_service.create_distribution_records(distribution_records)

        # Update record statuses
        for record_status in record_statuses:
            record_status.distributed = True
            record_status.is_complete = True
            record_status.current_stage = "complete"

        file_status.distribution_loaded = True
        self.session.commit()

    def _build_response(self, file_status: FileProcessingStatus) -> dict:
        """Build response dictionary from file status."""
        stages_completed = []
        if file_status.cohort_loaded:
            stages_completed.append("cohort")
        if file_status.demographics_loaded:
            stages_completed.append("demographics")
        if file_status.participant_management_loaded:
            stages_completed.append("participant_management")
        if file_status.validation_complete:
            stages_completed.append("validation")
        if file_status.transformation_complete:
            stages_completed.append("transformation")
        if file_status.distribution_loaded:
            stages_completed.append("distribution")

        return {
            "file_id": file_status.file_id,
            "filename": file_status.filename,
            "total_records": file_status.total_records,
            "records_processed": file_status.total_records,
            "records_passed": file_status.records_passed,
            "records_failed": file_status.records_failed,
            "stages_completed": stages_completed,
            "current_stage": file_status.current_stage,
            "is_complete": file_status.is_complete,
            "has_errors": file_status.has_errors,
        }

    def get_file_status(self, file_id: int) -> FileProcessingStatus | None:
        """Get processing status for a file."""
        return (
            self.session.query(FileProcessingStatus)
            .filter(FileProcessingStatus.file_id == file_id)
            .first()
        )

    def get_record_status(
        self, file_id: int, nhs_number: int
    ) -> RecordProcessingStatus | None:
        """Get processing status for a specific record."""
        return (
            self.session.query(RecordProcessingStatus)
            .filter(
                RecordProcessingStatus.file_id == file_id,
                RecordProcessingStatus.nhs_number == nhs_number,
            )
            .first()
        )
