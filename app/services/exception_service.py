"""
Exception service for managing exception records.

This service handles:
1. Creating exception records for validation/transformation failures
2. Resolving all exceptions for a given NHS number
"""

from datetime import UTC, datetime

from sqlalchemy.orm import Session

from app.db.schema import ExceptionManagement
from app.models.exception import ExceptionRecordCreate


class ExceptionService:
    """
    Service for managing exception records.

    Exception records track validation failures, transformation errors,
    and data quality issues. Exceptions can be resolved by NHS number.
    """

    def __init__(self, session: Session):
        """
        Initialize the exception service.

        Args:
            session: SQLAlchemy database session
        """
        self.session = session

    def create_exceptions(
        self, exceptions: list[ExceptionRecordCreate]
    ) -> tuple[int, list[int]]:
        """
        Create one or more exception records.

        Args:
            exceptions: List of exception records to create

        Returns:
            Tuple of (count of exceptions created, list of exception IDs)
        """
        created_ids = []

        for exception_data in exceptions:
            # Create new exception record
            exception_record = ExceptionManagement(
                category=exception_data.category,
                rule_id=exception_data.rule_id,
                rule_description=exception_data.rule_description,
                is_fatal=exception_data.is_fatal,
                nhs_number=exception_data.nhs_number,
                file_name=exception_data.file_name,
                error_record=exception_data.error_record,
                cohort_name=exception_data.cohort_name,
                screening_name=exception_data.screening_name,
                exception_date=exception_data.exception_date or datetime.now(UTC),
                date_created=datetime.now(UTC),
                date_resolved=None,  # New exceptions are unresolved
                servicenow_id=exception_data.servicenow_id,
                servicenow_created_date=exception_data.servicenow_created_date,
            )

            self.session.add(exception_record)
            self.session.flush()  # Flush to get the ID
            created_ids.append(exception_record.exception_id)

        self.session.commit()
        return len(created_ids), created_ids

    def resolve_exceptions(self, nhs_number: str) -> tuple[int, datetime]:
        """
        Resolve all exceptions for a given NHS number.

        This method finds all unresolved exceptions (date_resolved is NULL)
        for the specified NHS number and marks them as resolved with the
        current timestamp.

        Args:
            nhs_number: NHS number to resolve exceptions for

        Returns:
            Tuple of (count of exceptions resolved, resolution timestamp)

        Raises:
            ValueError: If no unresolved exceptions found for NHS number
        """
        resolution_date = datetime.now(UTC)

        # Find all unresolved exceptions for this NHS number
        unresolved_exceptions = (
            self.session.query(ExceptionManagement)
            .filter(
                ExceptionManagement.nhs_number == nhs_number,
                ExceptionManagement.date_resolved == None,
            )
            .all()
        )

        if not unresolved_exceptions:
            raise ValueError(
                f"No unresolved exceptions found for NHS number {nhs_number}"
            )

        # Mark all as resolved
        for exception in unresolved_exceptions:
            exception.date_resolved = resolution_date
            exception.record_updated_date = resolution_date

        self.session.commit()

        return len(unresolved_exceptions), resolution_date
