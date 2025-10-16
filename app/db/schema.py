from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import BigInteger, Boolean, Integer, SmallInteger, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from app.core.config import config

engine = create_engine(config.db_url, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


class FileMetadata(Base):
    __tablename__ = "file_metadata"

    file_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    filename: Mapped[str] = mapped_column(String, nullable=False)
    file_path: Mapped[str] = mapped_column(String, nullable=False)
    file_type: Mapped[str] = mapped_column(String, nullable=False)  # "csv" or "parquet"
    file_hash: Mapped[str] = mapped_column(String, nullable=False, unique=True, index=True)
    file_size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    upload_timestamp: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(UTC)
    )
    records_loaded: Mapped[int] = mapped_column(Integer, nullable=False)


class ParticipantManagement(Base):
    __tablename__ = "participant_management"

    participant_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    screening_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    nhs_number: Mapped[int] = mapped_column(BigInteger, nullable=False, unique=True, index=True)
    record_type: Mapped[str] = mapped_column(String(10), nullable=False)
    eligibility_flag: Mapped[int] = mapped_column(Integer, nullable=False)
    reason_for_removal: Mapped[str | None] = mapped_column(String(10), nullable=True)
    reason_for_removal_from_dt: Mapped[datetime | None] = mapped_column(nullable=True)
    business_rule_version: Mapped[str | None] = mapped_column(String(10), nullable=True)
    exception_flag: Mapped[int] = mapped_column(Integer, nullable=False)
    blocked_flag: Mapped[int] = mapped_column(Integer, nullable=False)
    referral_flag: Mapped[int] = mapped_column(Integer, nullable=False)
    next_test_due_date: Mapped[datetime | None] = mapped_column(nullable=True)
    next_test_due_date_calc_method: Mapped[str | None] = mapped_column(String, nullable=True)
    participant_screening_status: Mapped[str | None] = mapped_column(String, nullable=True)
    screening_ceased_reason: Mapped[str | None] = mapped_column(String, nullable=True)
    is_higher_risk: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_higher_risk_active: Mapped[int | None] = mapped_column(Integer, nullable=True)
    higher_risk_next_test_due_date: Mapped[datetime | None] = mapped_column(nullable=True)
    higher_risk_referral_reason_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    date_irradiated: Mapped[datetime | None] = mapped_column(nullable=True)
    gene_code_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    src_system_processed_datetime: Mapped[datetime | None] = mapped_column(nullable=True)

    # Traceability field
    cohort_update_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)

    # Audit fields
    record_insert_datetime: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(UTC)
    )
    record_update_datetime: Mapped[datetime | None] = mapped_column(nullable=True)


class ParticipantDemographic(Base):
    __tablename__ = "participant_demographic"

    participant_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nhs_number: Mapped[int] = mapped_column(BigInteger, nullable=False, unique=True, index=True)
    superseded_by_nhs_number: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Care provider information
    primary_care_provider: Mapped[str | None] = mapped_column(String, nullable=True)
    primary_care_provider_from_dt: Mapped[str | None] = mapped_column(String, nullable=True)
    current_posting: Mapped[str | None] = mapped_column(String, nullable=True)
    current_posting_from_dt: Mapped[str | None] = mapped_column(String, nullable=True)

    # Name fields
    name_prefix: Mapped[str | None] = mapped_column(String, nullable=True)
    given_name: Mapped[str | None] = mapped_column(String, nullable=True)
    other_given_name: Mapped[str | None] = mapped_column(String, nullable=True)
    family_name: Mapped[str | None] = mapped_column(String, nullable=True)
    previous_family_name: Mapped[str | None] = mapped_column(String, nullable=True)

    # Personal details
    date_of_birth: Mapped[str | None] = mapped_column(String, nullable=True)
    gender: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Address fields
    address_line_1: Mapped[str | None] = mapped_column(String, nullable=True)
    address_line_2: Mapped[str | None] = mapped_column(String, nullable=True)
    address_line_3: Mapped[str | None] = mapped_column(String, nullable=True)
    address_line_4: Mapped[str | None] = mapped_column(String, nullable=True)
    address_line_5: Mapped[str | None] = mapped_column(String, nullable=True)
    post_code: Mapped[str | None] = mapped_column(String, nullable=True)
    paf_key: Mapped[str | None] = mapped_column(String, nullable=True)
    usual_address_from_dt: Mapped[str | None] = mapped_column(String, nullable=True)

    # Death information
    date_of_death: Mapped[str | None] = mapped_column(String, nullable=True)
    death_status: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Contact information
    telephone_number_home: Mapped[str | None] = mapped_column(String, nullable=True)
    telephone_number_home_from_dt: Mapped[str | None] = mapped_column(String, nullable=True)
    telephone_number_mob: Mapped[str | None] = mapped_column(String, nullable=True)
    telephone_number_mob_from_dt: Mapped[str | None] = mapped_column(String, nullable=True)
    email_address_home: Mapped[str | None] = mapped_column(String, nullable=True)
    email_address_home_from_dt: Mapped[str | None] = mapped_column(String, nullable=True)

    # Language and accessibility
    preferred_language: Mapped[str | None] = mapped_column(String, nullable=True)
    interpreter_required: Mapped[int | None] = mapped_column(Integer, nullable=True)
    invalid_flag: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Audit fields
    record_insert_datetime: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(UTC)
    )
    record_update_datetime: Mapped[datetime | None] = mapped_column(nullable=True)


class CohortUpdate(Base):
    __tablename__ = "cohort_update"

    # System-generated columns
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    create_date: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(UTC)
    )
    file_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    # Record metadata
    record_type: Mapped[str | None] = mapped_column(String, nullable=True)

    # Boolean fields
    eligibility: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    is_interpreter_required: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    invalid_flag: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    # Number fields
    change_time_stamp: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    serial_change_number: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    nhs_number: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    superseded_by_nhs_number: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    gender: Mapped[int | None] = mapped_column(Integer, nullable=True)
    death_status: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # String fields
    primary_care_provider: Mapped[str | None] = mapped_column(String, nullable=True)
    primary_care_effective_from_date: Mapped[str | None] = mapped_column(
        String, nullable=True
    )
    current_posting: Mapped[str | None] = mapped_column(String, nullable=True)
    current_posting_effective_from_date: Mapped[str | None] = mapped_column(
        String, nullable=True
    )
    name_prefix: Mapped[str | None] = mapped_column(String, nullable=True)
    given_name: Mapped[str | None] = mapped_column(String, nullable=True)
    other_given_name: Mapped[str | None] = mapped_column(String, nullable=True)
    family_name: Mapped[str | None] = mapped_column(String, nullable=True)
    previous_family_name: Mapped[str | None] = mapped_column(String, nullable=True)
    date_of_birth: Mapped[str | None] = mapped_column(String, nullable=True)
    address_line_1: Mapped[str | None] = mapped_column(String, nullable=True)
    address_line_2: Mapped[str | None] = mapped_column(String, nullable=True)
    address_line_3: Mapped[str | None] = mapped_column(String, nullable=True)
    address_line_4: Mapped[str | None] = mapped_column(String, nullable=True)
    address_line_5: Mapped[str | None] = mapped_column(String, nullable=True)
    postcode: Mapped[str | None] = mapped_column(String, nullable=True)
    paf_key: Mapped[str | None] = mapped_column(String, nullable=True)
    address_effective_from_date: Mapped[str | None] = mapped_column(
        String, nullable=True
    )
    reason_for_removal: Mapped[str | None] = mapped_column(String, nullable=True)
    reason_for_removal_effective_from_date: Mapped[str | None] = mapped_column(
        String, nullable=True
    )
    date_of_death: Mapped[str | None] = mapped_column(String, nullable=True)
    home_telephone_number: Mapped[str | None] = mapped_column(String, nullable=True)
    home_telephone_effective_from_date: Mapped[str | None] = mapped_column(
        String, nullable=True
    )
    mobile_telephone_number: Mapped[str | None] = mapped_column(String, nullable=True)
    mobile_telephone_effective_from_date: Mapped[str | None] = mapped_column(
        String, nullable=True
    )
    email_address: Mapped[str | None] = mapped_column(String, nullable=True)
    email_address_effective_from_date: Mapped[str | None] = mapped_column(
        String, nullable=True
    )
    preferred_language: Mapped[str | None] = mapped_column(String, nullable=True)


class GpPractice(Base):
    __tablename__ = "gp_practice"

    gp_practice_code: Mapped[str] = mapped_column(String, primary_key=True)
    bso_code: Mapped[str | None] = mapped_column(String, nullable=True)
    country_category: Mapped[str | None] = mapped_column(String, nullable=True)
    audit_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    audit_created_timestamp: Mapped[datetime | None] = mapped_column(nullable=True)
    audit_last_modified_timestamp: Mapped[datetime | None] = mapped_column(nullable=True)
    audit_text: Mapped[str | None] = mapped_column(String, nullable=True)


class FileProcessingStatus(Base):
    """
    Track processing status for each file through the pipeline.

    This table maintains the overall status of a file as it progresses
    through each stage of the cohort processing workflow.
    """
    __tablename__ = "file_processing_status"

    file_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    filename: Mapped[str] = mapped_column(String, nullable=False)

    # Stage completion flags
    cohort_loaded: Mapped[bool] = mapped_column(Boolean, default=False)
    demographics_loaded: Mapped[bool] = mapped_column(Boolean, default=False)
    participant_management_loaded: Mapped[bool] = mapped_column(Boolean, default=False)
    validation_complete: Mapped[bool] = mapped_column(Boolean, default=False)
    transformation_complete: Mapped[bool] = mapped_column(Boolean, default=False)
    distribution_loaded: Mapped[bool] = mapped_column(Boolean, default=False)

    # Stage completion timestamps
    cohort_loaded_at: Mapped[datetime | None] = mapped_column(nullable=True)
    demographics_loaded_at: Mapped[datetime | None] = mapped_column(nullable=True)
    participant_management_loaded_at: Mapped[datetime | None] = mapped_column(nullable=True)
    validation_complete_at: Mapped[datetime | None] = mapped_column(nullable=True)
    transformation_complete_at: Mapped[datetime | None] = mapped_column(nullable=True)
    distribution_loaded_at: Mapped[datetime | None] = mapped_column(nullable=True)

    # Counts
    total_records: Mapped[int] = mapped_column(Integer, default=0)
    records_passed: Mapped[int] = mapped_column(Integer, default=0)
    records_failed: Mapped[int] = mapped_column(Integer, default=0)

    # Overall status
    current_stage: Mapped[str] = mapped_column(String, default="cohort_loading")
    is_complete: Mapped[bool] = mapped_column(Boolean, default=False)
    has_errors: Mapped[bool] = mapped_column(Boolean, default=False)

    # Timestamps
    started_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))
    completed_at: Mapped[datetime | None] = mapped_column(nullable=True)
    last_updated: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )


class RecordProcessingStatus(Base):
    """
    Track processing status for individual records within a file.

    This table maintains the status of each NHS number record as it
    progresses through each stage of processing.
    """
    __tablename__ = "record_processing_status"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    file_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    nhs_number: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    cohort_update_id: Mapped[int] = mapped_column(Integer, nullable=False)

    # Stage completion flags
    demographics_loaded: Mapped[bool] = mapped_column(Boolean, default=False)
    participant_management_loaded: Mapped[bool] = mapped_column(Boolean, default=False)
    validation_passed: Mapped[bool] = mapped_column(Boolean, default=False)
    transformation_applied: Mapped[bool] = mapped_column(Boolean, default=False)
    distributed: Mapped[bool] = mapped_column(Boolean, default=False)

    # Stage completion timestamps
    demographics_loaded_at: Mapped[datetime | None] = mapped_column(nullable=True)
    participant_management_loaded_at: Mapped[datetime | None] = mapped_column(nullable=True)
    validation_passed_at: Mapped[datetime | None] = mapped_column(nullable=True)
    transformation_applied_at: Mapped[datetime | None] = mapped_column(nullable=True)
    distributed_at: Mapped[datetime | None] = mapped_column(nullable=True)

    # Error tracking
    has_validation_errors: Mapped[bool] = mapped_column(Boolean, default=False)
    has_transformation_errors: Mapped[bool] = mapped_column(Boolean, default=False)
    exception_count: Mapped[int] = mapped_column(Integer, default=0)

    # Current status
    current_stage: Mapped[str] = mapped_column(String, default="demographics_loading")
    is_complete: Mapped[bool] = mapped_column(Boolean, default=False)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )


class ExceptionManagement(Base):
    """
    Exception tracking for validation and transformation failures.

    This table tracks exceptions that occur during data processing,
    such as validation failures, transformation errors, or data quality issues.
    Exceptions can be resolved by setting date_resolved.
    """
    __tablename__ = "exception_management"

    # Primary key
    exception_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    # Exception details
    category: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rule_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rule_description: Mapped[str | None] = mapped_column(String, nullable=True)
    is_fatal: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)

    # Participant and file context
    nhs_number: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True)
    file_name: Mapped[str | None] = mapped_column(String(250), nullable=True)
    error_record: Mapped[str | None] = mapped_column(String, nullable=True)

    # Cohort context
    cohort_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    screening_name: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)

    # Dates
    exception_date: Mapped[datetime | None] = mapped_column(nullable=True)
    date_created: Mapped[datetime] = mapped_column(default=lambda: datetime.now(UTC))
    date_resolved: Mapped[datetime | None] = mapped_column(nullable=True)
    record_updated_date: Mapped[datetime | None] = mapped_column(nullable=True)

    # ServiceNow integration
    servicenow_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    servicenow_created_date: Mapped[datetime | None] = mapped_column(nullable=True)


class CohortDistribution(Base):
    """
    Distribution records ready for downstream systems.

    This table stores participant data that has been validated and transformed,
    ready to be extracted by downstream systems. The is_extracted flag tracks
    whether the record has been collected, and request_id groups extractions.
    """
    __tablename__ = "cohort_distribution"

    # Primary key
    cohort_distribution_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )

    # Participant identifiers
    nhs_number: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    participant_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    superseded_nhs_number: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    # Care provider information
    primary_care_provider: Mapped[str | None] = mapped_column(String(10), nullable=True)
    primary_care_provider_from_dt: Mapped[datetime | None] = mapped_column(nullable=True)
    current_posting: Mapped[str | None] = mapped_column(String(10), nullable=True)
    current_posting_from_dt: Mapped[datetime | None] = mapped_column(nullable=True)

    # Name fields
    name_prefix: Mapped[str | None] = mapped_column(String(35), nullable=True)
    given_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    other_given_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    family_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    previous_family_name: Mapped[str | None] = mapped_column(String(100), nullable=True)

    # Personal details
    date_of_birth: Mapped[datetime | None] = mapped_column(nullable=True)
    date_of_death: Mapped[datetime | None] = mapped_column(nullable=True)
    gender: Mapped[int] = mapped_column(SmallInteger, nullable=False)

    # Address fields
    address_line_1: Mapped[str | None] = mapped_column(String(100), nullable=True)
    address_line_2: Mapped[str | None] = mapped_column(String(100), nullable=True)
    address_line_3: Mapped[str | None] = mapped_column(String(100), nullable=True)
    address_line_4: Mapped[str | None] = mapped_column(String(100), nullable=True)
    address_line_5: Mapped[str | None] = mapped_column(String(100), nullable=True)
    post_code: Mapped[str | None] = mapped_column(String(10), nullable=True)
    usual_address_from_dt: Mapped[datetime | None] = mapped_column(nullable=True)

    # Removal information
    reason_for_removal: Mapped[str | None] = mapped_column(String(10), nullable=True)
    reason_for_removal_from_dt: Mapped[datetime | None] = mapped_column(nullable=True)

    # Contact information
    telephone_number_home: Mapped[str | None] = mapped_column(String(35), nullable=True)
    telephone_number_home_from_dt: Mapped[datetime | None] = mapped_column(nullable=True)
    telephone_number_mob: Mapped[str | None] = mapped_column(String(35), nullable=True)
    telephone_number_mob_from_dt: Mapped[datetime | None] = mapped_column(nullable=True)
    email_address_home: Mapped[str | None] = mapped_column(String(100), nullable=True)
    email_address_home_from_dt: Mapped[datetime | None] = mapped_column(nullable=True)

    # Language and accessibility
    preferred_language: Mapped[str | None] = mapped_column(String(35), nullable=True)
    interpreter_required: Mapped[int] = mapped_column(SmallInteger, nullable=False)

    # Extraction tracking
    is_extracted: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, default=0, index=True
    )
    request_id: Mapped[UUID | None] = mapped_column(String(36), nullable=True, index=True)

    # Audit fields
    record_insert_datetime: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(UTC)
    )
    record_update_datetime: Mapped[datetime | None] = mapped_column(nullable=True)
