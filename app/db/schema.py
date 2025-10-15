from datetime import UTC, datetime

from sqlalchemy import BigInteger, Boolean, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from app.core.config import config

engine = create_engine(config.db_url, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String, index=True)


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
