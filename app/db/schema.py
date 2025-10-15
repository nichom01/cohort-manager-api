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
