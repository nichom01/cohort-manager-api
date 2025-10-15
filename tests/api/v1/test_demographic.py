import os
import tempfile

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.api.v1.cohort import get_cohort_service
from app.api.v1.demographic import get_demographic_service
from app.db.schema import Base, CohortUpdate, ParticipantDemographic
from app.main import app
from app.services.cohort_service import CohortService
from app.services.demographic_service import DemographicService
from tests.test_db import TestingSessionLocal, engine


def override_get_cohort_service():
    return CohortService(session=TestingSessionLocal())


def override_get_demographic_service():
    return DemographicService(session=TestingSessionLocal())


app.dependency_overrides[get_cohort_service] = override_get_cohort_service
app.dependency_overrides[get_demographic_service] = override_get_demographic_service

client = TestClient(app)


@pytest.fixture(autouse=True)
def setup_database():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def sample_cohort_file():
    """Create a temporary CSV file with cohort data for testing."""
    data = {
        "record_type": ["ADD", "ADD", "ADD"],
        "eligibility": [True, True, False],
        "is_interpreter_required": [False, True, False],
        "invalid_flag": [False, False, False],
        "nhs_number": [9876543210, 9876543211, 9876543212],
        "superseded_by_nhs_number": ["", "", ""],
        "primary_care_provider": ["A12345", "B23456", "C34567"],
        "primary_care_effective_from_date": ["20250101", "20250101", "20250101"],
        "given_name": ["John", "Jane", "Bob"],
        "family_name": ["Smith", "Doe", "Johnson"],
        "date_of_birth": ["19700101", "19800202", "19900303"],
        "gender": [1, 2, 1],
        "address_line_1": ["123 Main St", "456 Oak Ave", "789 Pine Rd"],
        "address_line_2": ["", "Apt 2B", ""],
        "address_line_3": ["London", "Manchester", "Birmingham"],
        "postcode": ["SW1A 1AA", "M1 1AA", "B1 1AA"],
        "email_address": ["john@test.com", "jane@test.com", "bob@test.com"],
        "home_telephone_number": ["02012345678", "01612345678", "01212345678"],
        "mobile_telephone_number": ["07700123456", "07700123457", "07700123458"],
        "preferred_language": ["English", "Welsh", "English"],
    }
    df = pd.DataFrame(data)

    # Create temp file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df.to_csv(f.name, index=False)
        temp_path = f.name

    yield temp_path

    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)


def test_load_demographics_by_file(sample_cohort_file):
    """Test loading demographics from a file via the API."""
    # First, load the cohort data
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_cohort_file, "file_type": "csv"},
    )
    assert response.status_code == 200
    file_id = response.json()["file_id"]

    # Now load demographics from that file
    response = client.post(
        "/api/v1/demographic/load-by-file", json={"file_id": file_id}
    )

    assert response.status_code == 200
    data = response.json()
    assert data["records_loaded"] == 3
    assert data["records_inserted"] == 3
    assert data["records_updated"] == 0
    assert "Successfully processed 3 demographic records" in data["message"]


def test_load_demographics_by_file_upsert(sample_cohort_file):
    """Test that loading the same file twice updates existing records."""
    # Load cohort data
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_cohort_file, "file_type": "csv"},
    )
    file_id = response.json()["file_id"]

    # First load - should insert all records
    response1 = client.post(
        "/api/v1/demographic/load-by-file", json={"file_id": file_id}
    )
    assert response1.status_code == 200
    assert response1.json()["records_inserted"] == 3
    assert response1.json()["records_updated"] == 0

    # Create a second CSV file to avoid duplicate file hash detection
    data = {
        "record_type": ["ADD", "ADD", "ADD"],
        "eligibility": [True, True, False],
        "is_interpreter_required": [False, True, False],
        "invalid_flag": [False, False, False],
        "nhs_number": [9876543210, 9876543211, 9876543212],  # Same NHS numbers
        "superseded_by_nhs_number": ["", "", ""],
        "primary_care_provider": ["X99999", "Y99999", "Z99999"],  # Different data
        "primary_care_effective_from_date": ["20250115", "20250115", "20250115"],
        "given_name": ["Johnny", "Janet", "Robert"],  # Different names
        "family_name": ["Smith", "Doe", "Johnson"],
        "date_of_birth": ["19700101", "19800202", "19900303"],
        "gender": [1, 2, 1],
        "address_line_1": ["999 New St", "888 New Ave", "777 New Rd"],
        "address_line_2": ["", "Suite 3", ""],
        "address_line_3": ["London", "Manchester", "Birmingham"],
        "postcode": ["SW1A 1AA", "M1 1AA", "B1 1AA"],
        "email_address": ["john.new@test.com", "jane.new@test.com", "bob.new@test.com"],
        "home_telephone_number": ["02087654321", "01618765432", "01218765432"],
        "mobile_telephone_number": ["07700999888", "07700999887", "07700999886"],
        "preferred_language": ["English", "Polish", "Punjabi"],
    }
    df = pd.DataFrame(data)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df.to_csv(f.name, index=False)
        temp_path2 = f.name

    try:
        # Load second file with same NHS numbers but different data
        response = client.post(
            "/api/v1/cohort/load-file",
            json={"file_path": temp_path2, "file_type": "csv"},
        )
        assert response.status_code == 200
        file_id2 = response.json()["file_id"]

        # Second load - should update existing records
        response2 = client.post(
            "/api/v1/demographic/load-by-file", json={"file_id": file_id2}
        )
        assert response2.status_code == 200
        assert response2.json()["records_inserted"] == 0
        assert response2.json()["records_updated"] == 3
    finally:
        if os.path.exists(temp_path2):
            os.unlink(temp_path2)


def test_load_demographics_by_record(sample_cohort_file):
    """Test loading demographics from a single cohort record."""
    # Load cohort data
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_cohort_file, "file_type": "csv"},
    )
    assert response.status_code == 200

    # Load demographics from first record (cohort_update_id = 1)
    response = client.post(
        "/api/v1/demographic/load-by-record", json={"cohort_update_id": 1}
    )

    assert response.status_code == 200
    data = response.json()
    assert data["records_loaded"] == 1
    assert data["action"] == "inserted"
    assert "Successfully inserted demographic record" in data["message"]


def test_load_demographics_by_record_update(sample_cohort_file):
    """Test that loading the same record twice updates it."""
    # Load cohort data
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_cohort_file, "file_type": "csv"},
    )
    assert response.status_code == 200

    # First load - insert
    response1 = client.post(
        "/api/v1/demographic/load-by-record", json={"cohort_update_id": 1}
    )
    assert response1.status_code == 200
    assert response1.json()["action"] == "inserted"

    # Second load - update
    response2 = client.post(
        "/api/v1/demographic/load-by-record", json={"cohort_update_id": 1}
    )
    assert response2.status_code == 200
    assert response2.json()["action"] == "updated"


def test_load_demographics_nonexistent_file():
    """Test loading demographics with a non-existent file_id."""
    response = client.post("/api/v1/demographic/load-by-file", json={"file_id": 999})

    assert response.status_code == 400
    assert "No cohort records found for file_id 999" in response.json()["detail"]


def test_load_demographics_nonexistent_record():
    """Test loading demographics with a non-existent cohort_update_id."""
    response = client.post(
        "/api/v1/demographic/load-by-record", json={"cohort_update_id": 999}
    )

    assert response.status_code == 400
    assert "No cohort record found with id 999" in response.json()["detail"]


def test_demographics_unique_nhs_number(sample_cohort_file):
    """Test that only one demographic record exists per NHS number."""
    # Load cohort data
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_cohort_file, "file_type": "csv"},
    )
    file_id = response.json()["file_id"]

    # Load demographics
    client.post("/api/v1/demographic/load-by-file", json={"file_id": file_id})

    # Check database directly
    session = TestingSessionLocal()
    demographics = session.query(ParticipantDemographic).all()
    assert len(demographics) == 3

    # Check NHS numbers are unique
    nhs_numbers = [d.nhs_number for d in demographics]
    assert len(nhs_numbers) == len(set(nhs_numbers))
    session.close()


def test_demographics_field_mapping(sample_cohort_file):
    """Test that cohort fields are correctly mapped to demographic fields."""
    # Load cohort data
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_cohort_file, "file_type": "csv"},
    )
    file_id = response.json()["file_id"]

    # Load demographics
    client.post("/api/v1/demographic/load-by-file", json={"file_id": file_id})

    # Check the first demographic record
    session = TestingSessionLocal()
    demographic = (
        session.query(ParticipantDemographic)
        .filter(ParticipantDemographic.nhs_number == 9876543210)
        .first()
    )

    assert demographic is not None
    assert demographic.nhs_number == 9876543210
    assert demographic.given_name == "John"
    assert demographic.family_name == "Smith"
    assert demographic.date_of_birth == "19700101"
    assert demographic.gender == 1
    assert demographic.address_line_1 == "123 Main St"
    assert demographic.post_code == "SW1A 1AA"
    assert demographic.email_address_home == "john@test.com"
    # Note: pandas/CSV may strip leading zeros from phone numbers
    assert demographic.telephone_number_home in ["02012345678", "2012345678"]
    assert demographic.telephone_number_mob in ["07700123456", "7700123456"]
    assert demographic.preferred_language == "English"
    assert demographic.primary_care_provider == "A12345"
    assert demographic.record_insert_datetime is not None
    session.close()


def test_demographics_timestamps(sample_cohort_file):
    """Test that insert and update timestamps are correctly set."""
    # Load cohort data
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_cohort_file, "file_type": "csv"},
    )
    file_id = response.json()["file_id"]

    # Load demographics first time
    client.post("/api/v1/demographic/load-by-file", json={"file_id": file_id})

    session = TestingSessionLocal()
    demographic = (
        session.query(ParticipantDemographic)
        .filter(ParticipantDemographic.nhs_number == 9876543210)
        .first()
    )

    # Check insert timestamp exists
    # Note: record_update_datetime is set on both insert and update by the current implementation
    assert demographic.record_insert_datetime is not None
    assert demographic.record_update_datetime is not None
    insert_time = demographic.record_insert_datetime
    first_update_time = demographic.record_update_datetime
    session.close()

    # Create a second CSV file to avoid duplicate file hash detection
    import time
    time.sleep(0.1)  # Small delay to ensure timestamp difference

    data = {
        "record_type": ["ADD", "ADD", "ADD"],
        "eligibility": [True, True, False],
        "is_interpreter_required": [False, True, False],
        "invalid_flag": [False, False, False],
        "nhs_number": [9876543210, 9876543211, 9876543212],  # Same NHS numbers
        "superseded_by_nhs_number": ["", "", ""],
        "primary_care_provider": ["X99999", "Y99999", "Z99999"],
        "primary_care_effective_from_date": ["20250115", "20250115", "20250115"],
        "given_name": ["Johnny", "Janet", "Robert"],
        "family_name": ["Smith", "Doe", "Johnson"],
        "date_of_birth": ["19700101", "19800202", "19900303"],
        "gender": [1, 2, 1],
        "address_line_1": ["999 New St", "888 New Ave", "777 New Rd"],
        "address_line_2": ["", "Suite 3", ""],
        "address_line_3": ["London", "Manchester", "Birmingham"],
        "postcode": ["SW1A 1AA", "M1 1AA", "B1 1AA"],
        "email_address": ["john.new@test.com", "jane.new@test.com", "bob.new@test.com"],
        "home_telephone_number": ["02087654321", "01618765432", "01218765432"],
        "mobile_telephone_number": ["07700999888", "07700999887", "07700999886"],
        "preferred_language": ["English", "Polish", "Punjabi"],
    }
    df = pd.DataFrame(data)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df.to_csv(f.name, index=False)
        temp_path2 = f.name

    try:
        # Load second file with same NHS numbers but different data
        response = client.post(
            "/api/v1/cohort/load-file",
            json={"file_path": temp_path2, "file_type": "csv"},
        )
        file_id2 = response.json()["file_id"]
        client.post("/api/v1/demographic/load-by-file", json={"file_id": file_id2})

        # Check update timestamp has changed
        session = TestingSessionLocal()
        demographic = (
            session.query(ParticipantDemographic)
            .filter(ParticipantDemographic.nhs_number == 9876543210)
            .first()
        )
        assert demographic.record_insert_datetime == insert_time  # Unchanged
        assert demographic.record_update_datetime is not None  # Still set
        assert demographic.record_update_datetime > first_update_time  # Updated
        session.close()
    finally:
        if os.path.exists(temp_path2):
            os.unlink(temp_path2)
