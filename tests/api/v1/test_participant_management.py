import os
import tempfile

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.api.v1.cohort import get_cohort_service
from app.api.v1.participant_management import get_participant_management_service
from app.db.schema import Base, ParticipantManagement
from app.main import app
from app.services.cohort_service import CohortService
from app.services.participant_management_service import ParticipantManagementService
from tests.test_db import TestingSessionLocal, engine


def override_get_cohort_service():
    return CohortService(session=TestingSessionLocal())


def override_get_participant_management_service():
    return ParticipantManagementService(session=TestingSessionLocal())


app.dependency_overrides[get_cohort_service] = override_get_cohort_service
app.dependency_overrides[
    get_participant_management_service
] = override_get_participant_management_service

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
        "record_type": ["ADD", "ADD", "AMENDED"],
        "eligibility": [True, True, False],
        "is_interpreter_required": [False, True, False],
        "invalid_flag": [False, False, False],
        "nhs_number": [9876543210, 9876543211, 9876543212],
        "superseded_by_nhs_number": ["", "", ""],
        "reason_for_removal": ["", "", "DEA"],
        "primary_care_provider": ["A12345", "B23456", "C34567"],
        "given_name": ["John", "Jane", "Bob"],
        "family_name": ["Smith", "Doe", "Johnson"],
        "date_of_birth": ["19700101", "19800202", "19900303"],
        "gender": [1, 2, 1],
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


def test_load_participant_management_by_file(sample_cohort_file):
    """Test loading participant management from a file via the API."""
    # First, load the cohort data
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_cohort_file, "file_type": "csv"},
    )
    assert response.status_code == 200
    file_id = response.json()["file_id"]

    # Now load participant management from that file
    response = client.post(
        "/api/v1/participant-management/load-by-file", json={"file_id": file_id}
    )

    assert response.status_code == 200
    data = response.json()
    assert data["records_loaded"] == 3
    assert data["records_inserted"] == 3
    assert data["records_updated"] == 0
    assert "Successfully processed 3 participant management records" in data["message"]


def test_load_participant_management_by_file_upsert(sample_cohort_file):
    """Test that loading the same file twice updates existing records."""
    # Load cohort data
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_cohort_file, "file_type": "csv"},
    )
    file_id = response.json()["file_id"]

    # First load - should insert all records
    response1 = client.post(
        "/api/v1/participant-management/load-by-file", json={"file_id": file_id}
    )
    assert response1.status_code == 200
    assert response1.json()["records_inserted"] == 3
    assert response1.json()["records_updated"] == 0

    # Create a second CSV file to avoid duplicate file hash detection
    data = {
        "record_type": ["ADD", "ADD", "AMENDED"],
        "eligibility": [True, False, False],  # Changed values
        "is_interpreter_required": [False, True, False],
        "invalid_flag": [False, False, False],
        "nhs_number": [9876543210, 9876543211, 9876543212],  # Same NHS numbers
        "superseded_by_nhs_number": ["", "", ""],
        "reason_for_removal": ["", "RDR", "DEA"],  # Changed values
        "primary_care_provider": ["X12345", "Y23456", "Z34567"],
        "given_name": ["John", "Jane", "Bob"],
        "family_name": ["Smith", "Doe", "Johnson"],
        "date_of_birth": ["19700101", "19800202", "19900303"],
        "gender": [1, 2, 1],
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
            "/api/v1/participant-management/load-by-file", json={"file_id": file_id2}
        )
        assert response2.status_code == 200
        assert response2.json()["records_inserted"] == 0
        assert response2.json()["records_updated"] == 3
    finally:
        if os.path.exists(temp_path2):
            os.unlink(temp_path2)


def test_load_participant_management_by_record(sample_cohort_file):
    """Test loading participant management from a single cohort record."""
    # Load cohort data
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_cohort_file, "file_type": "csv"},
    )
    assert response.status_code == 200

    # Load participant management from first record (cohort_update_id = 1)
    response = client.post(
        "/api/v1/participant-management/load-by-record", json={"cohort_update_id": 1}
    )

    assert response.status_code == 200
    data = response.json()
    assert data["records_loaded"] == 1
    assert data["action"] == "inserted"
    assert "Successfully inserted participant management record" in data["message"]


def test_load_participant_management_by_record_update(sample_cohort_file):
    """Test that loading the same record twice updates it."""
    # Load cohort data
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_cohort_file, "file_type": "csv"},
    )
    assert response.status_code == 200

    # First load - insert
    response1 = client.post(
        "/api/v1/participant-management/load-by-record", json={"cohort_update_id": 1}
    )
    assert response1.status_code == 200
    assert response1.json()["action"] == "inserted"

    # Second load - update
    response2 = client.post(
        "/api/v1/participant-management/load-by-record", json={"cohort_update_id": 1}
    )
    assert response2.status_code == 200
    assert response2.json()["action"] == "updated"


def test_load_participant_management_nonexistent_file():
    """Test loading participant management with a non-existent file_id."""
    response = client.post(
        "/api/v1/participant-management/load-by-file", json={"file_id": 999}
    )

    assert response.status_code == 400
    assert "No cohort records found for file_id 999" in response.json()["detail"]


def test_load_participant_management_nonexistent_record():
    """Test loading participant management with a non-existent cohort_update_id."""
    response = client.post(
        "/api/v1/participant-management/load-by-record", json={"cohort_update_id": 999}
    )

    assert response.status_code == 400
    assert "No cohort record found with id 999" in response.json()["detail"]


def test_participant_management_unique_nhs_number(sample_cohort_file):
    """Test that only one participant management record exists per NHS number."""
    # Load cohort data
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_cohort_file, "file_type": "csv"},
    )
    file_id = response.json()["file_id"]

    # Load participant management
    client.post("/api/v1/participant-management/load-by-file", json={"file_id": file_id})

    # Check database directly
    session = TestingSessionLocal()
    participants = session.query(ParticipantManagement).all()
    assert len(participants) == 3

    # Check NHS numbers are unique
    nhs_numbers = [p.nhs_number for p in participants]
    assert len(nhs_numbers) == len(set(nhs_numbers))
    session.close()


def test_participant_management_field_mapping(sample_cohort_file):
    """Test that cohort fields are correctly mapped to participant management fields."""
    # Load cohort data
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_cohort_file, "file_type": "csv"},
    )
    file_id = response.json()["file_id"]

    # Load participant management
    client.post("/api/v1/participant-management/load-by-file", json={"file_id": file_id})

    # Check the first participant management record
    session = TestingSessionLocal()
    participant = (
        session.query(ParticipantManagement)
        .filter(ParticipantManagement.nhs_number == 9876543210)
        .first()
    )

    assert participant is not None
    assert participant.nhs_number == 9876543210
    assert participant.screening_id == 9876543210  # Uses NHS number as placeholder
    assert participant.record_type == "ADD"
    assert participant.eligibility_flag == 1  # True -> 1
    # Empty strings become None for nullable fields
    assert participant.reason_for_removal in ["", None]
    assert participant.cohort_update_id == 1  # First record
    assert participant.record_insert_datetime is not None
    session.close()


def test_participant_management_traceability(sample_cohort_file):
    """Test that cohort_update_id correctly traces back to source record."""
    # Load cohort data
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_cohort_file, "file_type": "csv"},
    )
    file_id = response.json()["file_id"]

    # Load participant management
    client.post("/api/v1/participant-management/load-by-file", json={"file_id": file_id})

    # Check traceability for each record
    session = TestingSessionLocal()
    participants = session.query(ParticipantManagement).order_by(
        ParticipantManagement.nhs_number
    ).all()

    # Each participant should have a cohort_update_id
    assert participants[0].cohort_update_id == 1
    assert participants[1].cohort_update_id == 2
    assert participants[2].cohort_update_id == 3
    session.close()


def test_participant_management_traceability_update(sample_cohort_file):
    """Test that cohort_update_id is updated when record is reprocessed."""
    # Load cohort data first time
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_cohort_file, "file_type": "csv"},
    )
    file_id1 = response.json()["file_id"]

    # Load participant management first time
    client.post("/api/v1/participant-management/load-by-file", json={"file_id": file_id1})

    session = TestingSessionLocal()
    participant = (
        session.query(ParticipantManagement)
        .filter(ParticipantManagement.nhs_number == 9876543210)
        .first()
    )
    assert participant.cohort_update_id == 1
    session.close()

    # Create a second CSV file to avoid duplicate file hash detection
    data = {
        "record_type": ["ADD", "ADD", "AMENDED"],
        "eligibility": [True, False, False],
        "is_interpreter_required": [False, True, False],
        "invalid_flag": [False, False, False],
        "nhs_number": [9876543210, 9876543211, 9876543212],  # Same NHS numbers
        "superseded_by_nhs_number": ["", "", ""],
        "reason_for_removal": ["", "RDR", "DEA"],
        "primary_care_provider": ["X12345", "Y23456", "Z34567"],
        "given_name": ["John", "Jane", "Bob"],
        "family_name": ["Smith", "Doe", "Johnson"],
        "date_of_birth": ["19700101", "19800202", "19900303"],
        "gender": [1, 2, 1],
    }
    df = pd.DataFrame(data)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df.to_csv(f.name, index=False)
        temp_path2 = f.name

    try:
        # Load cohort data second time with different file
        response = client.post(
            "/api/v1/cohort/load-file",
            json={"file_path": temp_path2, "file_type": "csv"},
        )
        file_id2 = response.json()["file_id"]

        # Load participant management second time
        client.post("/api/v1/participant-management/load-by-file", json={"file_id": file_id2})

        # Check that cohort_update_id is updated to the new record
        session = TestingSessionLocal()
        participant = (
            session.query(ParticipantManagement)
            .filter(ParticipantManagement.nhs_number == 9876543210)
            .first()
        )
        assert participant.cohort_update_id == 4  # First record from second file
        session.close()
    finally:
        if os.path.exists(temp_path2):
            os.unlink(temp_path2)


def test_participant_management_timestamps(sample_cohort_file):
    """Test that insert and update timestamps are correctly set."""
    # Load cohort data
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_cohort_file, "file_type": "csv"},
    )
    file_id = response.json()["file_id"]

    # Load participant management first time
    client.post("/api/v1/participant-management/load-by-file", json={"file_id": file_id})

    session = TestingSessionLocal()
    participant = (
        session.query(ParticipantManagement)
        .filter(ParticipantManagement.nhs_number == 9876543210)
        .first()
    )

    # Check insert timestamp exists
    # Note: record_update_datetime is set on both insert and update by the current implementation
    assert participant.record_insert_datetime is not None
    assert participant.record_update_datetime is not None
    insert_time = participant.record_insert_datetime
    first_update_time = participant.record_update_datetime
    session.close()

    # Create a second CSV file to avoid duplicate file hash detection
    import time
    time.sleep(0.1)  # Small delay to ensure timestamp difference

    data = {
        "record_type": ["ADD", "ADD", "AMENDED"],
        "eligibility": [True, False, False],
        "is_interpreter_required": [False, True, False],
        "invalid_flag": [False, False, False],
        "nhs_number": [9876543210, 9876543211, 9876543212],  # Same NHS numbers
        "superseded_by_nhs_number": ["", "", ""],
        "reason_for_removal": ["", "RDR", "DEA"],
        "primary_care_provider": ["X12345", "Y23456", "Z34567"],
        "given_name": ["John", "Jane", "Bob"],
        "family_name": ["Smith", "Doe", "Johnson"],
        "date_of_birth": ["19700101", "19800202", "19900303"],
        "gender": [1, 2, 1],
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
        client.post("/api/v1/participant-management/load-by-file", json={"file_id": file_id2})

        # Check update timestamp has changed
        session = TestingSessionLocal()
        participant = (
            session.query(ParticipantManagement)
            .filter(ParticipantManagement.nhs_number == 9876543210)
            .first()
        )
        assert participant.record_insert_datetime == insert_time  # Unchanged
        assert participant.record_update_datetime is not None  # Still set
        assert participant.record_update_datetime > first_update_time  # Updated
        session.close()
    finally:
        if os.path.exists(temp_path2):
            os.unlink(temp_path2)


def test_participant_management_eligibility_flag_conversion(sample_cohort_file):
    """Test that boolean eligibility is correctly converted to integer flag."""
    # Load cohort data
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_cohort_file, "file_type": "csv"},
    )
    file_id = response.json()["file_id"]

    # Load participant management
    client.post("/api/v1/participant-management/load-by-file", json={"file_id": file_id})

    # Check eligibility flag conversion
    session = TestingSessionLocal()
    participants = session.query(ParticipantManagement).order_by(
        ParticipantManagement.nhs_number
    ).all()

    # First two records have eligibility=True -> eligibility_flag=1
    assert participants[0].eligibility_flag == 1
    assert participants[1].eligibility_flag == 1

    # Third record has eligibility=False -> eligibility_flag=0
    assert participants[2].eligibility_flag == 0
    session.close()


def test_participant_management_reason_for_removal(sample_cohort_file):
    """Test that reason_for_removal is correctly mapped."""
    # Load cohort data
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_cohort_file, "file_type": "csv"},
    )
    file_id = response.json()["file_id"]

    # Load participant management
    client.post("/api/v1/participant-management/load-by-file", json={"file_id": file_id})

    # Check reason_for_removal mapping
    session = TestingSessionLocal()
    participants = session.query(ParticipantManagement).order_by(
        ParticipantManagement.nhs_number
    ).all()

    # First two records have empty reason_for_removal (empty strings become None)
    assert participants[0].reason_for_removal in ["", None]
    assert participants[1].reason_for_removal in ["", None]

    # Third record has reason_for_removal="DEA"
    assert participants[2].reason_for_removal == "DEA"
    session.close()
