import pytest
import tempfile
import pandas as pd
from fastapi.testclient import TestClient

from app.api.v1.orchestration import get_orchestration_service
from app.db.schema import Base
from app.main import app
from app.services.orchestration_service import OrchestrationService
from tests.test_db import TestingSessionLocal, engine


def override_get_orchestration_service():
    return OrchestrationService(session=TestingSessionLocal())


app.dependency_overrides[get_orchestration_service] = override_get_orchestration_service

client = TestClient(app)


@pytest.fixture(autouse=True)
def setup_database():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


def test_process_file_complete_pipeline():
    """Test processing a file through the complete pipeline."""
    # Create a test CSV file
    test_data = {
        "nhs_number": [1234567890, 1234567891],
        "given_name": ["John", "Jane"],
        "family_name": ["Doe", "Smith"],
        "date_of_birth": ["19800101", "19900202"],
        "gender": [1, 2],
        "primary_care_provider": ["GP001", "GP002"],
        "eligibility": [True, True],
        "record_type": ["ADD", "ADD"],
    }

    df = pd.DataFrame(test_data)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df.to_csv(f.name, index=False)
        temp_file = f.name

    # Process the file
    response = client.post(
        "/api/v1/orchestration/process-file",
        json={"file_path": temp_file, "file_type": "csv"},
    )

    assert response.status_code == 200
    data = response.json()

    # Verify response structure
    assert "file_id" in data
    assert "filename" in data
    assert data["total_records"] == 2
    assert "stages_completed" in data
    assert data["is_complete"] is True


def test_get_file_status():
    """Test getting file processing status."""
    # Create and process a file first
    test_data = {"nhs_number": [9999999999], "record_type": ["ADD"], "eligibility": [True]}
    df = pd.DataFrame(test_data)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df.to_csv(f.name, index=False)
        temp_file = f.name

    process_response = client.post(
        "/api/v1/orchestration/process-file",
        json={"file_path": temp_file, "file_type": "csv"},
    )

    file_id = process_response.json()["file_id"]

    # Get status
    status_response = client.get(f"/api/v1/orchestration/file-status/{file_id}")

    assert status_response.status_code == 200
    status_data = status_response.json()

    assert status_data["file_id"] == file_id
    assert "current_stage" in status_data
    assert "cohort_loaded" in status_data
    assert "demographics_loaded" in status_data


def test_get_file_status_not_found():
    """Test getting status for non-existent file."""
    response = client.get("/api/v1/orchestration/file-status/99999")
    assert response.status_code == 404


def test_get_record_status():
    """Test getting individual record processing status."""
    # Create and process a file
    test_data = {
        "nhs_number": [5555555555],
        "given_name": ["Test"],
        "family_name": ["User"],
        "record_type": ["ADD"],
        "eligibility": [True],
    }
    df = pd.DataFrame(test_data)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        df.to_csv(f.name, index=False)
        temp_file = f.name

    process_response = client.post(
        "/api/v1/orchestration/process-file",
        json={"file_path": temp_file, "file_type": "csv"},
    )

    file_id = process_response.json()["file_id"]
    nhs_number = 5555555555

    # Get record status
    record_response = client.get(
        f"/api/v1/orchestration/record-status/{file_id}/{nhs_number}"
    )

    assert record_response.status_code == 200
    record_data = record_response.json()

    assert record_data["file_id"] == file_id
    assert record_data["nhs_number"] == nhs_number
    assert "current_stage" in record_data
    assert "demographics_loaded" in record_data
    assert "validation_passed" in record_data


def test_get_record_status_not_found():
    """Test getting status for non-existent record."""
    response = client.get("/api/v1/orchestration/record-status/99999/1234567890")
    assert response.status_code == 404
