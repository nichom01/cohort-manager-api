import os
import tempfile

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app.api.v1.cohort import get_cohort_service
from app.db.schema import Base
from app.main import app
from app.services.cohort_service import CohortService
from tests.test_db import TestingSessionLocal, engine


def override_get_cohort_service():
    return CohortService(session=TestingSessionLocal())


app.dependency_overrides[get_cohort_service] = override_get_cohort_service

client = TestClient(app)


@pytest.fixture(autouse=True)
def setup_database():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def sample_csv_file():
    """Create a temporary CSV file for testing."""
    data = {
        "record_type": ["ADD", "AMENDED"],
        "eligibility": [True, False],
        "is_interpreter_required": [False, True],
        "invalid_flag": [False, False],
        "nhs_number": [9876543210, 9876543211],
        "given_name": ["John", "Jane"],
        "family_name": ["Smith", "Doe"],
        "email_address": ["test@email.com", "jane@email.com"],
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


@pytest.fixture
def sample_parquet_file():
    """Create a temporary Parquet file for testing."""
    data = {
        "record_type": ["ADD"],
        "eligibility": [True],
        "nhs_number": [1234567890],
        "given_name": ["Test"],
        "family_name": ["User"],
    }
    df = pd.DataFrame(data)

    # Create temp file
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        temp_path = f.name

    df.to_parquet(temp_path)

    yield temp_path

    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)


def test_load_csv_file(sample_csv_file):
    """Test loading a CSV file through the API."""
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_csv_file, "file_type": "csv"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["file_id"] == 1
    assert data["records_loaded"] == 2
    assert "Successfully loaded" in data["message"]


def test_load_parquet_file(sample_parquet_file):
    """Test loading a Parquet file through the API."""
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_parquet_file, "file_type": "parquet"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["file_id"] == 1
    assert data["records_loaded"] == 1


def test_load_nonexistent_file():
    """Test loading a file that doesn't exist."""
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": "/nonexistent/file.csv", "file_type": "csv"},
    )

    assert response.status_code == 404
    assert "File not found" in response.json()["detail"]


def test_load_unsupported_file_type(sample_csv_file):
    """Test loading with unsupported file type."""
    response = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_csv_file, "file_type": "xlsx"},
    )

    assert response.status_code == 400
    assert "Unsupported file type" in response.json()["detail"]


def test_sequential_file_ids(sample_csv_file, sample_parquet_file):
    """Test that file IDs are sequential."""
    # Load first file
    response1 = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_csv_file, "file_type": "csv"},
    )
    assert response1.status_code == 200
    assert response1.json()["file_id"] == 1

    # Load second file (use different file to avoid duplicate detection)
    response2 = client.post(
        "/api/v1/cohort/load-file",
        json={"file_path": sample_parquet_file, "file_type": "parquet"},
    )
    assert response2.status_code == 200
    assert response2.json()["file_id"] == 2
