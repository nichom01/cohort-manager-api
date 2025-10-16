import pytest
from datetime import datetime
from uuid import UUID
from fastapi.testclient import TestClient

from app.api.v1.distribution import get_distribution_service
from app.db.schema import Base, CohortDistribution
from app.main import app
from app.services.distribution_service import DistributionService
from tests.test_db import TestingSessionLocal, engine


def override_get_distribution_service():
    return DistributionService(session=TestingSessionLocal())


app.dependency_overrides[get_distribution_service] = override_get_distribution_service

client = TestClient(app)


@pytest.fixture(autouse=True)
def setup_database():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


def test_create_distribution_records():
    """Test creating single distribution record."""
    response = client.post(
        "/api/v1/distribution/create",
        json={
            "records": [
                {
                    "nhs_number": 1234567890,
                    "participant_id": 1,
                    "gender": 1,
                    "interpreter_required": 0,
                    "given_name": "John",
                    "family_name": "Doe",
                }
            ]
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["records_created"] == 1
    assert len(data["distribution_ids"]) == 1

    # Verify record in database
    session = TestingSessionLocal()
    record = session.query(CohortDistribution).first()
    assert record.nhs_number == 1234567890
    assert record.is_extracted == 0
    assert record.request_id is None
    session.close()


def test_create_multiple_distribution_records():
    """Test creating multiple distribution records at once."""
    response = client.post(
        "/api/v1/distribution/create",
        json={
            "records": [
                {
                    "nhs_number": 1234567890,
                    "participant_id": 1,
                    "gender": 1,
                    "interpreter_required": 0,
                },
                {
                    "nhs_number": 1234567891,
                    "participant_id": 2,
                    "gender": 2,
                    "interpreter_required": 1,
                },
                {
                    "nhs_number": 1234567892,
                    "participant_id": 3,
                    "gender": 1,
                    "interpreter_required": 0,
                },
            ]
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["records_created"] == 3
    assert len(data["distribution_ids"]) == 3

    # Verify all records in database
    session = TestingSessionLocal()
    records = session.query(CohortDistribution).all()
    assert len(records) == 3
    assert all(r.is_extracted == 0 for r in records)
    session.close()


def test_extract_new_records():
    """Test extracting new unextracted records."""
    # First create some records
    client.post(
        "/api/v1/distribution/create",
        json={
            "records": [
                {
                    "nhs_number": 1234567890,
                    "participant_id": 1,
                    "gender": 1,
                    "interpreter_required": 0,
                },
                {
                    "nhs_number": 1234567891,
                    "participant_id": 2,
                    "gender": 2,
                    "interpreter_required": 1,
                },
            ]
        },
    )

    # Extract new records
    response = client.post(
        "/api/v1/distribution/extract-new",
        json={},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["records_extracted"] == 2
    assert "request_id" in data
    request_id = data["request_id"]

    # Verify request_id is a valid UUID
    UUID(request_id)

    # Verify records are marked as extracted
    session = TestingSessionLocal()
    records = session.query(CohortDistribution).all()
    assert all(r.is_extracted == 1 for r in records)
    assert all(r.request_id == request_id for r in records)
    assert all(r.record_update_datetime is not None for r in records)
    session.close()


def test_extract_new_records_with_limit():
    """Test extracting with a limit."""
    # Create 5 records
    client.post(
        "/api/v1/distribution/create",
        json={
            "records": [
                {
                    "nhs_number": 1234567890 + i,
                    "participant_id": i,
                    "gender": 1,
                    "interpreter_required": 0,
                }
                for i in range(5)
            ]
        },
    )

    # Extract only 3 records
    response = client.post(
        "/api/v1/distribution/extract-new",
        json={"limit": 3},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["records_extracted"] == 3

    # Verify only 3 records extracted
    session = TestingSessionLocal()
    extracted = session.query(CohortDistribution).filter(
        CohortDistribution.is_extracted == 1
    ).all()
    unextracted = session.query(CohortDistribution).filter(
        CohortDistribution.is_extracted == 0
    ).all()
    assert len(extracted) == 3
    assert len(unextracted) == 2
    session.close()


def test_extract_new_records_empty():
    """Test extracting when no records available."""
    response = client.post(
        "/api/v1/distribution/extract-new",
        json={},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["records_extracted"] == 0
    assert len(data["records"]) == 0


def test_replay_extraction():
    """Test replaying a previous extraction."""
    # Create and extract records
    client.post(
        "/api/v1/distribution/create",
        json={
            "records": [
                {
                    "nhs_number": 1234567890,
                    "participant_id": 1,
                    "gender": 1,
                    "interpreter_required": 0,
                },
                {
                    "nhs_number": 1234567891,
                    "participant_id": 2,
                    "gender": 2,
                    "interpreter_required": 1,
                },
            ]
        },
    )

    extract_response = client.post(
        "/api/v1/distribution/extract-new",
        json={},
    )
    request_id = extract_response.json()["request_id"]

    # Replay the extraction
    replay_response = client.post(
        "/api/v1/distribution/replay",
        json={"request_id": request_id},
    )

    assert replay_response.status_code == 200
    data = replay_response.json()
    assert data["request_id"] == request_id
    assert data["records_found"] == 2
    assert len(data["records"]) == 2

    # Verify same records returned
    original_records = extract_response.json()["records"]
    replayed_records = data["records"]

    assert len(original_records) == len(replayed_records)
    for orig, replay in zip(original_records, replayed_records):
        assert orig["cohort_distribution_id"] == replay["cohort_distribution_id"]
        assert orig["nhs_number"] == replay["nhs_number"]


def test_replay_extraction_not_found():
    """Test replaying with non-existent request_id."""
    fake_request_id = "00000000-0000-0000-0000-000000000000"

    response = client.post(
        "/api/v1/distribution/replay",
        json={"request_id": fake_request_id},
    )

    assert response.status_code == 404
    assert "No records found" in response.json()["detail"]


def test_multiple_extractions():
    """Test multiple extraction cycles."""
    # Create initial records
    client.post(
        "/api/v1/distribution/create",
        json={
            "records": [
                {
                    "nhs_number": 1000000000 + i,
                    "participant_id": i,
                    "gender": 1,
                    "interpreter_required": 0,
                }
                for i in range(3)
            ]
        },
    )

    # First extraction
    response1 = client.post("/api/v1/distribution/extract-new", json={})
    assert response1.json()["records_extracted"] == 3
    request_id_1 = response1.json()["request_id"]

    # Create more records
    client.post(
        "/api/v1/distribution/create",
        json={
            "records": [
                {
                    "nhs_number": 2000000000 + i,
                    "participant_id": 100 + i,
                    "gender": 2,
                    "interpreter_required": 1,
                }
                for i in range(2)
            ]
        },
    )

    # Second extraction
    response2 = client.post("/api/v1/distribution/extract-new", json={})
    assert response2.json()["records_extracted"] == 2
    request_id_2 = response2.json()["request_id"]

    # Verify different request_ids
    assert request_id_1 != request_id_2

    # Verify both extractions can be replayed
    replay1 = client.post(
        "/api/v1/distribution/replay",
        json={"request_id": request_id_1},
    )
    replay2 = client.post(
        "/api/v1/distribution/replay",
        json={"request_id": request_id_2},
    )

    assert replay1.json()["records_found"] == 3
    assert replay2.json()["records_found"] == 2


def test_distribution_record_fields():
    """Test that all distribution record fields are persisted correctly."""
    response = client.post(
        "/api/v1/distribution/create",
        json={
            "records": [
                {
                    "nhs_number": 9876543210,
                    "participant_id": 42,
                    "superseded_nhs_number": 1111111111,
                    "primary_care_provider": "GP12345",
                    "current_posting": "POST123",
                    "name_prefix": "Dr",
                    "given_name": "Jane",
                    "other_given_name": "Marie",
                    "family_name": "Smith",
                    "previous_family_name": "Jones",
                    "gender": 2,
                    "address_line_1": "123 Main St",
                    "address_line_2": "Apt 4B",
                    "post_code": "SW1A 1AA",
                    "telephone_number_home": "02012345678",
                    "telephone_number_mob": "07700123456",
                    "email_address_home": "jane@example.com",
                    "preferred_language": "English",
                    "interpreter_required": 0,
                }
            ]
        },
    )

    assert response.status_code == 200

    # Extract and verify all fields
    extract_response = client.post("/api/v1/distribution/extract-new", json={})
    record = extract_response.json()["records"][0]

    assert record["nhs_number"] == 9876543210
    assert record["participant_id"] == 42
    assert record["superseded_nhs_number"] == 1111111111
    assert record["primary_care_provider"] == "GP12345"
    assert record["given_name"] == "Jane"
    assert record["family_name"] == "Smith"
    assert record["post_code"] == "SW1A 1AA"
    assert record["email_address_home"] == "jane@example.com"
