import pytest
from datetime import datetime
from fastapi.testclient import TestClient

from app.api.v1.exception import get_exception_service
from app.db.schema import Base, ExceptionManagement
from app.main import app
from app.services.exception_service import ExceptionService
from tests.test_db import TestingSessionLocal, engine


def override_get_exception_service():
    return ExceptionService(session=TestingSessionLocal())


app.dependency_overrides[get_exception_service] = override_get_exception_service

client = TestClient(app)


@pytest.fixture(autouse=True)
def setup_database():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


def test_create_single_exception():
    """Test creating a single exception record."""
    response = client.post(
        "/api/v1/exception/create",
        json={
            "exceptions": [
                {
                    "nhs_number": "1234567890",
                    "rule_description": "Invalid postcode format",
                    "is_fatal": 0,
                    "category": 1,
                    "file_name": "test_file.csv",
                }
            ]
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["exceptions_created"] == 1
    assert len(data["exception_ids"]) == 1

    # Verify in database
    session = TestingSessionLocal()
    exception = session.query(ExceptionManagement).first()
    assert exception.nhs_number == "1234567890"
    assert exception.rule_description == "Invalid postcode format"
    assert exception.date_resolved is None  # Unresolved
    session.close()


def test_create_multiple_exceptions():
    """Test creating multiple exception records at once."""
    response = client.post(
        "/api/v1/exception/create",
        json={
            "exceptions": [
                {
                    "nhs_number": "1111111111",
                    "rule_description": "Missing required field",
                    "is_fatal": 1,
                },
                {
                    "nhs_number": "2222222222",
                    "rule_description": "Data format error",
                    "is_fatal": 0,
                },
                {
                    "nhs_number": "3333333333",
                    "rule_description": "Validation failed",
                    "is_fatal": 0,
                },
            ]
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["exceptions_created"] == 3
    assert len(data["exception_ids"]) == 3

    # Verify all in database
    session = TestingSessionLocal()
    exceptions = session.query(ExceptionManagement).all()
    assert len(exceptions) == 3
    assert all(e.date_resolved is None for e in exceptions)
    session.close()


def test_resolve_exceptions():
    """Test resolving all exceptions for an NHS number."""
    # First create some exceptions
    client.post(
        "/api/v1/exception/create",
        json={
            "exceptions": [
                {
                    "nhs_number": "9876543210",
                    "rule_description": "Error 1",
                    "is_fatal": 0,
                },
                {
                    "nhs_number": "9876543210",
                    "rule_description": "Error 2",
                    "is_fatal": 1,
                },
                {
                    "nhs_number": "9876543210",
                    "rule_description": "Error 3",
                    "is_fatal": 0,
                },
            ]
        },
    )

    # Resolve all exceptions for this NHS number
    response = client.post(
        "/api/v1/exception/resolve",
        json={"nhs_number": "9876543210"},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["nhs_number"] == "9876543210"
    assert data["exceptions_resolved"] == 3
    assert "resolution_date" in data

    # Verify all are resolved
    session = TestingSessionLocal()
    exceptions = session.query(ExceptionManagement).filter(
        ExceptionManagement.nhs_number == "9876543210"
    ).all()
    assert len(exceptions) == 3
    assert all(e.date_resolved is not None for e in exceptions)
    assert all(e.record_updated_date is not None for e in exceptions)
    session.close()


def test_resolve_exceptions_not_found():
    """Test resolving when no unresolved exceptions exist."""
    response = client.post(
        "/api/v1/exception/resolve",
        json={"nhs_number": "9999999999"},
    )

    assert response.status_code == 404
    assert "No unresolved exceptions found" in response.json()["detail"]


def test_resolve_only_unresolved_exceptions():
    """Test that resolve only affects unresolved exceptions."""
    # Create exceptions
    client.post(
        "/api/v1/exception/create",
        json={
            "exceptions": [
                {"nhs_number": "5555555555", "rule_description": "Error 1"},
                {"nhs_number": "5555555555", "rule_description": "Error 2"},
                {"nhs_number": "5555555555", "rule_description": "Error 3"},
            ]
        },
    )

    # Resolve first time
    response1 = client.post(
        "/api/v1/exception/resolve",
        json={"nhs_number": "5555555555"},
    )
    assert response1.json()["exceptions_resolved"] == 3

    # Try to resolve again - should fail
    response2 = client.post(
        "/api/v1/exception/resolve",
        json={"nhs_number": "5555555555"},
    )
    assert response2.status_code == 404


def test_resolve_does_not_affect_other_nhs_numbers():
    """Test that resolving for one NHS number doesn't affect others."""
    # Create exceptions for multiple NHS numbers
    client.post(
        "/api/v1/exception/create",
        json={
            "exceptions": [
                {"nhs_number": "1000000001", "rule_description": "Error 1"},
                {"nhs_number": "1000000001", "rule_description": "Error 2"},
                {"nhs_number": "2000000002", "rule_description": "Error 3"},
                {"nhs_number": "2000000002", "rule_description": "Error 4"},
            ]
        },
    )

    # Resolve for first NHS number only
    client.post(
        "/api/v1/exception/resolve",
        json={"nhs_number": "1000000001"},
    )

    # Verify first NHS number resolved
    session = TestingSessionLocal()
    resolved = session.query(ExceptionManagement).filter(
        ExceptionManagement.nhs_number == "1000000001"
    ).all()
    assert all(e.date_resolved is not None for e in resolved)

    # Verify second NHS number still unresolved
    unresolved = session.query(ExceptionManagement).filter(
        ExceptionManagement.nhs_number == "2000000002"
    ).all()
    assert all(e.date_resolved is None for e in unresolved)
    session.close()


def test_exception_all_fields():
    """Test that all exception fields are persisted correctly."""
    response = client.post(
        "/api/v1/exception/create",
        json={
            "exceptions": [
                {
                    "category": 2,
                    "rule_id": 123,
                    "rule_description": "Comprehensive validation failure",
                    "is_fatal": 1,
                    "nhs_number": "8888888888",
                    "file_name": "batch_123.csv",
                    "error_record": '{"field": "postcode", "value": "INVALID"}',
                    "cohort_name": "Breast Screening",
                    "screening_name": "BSS",
                    "servicenow_id": "INC0012345",
                }
            ]
        },
    )

    assert response.status_code == 200

    # Verify all fields
    session = TestingSessionLocal()
    exception = session.query(ExceptionManagement).first()
    assert exception.category == 2
    assert exception.rule_id == 123
    assert exception.rule_description == "Comprehensive validation failure"
    assert exception.is_fatal == 1
    assert exception.nhs_number == "8888888888"
    assert exception.file_name == "batch_123.csv"
    assert exception.error_record == '{"field": "postcode", "value": "INVALID"}'
    assert exception.cohort_name == "Breast Screening"
    assert exception.screening_name == "BSS"
    assert exception.servicenow_id == "INC0012345"
    assert exception.date_created is not None
    assert exception.date_resolved is None
    session.close()


def test_exception_minimal_fields():
    """Test creating exception with only required fields."""
    response = client.post(
        "/api/v1/exception/create",
        json={
            "exceptions": [
                {}  # All fields are optional
            ]
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["exceptions_created"] == 1

    # Verify record created
    session = TestingSessionLocal()
    exception = session.query(ExceptionManagement).first()
    assert exception is not None
    assert exception.date_created is not None
    session.close()


def test_multiple_exceptions_same_nhs_number():
    """Test creating multiple exceptions for same NHS number."""
    response = client.post(
        "/api/v1/exception/create",
        json={
            "exceptions": [
                {
                    "nhs_number": "7777777777",
                    "rule_description": "Validation error 1",
                },
                {
                    "nhs_number": "7777777777",
                    "rule_description": "Validation error 2",
                },
                {
                    "nhs_number": "7777777777",
                    "rule_description": "Validation error 3",
                },
            ]
        },
    )

    assert response.status_code == 200
    assert response.json()["exceptions_created"] == 3

    # All should be resolvable together
    resolve_response = client.post(
        "/api/v1/exception/resolve",
        json={"nhs_number": "7777777777"},
    )

    assert resolve_response.status_code == 200
    assert resolve_response.json()["exceptions_resolved"] == 3
