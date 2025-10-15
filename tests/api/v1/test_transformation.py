import pytest
from fastapi.testclient import TestClient

from app.api.v1.transformation import get_transformation_service
from app.db.schema import Base, ParticipantDemographic, ParticipantManagement
from app.main import app
from app.services.transformation_service import TransformationService
from tests.test_db import TestingSessionLocal, engine


def override_get_transformation_service():
    return TransformationService(session=TestingSessionLocal())


app.dependency_overrides[get_transformation_service] = (
    override_get_transformation_service
)

client = TestClient(app)


@pytest.fixture(autouse=True)
def setup_database():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def sample_participant_with_no_postcode():
    """Create a participant with no postcode (will trigger transformation)."""
    session = TestingSessionLocal()

    demographic = ParticipantDemographic(
        nhs_number=9876543210,
        primary_care_provider="A12345",
        given_name="John",
        family_name="Smith",
        date_of_birth="19700101",
        gender=1,
        address_line_1="123 Main St",
        post_code=None,  # No postcode - should trigger transformation
    )
    session.add(demographic)

    management = ParticipantManagement(
        nhs_number=9876543210,
        screening_id=9876543210,
        record_type="ADD",
        eligibility_flag=1,
        exception_flag=0,
        blocked_flag=0,
        referral_flag=0,
    )
    session.add(management)

    session.commit()
    session.close()

    return 9876543210


@pytest.fixture
def sample_participant_ineligible():
    """Create an ineligible participant (will trigger transformation)."""
    session = TestingSessionLocal()

    demographic = ParticipantDemographic(
        nhs_number=9876543211,
        primary_care_provider="A12345",
        given_name="Jane",
        family_name="Doe",
        date_of_birth="19800202",
        gender=2,
        address_line_1="456 Oak Ave",
        post_code="SW1A 1AA",
    )
    session.add(demographic)

    management = ParticipantManagement(
        nhs_number=9876543211,
        screening_id=9876543211,
        record_type="ADD",
        eligibility_flag=0,  # Not eligible - should trigger transformation
        exception_flag=0,
        blocked_flag=0,
        referral_flag=0,
    )
    session.add(management)

    session.commit()
    session.close()

    return 9876543211


@pytest.fixture
def sample_participant_with_special_chars():
    """Create a participant with special characters in name (will trigger transformation)."""
    session = TestingSessionLocal()

    demographic = ParticipantDemographic(
        nhs_number=9876543212,
        primary_care_provider="A12345",
        given_name="Mary-Jane",  # Has hyphen - should be replaced
        family_name="O'Connor",  # Has apostrophe - should be removed
        date_of_birth="19900303",
        gender=2,
        address_line_1="789 Pine Rd",
        post_code="SW 1A 1AA",  # Has space - should be removed
        telephone_number_home="020-1234-5678",  # Has hyphens - should be removed
    )
    session.add(demographic)

    management = ParticipantManagement(
        nhs_number=9876543212,
        screening_id=9876543212,
        record_type="ADD",
        eligibility_flag=1,
        exception_flag=0,
        blocked_flag=0,
        referral_flag=0,
    )
    session.add(management)

    session.commit()
    session.close()

    return 9876543212


def test_transform_participant_no_postcode(sample_participant_with_no_postcode):
    """Test transformation that sets default postcode."""
    response = client.post(
        "/api/v1/transformation/transform-participant",
        json={"nhs_number": sample_participant_with_no_postcode},
    )

    assert response.status_code == 200
    data = response.json()

    assert data["nhs_number"] == sample_participant_with_no_postcode
    assert data["summary"]["rules_applied"] > 0

    # Check that the conditional rule for postcode was applied
    conditional_results = {r["rule_name"]: r for r in data["conditional_results"]}
    assert "set_default_postcode" in conditional_results
    assert conditional_results["set_default_postcode"]["applied"] is True

    # Verify the postcode was actually changed in the database
    session = TestingSessionLocal()
    demographic = (
        session.query(ParticipantDemographic)
        .filter(ParticipantDemographic.nhs_number == sample_participant_with_no_postcode)
        .first()
    )
    assert demographic.post_code == "UNKNOWN"
    session.close()


def test_transform_participant_ineligible(sample_participant_ineligible):
    """Test transformation that sets screening status for ineligible participant."""
    response = client.post(
        "/api/v1/transformation/transform-participant",
        json={"nhs_number": sample_participant_ineligible},
    )

    assert response.status_code == 200
    data = response.json()

    assert data["nhs_number"] == sample_participant_ineligible
    assert data["summary"]["rules_applied"] > 0

    # Check that the conditional rule for ineligibility was applied
    conditional_results = {r["rule_name"]: r for r in data["conditional_results"]}
    assert "set_ceased_status_for_ineligible" in conditional_results
    assert conditional_results["set_ceased_status_for_ineligible"]["applied"] is True

    # Verify the screening status was changed
    session = TestingSessionLocal()
    management = (
        session.query(ParticipantManagement)
        .filter(ParticipantManagement.nhs_number == sample_participant_ineligible)
        .first()
    )
    assert management.participant_screening_status == "CEASED"
    assert management.screening_ceased_reason == "INELIGIBLE"
    session.close()


def test_transform_participant_special_chars(sample_participant_with_special_chars):
    """Test transformation that removes special characters."""
    response = client.post(
        "/api/v1/transformation/transform-participant",
        json={"nhs_number": sample_participant_with_special_chars},
    )

    assert response.status_code == 200
    data = response.json()

    assert data["nhs_number"] == sample_participant_with_special_chars
    assert data["summary"]["rules_applied"] > 0

    # Check that replacement rules were applied
    replacement_results = {r["rule_name"]: r for r in data["replacement_results"]}
    assert len(replacement_results) > 0

    # Verify the transformations were applied
    session = TestingSessionLocal()
    demographic = (
        session.query(ParticipantDemographic)
        .filter(ParticipantDemographic.nhs_number == sample_participant_with_special_chars)
        .first()
    )

    # Name transformations
    assert demographic.given_name == "Mary Jane"  # Hyphen replaced with space
    assert demographic.family_name == "OConnor"  # Apostrophe removed

    # Postcode transformation
    assert demographic.post_code == "SW1A1AA"  # Spaces removed

    # Phone number transformation
    assert demographic.telephone_number_home == "02012345678"  # Hyphens removed

    session.close()


def test_transform_participant_not_found():
    """Test transformation when participant doesn't exist."""
    response = client.post(
        "/api/v1/transformation/transform-participant",
        json={"nhs_number": 9999999999},
    )

    assert response.status_code == 404
    assert "No participant found" in response.json()["detail"]


def test_transform_batch(
    sample_participant_with_no_postcode,
    sample_participant_ineligible,
):
    """Test batch transformation of multiple participants."""
    response = client.post(
        "/api/v1/transformation/transform-batch",
        json={
            "nhs_numbers": [
                sample_participant_with_no_postcode,
                sample_participant_ineligible,
            ]
        },
    )

    assert response.status_code == 200
    data = response.json()

    assert data["summary"]["total_participants"] == 2
    assert data["summary"]["successful"] == 2
    assert data["summary"]["failed"] == 0

    # Check both participants were transformed
    assert str(sample_participant_with_no_postcode) in data["results"]
    assert str(sample_participant_ineligible) in data["results"]


def test_transform_batch_with_invalid_participant(sample_participant_with_no_postcode):
    """Test batch transformation with one invalid participant."""
    response = client.post(
        "/api/v1/transformation/transform-batch",
        json={
            "nhs_numbers": [
                sample_participant_with_no_postcode,
                9999999999,  # Invalid
            ]
        },
    )

    assert response.status_code == 200
    data = response.json()

    assert data["summary"]["total_participants"] == 2
    assert data["summary"]["successful"] == 1
    assert data["summary"]["failed"] == 1

    # Check valid participant was transformed
    assert str(sample_participant_with_no_postcode) in data["results"]

    # Check invalid participant has error
    assert "9999999999" in data["results"]
    invalid_result = data["results"]["9999999999"]
    assert "error" in invalid_result


def test_transformation_updates_timestamp(sample_participant_with_no_postcode):
    """Test that transformation updates the record_update_datetime."""
    session = TestingSessionLocal()

    # Get original timestamp
    demographic = (
        session.query(ParticipantDemographic)
        .filter(ParticipantDemographic.nhs_number == sample_participant_with_no_postcode)
        .first()
    )
    original_timestamp = demographic.record_update_datetime
    session.close()

    # Apply transformation
    response = client.post(
        "/api/v1/transformation/transform-participant",
        json={"nhs_number": sample_participant_with_no_postcode},
    )

    assert response.status_code == 200

    # Check timestamp was updated
    session = TestingSessionLocal()
    demographic = (
        session.query(ParticipantDemographic)
        .filter(ParticipantDemographic.nhs_number == sample_participant_with_no_postcode)
        .first()
    )
    assert demographic.record_update_datetime is not None
    if original_timestamp:
        assert demographic.record_update_datetime > original_timestamp
    session.close()


def test_transformation_reports_changes(sample_participant_with_special_chars):
    """Test that transformation reports all field changes."""
    response = client.post(
        "/api/v1/transformation/transform-participant",
        json={"nhs_number": sample_participant_with_special_chars},
    )

    assert response.status_code == 200
    data = response.json()

    # Check that changes are reported
    assert data["summary"]["total_field_changes"] > 0

    # Check that replacement rules report changes
    for result in data["replacement_results"]:
        if result["applied"]:
            assert len(result["changes"]) > 0
            # Each change should have old and new values
            for field, change in result["changes"].items():
                assert "old" in change
                assert "new" in change
