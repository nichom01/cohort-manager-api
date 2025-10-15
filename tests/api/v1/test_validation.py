import pytest
from fastapi.testclient import TestClient

from app.api.v1.validation import get_validation_service
from app.db.schema import Base, GpPractice, ParticipantDemographic, ParticipantManagement
from app.main import app
from app.services.validation_service import ValidationService
from tests.test_db import TestingSessionLocal, engine


def override_get_validation_service():
    return ValidationService(session=TestingSessionLocal())


app.dependency_overrides[get_validation_service] = override_get_validation_service

client = TestClient(app)


@pytest.fixture(autouse=True)
def setup_database():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def sample_gp_practices():
    """Create sample GP practices in the database."""
    session = TestingSessionLocal()

    gp_practices = [
        GpPractice(
            gp_practice_code="A12345",
            bso_code="BSO001",
            country_category="England",
        ),
        GpPractice(
            gp_practice_code="B23456",
            bso_code="BSO002",
            country_category="Wales",
        ),
        GpPractice(
            gp_practice_code="C34567",
            bso_code="BSO003",
            country_category="Scotland",
        ),
    ]

    for gp in gp_practices:
        session.add(gp)

    session.commit()
    session.close()

    return gp_practices


@pytest.fixture
def sample_participant():
    """Create a sample participant with demographic and management records."""
    session = TestingSessionLocal()

    # Create demographic record
    demographic = ParticipantDemographic(
        nhs_number=9876543210,
        primary_care_provider="A12345",  # Valid GP practice
        given_name="John",
        family_name="Smith",
        date_of_birth="19700101",
        gender=1,
        address_line_1="123 Main St",
        post_code="SW1A 1AA",
    )
    session.add(demographic)

    # Create participant management record
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


def test_validate_participant_all_pass(sample_gp_practices, sample_participant):
    """Test validation when all rules pass."""
    response = client.post(
        "/api/v1/validation/validate-participant",
        json={"nhs_number": sample_participant},
    )

    assert response.status_code == 200
    data = response.json()

    assert data["nhs_number"] == sample_participant
    assert data["total_rules"] == 5  # We have 5 validation rules
    assert data["passed_rules"] == 5
    assert data["failed_rules"] == 0
    assert data["has_errors"] is False
    assert data["has_warnings"] is False

    # Check that all validation results are present
    assert len(data["validation_results"]) == 5
    for result in data["validation_results"]:
        assert result["passed"] is True


def test_validate_participant_invalid_gp_practice(sample_gp_practices):
    """Test validation when GP practice is invalid."""
    session = TestingSessionLocal()

    # Create participant with invalid GP practice
    demographic = ParticipantDemographic(
        nhs_number=9876543211,
        primary_care_provider="Z99999",  # Invalid GP practice
        given_name="Jane",
        family_name="Doe",
        date_of_birth="19800202",
        gender=2,
        address_line_1="456 Oak Ave",
        post_code="M1 1AA",
    )
    session.add(demographic)

    management = ParticipantManagement(
        nhs_number=9876543211,
        screening_id=9876543211,
        record_type="ADD",
        eligibility_flag=1,
        exception_flag=0,
        blocked_flag=0,
        referral_flag=0,
    )
    session.add(management)

    session.commit()
    session.close()

    response = client.post(
        "/api/v1/validation/validate-participant",
        json={"nhs_number": 9876543211},
    )

    assert response.status_code == 200
    data = response.json()

    assert data["has_errors"] is True
    assert data["failed_rules"] > 0

    # Find the GP practice validation result
    gp_result = next(
        (r for r in data["validation_results"] if r["rule_name"] == "primary_care_provider_exists"),
        None,
    )
    assert gp_result is not None
    assert gp_result["passed"] is False
    assert "not found in GP Practice dataset" in gp_result["message"]


def test_validate_participant_missing_name(sample_gp_practices):
    """Test validation when participant name is missing."""
    session = TestingSessionLocal()

    # Create participant with missing name
    demographic = ParticipantDemographic(
        nhs_number=9876543212,
        primary_care_provider="A12345",
        given_name=None,  # Missing
        family_name="Johnson",
        date_of_birth="19900303",
        gender=1,
        address_line_1="789 Pine Rd",
        post_code="B1 1AA",
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

    response = client.post(
        "/api/v1/validation/validate-participant",
        json={"nhs_number": 9876543212},
    )

    assert response.status_code == 200
    data = response.json()

    assert data["has_errors"] is True

    # Find the name validation result
    name_result = next(
        (r for r in data["validation_results"] if r["rule_name"] == "name_present"),
        None,
    )
    assert name_result is not None
    assert name_result["passed"] is False
    assert "given name" in name_result["message"]


def test_validate_participant_nhs_number_mismatch(sample_gp_practices):
    """Test validation when NHS numbers don't match between records."""
    session = TestingSessionLocal()

    # Create demographic with one NHS number
    demographic = ParticipantDemographic(
        nhs_number=9876543213,
        primary_care_provider="A12345",
        given_name="Bob",
        family_name="Brown",
        date_of_birth="19850505",
        gender=1,
        address_line_1="111 Elm St",
        post_code="L1 1AA",
    )
    session.add(demographic)

    # Create management with different NHS number
    management = ParticipantManagement(
        nhs_number=9876543214,  # Different!
        screening_id=9876543214,
        record_type="ADD",
        eligibility_flag=1,
        exception_flag=0,
        blocked_flag=0,
        referral_flag=0,
    )
    session.add(management)

    session.commit()
    session.close()

    # Validate the demographic NHS number
    response = client.post(
        "/api/v1/validation/validate-participant",
        json={"nhs_number": 9876543213},
    )

    assert response.status_code == 200
    data = response.json()

    # NHS number consistency rule should pass because management record not found for this NHS number
    consistency_result = next(
        (r for r in data["validation_results"] if r["rule_name"] == "nhs_number_consistency"),
        None,
    )
    assert consistency_result is not None


def test_validate_participant_not_found():
    """Test validation when participant doesn't exist."""
    response = client.post(
        "/api/v1/validation/validate-participant",
        json={"nhs_number": 9999999999},
    )

    assert response.status_code == 404
    assert "No participant found" in response.json()["detail"]


def test_validate_batch(sample_gp_practices, sample_participant):
    """Test batch validation of multiple participants."""
    session = TestingSessionLocal()

    # Create a second participant
    demographic2 = ParticipantDemographic(
        nhs_number=9876543220,
        primary_care_provider="B23456",
        given_name="Alice",
        family_name="Williams",
        date_of_birth="19750808",
        gender=2,
        address_line_1="222 Maple Ave",
        post_code="G1 1AA",
    )
    session.add(demographic2)

    management2 = ParticipantManagement(
        nhs_number=9876543220,
        screening_id=9876543220,
        record_type="ADD",
        eligibility_flag=1,
        exception_flag=0,
        blocked_flag=0,
        referral_flag=0,
    )
    session.add(management2)

    session.commit()
    session.close()

    response = client.post(
        "/api/v1/validation/validate-batch",
        json={"nhs_numbers": [sample_participant, 9876543220]},
    )

    assert response.status_code == 200
    data = response.json()

    assert data["total_participants"] == 2
    assert len(data["results"]) == 2
    assert str(sample_participant) in data["results"]
    assert "9876543220" in data["results"]

    # Check first participant results
    participant1_result = data["results"][str(sample_participant)]
    assert participant1_result["nhs_number"] == sample_participant
    assert participant1_result["total_rules"] == 5

    # Check second participant results
    participant2_result = data["results"]["9876543220"]
    assert participant2_result["nhs_number"] == 9876543220
    assert participant2_result["total_rules"] == 5


def test_validate_batch_with_errors(sample_gp_practices):
    """Test batch validation with some participants having errors."""
    session = TestingSessionLocal()

    # Create one valid participant
    demographic1 = ParticipantDemographic(
        nhs_number=9876543230,
        primary_care_provider="A12345",
        given_name="Valid",
        family_name="User",
        date_of_birth="19700101",
        gender=1,
        address_line_1="123 Valid St",
        post_code="SW1A 1AA",
    )
    session.add(demographic1)

    management1 = ParticipantManagement(
        nhs_number=9876543230,
        screening_id=9876543230,
        record_type="ADD",
        eligibility_flag=1,
        exception_flag=0,
        blocked_flag=0,
        referral_flag=0,
    )
    session.add(management1)

    # Create one participant with invalid GP practice
    demographic2 = ParticipantDemographic(
        nhs_number=9876543231,
        primary_care_provider="INVALID",
        given_name="Invalid",
        family_name="User",
        date_of_birth="19800101",
        gender=2,
        address_line_1="456 Invalid St",
        post_code="M1 1AA",
    )
    session.add(demographic2)

    management2 = ParticipantManagement(
        nhs_number=9876543231,
        screening_id=9876543231,
        record_type="ADD",
        eligibility_flag=1,
        exception_flag=0,
        blocked_flag=0,
        referral_flag=0,
    )
    session.add(management2)

    session.commit()
    session.close()

    response = client.post(
        "/api/v1/validation/validate-batch",
        json={"nhs_numbers": [9876543230, 9876543231]},
    )

    assert response.status_code == 200
    data = response.json()

    assert data["total_participants"] == 2
    assert data["participants_with_errors"] >= 1  # At least one participant has errors

    # Check that one participant passed all rules
    valid_result = data["results"]["9876543230"]
    assert valid_result["has_errors"] is False

    # Check that one participant has errors
    invalid_result = data["results"]["9876543231"]
    assert invalid_result["has_errors"] is True


def test_validate_participant_missing_postcode(sample_gp_practices):
    """Test validation when postcode is missing (warning)."""
    session = TestingSessionLocal()

    demographic = ParticipantDemographic(
        nhs_number=9876543240,
        primary_care_provider="A12345",
        given_name="NoPostcode",
        family_name="User",
        date_of_birth="19700101",
        gender=1,
        address_line_1="123 St",
        post_code=None,  # Missing postcode
    )
    session.add(demographic)

    management = ParticipantManagement(
        nhs_number=9876543240,
        screening_id=9876543240,
        record_type="ADD",
        eligibility_flag=1,
        exception_flag=0,
        blocked_flag=0,
        referral_flag=0,
    )
    session.add(management)

    session.commit()
    session.close()

    response = client.post(
        "/api/v1/validation/validate-participant",
        json={"nhs_number": 9876543240},
    )

    assert response.status_code == 200
    data = response.json()

    assert data["has_warnings"] is True

    # Find the postcode validation result
    postcode_result = next(
        (r for r in data["validation_results"] if r["rule_name"] == "postcode_present"),
        None,
    )
    assert postcode_result is not None
    assert postcode_result["passed"] is False
    assert postcode_result["severity"] == "WARNING"
    assert "missing" in postcode_result["message"].lower()


def test_idempotency(sample_gp_practices, sample_participant):
    """Test that validation is idempotent - running multiple times gives same results."""
    # First validation
    response1 = client.post(
        "/api/v1/validation/validate-participant",
        json={"nhs_number": sample_participant},
    )

    # Second validation
    response2 = client.post(
        "/api/v1/validation/validate-participant",
        json={"nhs_number": sample_participant},
    )

    # Third validation
    response3 = client.post(
        "/api/v1/validation/validate-participant",
        json={"nhs_number": sample_participant},
    )

    assert response1.status_code == 200
    assert response2.status_code == 200
    assert response3.status_code == 200

    data1 = response1.json()
    data2 = response2.json()
    data3 = response3.json()

    # All should have same results
    assert data1["total_rules"] == data2["total_rules"] == data3["total_rules"]
    assert data1["passed_rules"] == data2["passed_rules"] == data3["passed_rules"]
    assert data1["failed_rules"] == data2["failed_rules"] == data3["failed_rules"]
    assert data1["has_errors"] == data2["has_errors"] == data3["has_errors"]
