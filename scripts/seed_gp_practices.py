"""
Seed GP practice data to support test data validation.

This script populates the gp_practice table with GP practice codes
that are referenced in test data files and test cases.
"""

from datetime import UTC, datetime

from app.db.schema import Base, GpPractice, SessionLocal, engine


def seed_gp_practices():
    """Create GP practice records for test data."""

    # Create tables if they don't exist
    Base.metadata.create_all(bind=engine)

    session = SessionLocal()

    try:
        # Define GP practices covering all test data codes
        gp_practices = [
            # Orchestration test codes
            {"gp_practice_code": "GP001", "bso_code": "BSO001", "country_category": "England"},
            {"gp_practice_code": "GP002", "bso_code": "BSO002", "country_category": "England"},

            # CSV file test codes (GP0001-GP0020)
            {"gp_practice_code": "GP0001", "bso_code": "BSO0001", "country_category": "England"},
            {"gp_practice_code": "GP0002", "bso_code": "BSO0002", "country_category": "England"},
            {"gp_practice_code": "GP0003", "bso_code": "BSO0003", "country_category": "England"},
            {"gp_practice_code": "GP0004", "bso_code": "BSO0004", "country_category": "England"},
            {"gp_practice_code": "GP0005", "bso_code": "BSO0005", "country_category": "Wales"},
            {"gp_practice_code": "GP0006", "bso_code": "BSO0006", "country_category": "England"},
            {"gp_practice_code": "GP0007", "bso_code": "BSO0007", "country_category": "England"},
            {"gp_practice_code": "GP0008", "bso_code": "BSO0008", "country_category": "England"},
            {"gp_practice_code": "GP0009", "bso_code": "BSO0009", "country_category": "England"},
            {"gp_practice_code": "GP0010", "bso_code": "BSO0010", "country_category": "Wales"},
            {"gp_practice_code": "GP0011", "bso_code": "BSO0011", "country_category": "England"},
            {"gp_practice_code": "GP0012", "bso_code": "BSO0012", "country_category": "England"},
            {"gp_practice_code": "GP0013", "bso_code": "BSO0013", "country_category": "England"},
            {"gp_practice_code": "GP0014", "bso_code": "BSO0014", "country_category": "England"},
            {"gp_practice_code": "GP0015", "bso_code": "BSO0015", "country_category": "Wales"},
            {"gp_practice_code": "GP0016", "bso_code": "BSO0016", "country_category": "England"},
            {"gp_practice_code": "GP0017", "bso_code": "BSO0017", "country_category": "England"},
            {"gp_practice_code": "GP0018", "bso_code": "BSO0018", "country_category": "England"},
            {"gp_practice_code": "GP0019", "bso_code": "BSO0019", "country_category": "England"},
            {"gp_practice_code": "GP0020", "bso_code": "BSO0020", "country_category": "Wales"},

            # Test case codes (A-series)
            {"gp_practice_code": "A12345", "bso_code": "BSOA12345", "country_category": "England"},
            {"gp_practice_code": "B23456", "bso_code": "BSOB23456", "country_category": "England"},
            {"gp_practice_code": "C34567", "bso_code": "BSOC34567", "country_category": "England"},

            # Test case codes (X/Y/Z-series)
            {"gp_practice_code": "X12345", "bso_code": "BSOX12345", "country_category": "Scotland"},
            {"gp_practice_code": "Y23456", "bso_code": "BSOY23456", "country_category": "Scotland"},
            {"gp_practice_code": "Z34567", "bso_code": "BSOZ34567", "country_category": "Scotland"},
            {"gp_practice_code": "X99999", "bso_code": "BSOX99999", "country_category": "Scotland"},
            {"gp_practice_code": "Y99999", "bso_code": "BSOY99999", "country_category": "Scotland"},
            {"gp_practice_code": "Z99999", "bso_code": "BSOZ99999", "country_category": "Scotland"},

            # Distribution test code
            {"gp_practice_code": "GP12345", "bso_code": "BSO12345", "country_category": "England"},

            # Additional common test codes
            {"gp_practice_code": "TEST001", "bso_code": "BSOTEST001", "country_category": "England"},
            {"gp_practice_code": "TEST002", "bso_code": "BSOTEST002", "country_category": "Wales"},
            {"gp_practice_code": "TEST003", "bso_code": "BSOTEST003", "country_category": "Scotland"},
        ]

        # Add timestamp to all records
        current_time = datetime.now(UTC)
        for gp in gp_practices:
            gp["audit_created_timestamp"] = current_time
            gp["audit_last_modified_timestamp"] = current_time
            gp["audit_text"] = "Seed data for testing"

        # Check existing records and only insert new ones
        existing_codes = {gp.gp_practice_code for gp in session.query(GpPractice).all()}
        new_practices = [gp for gp in gp_practices if gp["gp_practice_code"] not in existing_codes]

        if new_practices:
            gp_objects = [GpPractice(**gp) for gp in new_practices]
            session.bulk_save_objects(gp_objects)
            session.commit()
            print(f"✓ Inserted {len(new_practices)} GP practice records")
        else:
            print("✓ All GP practices already exist")

        # Display summary
        total_count = session.query(GpPractice).count()
        print(f"✓ Total GP practices in database: {total_count}")

    except Exception as e:
        session.rollback()
        print(f"✗ Error seeding GP practices: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    seed_gp_practices()
