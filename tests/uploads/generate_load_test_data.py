"""
Generate load test data with 25,000 cohort records.

This script creates CSV and Parquet files with 25,000 records for load testing
the cohort data loading API. All records are of type 'ADD' with realistic test data.

Usage:
    python generate_load_test_data.py
"""

import pandas as pd
from datetime import datetime, timedelta
import random

# Configuration
NUM_RECORDS = 25000
OUTPUT_CSV = "test_cohort_data_25000_records.csv"
OUTPUT_PARQUET = "test_cohort_data_25000_records.parquet"

# Sample data pools for realistic variation
PREFIXES = ["Mr", "Mrs", "Miss", "Ms", "Dr", ""]
GIVEN_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
]
FAMILY_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
]
STREET_NAMES = [
    "High Street", "Main Road", "Church Lane", "Station Road", "Park Avenue",
    "Victoria Road", "Green Lane", "Manor Road", "King Street", "Queen Street",
    "London Road", "Mill Lane", "The Avenue", "Windsor Road", "Albert Street",
]
TOWNS = [
    "London", "Birmingham", "Manchester", "Leeds", "Liverpool", "Newcastle",
    "Sheffield", "Bristol", "Reading", "Oxford", "Cambridge", "Brighton",
    "Southampton", "Plymouth", "Norwich", "Exeter", "York", "Bath",
]
COUNTIES = [
    "Greater London", "West Midlands", "Greater Manchester", "West Yorkshire",
    "Merseyside", "Tyne and Wear", "South Yorkshire", "Bristol", "Berkshire",
    "Oxfordshire", "Cambridgeshire", "East Sussex", "Hampshire", "Devon",
]
GP_PRACTICES = [
    "A12345", "B23456", "C34567", "D45678", "E56789", "F67890", "G78901",
    "H89012", "J90123", "K01234", "L12345", "M23456", "N34567", "P45678",
]
LANGUAGES = ["English", "Welsh", "Polish", "Urdu", "Bengali", "Gujarati", "Punjabi"]


def generate_nhs_number(index):
    """Generate a unique NHS number starting from 9000000001."""
    return 9000000000 + index + 1


def generate_postcode(index):
    """Generate a realistic UK postcode."""
    areas = ["SW", "SE", "NW", "NE", "E", "W", "N", "S", "B", "M", "L", "LS"]
    area = random.choice(areas)
    district = random.randint(1, 99)
    sector = random.randint(0, 9)
    unit = f"{chr(65 + random.randint(0, 25))}{chr(65 + random.randint(0, 25))}"
    return f"{area}{district} {sector}{unit}"


def generate_date_of_birth(index):
    """Generate DOB for ages between 50-74 (screening age range)."""
    today = datetime.now()
    age = 50 + (index % 25)  # Ages 50-74
    dob = today - timedelta(days=age * 365 + random.randint(0, 364))
    return dob.strftime("%Y%m%d")


def generate_phone_number():
    """Generate a realistic UK phone number."""
    prefixes = ["01", "02", "07"]
    prefix = random.choice(prefixes)
    if prefix == "07":  # Mobile
        return f"07{random.randint(100000000, 999999999)}"
    else:  # Landline
        return f"{prefix}{random.randint(10, 99)}{random.randint(10000000, 99999999)}"


def generate_email(given_name, family_name, index):
    """Generate a realistic email address."""
    domains = ["gmail.com", "yahoo.co.uk", "hotmail.com", "outlook.com", "btinternet.com"]
    return f"{given_name.lower()}.{family_name.lower()}{index % 100}@{random.choice(domains)}"


def generate_address(index):
    """Generate a realistic UK address."""
    house_num = random.randint(1, 999)
    street = random.choice(STREET_NAMES)
    town = random.choice(TOWNS)
    county = random.choice(COUNTIES)

    return {
        "address_line_1": f"{house_num} {street}",
        "address_line_2": f"Apartment {random.randint(1, 50)}" if index % 3 == 0 else "",
        "address_line_3": town,
        "address_line_4": county,
        "address_line_5": "United Kingdom",
    }


def generate_record(index):
    """Generate a single cohort record."""
    nhs_number = generate_nhs_number(index)
    given_name = random.choice(GIVEN_NAMES)
    family_name = random.choice(FAMILY_NAMES)
    address = generate_address(index)

    # Current timestamp for change tracking
    change_timestamp = int(datetime.now().timestamp() * 1000)
    today = datetime.now().strftime("%Y%m%d")

    record = {
        "record_type": "ADD",
        "change_time_stamp": change_timestamp,
        "serial_change_number": 100000 + index,
        "nhs_number": nhs_number,
        "superseded_by_nhs_number": "",
        "primary_care_provider": random.choice(GP_PRACTICES),
        "primary_care_effective_from_date": today,
        "current_posting": random.choice(GP_PRACTICES) if index % 10 == 0 else "",
        "current_posting_effective_from_date": today if index % 10 == 0 else "",
        "name_prefix": random.choice(PREFIXES),
        "given_name": given_name,
        "other_given_name": random.choice(GIVEN_NAMES) if index % 5 == 0 else "",
        "family_name": family_name,
        "previous_family_name": random.choice(FAMILY_NAMES) if index % 8 == 0 else "",
        "date_of_birth": generate_date_of_birth(index),
        "gender": random.choice([1, 2]),  # 1=Male, 2=Female
        "address_line_1": address["address_line_1"],
        "address_line_2": address["address_line_2"],
        "address_line_3": address["address_line_3"],
        "address_line_4": address["address_line_4"],
        "address_line_5": address["address_line_5"],
        "postcode": generate_postcode(index),
        "paf_key": f"PAF{random.randint(10000000, 99999999)}",
        "address_effective_from_date": today,
        "reason_for_removal": "",
        "reason_for_removal_effective_from_date": "",
        "date_of_death": "",
        "death_status": "",
        "home_telephone_number": generate_phone_number() if index % 3 != 0 else "",
        "home_telephone_effective_from_date": today if index % 3 != 0 else "",
        "mobile_telephone_number": generate_phone_number() if index % 2 == 0 else "",
        "mobile_telephone_effective_from_date": today if index % 2 == 0 else "",
        "email_address": generate_email(given_name, family_name, index) if index % 4 == 0 else "",
        "email_address_effective_from_date": today if index % 4 == 0 else "",
        "preferred_language": random.choice(LANGUAGES) if index % 10 == 0 else "English",
        "is_interpreter_required": True if index % 20 == 0 else False,
        "eligibility": True,
        "invalid_flag": False,
    }

    return record


def main():
    """Generate load test data files."""
    print(f"Generating {NUM_RECORDS} test records...")

    # Generate all records
    records = [generate_record(i) for i in range(NUM_RECORDS)]

    # Create DataFrame
    df = pd.DataFrame(records)

    print(f"Generated {len(df)} records")
    print(f"NHS number range: {df['nhs_number'].min()} to {df['nhs_number'].max()}")

    # Save as CSV
    print(f"\nSaving CSV to {OUTPUT_CSV}...")
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"CSV file size: {df.to_csv(index=False).__sizeof__() / 1024:.2f} KB")

    # Save as Parquet
    print(f"\nSaving Parquet to {OUTPUT_PARQUET}...")
    df.to_parquet(OUTPUT_PARQUET, index=False, engine='pyarrow')

    print("\n✓ Load test data generation complete!")
    print(f"\nFiles created:")
    print(f"  - {OUTPUT_CSV}")
    print(f"  - {OUTPUT_PARQUET}")
    print(f"\nTest with:")
    print(f'  curl -X POST "http://localhost:8000/api/v1/cohort/load-file" \\')
    print(f'    -H "Content-Type: application/json" \\')
    print(f'    -d \'{{"file_path": "tests/uploads/{OUTPUT_CSV}", "file_type": "csv"}}\'')


if __name__ == "__main__":
    main()
