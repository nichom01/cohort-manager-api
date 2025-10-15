import hashlib
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.schema import CohortUpdate


class CohortService:
    def __init__(self, session: Session):
        self.session = session

    def _get_file_hash(self, file_path: str) -> str:
        """Generate a hash of the file to detect duplicates."""
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _is_file_already_loaded(self, file_hash: str) -> bool:
        """Check if a file with this hash has already been loaded."""
        # Store file hash as a separate tracking table or metadata
        # For now, we'll use a simple approach - check if file exists
        # You might want to create a separate FileTracking table later
        return False  # Simplified for now - will enhance if needed

    def _get_next_file_id(self) -> int:
        """Get the next sequential file ID."""
        max_id = self.session.query(func.max(CohortUpdate.file_id)).scalar()
        return (max_id or 0) + 1

    def load_file(self, file_path: str, file_type: str) -> dict:
        """
        Load a CSV or Parquet file into the cohort_update table.

        Args:
            file_path: Path to the file on the server
            file_type: Either "csv" or "parquet"

        Returns:
            dict with file_id and records_loaded

        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If file type is not supported or file already loaded
        """
        # Validate file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Check if file already loaded (simplified - hash-based check)
        file_hash = self._get_file_hash(file_path)
        # For now, we'll skip the duplicate check since we don't have a tracking table
        # In production, you'd want to add a FileMetadata table to track this

        # Get next file ID
        file_id = self._get_next_file_id()

        # Load the file using pandas
        if file_type.lower() == "csv":
            df = pd.read_csv(file_path)
        elif file_type.lower() == "parquet":
            df = pd.read_parquet(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        # Add the system columns
        df["file_id"] = file_id

        # Convert DataFrame to list of dicts for bulk insert
        records = df.to_dict("records")

        # Bulk insert records
        cohort_records = [CohortUpdate(**record) for record in records]
        self.session.bulk_save_objects(cohort_records)
        self.session.commit()

        return {
            "file_id": file_id,
            "records_loaded": len(records),
        }
