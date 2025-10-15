import hashlib
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.db.schema import CohortUpdate, FileMetadata


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
        existing = (
            self.session.query(FileMetadata)
            .filter(FileMetadata.file_hash == file_hash)
            .first()
        )
        return existing is not None

    def load_file(self, file_path: str, file_type: str) -> dict:
        """
        Load a CSV or Parquet file into the cohort_update table.

        Args:
            file_path: Path to the file on the server
            file_type: Either "csv" or "parquet"

        Returns:
            dict with file_id, records_loaded, filename, upload_timestamp, file_hash

        Raises:
            FileNotFoundError: If the file doesn't exist
            ValueError: If file type is not supported or file already loaded
        """
        # Validate file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Check if file already loaded
        file_hash = self._get_file_hash(file_path)
        if self._is_file_already_loaded(file_hash):
            raise ValueError(
                f"File with hash {file_hash} has already been loaded. Duplicate files are not allowed."
            )

        # Get file metadata
        file_size = os.path.getsize(file_path)
        filename = Path(file_path).name

        # Load the file using pandas
        if file_type.lower() == "csv":
            df = pd.read_csv(file_path)
        elif file_type.lower() == "parquet":
            df = pd.read_parquet(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        records_count = len(df)

        # Create file metadata record
        file_metadata = FileMetadata(
            filename=filename,
            file_path=file_path,
            file_type=file_type.lower(),
            file_hash=file_hash,
            file_size_bytes=file_size,
            records_loaded=records_count,
        )
        self.session.add(file_metadata)
        self.session.flush()  # Get the file_id without committing

        # Add the system columns
        df["file_id"] = file_metadata.file_id

        # Convert DataFrame to list of dicts for bulk insert
        records = df.to_dict("records")

        # Bulk insert records
        cohort_records = [CohortUpdate(**record) for record in records]
        self.session.bulk_save_objects(cohort_records)
        self.session.commit()

        return {
            "file_id": file_metadata.file_id,
            "records_loaded": records_count,
            "filename": filename,
            "upload_timestamp": file_metadata.upload_timestamp.isoformat(),
            "file_hash": file_hash,
        }
