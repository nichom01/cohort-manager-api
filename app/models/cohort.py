from pydantic import BaseModel


class LoadFileRequest(BaseModel):
    file_path: str
    file_type: str  # "csv" or "parquet"


class LoadFileResponse(BaseModel):
    file_id: int
    records_loaded: int
    filename: str
    upload_timestamp: str
    file_hash: str
    message: str
