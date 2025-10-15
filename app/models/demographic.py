from pydantic import BaseModel


class LoadDemographicsByFileRequest(BaseModel):
    file_id: int


class LoadDemographicByRecordRequest(BaseModel):
    cohort_update_id: int


class LoadDemographicsResponse(BaseModel):
    records_loaded: int
    records_inserted: int | None = None
    records_updated: int | None = None
    action: str | None = None
    message: str
