from pydantic import BaseModel


class LoadParticipantManagementByFileRequest(BaseModel):
    file_id: int


class LoadParticipantManagementByRecordRequest(BaseModel):
    cohort_update_id: int


class LoadParticipantManagementResponse(BaseModel):
    records_loaded: int
    records_inserted: int | None = None
    records_updated: int | None = None
    action: str | None = None
    message: str
