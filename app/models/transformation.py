from typing import Any

from pydantic import BaseModel


class TransformParticipantRequest(BaseModel):
    nhs_number: int


class TransformBatchRequest(BaseModel):
    nhs_numbers: list[int]


class TransformationResultModel(BaseModel):
    rule_name: str
    applied: bool
    changes: dict[str, Any]
    message: str


class TransformationSummary(BaseModel):
    total_rules: int
    rules_applied: int
    total_field_changes: int


class ParticipantRecords(BaseModel):
    """Container for demographic and participant management records."""

    demographic: dict[str, Any] | None
    participant_management: dict[str, Any] | None


class TransformParticipantResponse(BaseModel):
    nhs_number: int
    inbound: ParticipantRecords
    outbound: ParticipantRecords
    conditional_results: list[TransformationResultModel]
    replacement_results: list[TransformationResultModel]
    summary: TransformationSummary


class TransformBatchSummary(BaseModel):
    total_participants: int
    successful: int
    failed: int


class TransformBatchResponse(BaseModel):
    results: dict[int, Any]  # NHS number -> TransformParticipantResponse or error
    summary: TransformBatchSummary
