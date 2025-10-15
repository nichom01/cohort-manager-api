from pydantic import BaseModel


class ValidateParticipantRequest(BaseModel):
    nhs_number: int


class ValidateBatchRequest(BaseModel):
    nhs_numbers: list[int]


class ValidationResultModel(BaseModel):
    rule_name: str
    passed: bool
    message: str
    severity: str


class ValidateParticipantResponse(BaseModel):
    nhs_number: int
    validation_results: list[ValidationResultModel]
    total_rules: int
    passed_rules: int
    failed_rules: int
    has_errors: bool
    has_warnings: bool


class ValidateBatchResponse(BaseModel):
    results: dict[int, ValidateParticipantResponse]
    total_participants: int
    participants_with_errors: int
    participants_with_warnings: int
