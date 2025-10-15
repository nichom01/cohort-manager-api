"""
Transformation rules for participant data.

This module defines transformation rules that can modify demographic and
participant management records based on conditions or character replacements.

Two types of rules:
1. Conditional rules: If condition is true, update specified fields
2. Replacement rules: Replace character X with Y in specified fields
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Optional

from app.db.schema import ParticipantDemographic, ParticipantManagement


@dataclass
class TransformationResult:
    """Result of a transformation rule execution."""

    rule_name: str
    applied: bool
    changes: dict[str, Any]  # Field name -> new value
    message: str


class TransformationRule(ABC):
    """Base class for transformation rules."""

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def apply(
        self,
        demographic: Optional[ParticipantDemographic],
        participant_management: Optional[ParticipantManagement],
    ) -> TransformationResult:
        """
        Apply the transformation rule to the participant records.

        Args:
            demographic: Participant demographic record (can be modified)
            participant_management: Participant management record (can be modified)

        Returns:
            TransformationResult describing what was changed
        """
        pass


class ConditionalTransformationRule(TransformationRule):
    """
    Type 1: Conditional transformation rule.

    If a condition evaluates as true, update specified fields with values.
    These rules can be executed in parallel.
    """

    def __init__(
        self,
        name: str,
        condition: Callable[
            [Optional[ParticipantDemographic], Optional[ParticipantManagement]], bool
        ],
        updates: dict[str, tuple[str, Any]],  # field_name -> (record_type, new_value)
    ):
        """
        Initialize a conditional transformation rule.

        Args:
            name: Name of the rule
            condition: Function that returns True if transformation should be applied
            updates: Dictionary mapping field names to (record_type, new_value) tuples
                    record_type can be "demographic" or "management"
        """
        super().__init__(name)
        self.condition = condition
        self.updates = updates

    def apply(
        self,
        demographic: Optional[ParticipantDemographic],
        participant_management: Optional[ParticipantManagement],
    ) -> TransformationResult:
        """Apply the conditional transformation rule."""
        # Evaluate condition
        if not self.condition(demographic, participant_management):
            return TransformationResult(
                rule_name=self.name,
                applied=False,
                changes={},
                message="Condition not met",
            )

        # Apply updates
        changes = {}
        for field_name, (record_type, new_value) in self.updates.items():
            if record_type == "demographic" and demographic:
                old_value = getattr(demographic, field_name, None)
                setattr(demographic, field_name, new_value)
                changes[f"demographic.{field_name}"] = {
                    "old": old_value,
                    "new": new_value,
                }
            elif record_type == "management" and participant_management:
                old_value = getattr(participant_management, field_name, None)
                setattr(participant_management, field_name, new_value)
                changes[f"management.{field_name}"] = {
                    "old": old_value,
                    "new": new_value,
                }

        return TransformationResult(
            rule_name=self.name,
            applied=True,
            changes=changes,
            message=f"Applied {len(changes)} field updates",
        )


class CharacterReplacementRule(TransformationRule):
    """
    Type 2: Character replacement rule.

    Replace character X with Y in specified fields.
    These rules can be grouped and run together efficiently.
    """

    def __init__(
        self,
        name: str,
        replacements: list[tuple[str, str]],  # List of (old_char, new_char)
        fields: list[tuple[str, str]],  # List of (record_type, field_name)
    ):
        """
        Initialize a character replacement rule.

        Args:
            name: Name of the rule
            replacements: List of (old_character, new_character) tuples
            fields: List of (record_type, field_name) tuples to apply replacements to
                   record_type can be "demographic" or "management"
        """
        super().__init__(name)
        self.replacements = replacements
        self.fields = fields

    def apply(
        self,
        demographic: Optional[ParticipantDemographic],
        participant_management: Optional[ParticipantManagement],
    ) -> TransformationResult:
        """Apply the character replacement rule."""
        changes = {}

        for record_type, field_name in self.fields:
            record = (
                demographic if record_type == "demographic" else participant_management
            )

            if not record:
                continue

            old_value = getattr(record, field_name, None)

            # Only process string fields
            if not isinstance(old_value, str):
                continue

            # Apply all replacements
            new_value = old_value
            for old_char, new_char in self.replacements:
                new_value = new_value.replace(old_char, new_char)

            # Only record changes if value actually changed
            if new_value != old_value:
                setattr(record, field_name, new_value)
                changes[f"{record_type}.{field_name}"] = {
                    "old": old_value,
                    "new": new_value,
                }

        return TransformationResult(
            rule_name=self.name,
            applied=len(changes) > 0,
            changes=changes,
            message=f"Applied replacements to {len(changes)} fields"
            if changes
            else "No changes needed",
        )


# Example conditional transformation rules


def create_example_conditional_rules() -> list[ConditionalTransformationRule]:
    """Create example conditional transformation rules."""

    # Rule 1: If no postcode, set a default invalid postcode marker
    def no_postcode_condition(
        demographic: Optional[ParticipantDemographic],
        management: Optional[ParticipantManagement],
    ) -> bool:
        return demographic is not None and not demographic.post_code

    rule1 = ConditionalTransformationRule(
        name="set_default_postcode",
        condition=no_postcode_condition,
        updates={"post_code": ("demographic", "UNKNOWN")},
    )

    # Rule 2: If eligibility flag is 0, set screening status to CEASED
    def not_eligible_condition(
        demographic: Optional[ParticipantDemographic],
        management: Optional[ParticipantManagement],
    ) -> bool:
        return management is not None and management.eligibility_flag == 0

    rule2 = ConditionalTransformationRule(
        name="set_ceased_status_for_ineligible",
        condition=not_eligible_condition,
        updates={
            "participant_screening_status": ("management", "CEASED"),
            "screening_ceased_reason": ("management", "INELIGIBLE"),
        },
    )

    # Rule 3: If blocked flag is set, add exception flag
    def blocked_condition(
        demographic: Optional[ParticipantDemographic],
        management: Optional[ParticipantManagement],
    ) -> bool:
        return management is not None and management.blocked_flag == 1

    rule3 = ConditionalTransformationRule(
        name="set_exception_for_blocked",
        condition=blocked_condition,
        updates={"exception_flag": ("management", 1)},
    )

    return [rule1, rule2, rule3]


# Example character replacement rules


def create_example_replacement_rules() -> list[CharacterReplacementRule]:
    """Create example character replacement rules."""

    # Rule 1: Remove special characters from names
    rule1 = CharacterReplacementRule(
        name="remove_special_chars_from_names",
        replacements=[
            ("'", ""),  # Remove apostrophes
            ("-", " "),  # Replace hyphens with spaces
            (".", ""),  # Remove periods
        ],
        fields=[
            ("demographic", "given_name"),
            ("demographic", "family_name"),
            ("demographic", "other_given_name"),
        ],
    )

    # Rule 2: Normalize postcodes (remove spaces, convert to uppercase)
    rule2 = CharacterReplacementRule(
        name="normalize_postcodes",
        replacements=[(" ", "")],  # Remove spaces
        fields=[("demographic", "post_code")],
    )

    # Rule 3: Clean phone numbers (remove common separators)
    rule3 = CharacterReplacementRule(
        name="clean_phone_numbers",
        replacements=[
            (" ", ""),  # Remove spaces
            ("-", ""),  # Remove hyphens
            ("(", ""),  # Remove opening parentheses
            (")", ""),  # Remove closing parentheses
        ],
        fields=[
            ("demographic", "telephone_number_home"),
            ("demographic", "telephone_number_mob"),
        ],
    )

    return [rule1, rule2, rule3]


# Pre-defined rule sets
ALL_CONDITIONAL_RULES = create_example_conditional_rules()
ALL_REPLACEMENT_RULES = create_example_replacement_rules()
