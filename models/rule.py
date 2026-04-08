# models/rule.py
#
# Purpose:
#   Defines the Rule data model using Pydantic. Enforces field-level validation
#   for all rule objects produced by the extractor and consumed by the validator
#   and Firestore client. Acts as the single source of truth for what a valid
#   rule looks like across the pipeline.
#
# Fields:
#   rule_id       (str)  — unique identifier, must match R<digits> format (e.g. R001)
#   category      (str)  — topic label from KNOWN_CATEGORIES; falls back to "General"
#   title         (str)  — short human-readable rule name, must be non-empty
#   description   (str)  — full rule statement, minimum 20 characters
#   source        (str)  — origin URL or file path, must be non-empty
#   version       (int)  — increments each time the rule content changes (default: 1)
#   content_hash  (str)  — SHA-256 of title + description, auto-set on creation
#
# Key class:
#   Rule — Pydantic BaseModel with the following convenience methods:
#     - to_dict()                      serialize to plain dict
#     - from_dict(data)                deserialize from plain dict
#     - from_extractor_output(data)    build from raw extractor JSON output
#
# Dependencies:
#   - utils/hashing.py (hash_rule_content) for auto-setting content_hash

import re
from typing import Optional
from pydantic import BaseModel, field_validator, model_validator


KNOWN_CATEGORIES = {
    "Accessibility", "Aircraft Safety", "Assistance", "Baggage", "Boarding",
    "Booking", "Check-in", "Complaints", "Compensation", "Documentation",
    "General", "Information", "Infrastructure", "Legal Rights", "Medical",
    "Pre-Flight", "Safety", "Security", "Service", "Training", "Travel Policy",
}


class Rule(BaseModel):
    rule_id:     str
    category:    str
    title:       str
    description: str
    source:      str
    version:     Optional[int] = 1       # increments each time a rule is updated
    content_hash: Optional[str] = None   # set automatically from description

    # ── field validators ───────────────────────────────────────────────────────

    @field_validator("rule_id")
    @classmethod
    def rule_id_format(cls, v: str) -> str:
        if not re.fullmatch(r"R\d+", v):
            raise ValueError(f"rule_id must match R<number> format, got '{v}'")
        return v

    @field_validator("title")
    @classmethod
    def title_not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("title must not be empty")
        return v

    @field_validator("description")
    @classmethod
    def description_min_length(cls, v: str) -> str:
        v = v.strip()
        if len(v) < 20:
            raise ValueError(f"description too short ({len(v)} chars) — must be at least 20")
        return v

    @field_validator("category")
    @classmethod
    def category_known(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("category must not be empty")
        if v not in KNOWN_CATEGORIES:
            # warn but don't block — unknown category gets normalised to "General"
            return "General"
        return v

    @field_validator("source")
    @classmethod
    def source_not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("source must not be empty")
        return v

    # ── auto-set content_hash from description ─────────────────────────────────

    @model_validator(mode="after")
    def set_content_hash(self) -> "Rule":
        from utils.hashing import hash_rule_content
        if not self.content_hash:
            self.content_hash = hash_rule_content(self.description, self.title)
        return self

    # ── convenience methods ────────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: dict) -> "Rule":
        return cls(**data)

    @classmethod
    def from_extractor_output(cls, data: dict) -> "Rule":
        """
        Build a Rule from raw extractor output.
        Tolerates missing optional fields (version, content_hash).
        """
        return cls(
            rule_id=data.get("rule_id", "R000"),
            category=data.get("category", "General"),
            title=data.get("title", ""),
            description=data.get("description", ""),
            source=data.get("source", "unknown"),
        )
