# models/rule.py
#
# Purpose:
#   Defines the Airline, Airport, and Service data models using Pydantic.
#   Each entity (airline or airport) contains a services array describing
#   PRM-relevant assistance options and policies extracted from that source.
#
# Models:
#   Service  — one PRM service/policy item (type, description, is_presented)
#   Airline  — an airline entity with its services (airline_id, name, source, services)
#   Airport  — an airport entity with its services (airport_id, name, source, services)

from pydantic import BaseModel, field_validator


class Service(BaseModel):
    type: str
    description: str
    is_presented: bool = True

    @field_validator("type")
    @classmethod
    def type_not_empty(cls, v: str) -> str:
        v = v.strip().lower().replace(" ", "_")
        if not v:
            raise ValueError("type must not be empty")
        return v

    @field_validator("description")
    @classmethod
    def description_min_length(cls, v: str) -> str:
        v = v.strip()
        if len(v) < 20:
            raise ValueError(f"description too short ({len(v)} chars) — must be at least 20")
        return v

    def to_dict(self) -> dict:
        return self.model_dump()


class Airline(BaseModel):
    airline_id: str
    name: str
    source: str
    services: list[Service]

    @field_validator("airline_id", "name", "source")
    @classmethod
    def not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("field must not be empty")
        return v

    def to_dict(self) -> dict:
        return {
            "airline_id": self.airline_id,
            "name": self.name,
            "source": self.source,
            "services": [s.to_dict() for s in self.services],
        }


class Airport(BaseModel):
    airport_id: str
    name: str
    source: str
    services: list[Service]

    @field_validator("airport_id", "name", "source")
    @classmethod
    def not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("field must not be empty")
        return v

    def to_dict(self) -> dict:
        return {
            "airport_id": self.airport_id,
            "name": self.name,
            "source": self.source,
            "services": [s.to_dict() for s in self.services],
        }
