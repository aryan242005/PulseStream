from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class EventIn(BaseModel):
    key: str = Field(min_length=1, max_length=200)
    kind: str = Field(min_length=1, max_length=100)
    payload: dict[str, Any] = Field(default_factory=dict)
    event_id: UUID = Field(default_factory=uuid4)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class EventEnvelope(EventIn):
    shard: int


class ShipmentIn(BaseModel):
    shipment_id: str = Field(min_length=1, max_length=100, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    description: str = Field(min_length=1, max_length=200)
    min_temp_c: float = Field(ge=-80, le=80)
    max_temp_c: float = Field(ge=-80, le=80)

    def model_post_init(self, __context):
        if self.min_temp_c >= self.max_temp_c:
            raise ValueError("min_temp_c must be less than max_temp_c")


class TemperatureReadingIn(BaseModel):
    sensor_id: str = Field(min_length=1, max_length=100)
    temperature_c: float = Field(ge=-100, le=100)
    event_id: UUID = Field(default_factory=uuid4)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

