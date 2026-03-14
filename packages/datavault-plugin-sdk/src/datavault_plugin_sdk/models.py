from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

JsonObject = dict[str, Any]


class QueryMethodSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    description: str
    params_schema: JsonObject = Field(default_factory=lambda: {"type": "object"})
    returns_many: bool = True


class DataTypeDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type_id: str
    display_name: str
    version: str
    description: str
    record_schema: JsonObject
    query_methods: list[QueryMethodSpec]


class DataProviderDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider_id: str
    display_name: str
    version: str
    description: str
    normalized_type_ids: list[str]


class DataConsumerDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    consumer_type_id: str
    display_name: str
    version: str
    description: str


class NormalizedRecordInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type_id: str
    provider_id: str
    source_id: str
    occurred_at: datetime
    captured_at: datetime | None = None
    payload: JsonObject


class NormalizedRecord(NormalizedRecordInput):
    model_config = ConfigDict(extra="forbid")

    record_id: str
    ingested_at: datetime


class QueryResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[JsonObject]
    summary: str | None = None
    next_cursor: str | None = None
