from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from datavault_plugin_sdk import (
    DataTypePlugin,
    JsonObject,
    NormalizedRecord,
    QueryMethodSpec,
    QueryResult,
)


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class LocationDataTypePlugin(DataTypePlugin):
    type_id = "location"
    display_name = "Location"
    description = "Normalized location history across multiple device providers."
    record_schema = {
        "type": "object",
        "properties": {
            "latitude": {"type": "number"},
            "longitude": {"type": "number"},
            "accuracy_meters": {"type": "number"},
            "source_device": {"type": "string"},
        },
        "required": ["latitude", "longitude"],
    }

    def query_methods(self) -> Sequence[QueryMethodSpec]:
        return [
            QueryMethodSpec(
                name="latest",
                description="Return the most recent normalized location sample.",
                params_schema={"type": "object", "properties": {}},
                returns_many=False,
            ),
            QueryMethodSpec(
                name="history",
                description="Return recent normalized location samples.",
                params_schema={
                    "type": "object",
                    "properties": {
                        "start_at": {"type": "string", "format": "date-time"},
                        "end_at": {"type": "string", "format": "date-time"},
                        "limit": {"type": "integer", "minimum": 1, "maximum": 500},
                    },
                },
            ),
        ]

    def execute_query(
        self,
        query_name: str,
        records: Sequence[NormalizedRecord],
        params: JsonObject,
    ) -> QueryResult:
        ordered = sorted(records, key=lambda item: item.occurred_at, reverse=True)
        if query_name == "latest":
            items = [ordered[0].model_dump(mode="json")] if ordered else []
            return QueryResult(items=items)
        if query_name == "history":
            start_at = _parse_datetime(params.get("start_at")) if params else None
            end_at = _parse_datetime(params.get("end_at")) if params else None
            limit = int(params.get("limit", 50))
            filtered = []
            for record in ordered:
                if start_at is not None and record.occurred_at < start_at:
                    continue
                if end_at is not None and record.occurred_at > end_at:
                    continue
                filtered.append(record.model_dump(mode="json"))
                if len(filtered) >= limit:
                    break
            return QueryResult(items=filtered)
        raise ValueError(f"unknown location query: {query_name}")


def get_plugin() -> LocationDataTypePlugin:
    return LocationDataTypePlugin()
