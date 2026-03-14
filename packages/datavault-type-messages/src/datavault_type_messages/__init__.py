from __future__ import annotations

from collections.abc import Sequence

from datavault_plugin_sdk import (
    DataTypePlugin,
    JsonObject,
    NormalizedRecord,
    QueryMethodSpec,
    QueryResult,
)


class MessagesDataTypePlugin(DataTypePlugin):
    type_id = "messages"
    display_name = "Messages"
    description = "Normalized message records surfaced through a unified API."
    record_schema = {
        "type": "object",
        "properties": {
            "sender": {"type": "string"},
            "recipients": {"type": "array", "items": {"type": "string"}},
            "body": {"type": "string"},
            "thread_id": {"type": "string"},
        },
        "required": ["sender", "body"],
    }

    def query_methods(self) -> Sequence[QueryMethodSpec]:
        return [
            QueryMethodSpec(
                name="recent",
                description="Return recent normalized messages.",
                params_schema={
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "minimum": 1, "maximum": 500},
                    },
                },
            ),
            QueryMethodSpec(
                name="thread",
                description="Return messages for a participant or thread.",
                params_schema={
                    "type": "object",
                    "properties": {
                        "participant": {"type": "string"},
                        "thread_id": {"type": "string"},
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
        if query_name == "recent":
            limit = int(params.get("limit", 25))
            return QueryResult(
                items=[record.model_dump(mode="json") for record in ordered[:limit]]
            )
        if query_name == "thread":
            participant = params.get("participant")
            thread_id = params.get("thread_id")
            if not participant and not thread_id:
                raise ValueError("thread query requires participant or thread_id")
            limit = int(params.get("limit", 50))
            items: list[dict[str, object]] = []
            for record in ordered:
                payload = record.payload
                participants = [payload.get("sender", "")] + list(
                    payload.get("recipients", [])
                )
                if participant and participant not in participants:
                    continue
                if thread_id and payload.get("thread_id") != thread_id:
                    continue
                items.append(record.model_dump(mode="json"))
                if len(items) >= limit:
                    break
            return QueryResult(items=items)
        raise ValueError(f"unknown messages query: {query_name}")


def get_plugin() -> MessagesDataTypePlugin:
    return MessagesDataTypePlugin()
