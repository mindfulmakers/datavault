from __future__ import annotations

from datetime import datetime

from datavault_plugin_sdk import DataProviderPlugin, JsonObject, NormalizedRecordInput


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class MessagesDemoProviderPlugin(DataProviderPlugin):
    provider_id = "messages_demo"
    display_name = "Messaging Provider"
    description = "Normalizes message payloads into the shared messages type."
    normalized_type_ids = ("messages",)

    def normalize_payload(self, payload: JsonObject) -> list[NormalizedRecordInput]:
        messages = payload.get("messages")
        if not isinstance(messages, list):
            raise ValueError("messages_demo payload requires a messages list")
        records: list[NormalizedRecordInput] = []
        for message in messages:
            records.append(
                NormalizedRecordInput(
                    type_id="messages",
                    provider_id=self.provider_id,
                    source_id=str(message.get("id", message.get("sent_at"))),
                    occurred_at=_parse_datetime(str(message["sent_at"])),
                    captured_at=_parse_datetime(str(message["sent_at"])),
                    payload={
                        "sender": str(message["from"]),
                        "recipients": [str(value) for value in message.get("to", [])],
                        "body": str(message["body"]),
                        "thread_id": str(message.get("thread_id", "")),
                    },
                )
            )
        return records


def get_plugin() -> MessagesDemoProviderPlugin:
    return MessagesDemoProviderPlugin()
