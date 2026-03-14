from __future__ import annotations

from datetime import datetime

from datavault_plugin_sdk import DataProviderPlugin, JsonObject, NormalizedRecordInput


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class IOSLocationProviderPlugin(DataProviderPlugin):
    provider_id = "ios_location"
    display_name = "iOS Location"
    description = "Normalizes iOS location samples into the shared location type."
    normalized_type_ids = ("location",)

    def normalize_payload(self, payload: JsonObject) -> list[NormalizedRecordInput]:
        samples = payload.get("samples")
        if not isinstance(samples, list):
            raise ValueError("ios_location payload requires a samples list")
        records: list[NormalizedRecordInput] = []
        device_id = str(payload.get("device_id", "ios-device"))
        for sample in samples:
            coords = sample.get("coords", {})
            records.append(
                NormalizedRecordInput(
                    type_id="location",
                    provider_id=self.provider_id,
                    source_id=str(sample.get("id", sample.get("timestamp"))),
                    occurred_at=_parse_datetime(str(sample["timestamp"])),
                    captured_at=_parse_datetime(str(sample["timestamp"])),
                    payload={
                        "latitude": float(coords["lat"]),
                        "longitude": float(coords["lon"]),
                        "accuracy_meters": float(sample.get("accuracy_meters", 0.0)),
                        "source_device": device_id,
                    },
                )
            )
        return records


def get_plugin() -> IOSLocationProviderPlugin:
    return IOSLocationProviderPlugin()
