from __future__ import annotations

from datetime import datetime

from datavault_plugin_sdk import DataProviderPlugin, JsonObject, NormalizedRecordInput


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class GarminLocationProviderPlugin(DataProviderPlugin):
    provider_id = "garmin_location"
    display_name = "Garmin Watch Location"
    description = "Normalizes Garmin watch location samples into the shared location type."
    normalized_type_ids = ("location",)

    def normalize_payload(self, payload: JsonObject) -> list[NormalizedRecordInput]:
        samples = payload.get("samples")
        if not isinstance(samples, list):
            raise ValueError("garmin_location payload requires a samples list")
        records: list[NormalizedRecordInput] = []
        device_id = str(payload.get("watch_id", "garmin-watch"))
        for sample in samples:
            position = sample.get("position", {})
            records.append(
                NormalizedRecordInput(
                    type_id="location",
                    provider_id=self.provider_id,
                    source_id=str(sample.get("sample_id", sample.get("captured_at"))),
                    occurred_at=_parse_datetime(str(sample["captured_at"])),
                    captured_at=_parse_datetime(str(sample["captured_at"])),
                    payload={
                        "latitude": float(position["latitude"]),
                        "longitude": float(position["longitude"]),
                        "accuracy_meters": float(sample.get("accuracy", 0.0)),
                        "source_device": device_id,
                    },
                )
            )
        return records


def get_plugin() -> GarminLocationProviderPlugin:
    return GarminLocationProviderPlugin()
