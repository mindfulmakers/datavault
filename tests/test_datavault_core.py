from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from datavault_core.app import create_app
from datavault_core.services import ApprovalWebhookSender, DataVaultServices
from datavault_core.settings import Settings


class FakeApprovalSender(ApprovalWebhookSender):
    def __init__(self) -> None:
        super().__init__(timeout_seconds=0.1)
        self.payloads: list[dict[str, object]] = []

    async def send(self, webhook_url: str, payload: dict[str, object]) -> None:
        self.payloads.append({"webhook_url": webhook_url, **payload})


def build_client(tmp_path: Path) -> tuple[TestClient, FakeApprovalSender]:
    sender = FakeApprovalSender()
    settings = Settings(
        database_path=tmp_path / "datavault.sqlite3",
        public_base_url="http://testserver",
        guardian_strategy="stub",
    )
    services = DataVaultServices(settings, approval_sender=sender)
    return TestClient(create_app(settings, services=services)), sender


def register_app_and_token(client: TestClient, name: str) -> dict[str, str]:
    registration = client.post(
        "/v1/apps/register",
        json={
            "display_name": name,
            "webhook_url": "https://example.com/approval-webhook",
        },
    )
    assert registration.status_code == 200
    registration_body = registration.json()
    token = client.post(
        "/v1/oauth/token",
        json={
            "consumer_id": registration_body["consumer_id"],
            "client_secret": registration_body["client_secret"],
        },
    )
    assert token.status_code == 200
    return {
        "consumer_id": registration_body["consumer_id"],
        "access_token": token.json()["access_token"],
    }


def test_registry_lists_installed_plugins(tmp_path: Path) -> None:
    client, _ = build_client(tmp_path)
    response = client.get("/v1/registry")
    assert response.status_code == 200
    body = response.json()
    assert {item["type_id"] for item in body["data_types"]} == {"location", "messages"}
    assert {item["provider_id"] for item in body["providers"]} == {
        "garmin_location",
        "ios_location",
        "messages_demo",
    }
    location_type = next(
        item for item in body["data_types"] if item["type_id"] == "location"
    )
    assert {method["name"] for method in location_type["query_methods"]} == {
        "history",
        "latest",
    }


def test_location_queries_require_approval_then_honor_snooze_scope(tmp_path: Path) -> None:
    client, sender = build_client(tmp_path)
    app_a = register_app_and_token(client, "Mobile App")
    app_b = register_app_and_token(client, "Partner App")

    ingest = client.post(
        "/v1/providers/ios_location/ingest",
        json={
            "device_id": "iphone-1",
            "samples": [
                {
                    "id": "ios-1",
                    "timestamp": "2026-03-14T15:30:00Z",
                    "coords": {"lat": 40.7128, "lon": -74.0060},
                    "accuracy_meters": 12.5,
                }
            ],
        },
    )
    assert ingest.status_code == 200
    assert ingest.json()["accepted_count"] == 1

    pending = client.post(
        "/v1/types/location/queries/latest",
        headers={"Authorization": f"Bearer {app_a['access_token']}"},
        json={"params": {}},
    )
    assert pending.status_code == 202
    approval_id = pending.json()["approval_id"]
    assert sender.payloads[-1]["approval_id"] == approval_id

    decision = client.post(
        f"/v1/approvals/{approval_id}/decision",
        json={
            "approval_token": sender.payloads[-1]["approval_token"],
            "decision": "approve",
            "snooze_minutes": 30,
        },
    )
    assert decision.status_code == 200

    approved = client.post(
        "/v1/types/location/queries/latest",
        headers={"Authorization": f"Bearer {app_a['access_token']}"},
        json={"params": {}},
    )
    assert approved.status_code == 200
    assert approved.json()["count"] == 1

    other_type = client.post(
        "/v1/types/messages/queries/recent",
        headers={"Authorization": f"Bearer {app_a['access_token']}"},
        json={"params": {}},
    )
    assert other_type.status_code == 202

    other_consumer = client.post(
        "/v1/types/location/queries/latest",
        headers={"Authorization": f"Bearer {app_b['access_token']}"},
        json={"params": {}},
    )
    assert other_consumer.status_code == 202


def test_location_records_unify_ios_and_garmin_sources(tmp_path: Path) -> None:
    client, sender = build_client(tmp_path)
    app = register_app_and_token(client, "Web App")

    client.post(
        "/v1/providers/ios_location/ingest",
        json={
            "device_id": "iphone-1",
            "samples": [
                {
                    "id": "ios-1",
                    "timestamp": "2026-03-14T10:00:00Z",
                    "coords": {"lat": 40.0, "lon": -73.0},
                    "accuracy_meters": 10.0,
                }
            ],
        },
    )
    client.post(
        "/v1/providers/garmin_location/ingest",
        json={
            "watch_id": "garmin-1",
            "samples": [
                {
                    "sample_id": "garmin-1",
                    "captured_at": "2026-03-14T11:00:00Z",
                    "position": {"latitude": 41.0, "longitude": -72.5},
                    "accuracy": 5.0,
                }
            ],
        },
    )

    pending = client.get(
        "/v1/types/location/records",
        headers={"Authorization": f"Bearer {app['access_token']}"},
    )
    approval_id = pending.json()["approval_id"]
    client.post(
        f"/v1/approvals/{approval_id}/decision",
        json={
            "approval_token": sender.payloads[-1]["approval_token"],
            "decision": "approve",
            "snooze_minutes": 15,
        },
    )

    response = client.get(
        "/v1/types/location/records",
        headers={"Authorization": f"Bearer {app['access_token']}"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2
    assert {item["provider_id"] for item in body["items"]} == {
        "garmin_location",
        "ios_location",
    }


def test_messages_query_supports_summary_mode(tmp_path: Path) -> None:
    client, sender = build_client(tmp_path)
    app = register_app_and_token(client, "AI Agent")

    ingest = client.post(
        "/v1/providers/messages_demo/ingest",
        json={
            "messages": [
                {
                    "id": "msg-1",
                    "from": "alex@example.com",
                    "to": ["gabe@example.com"],
                    "body": "Meet me at the cafe later.",
                    "thread_id": "thread-1",
                    "sent_at": "2026-03-14T09:00:00Z",
                },
                {
                    "id": "msg-2",
                    "from": "gabe@example.com",
                    "to": ["alex@example.com"],
                    "body": "Sounds good.",
                    "thread_id": "thread-1",
                    "sent_at": "2026-03-14T09:05:00Z",
                },
            ]
        },
    )
    assert ingest.status_code == 200

    pending = client.post(
        "/v1/types/messages/queries/thread",
        headers={"Authorization": f"Bearer {app['access_token']}"},
        json={
            "params": {"participant": "alex@example.com"},
            "response_mode": "summary",
        },
    )
    approval_id = pending.json()["approval_id"]
    client.post(
        f"/v1/approvals/{approval_id}/decision",
        json={
            "approval_token": sender.payloads[-1]["approval_token"],
            "decision": "approve",
            "snooze_minutes": 10,
        },
    )

    response = client.post(
        "/v1/types/messages/queries/thread",
        headers={"Authorization": f"Bearer {app['access_token']}"},
        json={
            "params": {"participant": "alex@example.com"},
            "response_mode": "summary",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2
    assert "returned 2 item" in body["summary"]
