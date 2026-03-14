from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

from datavault_plugin_sdk import NormalizedRecord

from .guardian import GuardianService
from .plugins import PluginRegistry
from .settings import Settings
from .storage import (
    AppPrincipal,
    ApprovalRecord,
    SQLiteStorage,
)


class ApprovalRequiredError(Exception):
    def __init__(self, approval: ApprovalRecord) -> None:
        self.approval = approval
        super().__init__(approval.approval_id)


@dataclass(slots=True)
class ApprovalWebhookSender:
    timeout_seconds: float

    async def send(self, webhook_url: str, payload: dict[str, Any]) -> None:
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            response = await client.post(webhook_url, json=payload)
            response.raise_for_status()


class DataVaultServices:
    def __init__(
        self,
        settings: Settings,
        *,
        registry: PluginRegistry | None = None,
        storage: SQLiteStorage | None = None,
        guardian: GuardianService | None = None,
        approval_sender: ApprovalWebhookSender | None = None,
    ) -> None:
        self.settings = settings
        self.registry = registry or PluginRegistry.load_installed()
        self.storage = storage or SQLiteStorage(settings.database_path)
        self.guardian = guardian or GuardianService(settings.guardian_strategy)
        self.approval_sender = approval_sender or ApprovalWebhookSender(
            settings.approval_webhook_timeout_seconds
        )
        self.storage.initialize()

    def register_app(self, display_name: str, webhook_url: str) -> dict[str, Any]:
        registration = self.storage.register_app(display_name, webhook_url)
        return {
            "consumer_id": registration.consumer_id,
            "client_secret": registration.client_secret,
            "display_name": registration.display_name,
            "webhook_url": registration.webhook_url,
            "token_url": f"{self.settings.public_base_url}/v1/oauth/token",
        }

    def issue_token(self, consumer_id: str, client_secret: str) -> dict[str, Any] | None:
        token = self.storage.issue_token(
            consumer_id,
            client_secret,
            ttl_seconds=self.settings.token_ttl_seconds,
        )
        if token is None:
            return None
        return {
            "access_token": token.access_token,
            "token_type": "bearer",
            "expires_in": int((token.expires_at - datetime.now(tz=UTC)).total_seconds()),
        }

    def authenticate_access_token(self, access_token: str) -> AppPrincipal | None:
        return self.storage.authenticate_token(access_token)

    def registry_snapshot(self) -> dict[str, Any]:
        return {
            "data_types": [
                plugin.descriptor().model_dump(mode="json")
                for plugin in self.registry.data_types.values()
            ],
            "providers": [
                plugin.descriptor().model_dump(mode="json")
                for plugin in self.registry.providers.values()
            ],
            "consumers": [
                plugin.descriptor().model_dump(mode="json")
                for plugin in self.registry.consumers.values()
            ],
        }

    def ingest(self, provider_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        provider = self.registry.providers.get(provider_id)
        if provider is None:
            raise KeyError(f"unknown provider: {provider_id}")
        records = list(provider.normalize_payload(payload))
        accepted_count = self.storage.store_normalized_records(provider_id, records, payload)
        return {
            "provider_id": provider_id,
            "accepted_count": accepted_count,
            "normalized_type_ids": sorted({record.type_id for record in records}),
        }

    async def list_records(
        self,
        principal: AppPrincipal,
        *,
        type_id: str,
        provider_id: str | None,
        limit: int,
        start_at: datetime | None,
        end_at: datetime | None,
        response_mode: str,
    ) -> dict[str, Any]:
        await self._ensure_access(principal, type_id, "records", self._clean_params({
            "provider_id": provider_id,
            "limit": limit,
            "start_at": start_at.isoformat() if start_at else None,
            "end_at": end_at.isoformat() if end_at else None,
        }))
        records = self.storage.list_records(
            type_id=type_id,
            provider_id=provider_id,
            limit=limit,
            start_at=start_at,
            end_at=end_at,
        )
        items = [self._record_to_response(item, principal.consumer_id, "snooze_or_explicit") for item in records]
        response: dict[str, Any] = {
            "type_id": type_id,
            "items": items,
            "count": len(items),
        }
        if response_mode == "summary":
            response["summary"] = self.guardian.summarize_query_result(
                type_id=type_id,
                query_name="records",
                items=items,
            )
        return response

    async def execute_type_query(
        self,
        principal: AppPrincipal,
        *,
        type_id: str,
        query_name: str,
        params: dict[str, Any],
        response_mode: str,
    ) -> dict[str, Any]:
        data_type = self.registry.data_types.get(type_id)
        if data_type is None:
            raise KeyError(f"unknown data type: {type_id}")
        await self._ensure_access(principal, type_id, query_name, params)
        records = self.storage.list_records(type_id=type_id, limit=1000)
        result = data_type.execute_query(query_name, records, params)
        response = {
            "type_id": type_id,
            "query_name": query_name,
            "items": result.items,
            "count": len(result.items),
        }
        if response_mode == "summary":
            response["summary"] = result.summary or self.guardian.summarize_query_result(
                type_id=type_id,
                query_name=query_name,
                items=result.items,
            )
        elif result.summary is not None:
            response["summary"] = result.summary
        return response

    def get_approval(self, approval_id: str) -> ApprovalRecord | None:
        return self.storage.get_approval(approval_id)

    def submit_approval_decision(
        self,
        *,
        approval_id: str,
        approval_token: str,
        decision: str,
        snooze_minutes: int,
    ) -> ApprovalRecord | None:
        return self.storage.submit_approval_decision(
            approval_id=approval_id,
            approval_token=approval_token,
            decision=decision,
            snooze_minutes=snooze_minutes,
        )

    async def _ensure_access(
        self,
        principal: AppPrincipal,
        type_id: str,
        query_name: str,
        params: dict[str, Any],
    ) -> None:
        if type_id not in self.registry.data_types:
            raise KeyError(f"unknown data type: {type_id}")
        snooze = self.storage.get_active_snooze(principal.consumer_id, type_id)
        if snooze is not None:
            return
        summary = self.guardian.summarize_approval_request(
            consumer_name=principal.display_name,
            type_id=type_id,
            query_name=query_name,
            params=params,
        )
        approval = self.storage.create_approval(
            consumer_id=principal.consumer_id,
            type_id=type_id,
            query_name=query_name,
            query_params=params,
            summary=summary,
        )
        callback_url = (
            f"{self.settings.public_base_url}/v1/approvals/{approval.approval_id}/decision"
        )
        payload = {
            "approval_id": approval.approval_id,
            "approval_token": approval.approval_token,
            "consumer_id": principal.consumer_id,
            "consumer_name": principal.display_name,
            "type_id": type_id,
            "query_name": query_name,
            "params": params,
            "summary": summary,
            "callback_url": callback_url,
            "status_url": (
                f"{self.settings.public_base_url}/v1/approvals/{approval.approval_id}"
            ),
        }
        try:
            await self.approval_sender.send(principal.webhook_url, payload)
        except httpx.HTTPError as error:
            self.storage.record_approval_delivery_error(
                approval.approval_id,
                str(error),
            )
        raise ApprovalRequiredError(approval)

    def _record_to_response(
        self,
        record: NormalizedRecord,
        consumer_id: str,
        approval_mode: str,
    ) -> dict[str, Any]:
        return {
            "record_id": record.record_id,
            "type_id": record.type_id,
            "provider_id": record.provider_id,
            "source_id": record.source_id,
            "occurred_at": record.occurred_at.isoformat(),
            "captured_at": record.captured_at.isoformat() if record.captured_at else None,
            "ingested_at": record.ingested_at.isoformat(),
            "payload": record.payload,
            "consent_context": {
                "consumer_id": consumer_id,
                "mode": approval_mode,
            },
        }

    @staticmethod
    def _clean_params(params: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in params.items() if value is not None}
