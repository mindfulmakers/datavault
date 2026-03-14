from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from datavault_plugin_sdk import NormalizedRecord, NormalizedRecordInput


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _to_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat()


def _from_iso(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value)


def _hash_secret(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(slots=True)
class AppPrincipal:
    consumer_id: str
    display_name: str
    webhook_url: str


@dataclass(slots=True)
class AppRegistration:
    consumer_id: str
    client_secret: str
    display_name: str
    webhook_url: str


@dataclass(slots=True)
class TokenIssued:
    access_token: str
    expires_at: datetime


@dataclass(slots=True)
class SnoozeRecord:
    snooze_id: str
    consumer_id: str
    type_id: str
    snooze_until: datetime
    approval_id: str


@dataclass(slots=True)
class ApprovalRecord:
    approval_id: str
    approval_token: str
    consumer_id: str
    type_id: str
    query_name: str
    query_params: dict[str, Any]
    summary: str
    status: str
    created_at: datetime
    decided_at: datetime | None
    snooze_until: datetime | None


class SQLiteStorage:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS apps (
                    consumer_id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    webhook_url TEXT NOT NULL,
                    client_secret_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS access_tokens (
                    token_hash TEXT PRIMARY KEY,
                    consumer_id TEXT NOT NULL REFERENCES apps(consumer_id),
                    expires_at TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS normalized_records (
                    record_id TEXT PRIMARY KEY,
                    type_id TEXT NOT NULL,
                    provider_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    captured_at TEXT,
                    payload_json TEXT NOT NULL,
                    ingested_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS ingest_receipts (
                    receipt_id TEXT PRIMARY KEY,
                    provider_id TEXT NOT NULL,
                    accepted_count INTEGER NOT NULL,
                    raw_payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS approvals (
                    approval_id TEXT PRIMARY KEY,
                    approval_token_hash TEXT NOT NULL,
                    consumer_id TEXT NOT NULL REFERENCES apps(consumer_id),
                    type_id TEXT NOT NULL,
                    query_name TEXT NOT NULL,
                    query_params_json TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    status TEXT NOT NULL,
                    delivery_error TEXT,
                    created_at TEXT NOT NULL,
                    decided_at TEXT,
                    snooze_until TEXT
                );

                CREATE TABLE IF NOT EXISTS snoozes (
                    snooze_id TEXT PRIMARY KEY,
                    consumer_id TEXT NOT NULL REFERENCES apps(consumer_id),
                    type_id TEXT NOT NULL,
                    approval_id TEXT NOT NULL REFERENCES approvals(approval_id),
                    snooze_until TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )

    def register_app(self, display_name: str, webhook_url: str) -> AppRegistration:
        consumer_id = f"app_{uuid4().hex[:12]}"
        client_secret = secrets.token_urlsafe(24)
        created_at = _utc_now()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO apps (
                    consumer_id,
                    display_name,
                    webhook_url,
                    client_secret_hash,
                    created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    consumer_id,
                    display_name,
                    webhook_url,
                    _hash_secret(client_secret),
                    _to_iso(created_at),
                ),
            )
        return AppRegistration(
            consumer_id=consumer_id,
            client_secret=client_secret,
            display_name=display_name,
            webhook_url=webhook_url,
        )

    def issue_token(
        self,
        consumer_id: str,
        client_secret: str,
        ttl_seconds: int,
    ) -> TokenIssued | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT consumer_id
                FROM apps
                WHERE consumer_id = ? AND client_secret_hash = ?
                """,
                (consumer_id, _hash_secret(client_secret)),
            ).fetchone()
            if row is None:
                return None
            token = secrets.token_urlsafe(32)
            expires_at = _utc_now() + timedelta(seconds=ttl_seconds)
            connection.execute(
                """
                INSERT INTO access_tokens (
                    token_hash,
                    consumer_id,
                    expires_at,
                    created_at
                ) VALUES (?, ?, ?, ?)
                """,
                (_hash_secret(token), consumer_id, _to_iso(expires_at), _to_iso(_utc_now())),
            )
        return TokenIssued(access_token=token, expires_at=expires_at)

    def authenticate_token(self, access_token: str) -> AppPrincipal | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT apps.consumer_id, apps.display_name, apps.webhook_url, access_tokens.expires_at
                FROM access_tokens
                JOIN apps ON apps.consumer_id = access_tokens.consumer_id
                WHERE access_tokens.token_hash = ?
                """,
                (_hash_secret(access_token),),
            ).fetchone()
            if row is None:
                return None
            expires_at = _from_iso(row["expires_at"])
            if expires_at is None or expires_at <= _utc_now():
                return None
            return AppPrincipal(
                consumer_id=row["consumer_id"],
                display_name=row["display_name"],
                webhook_url=row["webhook_url"],
            )

    def create_approval(
        self,
        *,
        consumer_id: str,
        type_id: str,
        query_name: str,
        query_params: dict[str, Any],
        summary: str,
    ) -> ApprovalRecord:
        approval_id = f"approval_{uuid4().hex}"
        approval_token = secrets.token_urlsafe(24)
        created_at = _utc_now()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO approvals (
                    approval_id,
                    approval_token_hash,
                    consumer_id,
                    type_id,
                    query_name,
                    query_params_json,
                    summary,
                    status,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    approval_id,
                    _hash_secret(approval_token),
                    consumer_id,
                    type_id,
                    query_name,
                    json.dumps(query_params),
                    summary,
                    "pending",
                    _to_iso(created_at),
                ),
            )
        return ApprovalRecord(
            approval_id=approval_id,
            approval_token=approval_token,
            consumer_id=consumer_id,
            type_id=type_id,
            query_name=query_name,
            query_params=query_params,
            summary=summary,
            status="pending",
            created_at=created_at,
            decided_at=None,
            snooze_until=None,
        )

    def record_approval_delivery_error(self, approval_id: str, error_message: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE approvals SET delivery_error = ? WHERE approval_id = ?",
                (error_message, approval_id),
            )

    def get_approval(self, approval_id: str) -> ApprovalRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT approval_id,
                       consumer_id,
                       type_id,
                       query_name,
                       query_params_json,
                       summary,
                       status,
                       created_at,
                       decided_at,
                       snooze_until
                FROM approvals
                WHERE approval_id = ?
                """,
                (approval_id,),
            ).fetchone()
            if row is None:
                return None
            return ApprovalRecord(
                approval_id=row["approval_id"],
                approval_token="",
                consumer_id=row["consumer_id"],
                type_id=row["type_id"],
                query_name=row["query_name"],
                query_params=json.loads(row["query_params_json"]),
                summary=row["summary"],
                status=row["status"],
                created_at=_from_iso(row["created_at"]) or _utc_now(),
                decided_at=_from_iso(row["decided_at"]),
                snooze_until=_from_iso(row["snooze_until"]),
            )

    def submit_approval_decision(
        self,
        *,
        approval_id: str,
        approval_token: str,
        decision: str,
        snooze_minutes: int,
    ) -> ApprovalRecord | None:
        now = _utc_now()
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT approval_id,
                       approval_token_hash,
                       consumer_id,
                       type_id,
                       query_name,
                       query_params_json,
                       summary
                FROM approvals
                WHERE approval_id = ?
                """,
                (approval_id,),
            ).fetchone()
            if row is None or row["approval_token_hash"] != _hash_secret(approval_token):
                return None
            snooze_until = (
                now + timedelta(minutes=snooze_minutes)
                if decision == "approve" and snooze_minutes > 0
                else None
            )
            connection.execute(
                """
                UPDATE approvals
                SET status = ?, decided_at = ?, snooze_until = ?
                WHERE approval_id = ?
                """,
                (decision, _to_iso(now), _to_iso(snooze_until), approval_id),
            )
            if snooze_until is not None:
                connection.execute(
                    """
                    INSERT INTO snoozes (
                        snooze_id,
                        consumer_id,
                        type_id,
                        approval_id,
                        snooze_until,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"snooze_{uuid4().hex}",
                        row["consumer_id"],
                        row["type_id"],
                        approval_id,
                        _to_iso(snooze_until),
                        _to_iso(now),
                    ),
                )
        return ApprovalRecord(
            approval_id=approval_id,
            approval_token="",
            consumer_id=row["consumer_id"],
            type_id=row["type_id"],
            query_name=row["query_name"],
            query_params=json.loads(row["query_params_json"]),
            summary=row["summary"],
            status=decision,
            created_at=now,
            decided_at=now,
            snooze_until=snooze_until,
        )

    def get_active_snooze(self, consumer_id: str, type_id: str) -> SnoozeRecord | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT snooze_id, consumer_id, type_id, approval_id, snooze_until
                FROM snoozes
                WHERE consumer_id = ? AND type_id = ? AND snooze_until > ?
                ORDER BY snooze_until DESC
                LIMIT 1
                """,
                (consumer_id, type_id, _to_iso(_utc_now())),
            ).fetchone()
            if row is None:
                return None
            return SnoozeRecord(
                snooze_id=row["snooze_id"],
                consumer_id=row["consumer_id"],
                type_id=row["type_id"],
                approval_id=row["approval_id"],
                snooze_until=_from_iso(row["snooze_until"]) or _utc_now(),
            )

    def store_normalized_records(
        self,
        provider_id: str,
        records: list[NormalizedRecordInput],
        raw_payload: dict[str, Any],
    ) -> int:
        now = _utc_now()
        with self._connect() as connection:
            for record in records:
                connection.execute(
                    """
                    INSERT INTO normalized_records (
                        record_id,
                        type_id,
                        provider_id,
                        source_id,
                        occurred_at,
                        captured_at,
                        payload_json,
                        ingested_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"record_{uuid4().hex}",
                        record.type_id,
                        provider_id,
                        record.source_id,
                        _to_iso(record.occurred_at),
                        _to_iso(record.captured_at),
                        json.dumps(record.payload),
                        _to_iso(now),
                    ),
                )
            connection.execute(
                """
                INSERT INTO ingest_receipts (
                    receipt_id,
                    provider_id,
                    accepted_count,
                    raw_payload_json,
                    created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    f"receipt_{uuid4().hex}",
                    provider_id,
                    len(records),
                    json.dumps(raw_payload),
                    _to_iso(now),
                ),
            )
        return len(records)

    def list_records(
        self,
        *,
        type_id: str,
        provider_id: str | None = None,
        limit: int = 100,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
    ) -> list[NormalizedRecord]:
        query = [
            """
            SELECT record_id,
                   type_id,
                   provider_id,
                   source_id,
                   occurred_at,
                   captured_at,
                   payload_json,
                   ingested_at
            FROM normalized_records
            WHERE type_id = ?
            """
        ]
        params: list[Any] = [type_id]
        if provider_id is not None:
            query.append("AND provider_id = ?")
            params.append(provider_id)
        if start_at is not None:
            query.append("AND occurred_at >= ?")
            params.append(_to_iso(start_at))
        if end_at is not None:
            query.append("AND occurred_at <= ?")
            params.append(_to_iso(end_at))
        query.append("ORDER BY occurred_at DESC LIMIT ?")
        params.append(limit)
        statement = "\n".join(query)
        with self._connect() as connection:
            rows = connection.execute(statement, params).fetchall()
        return [
            NormalizedRecord(
                record_id=row["record_id"],
                type_id=row["type_id"],
                provider_id=row["provider_id"],
                source_id=row["source_id"],
                occurred_at=_from_iso(row["occurred_at"]) or _utc_now(),
                captured_at=_from_iso(row["captured_at"]),
                payload=json.loads(row["payload_json"]),
                ingested_at=_from_iso(row["ingested_at"]) or _utc_now(),
            )
            for row in rows
        ]

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        return connection
