from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class Settings:
    database_path: Path
    public_base_url: str
    bind_host: str = "127.0.0.1"
    bind_port: int = 8787
    approval_webhook_timeout_seconds: float = 5.0
    token_ttl_seconds: int = 3600
    guardian_strategy: str = "stub"

    @classmethod
    def from_env(cls, root_dir: Path | None = None) -> "Settings":
        root = root_dir or Path.cwd()
        database_path = Path(
            os.environ.get(
                "DATAVAULT_DATABASE_PATH",
                root / ".state" / "datavault.sqlite3",
            )
        )
        public_base_url = os.environ.get(
            "DATAVAULT_PUBLIC_BASE_URL",
            "http://127.0.0.1:8787",
        )
        return cls(
            database_path=database_path,
            public_base_url=public_base_url.rstrip("/"),
            bind_host=os.environ.get("DATAVAULT_BIND_HOST", "127.0.0.1"),
            bind_port=int(os.environ.get("DATAVAULT_BIND_PORT", "8787")),
            approval_webhook_timeout_seconds=float(
                os.environ.get("DATAVAULT_APPROVAL_WEBHOOK_TIMEOUT_SECONDS", "5.0")
            ),
            token_ttl_seconds=int(os.environ.get("DATAVAULT_TOKEN_TTL_SECONDS", "3600")),
            guardian_strategy=os.environ.get("DATAVAULT_GUARDIAN_STRATEGY", "stub"),
        )
