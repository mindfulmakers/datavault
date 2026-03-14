from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, HttpUrl

from .services import ApprovalRequiredError, DataVaultServices
from .settings import Settings
from .storage import AppPrincipal


class RegisterAppRequest(BaseModel):
    display_name: str = Field(min_length=1)
    webhook_url: HttpUrl


class OAuthTokenRequest(BaseModel):
    consumer_id: str
    client_secret: str


class ApprovalDecisionRequest(BaseModel):
    approval_token: str
    decision: Literal["approve", "deny"]
    snooze_minutes: int = Field(default=0, ge=0, le=10080)


class QueryExecutionRequest(BaseModel):
    params: dict[str, Any] = Field(default_factory=dict)
    response_mode: Literal["records", "summary"] = "records"


def create_app(
    settings: Settings | None = None,
    *,
    services: DataVaultServices | None = None,
) -> FastAPI:
    resolved_settings = settings or Settings.from_env()
    resolved_services = services or DataVaultServices(resolved_settings)
    app = FastAPI(title="Data Vault Core")
    app.state.services = resolved_services

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/apps/register")
    async def register_app(payload: RegisterAppRequest, request: Request) -> dict[str, Any]:
        services = get_services(request)
        return services.register_app(
            display_name=payload.display_name,
            webhook_url=str(payload.webhook_url),
        )

    @app.post("/v1/oauth/token")
    async def issue_oauth_token(
        payload: OAuthTokenRequest,
        request: Request,
    ) -> dict[str, Any]:
        services = get_services(request)
        token = services.issue_token(payload.consumer_id, payload.client_secret)
        if token is None:
            raise HTTPException(status_code=401, detail="invalid_client")
        return token

    @app.get("/v1/registry")
    async def registry(request: Request) -> dict[str, Any]:
        return get_services(request).registry_snapshot()

    @app.get("/v1/types")
    async def list_types(request: Request) -> dict[str, Any]:
        registry = get_services(request).registry_snapshot()
        return {"data_types": registry["data_types"]}

    @app.get("/v1/types/{type_id}/records")
    async def list_records(
        type_id: str,
        request: Request,
        principal: AppPrincipal = Depends(require_principal),
        provider_id: str | None = None,
        limit: int = 100,
        start_at: datetime | None = None,
        end_at: datetime | None = None,
        response_mode: Literal["records", "summary"] = "records",
    ) -> Any:
        services = get_services(request)
        try:
            return await services.list_records(
                principal,
                type_id=type_id,
                provider_id=provider_id,
                limit=limit,
                start_at=start_at,
                end_at=end_at,
                response_mode=response_mode,
            )
        except ApprovalRequiredError as error:
            return JSONResponse(
                status_code=202,
                content={
                    "status": "pending_approval",
                    "approval_id": error.approval.approval_id,
                    "summary": error.approval.summary,
                },
            )
        except KeyError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error

    @app.post("/v1/types/{type_id}/queries/{query_name}")
    async def execute_type_query(
        type_id: str,
        query_name: str,
        payload: QueryExecutionRequest,
        request: Request,
        principal: AppPrincipal = Depends(require_principal),
    ) -> Any:
        services = get_services(request)
        try:
            return await services.execute_type_query(
                principal,
                type_id=type_id,
                query_name=query_name,
                params=payload.params,
                response_mode=payload.response_mode,
            )
        except ApprovalRequiredError as error:
            return JSONResponse(
                status_code=202,
                content={
                    "status": "pending_approval",
                    "approval_id": error.approval.approval_id,
                    "summary": error.approval.summary,
                },
            )
        except KeyError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error

    @app.post("/v1/providers/{provider_id}/ingest")
    async def ingest(
        provider_id: str,
        payload: dict[str, Any],
        request: Request,
    ) -> Any:
        try:
            return get_services(request).ingest(provider_id, payload)
        except KeyError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except ValueError as error:
            raise HTTPException(status_code=400, detail=str(error)) from error

    @app.get("/v1/approvals/{approval_id}")
    async def get_approval(approval_id: str, request: Request) -> Any:
        approval = get_services(request).get_approval(approval_id)
        if approval is None:
            raise HTTPException(status_code=404, detail="approval not found")
        return {
            "approval_id": approval.approval_id,
            "consumer_id": approval.consumer_id,
            "type_id": approval.type_id,
            "query_name": approval.query_name,
            "query_params": approval.query_params,
            "summary": approval.summary,
            "status": approval.status,
            "created_at": approval.created_at.isoformat(),
            "decided_at": approval.decided_at.isoformat() if approval.decided_at else None,
            "snooze_until": approval.snooze_until.isoformat() if approval.snooze_until else None,
        }

    @app.post("/v1/approvals/{approval_id}/decision")
    async def submit_approval_decision(
        approval_id: str,
        payload: ApprovalDecisionRequest,
        request: Request,
    ) -> Any:
        approval = get_services(request).submit_approval_decision(
            approval_id=approval_id,
            approval_token=payload.approval_token,
            decision=payload.decision,
            snooze_minutes=payload.snooze_minutes,
        )
        if approval is None:
            raise HTTPException(status_code=404, detail="approval not found")
        return {
            "approval_id": approval.approval_id,
            "status": approval.status,
            "snooze_until": approval.snooze_until.isoformat() if approval.snooze_until else None,
        }

    return app


def get_services(request: Request) -> DataVaultServices:
    return request.app.state.services


def require_principal(
    request: Request,
    authorization: str | None = Header(default=None),
) -> AppPrincipal:
    if authorization is None or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing_bearer_token")
    token = authorization.removeprefix("Bearer ").strip()
    principal = get_services(request).authenticate_access_token(token)
    if principal is None:
        raise HTTPException(status_code=401, detail="invalid_token")
    return principal
