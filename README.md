# Data Vault

Data Vault is a local-first Python `uv` workspace centered on **Data Vault Core**, a FastAPI server that:

- ingests data from pluggable providers
- normalizes it into cohesive data types
- requires per-request approval before downstream apps can read data
- supports snoozed approval windows per `consumer + data_type`
- discovers new data types and providers from installed Python entry points

## Workspace Packages

- `packages/datavault-core`: the FastAPI server, SQLite storage, OAuth-style token issuance, approval flow, and LangChain Guardian summarization
- `packages/datavault-plugin-sdk`: shared plugin contracts and Pydantic models
- `packages/datavault-type-location`: normalized `location` type plugin with `latest` and `history` queries
- `packages/datavault-type-messages`: normalized `messages` type plugin with `recent` and `thread` queries
- `packages/datavault-provider-ios-location`: example iOS location provider
- `packages/datavault-provider-garmin-location`: example Garmin location provider
- `packages/datavault-provider-messages-demo`: example messaging provider

## Key API Concepts

- **Data types** provide a normalized schema plus query methods.
- **Data providers** accept source-specific payloads and emit normalized records.
- **Downstream consumers** authenticate to a vault, request data, and trigger approvals.
- **Guardian** summarizes approval requests and query results using LangChain composition with a deterministic default strategy.

## Approval Flow

1. A downstream app registers with the vault and receives a `consumer_id` and `client_secret`.
2. The app exchanges those credentials for a bearer token at `/v1/oauth/token`.
3. The app requests data from a type endpoint.
4. If there is no active snooze for that `consumer + data_type`, Data Vault Core creates an approval record and POSTs a webhook payload to the configured user-facing endpoint.
5. The user-facing system submits an approval or denial back to `/v1/approvals/{approval_id}/decision`.
6. Approved requests may create a snooze window so future requests for the same `consumer + data_type` bypass repeat prompts until the snooze expires.

## Quick Start

Install the workspace:

```bash
uv sync --all-packages --group dev
```

Run the FastAPI server:

```bash
uv run datavault-core serve
```

Run the test suite:

```bash
uv run pytest
```

## Notable Endpoints

- `GET /healthz`
- `POST /v1/apps/register`
- `POST /v1/oauth/token`
- `GET /v1/registry`
- `GET /v1/types/{type_id}/records`
- `POST /v1/types/{type_id}/queries/{query_name}`
- `POST /v1/providers/{provider_id}/ingest`
- `GET /v1/approvals/{approval_id}`
- `POST /v1/approvals/{approval_id}/decision`

## Local Development Notes

- The default SQLite database path is `.state/datavault.sqlite3`.
- The default base URL is `http://127.0.0.1:8787`.
- For local webhook/provider callbacks, expose the server with `ngrok` and set `DATAVAULT_PUBLIC_BASE_URL` accordingly.
