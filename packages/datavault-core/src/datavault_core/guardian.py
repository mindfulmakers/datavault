from __future__ import annotations

from typing import Any

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableLambda


class GuardianService:
    def __init__(self, strategy: str = "stub") -> None:
        self.strategy = strategy
        self._approval_chain = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "You summarize Data Vault approval requests for a user-facing "
                    "client. Keep it short and specific.",
                ),
                (
                    "human",
                    "Consumer: {consumer_name}\n"
                    "Data type: {type_id}\n"
                    "Query: {query_name}\n"
                    "Params: {params}",
                ),
            ]
        ) | RunnableLambda(self._render_approval_summary)
        self._result_chain = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    "You summarize Data Vault query results. Keep it concise.",
                ),
                (
                    "human",
                    "Data type: {type_id}\n"
                    "Query: {query_name}\n"
                    "Record count: {count}\n"
                    "Sample: {sample}",
                ),
            ]
        ) | RunnableLambda(self._render_result_summary)

    def summarize_approval_request(
        self,
        *,
        consumer_name: str,
        type_id: str,
        query_name: str,
        params: dict[str, Any],
    ) -> str:
        return self._approval_chain.invoke(
            {
                "consumer_name": consumer_name,
                "type_id": type_id,
                "query_name": query_name,
                "params": params,
            }
        )

    def summarize_query_result(
        self,
        *,
        type_id: str,
        query_name: str,
        items: list[dict[str, Any]],
    ) -> str:
        sample = items[:2]
        return self._result_chain.invoke(
            {
                "type_id": type_id,
                "query_name": query_name,
                "count": len(items),
                "sample": sample,
            }
        )

    def _render_approval_summary(self, prompt_value: Any) -> str:
        rendered = "\n".join(
            str(message.content) for message in prompt_value.to_messages()
        )
        if self.strategy != "stub":
            return rendered
        lines = [line.strip() for line in rendered.splitlines() if ":" in line]
        details = {key.strip(): value.strip() for key, value in (line.split(":", 1) for line in lines if line and line[0].isalpha())}
        return (
            f"{details.get('Consumer', 'An app')} wants {details.get('Query', 'a query')} "
            f"access to {details.get('Data type', 'data')}."
        )

    def _render_result_summary(self, prompt_value: Any) -> str:
        rendered = "\n".join(
            str(message.content) for message in prompt_value.to_messages()
        )
        if self.strategy != "stub":
            return rendered
        lines = [line.strip() for line in rendered.splitlines() if ":" in line]
        details = {key.strip(): value.strip() for key, value in (line.split(":", 1) for line in lines if line and line[0].isalpha())}
        return (
            f"{details.get('Data type', 'Data')} query {details.get('Query', 'records')} "
            f"returned {details.get('Record count', '0')} item(s)."
        )
