from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any

from .models import (
    DataConsumerDescriptor,
    DataProviderDescriptor,
    DataTypeDescriptor,
    JsonObject,
    NormalizedRecord,
    NormalizedRecordInput,
    QueryMethodSpec,
    QueryResult,
)


class DataTypePlugin(ABC):
    type_id: str
    display_name: str
    version: str = "0.1.0"
    description: str = ""
    record_schema: JsonObject = {"type": "object"}

    def descriptor(self) -> DataTypeDescriptor:
        return DataTypeDescriptor(
            type_id=self.type_id,
            display_name=self.display_name,
            version=self.version,
            description=self.description,
            record_schema=self.record_schema,
            query_methods=list(self.query_methods()),
        )

    def query_methods(self) -> Sequence[QueryMethodSpec]:
        return []

    @abstractmethod
    def execute_query(
        self,
        query_name: str,
        records: Sequence[NormalizedRecord],
        params: JsonObject,
    ) -> QueryResult:
        raise NotImplementedError


class DataProviderPlugin(ABC):
    provider_id: str
    display_name: str
    version: str = "0.1.0"
    description: str = ""
    normalized_type_ids: tuple[str, ...] = ()

    def descriptor(self) -> DataProviderDescriptor:
        return DataProviderDescriptor(
            provider_id=self.provider_id,
            display_name=self.display_name,
            version=self.version,
            description=self.description,
            normalized_type_ids=list(self.normalized_type_ids),
        )

    @abstractmethod
    def normalize_payload(self, payload: JsonObject) -> Sequence[NormalizedRecordInput]:
        raise NotImplementedError


class DataConsumerPlugin(ABC):
    consumer_type_id: str
    display_name: str
    version: str = "0.1.0"
    description: str = ""

    def descriptor(self) -> DataConsumerDescriptor:
        return DataConsumerDescriptor(
            consumer_type_id=self.consumer_type_id,
            display_name=self.display_name,
            version=self.version,
            description=self.description,
        )


PluginFactory = Any
