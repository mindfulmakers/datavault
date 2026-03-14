from __future__ import annotations

from collections.abc import Iterable
from importlib import metadata

from datavault_plugin_sdk import DataConsumerPlugin, DataProviderPlugin, DataTypePlugin

DATA_TYPE_GROUP = "datavault.data_types"
DATA_PROVIDER_GROUP = "datavault.data_providers"
DATA_CONSUMER_GROUP = "datavault.data_consumers"


class PluginRegistry:
    def __init__(
        self,
        data_types: dict[str, DataTypePlugin],
        providers: dict[str, DataProviderPlugin],
        consumers: dict[str, DataConsumerPlugin],
    ) -> None:
        self.data_types = data_types
        self.providers = providers
        self.consumers = consumers

    @classmethod
    def load_installed(cls) -> "PluginRegistry":
        return cls(
            data_types=_load_plugins(DATA_TYPE_GROUP, DataTypePlugin, "type_id"),
            providers=_load_plugins(
                DATA_PROVIDER_GROUP,
                DataProviderPlugin,
                "provider_id",
            ),
            consumers=_load_plugins(
                DATA_CONSUMER_GROUP,
                DataConsumerPlugin,
                "consumer_type_id",
            ),
        )


def _load_plugins(
    group: str,
    expected_cls: type[object],
    id_attribute: str,
) -> dict[str, object]:
    registry: dict[str, object] = {}
    for entry_point in metadata.entry_points().select(group=group):
        loaded = entry_point.load()
        for plugin in _coerce_plugins(loaded):
            if not isinstance(plugin, expected_cls):
                raise TypeError(
                    f"entry point {entry_point.name!r} in group {group!r} did not "
                    f"produce a {expected_cls.__name__}"
                )
            plugin_id = getattr(plugin, id_attribute)
            registry[plugin_id] = plugin
    return registry


def _coerce_plugins(candidate: object) -> Iterable[object]:
    resolved = candidate() if callable(candidate) and not isinstance(candidate, type) else candidate
    if isinstance(resolved, type):
        resolved = resolved()
    if isinstance(resolved, (list, tuple, set)):
        return list(resolved)
    return [resolved]
