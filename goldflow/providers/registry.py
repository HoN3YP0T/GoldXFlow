"""Provider registry: config `source` -> adapter class, with profile rules (synthetic data only in tests)."""

from __future__ import annotations

from typing import Any

from ..settings import Profile
from .base import Provider, ProviderNotAllowed

_REGISTRY: dict[str, type[Provider]] = {}


def register(provider_id: str):  # type: ignore[no-untyped-def]
    def deco(cls: type[Provider]) -> type[Provider]:
        if provider_id in _REGISTRY and _REGISTRY[provider_id] is not cls:
            raise ValueError(f"provider id {provider_id!r} already registered")
        _REGISTRY[provider_id] = cls
        return cls

    return deco


def _load_builtin() -> None:
    from . import binance, bitget, bybit, dummy, okx  # noqa: F401  (registration side effects)


def available_providers() -> dict[str, type[Provider]]:
    _load_builtin()
    return dict(_REGISTRY)


def create_provider(cfg: Any, profile: Profile = Profile.RESEARCH, provider_id: str | None = None) -> Provider:
    """Instantiate the provider named by the config (`source`) or `provider_id`."""
    _load_builtin()
    pid = provider_id or cfg["source"]
    cls = _REGISTRY.get(pid)
    if cls is None:
        raise KeyError(f"unknown provider {pid!r}; registered: {sorted(_REGISTRY)}")
    if cls.synthetic and profile is not Profile.TEST:
        raise ProviderNotAllowed(
            f"provider {pid!r} produces synthetic data and is only allowed in the 'test' profile "
            f"(current profile: {profile.value})"
        )
    return cls.from_config(cfg)  # type: ignore[attr-defined, no-any-return]
