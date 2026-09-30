# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Explicit provider registration and application-scoped provider binding."""

from __future__ import annotations

from collections.abc import Callable
from threading import RLock
from typing import Generic, TypeVar

from openviking_cli.exceptions import InvalidArgumentError

T = TypeVar("T")
ProviderFactory = Callable[..., T]


def _normalize(name: str, kind: str) -> str:
    normalized = name.strip().lower()
    if not normalized:
        raise ValueError(f"{kind} provider name must not be empty")
    return normalized


class ProviderCatalog(Generic[T]):
    """Process-wide catalog containing explicitly registered factories."""

    def __init__(self, kind: str) -> None:
        self._kind = kind
        self._factories: dict[str, ProviderFactory[T]] = {}
        self._lock = RLock()

    def register(
        self, name: str
    ) -> Callable[[ProviderFactory[T]], ProviderFactory[T]]:
        normalized = _normalize(name, self._kind)

        def decorator(factory: ProviderFactory[T]) -> ProviderFactory[T]:
            with self._lock:
                if normalized in self._factories:
                    raise ValueError(
                        f"{self._kind} provider already registered: {normalized}"
                    )
                self._factories[normalized] = factory
            return factory

        return decorator

    def factory(self, name: str) -> ProviderFactory[T]:
        normalized = _normalize(name, self._kind)
        with self._lock:
            factory = self._factories.get(normalized)
            if factory is not None:
                return factory
            available = ", ".join(self.names()) or "<none>"
        raise InvalidArgumentError(
            f"Unknown {self._kind} provider '{name}'. Available providers: {available}"
        )

    def names(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._factories))

    def registry(self) -> "ProviderRegistry[T]":
        return ProviderRegistry(self)

class ProviderRegistry(Generic[T]):
    """Application-scoped factories layered over a process-wide catalog."""

    def __init__(self, catalog: ProviderCatalog[T]) -> None:
        self._catalog = catalog
        self._factories: dict[str, ProviderFactory[T]] = {}

    def bind(self, name: str, factory: ProviderFactory[T]) -> None:
        """Bind a factory for this application, shadowing the global catalog."""
        normalized = _normalize(name, self._catalog._kind)
        if normalized in self._factories:
            raise ValueError(
                f"{self._catalog._kind} provider already bound: {normalized}"
            )
        self._factories[normalized] = factory

    def create(self, name: str, *, params: dict[str, object]) -> T:
        normalized = _normalize(name, self._catalog._kind)
        factory = self._factories.get(normalized)
        if factory is None:
            factory = self._catalog.factory(normalized)
        return factory(params=params)

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(set(self._catalog.names()) | set(self._factories)))
