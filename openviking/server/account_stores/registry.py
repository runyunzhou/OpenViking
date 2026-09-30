# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Explicit catalog and application registry for account store providers."""

from openviking.providers import ProviderCatalog, ProviderRegistry
from openviking.server.account_stores.base import AccountStore
from openviking_cli.exceptions import InvalidArgumentError

_ACCOUNT_STORE_CATALOG = ProviderCatalog[AccountStore](kind="account store")

register_account_store = _ACCOUNT_STORE_CATALOG.register


@register_account_store("file")
def _unbound_file_provider(*, params: dict[str, object]) -> AccountStore:
    del params
    raise InvalidArgumentError(
        "File account storage must be created by the application assembly"
    )


def list_account_store_providers() -> tuple[str, ...]:
    """Return built-in and explicitly registered provider names."""
    return _ACCOUNT_STORE_CATALOG.names()


def new_account_store_registry() -> ProviderRegistry[AccountStore]:
    """Create an application registry where runtime dependencies can be bound."""
    return _ACCOUNT_STORE_CATALOG.registry()
