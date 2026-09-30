# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Pluggable storage providers for account and API key data."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from openviking.server.account_stores.base import AccountStore
    from openviking.server.account_stores.registry import (
        list_account_store_providers,
        new_account_store_registry,
        register_account_store,
    )


def __getattr__(name: str):
    if name == "AccountStore":
        from openviking.server.account_stores.base import AccountStore

        return AccountStore
    if name in {
        "list_account_store_providers",
        "new_account_store_registry",
        "register_account_store",
    }:
        from openviking.server.account_stores import registry

        return getattr(registry, name)
    raise AttributeError(name)


__all__ = [
    "AccountStore",
    "list_account_store_providers",
    "new_account_store_registry",
    "register_account_store",
]
