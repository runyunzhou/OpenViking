# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

from types import SimpleNamespace

import pytest

from openviking.server.account_stores.mysql import MySQLAccountStore
from openviking.server.account_stores.redis_mysql import RedisMySQLAccountStore
from openviking.server.api_keys.legacy import FileStore
from openviking.server.config import AccountStoreConfig, ServerConfig
from openviking.server.store_assembly import (
    build_account_store,
    build_api_key_manager,
)
from openviking_cli.exceptions import InvalidArgumentError
from tests.server.account_store_fakes import InMemoryAGFS


def test_unknown_account_store_is_rejected():
    with pytest.raises(InvalidArgumentError, match="Unknown account store provider"):
        build_account_store(
            viking_fs=SimpleNamespace(agfs=InMemoryAGFS()),
            api_key_hashing_enabled=False,
            account_store_provider="missing",
            account_store_params=None,
            account_store_watch_enabled=False,
            account_store_watch_interval_seconds=30,
        )


def test_unknown_account_store_is_rejected_during_config_validation():
    with pytest.raises(ValueError, match="account_store.provider must be one of"):
        AccountStoreConfig(provider="missing")


def test_account_store_config_selects_provider_after_normalization():
    config = ServerConfig(account_store=AccountStoreConfig(provider=" FILE "))
    store = build_account_store(
        viking_fs=SimpleNamespace(agfs=InMemoryAGFS()),
        api_key_hashing_enabled=False,
        account_store_provider=config.account_store.provider,
        account_store_params=None,
        account_store_watch_enabled=False,
        account_store_watch_interval_seconds=30,
    )
    assert isinstance(store, FileStore)


def test_account_store_config_selects_mysql_provider(monkeypatch):
    monkeypatch.setenv("OV_RESOURCE_ID", "test-resource")
    store = build_account_store(
        viking_fs=None,
        api_key_hashing_enabled=False,
        account_store_provider="mysql",
        account_store_params={
            "user": "unused",
            "password": "unused",
            "database": "unused",
        },
        account_store_watch_enabled=False,
        account_store_watch_interval_seconds=30,
    )
    assert isinstance(store, MySQLAccountStore)


def test_account_store_config_selects_redis_mysql_provider(monkeypatch):
    monkeypatch.setenv("OV_RESOURCE_ID", "test-resource")
    store = build_account_store(
        viking_fs=None,
        api_key_hashing_enabled=False,
        account_store_provider="redis_mysql",
        account_store_params={
            "mysql": {
                "user": "unused",
                "password": "unused",
                "database": "unused",
            },
            "redis": {"url": "redis://unused"},
        },
        account_store_watch_enabled=False,
        account_store_watch_interval_seconds=30,
    )
    assert isinstance(store, RedisMySQLAccountStore)


async def test_manager_accepts_an_injected_account_store():
    account_store = FileStore(SimpleNamespace(agfs=InMemoryAGFS()))
    manager = build_api_key_manager(
        "root",
        None,
        account_store_provider="file",
        account_store=account_store,
    )
    await manager.load()
    try:
        key = await manager.create_account("acme", "alice")
        assert (await manager.resolve_identity(key)).user_id == "alice"
    finally:
        await manager.close()
