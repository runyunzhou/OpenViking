# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Cross-event-loop contract coverage for the relational account store."""

from __future__ import annotations

import asyncio
import json
import os
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import create_async_engine

from openviking.server.account_stores.mysql import MySQLAccountStore
from openviking.server.account_stores.mysql_schema import Account, Base


def _mysql_params() -> dict[str, object]:
    raw = os.environ.get("OV_TEST_MYSQL")
    if not raw:
        pytest.skip("Set OV_TEST_MYSQL to the MySQL account store test")
    params = json.loads(raw)
    if not isinstance(params, dict):
        pytest.fail("OV_TEST_MYSQL must contain a JSON object")
    return params


@pytest.mark.asyncio
async def test_mysql_account_store_accepts_calls_from_a_foreign_loop() -> None:
    service_name = f"runtime-test-{uuid4().hex[:12]}"
    params = _mysql_params()
    store = MySQLAccountStore(service_name=service_name, params=params)
    admin_engine = create_async_engine(store._url, **store._engine_options)
    try:
        async with admin_engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        await store.load()

        async def worker() -> dict[str, object] | None:
            await store.create_account_with_api_key("runtime-account", "alice", "key")
            return await store.get_account("runtime-account")

        account = await asyncio.to_thread(lambda: asyncio.run(worker()))
        assert account is not None
        assert account["account_id"] == "runtime-account"
    finally:
        await store.close()
        async with admin_engine.begin() as connection:
            await connection.execute(delete(Account).where(Account.service_name == service_name))
        await admin_engine.dispose()
