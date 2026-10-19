# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Runtime lifecycle regressions, including real MySQL pool cleanup."""

from __future__ import annotations

import asyncio
import importlib
import json
import os
import threading
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from openviking.server.account_stores.mysql import MySQLAccountStore
from openviking.server.account_stores.mysql_schema import Base as AccountBase
from openviking.service.task_domain import TaskRecord
from openviking.service.task_store_mysql import MySQLTaskWorkStore
from openviking.storage.mysql_runtime import MySQLRuntime


async def _wait(event: threading.Event) -> None:
    async with asyncio.timeout(5):
        while not event.is_set():
            await asyncio.sleep(0.005)


async def _stopped(runtime: MySQLRuntime) -> None:
    thread = runtime._thread
    if thread is not None:
        await asyncio.to_thread(thread.join, 5)
        assert not thread.is_alive(), "MySQL runtime leaked a thread or lease"


@pytest.mark.asyncio
async def test_cancel_waits_for_cleanup_after_last_release():
    runtime = MySQLRuntime()
    await runtime.acquire()
    started = threading.Event()
    cleaning = threading.Event()
    finish = threading.Event()
    cleaned = threading.Event()

    async def operation():
        try:
            started.set()
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await _wait(finish)
            cleaned.set()

    caller = asyncio.create_task(runtime.run(operation))
    try:
        await _wait(started)
        caller.cancel()
        await _wait(cleaning)
        await runtime.release()
        caller.cancel()
        await asyncio.sleep(0.03)
        assert not caller.done()
        assert not cleaned.is_set()
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(caller, 5)
        assert cleaned.is_set()
    finally:
        finish.set()
        await asyncio.gather(caller, return_exceptions=True)
        if runtime._leases:
            await runtime.release()
        await _stopped(runtime)


@pytest.mark.asyncio
async def test_cancel_before_submission_does_not_leave_an_idle_runtime(monkeypatch):
    runtime = MySQLRuntime()
    original = runtime._ensure_loop
    ready = asyncio.Event()

    async def delayed_loop():
        await original()
        ready.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(runtime, "_ensure_loop", delayed_loop)
    caller = asyncio.create_task(runtime.run(lambda: asyncio.sleep(0)))
    await asyncio.wait_for(ready.wait(), 5)
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    await _stopped(runtime)


@pytest.mark.asyncio
async def test_cancel_before_operation_starts_releases_its_admission(monkeypatch):
    runtime = MySQLRuntime()
    await runtime.acquire()
    loop = runtime._loop
    original = loop.create_task
    created = threading.Event()
    cancelled = threading.Event()

    def cancel_new_task(coroutine, **kwargs):
        task = original(coroutine, **kwargs)
        task.cancel()
        created.set()
        return task

    async def operation():
        cancelled.set()

    monkeypatch.setattr(loop, "create_task", cancel_new_task)
    try:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(runtime.run(operation), 5)
        assert created.is_set()
        assert not cancelled.is_set()
    finally:
        monkeypatch.setattr(loop, "create_task", original)
        await runtime.release()
        await _stopped(runtime)


@pytest.fixture(params=["task", "account"])
async def mysql_lifecycle_store(request):
    raw = os.environ.get("OV_TEST_MYSQL")
    if not raw:
        pytest.skip("Set OV_TEST_MYSQL to exercise real MySQL lifecycle")
    params = json.loads(raw)
    runtime = MySQLRuntime()
    if request.param == "task":
        params["table"] = f"ov_lifecycle_{uuid4().hex[:12]}"
        store = MySQLTaskWorkStore(params=params, runtime=runtime)
        metadata = store._models.Base.metadata
    else:
        store = MySQLAccountStore(
            service_name=f"lifecycle-{uuid4().hex[:12]}", params=params, runtime=runtime
        )
        metadata = AccountBase.metadata
    admin = create_async_engine(store._url, **store._engine_options)
    try:
        async with admin.begin() as connection:
            await connection.run_sync(metadata.create_all)
        yield store
    finally:
        await store.close()
        await _stopped(runtime)
        if request.param == "task":
            async with admin.begin() as connection:
                await connection.run_sync(metadata.drop_all)
        await admin.dispose()


def _record_engines(store, monkeypatch):
    module = importlib.import_module(type(store).__module__)
    original = module.create_async_engine
    engines = []

    def create(*args, **kwargs):
        engine = original(*args, **kwargs)
        engines.append(engine)
        return engine

    monkeypatch.setattr(module, "create_async_engine", create)
    return engines


@pytest.mark.asyncio
async def test_mysql_last_close_keeps_a_new_load_alive(mysql_lifecycle_store, monkeypatch):
    store = mysql_lifecycle_store
    runtime = store._mysql_runtime
    if isinstance(store, MySQLTaskWorkStore):
        new = MySQLTaskWorkStore(
            params={
                **json.loads(os.environ["OV_TEST_MYSQL"]),
                "table": store._models.Task.__tablename__,
            },
            runtime=runtime,
        )
    else:
        new = MySQLAccountStore(
            service_name=store._service_name,
            params=json.loads(os.environ["OV_TEST_MYSQL"]),
            runtime=runtime,
        )
    await store.load()
    original = runtime._ensure_loop
    admitted = asyncio.Event()
    submit = asyncio.Event()
    loading = None

    async def delayed_loop():
        loop = await original()
        if asyncio.current_task() is loading:
            admitted.set()
            await submit.wait()
        return loop

    monkeypatch.setattr(runtime, "_ensure_loop", delayed_loop)
    loading = asyncio.create_task(new.load())
    try:
        await asyncio.wait_for(admitted.wait(), 5)
        await store.close()
        submit.set()
        await asyncio.wait_for(loading, 5)
        assert new._engine is not None
        assert await runtime.run(lambda: asyncio.sleep(0, result="usable")) == "usable"
    finally:
        submit.set()
        await asyncio.gather(loading, return_exceptions=True)
        await new.close()
        await _stopped(runtime)


@pytest.mark.asyncio
async def test_mysql_concurrent_load_close_and_reload(mysql_lifecycle_store, monkeypatch):
    store = mysql_lifecycle_store
    engines = _record_engines(store, monkeypatch)
    entered = threading.Event()
    finish = threading.Event()
    original = AsyncSession.execute

    async def gated_execute(self, *args, **kwargs):
        result = await original(self, *args, **kwargs)
        if not entered.is_set():
            entered.set()
            await _wait(finish)
        return result

    monkeypatch.setattr(AsyncSession, "execute", gated_execute)
    first = asyncio.create_task(store.load())
    other = None
    try:
        await _wait(entered)
        other = asyncio.create_task(asyncio.to_thread(lambda: asyncio.run(store.load())))
        await asyncio.sleep(0.03)
        assert len(engines) == 1
        finish.set()
        await asyncio.wait_for(asyncio.gather(first, other), 5)
        pool = store._engine.sync_engine.pool
        assert pool.checkedin() > 0
        await asyncio.gather(store.close(), store.close())
        await _stopped(store._mysql_runtime)
        assert pool.checkedin() == pool.checkedout() == 0
        await asyncio.gather(
            store.load(), asyncio.to_thread(lambda: asyncio.run(store.load())), store.load()
        )
        assert len(engines) == 2
        assert store._engine is engines[-1]
    finally:
        finish.set()
        await asyncio.gather(first, *([other] if other else []), return_exceptions=True)


@pytest.mark.asyncio
async def test_mysql_failed_load_can_retry(mysql_lifecycle_store, monkeypatch):
    store = mysql_lifecycle_store
    engines = _record_engines(store, monkeypatch)
    original = AsyncSession.execute
    fail = True

    async def failed_execute(self, *args, **kwargs):
        nonlocal fail
        result = await original(self, *args, **kwargs)
        if fail:
            fail = False
            raise RuntimeError("initialization failed after opening a real connection")
        return result

    monkeypatch.setattr(AsyncSession, "execute", failed_execute)
    with pytest.raises(RuntimeError, match="initialization failed"):
        await store.load()
    await _stopped(store._mysql_runtime)
    assert engines[0].sync_engine.pool.checkedin() == 0
    assert not store._mysql_lease
    await asyncio.gather(store.load(), store.load())
    assert len(engines) == 2
    assert store._engine is engines[1]


@pytest.mark.asyncio
async def test_mysql_cancelled_load_waits_for_session_cleanup(mysql_lifecycle_store, monkeypatch):
    store = mysql_lifecycle_store
    engines = _record_engines(store, monkeypatch)
    entered = threading.Event()
    cleaning = threading.Event()
    finish = threading.Event()
    closed = threading.Event()
    execute = AsyncSession.execute
    close = AsyncSession.close

    async def gated_execute(self, *args, **kwargs):
        result = await execute(self, *args, **kwargs)
        if not entered.is_set():
            entered.set()
            await asyncio.Event().wait()
        return result

    async def gated_close(self):
        if not cleaning.is_set():
            cleaning.set()
            await _wait(finish)
        await close(self)
        closed.set()

    monkeypatch.setattr(AsyncSession, "execute", gated_execute)
    monkeypatch.setattr(AsyncSession, "close", gated_close)
    caller = asyncio.create_task(store.load())
    closing = None
    try:
        await _wait(entered)
        caller.cancel()
        await _wait(cleaning)
        closing = asyncio.create_task(store.close())
        caller.cancel()
        await asyncio.sleep(0.03)
        assert not caller.done()
        assert not closing.done()
        assert not closed.is_set()
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(caller, 5)
        await asyncio.wait_for(closing, 5)
        assert closed.is_set()
        assert not store._mysql_lease
        await _stopped(store._mysql_runtime)
        assert engines[0].sync_engine.pool.checkedin() == 0
        assert engines[0].sync_engine.pool.checkedout() == 0
        await asyncio.gather(store.load(), store.load())
        assert len(engines) == 2
    finally:
        finish.set()
        await asyncio.gather(caller, *([closing] if closing else []), return_exceptions=True)


@pytest.mark.asyncio
async def test_mysql_query_cancellation_waits_for_normal_session_exit(
    mysql_lifecycle_store, monkeypatch
):
    store = mysql_lifecycle_store
    await store.load()
    engine = store._engine
    entered, release = threading.Event(), threading.Event()
    original = AsyncSession.close

    async def gated_close(session):
        entered.set()
        await _wait(release)
        await original(session)

    monkeypatch.setattr(AsyncSession, "close", gated_close)
    query = (
        store.get_task("missing", account_id="account", user_id="alice")
        if isinstance(store, MySQLTaskWorkStore)
        else store.get_user("missing", "alice")
    )
    caller = asyncio.create_task(query)
    try:
        await _wait(entered)
        assert engine.sync_engine.pool.checkedout() == 1
        caller.cancel()
        await asyncio.sleep(0.03)
        caller.cancel()
        assert not caller.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(caller, 5)
        assert engine.sync_engine.pool.checkedout() == 0
        await store.close()
        await _stopped(store._mysql_runtime)
    finally:
        release.set()
        await asyncio.gather(caller, return_exceptions=True)


@pytest.mark.asyncio
async def test_mysql_cancelled_close_finishes_pool_disposal(mysql_lifecycle_store, monkeypatch):
    store = mysql_lifecycle_store
    await store.load()
    engine = store._engine
    pool = engine.sync_engine.pool
    entered, release = threading.Event(), threading.Event()
    connections = []

    async def populate():
        first, second = await engine.connect(), await engine.connect()
        for connection in (first, second):
            driver = connection.sync_connection.connection.driver_connection
            connections.append(driver)
            original = driver._writer.drain

            async def gated_drain(original=original):
                entered.set()
                await _wait(release)
                await original()

            monkeypatch.setattr(driver._writer, "drain", gated_drain)
        await first.close()
        await second.close()

    await store._mysql_runtime.run(populate)
    assert pool.checkedin() == 2
    caller = asyncio.create_task(store.close())
    retry = None
    try:
        await _wait(entered)
        caller.cancel()
        retry = asyncio.create_task(store.close())
        await asyncio.sleep(0.03)
        caller.cancel()
        assert not caller.done() and not retry.done()
        assert store._engine is engine
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(caller, 5)
        await asyncio.wait_for(retry, 5)
        assert pool.checkedin() == pool.checkedout() == 0
        assert all(connection.closed for connection in connections)
        await _stopped(store._mysql_runtime)
        await store.load()
        if isinstance(store, MySQLTaskWorkStore):
            await store.create(
                TaskRecord(task_id="after-close", task_type="probe", account_id="a", user_id="u")
            )
            assert (await store.get_task("after-close", account_id="a", user_id="u")).task_id == (
                "after-close"
            )
        else:
            await store.create_account("after-close", "alice")
            assert (await store.get_account("after-close"))["account_id"] == "after-close"
    finally:
        release.set()
        await asyncio.gather(caller, *([retry] if retry else []), return_exceptions=True)
