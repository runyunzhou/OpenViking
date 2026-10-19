# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Contract coverage for the relational Task/Work store."""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import event
from sqlalchemy.ext.asyncio import create_async_engine

from openviking.service.core import OpenVikingService
from openviking.service.task_domain import (
    DeletedTask,
    TaskRecord,
    TaskRetentionPolicy,
    TaskStatus,
    WorkRecord,
    WorkState,
)
from openviking.service.task_store import (
    StoreOperationCode,
    StoreOperationOutcome,
    TaskAlreadyActive,
)
from openviking.service.task_store_mysql import MySQLTaskWorkStore
from openviking.service.task_tracker import TaskTracker
from openviking.storage.queuefs.queue_manager import QueueManager


def _mysql_params() -> dict[str, object]:
    raw = os.environ.get("OV_TEST_MYSQL")
    if not raw:
        pytest.skip("Set OV_TEST_MYSQL to connection JSON for the MySQL Task store test")
    params = json.loads(raw)
    if not isinstance(params, dict):
        pytest.fail("OV_TEST_MYSQL must contain a JSON object")
    params["table"] = f"ov_tasks_test_{uuid4().hex[:12]}"
    return params


@pytest.fixture
async def mysql_stores():
    params = _mysql_params()
    first = MySQLTaskWorkStore(params=params)
    second = MySQLTaskWorkStore(params=params)
    admin_engine = create_async_engine(first._url, **first._engine_options)
    try:
        async with admin_engine.begin() as connection:
            await connection.run_sync(first._models.Base.metadata.create_all)
        await first.load()
        await second.load()
        yield first, second
    finally:
        await first.close()
        await second.close()
        async with admin_engine.begin() as connection:
            await connection.run_sync(first._models.Base.metadata.drop_all)
        await admin_engine.dispose()


@pytest.mark.asyncio
async def test_mysql_task_work_store_contract(mysql_stores):
    first, second = mysql_stores
    owner = {"account_id": f"account-{uuid4().hex}", "user_id": "alice"}
    other_owner = {"account_id": owner["account_id"], "user_id": "bob"}

    async def create_guarded(index: int) -> str:
        task = TaskRecord(
            task_id=f"guarded-{index}",
            task_type="content_write",
            resource_id="same-resource",
            **owner,
        )
        try:
            await (first if index % 2 else second).create(task, reject_if_active=True)
            return "created"
        except TaskAlreadyActive:
            return "rejected"

    results = await asyncio.gather(*(create_guarded(index) for index in range(12)))
    assert results.count("created") == 1
    assert results.count("rejected") == 11
    guarded_task_id = f"guarded-{results.index('created')}"
    await first.create(
        TaskRecord(
            task_id="guarded-null-resource",
            task_type="content_write",
            resource_id=None,
            **owner,
        ),
        reject_if_active=True,
    )
    await second.create(
        TaskRecord(
            task_id="guarded-empty-resource",
            task_type="content_write",
            resource_id="",
            **owner,
        ),
        reject_if_active=True,
    )

    task = TaskRecord(
        task_id="lifecycle",
        task_type="content_write",
        resource_id="resource",
        meta={"request": {"to": "viking://a"}},
        **owner,
    )
    await first.create(task)
    await second.start_task(task.task_id, stage="running", **owner)
    assert (
        await first.add_work(
            WorkRecord(work_id="semantic", task_id=task.task_id, queue_name="Semantic"),
            **owner,
        )
    ).outcome is StoreOperationOutcome.APPLIED
    assert (
        await second.add_work(
            WorkRecord(work_id="embedding", task_id=task.task_id, queue_name="Embedding"),
            **owner,
        )
    ).outcome is StoreOperationOutcome.APPLIED
    await first.start_work(task.task_id, "semantic", **owner)
    await second.mark_enqueue_complete(task.task_id, "embedding", **owner)
    assert await first.is_enqueue_complete(task.task_id, "embedding", **owner)
    await first.record_event(
        task.task_id,
        "waiting_for_descendants",
        operation="contract",
        **owner,
    )
    await second.record_outcome(
        task.task_id,
        result={"done": True},
        error=None,
        resource_id=None,
        cancelled=False,
        **owner,
    )
    await first.finish_work(
        task.task_id,
        "semantic",
        WorkState.DONE,
        error=None,
        errors=None,
        context_count_delta=1,
        **owner,
    )
    active = await second.get_task(task.task_id, **owner)
    assert active is not None and active.status is TaskStatus.RUNNING
    await second.finish_work(
        task.task_id,
        "embedding",
        WorkState.DONE,
        error=None,
        errors=None,
        context_count_delta=0,
        **owner,
    )
    terminal = await first.get_task_with_events(task.task_id, **owner)
    assert terminal is not None and terminal.status is TaskStatus.COMPLETED
    assert terminal.execution_events is not None
    summary = await second.summarize_works(task.task_id, work_ids=None, **owner)
    assert summary.queues["Semantic"].processed == 1
    assert summary.context_count == 1

    await first.finish_work(
        task.task_id,
        "embedding",
        WorkState.FAILED,
        error="late failure",
        errors=["late failure"],
        context_count_delta=0,
        **owner,
    )
    unchanged = await second.get_task(task.task_id, **owner)
    assert unchanged is not None and unchanged.status is TaskStatus.COMPLETED

    cancellation = TaskRecord(
        task_id="cancellation",
        task_type="add_resource",
        **owner,
    )
    await first.create(cancellation)
    await first.start_task(cancellation.task_id, stage=None, **owner)
    await first.add_work(
        WorkRecord(work_id="cancel-work", task_id=cancellation.task_id, queue_name="Semantic"),
        **owner,
    )
    cancelling = await second.request_cancellation(
        cancellation.task_id,
        cancellable_types={"add_resource"},
        allow_terminal=False,
        **owner,
    )
    assert cancelling.outcome is StoreOperationOutcome.APPLIED
    late = await first.add_work(
        WorkRecord(work_id="late", task_id=cancellation.task_id, queue_name="Semantic"),
        **owner,
    )
    assert late.outcome is StoreOperationOutcome.REJECTED
    assert late.code is StoreOperationCode.INVALID_STATE
    blocked = await first.delete_tasks(cancellation.task_id, **owner)
    assert not blocked.accepted
    assert blocked.result.code is StoreOperationCode.ACTIVE_TASK
    assert blocked.blockers[0].task_id == cancellation.task_id
    await first.settle_work(cancellation.task_id, "cancel-work", **owner)
    cancelled = await second.get_task(cancellation.task_id, **owner)
    assert cancelled is not None and cancelled.status is TaskStatus.CANCELLED

    hidden = TaskRecord(task_id="hidden", task_type="private", **other_owner)
    await first.create(hidden)
    assert await second.get_task(hidden.task_id, **owner) is None
    listed_ids = {item.task_id for item in await first.list(owner["account_id"], user_id="alice")}
    assert {"lifecycle", "cancellation", guarded_task_id} <= listed_ids

    delete_owner = {"account_id": f"delete-{uuid4().hex}", "user_id": "owner"}
    protected = TaskRecord(
        task_id="protected",
        task_type="protected_type",
        **delete_owner,
    )
    await first.create(protected)
    await first.start_task(protected.task_id, stage=None, **delete_owner)
    await first.add_work(
        WorkRecord(work_id="open", task_id=protected.task_id, queue_name="Semantic"),
        **delete_owner,
    )
    blocked = await second.delete_tasks(**delete_owner)
    assert not blocked.accepted
    deleted = await second.delete_tasks(
        account_id=delete_owner["account_id"],
        user_id=delete_owner["user_id"],
        allow_active_types={"protected_type"},
    )
    assert deleted.accepted
    assert deleted.deleted == (DeletedTask("protected", **delete_owner),)

    deleted = await first.delete_tasks(cancellation.task_id, **owner)
    assert deleted.accepted
    assert deleted.deleted == (DeletedTask("cancellation", **owner),)
    assert await first.get_work(cancellation.task_id, "cancel-work", **owner) is None

    expired = TaskRecord(
        task_id="expired",
        task_type="cleanup",
        status=TaskStatus.COMPLETED,
        created_at=1,
        updated_at=1,
        **owner,
    )
    await first.create(expired)
    result = await second.delete_expired(
        TaskRetentionPolicy(completed_seconds=0, failed_seconds=0),
        now=time.time() + 1,
        limit=2,
    )
    assert any(item.task_id == "expired" for item in result.deleted)


@pytest.mark.asyncio
async def test_mysql_recovers_abandoned_inline_work(mysql_stores):
    store, _ = mysql_stores
    owner = {"account_id": f"recover-{uuid4().hex}", "user_id": "alice"}

    completed = TaskRecord(task_id="recover-completed", task_type="content_write", **owner)
    await store.create(completed)
    await store.start_task(completed.task_id, stage="running", **owner)
    await store.add_work(
        WorkRecord(work_id="inline", task_id=completed.task_id, queue_name="Inline"),
        **owner,
    )
    await store.record_outcome(
        completed.task_id,
        result={"written": True},
        error=None,
        resource_id=None,
        cancelled=False,
        **owner,
    )

    interrupted = TaskRecord(task_id="recover-interrupted", task_type="content_write", **owner)
    await store.create(interrupted)
    await store.start_task(interrupted.task_id, stage="running", **owner)
    await store.add_work(
        WorkRecord(work_id="inline", task_id=interrupted.task_id, queue_name="Inline"),
        **owner,
    )

    assert await store.recover_abandoned_inline_work() == 2

    completed_task = await store.get_task(completed.task_id, **owner)
    completed_work = await store.get_work(completed.task_id, "inline", **owner)
    assert completed_task is not None and completed_task.status is TaskStatus.COMPLETED
    assert completed_work is not None and completed_work.state is WorkState.DONE
    assert completed_work.error is None

    interrupted_task = await store.get_task(interrupted.task_id, **owner)
    interrupted_work = await store.get_work(interrupted.task_id, "inline", **owner)
    assert interrupted_task is not None and interrupted_task.status is TaskStatus.FAILED
    assert interrupted_work is not None and interrupted_work.state is WorkState.FAILED
    assert interrupted_work.error == "Inline operation interrupted by process restart"


@pytest.mark.asyncio
async def test_mysql_task_field_mutations_do_not_overwrite_each_other(mysql_stores):
    first, second = mysql_stores
    owner = {"account_id": f"account-{uuid4().hex}", "user_id": "alice"}
    task = TaskRecord(
        task_id="disjoint-fields",
        task_type="content_write",
        meta={"existing": {"value": "kept"}},
        auth={"existing": {"value": "kept"}},
        **owner,
    )
    await first.create(task)
    await first.start_task(task.task_id, stage=None, **owner)

    await asyncio.gather(
        first.merge_meta(task.task_id, {"meta_only": {"value": 1}}, **owner),
        second.update_auth(task.task_id, {"auth_only": {"value": 2}}, **owner),
    )

    stored = await first.get_task(task.task_id, **owner)
    assert stored is not None
    assert stored.meta == {
        "existing": {"value": "kept"},
        "meta_only": {"value": 1},
    }
    assert stored.auth == {
        "existing": {"value": "kept"},
        "auth_only": {"value": 2},
    }


@pytest.mark.asyncio
async def test_mysql_internal_flag_and_execution_event_projection(mysql_stores):
    store, _ = mysql_stores
    owner = {"account_id": f"account-{uuid4().hex}", "user_id": "alice"}
    task = TaskRecord(
        task_id="internal-task",
        task_type="internal_type",
        meta={"internal": True, "request": {"to": "viking://internal"}},
        **owner,
    )
    await store.create(task)

    assert (
        await store.list(
            account_id=owner["account_id"],
            user_id=owner["user_id"],
            include_internal=False,
        )
        == []
    )
    basic = await store.get_task(task.task_id, **owner)
    assert basic is not None and basic.execution_events is None
    stored = await store.get_task_with_events(task.task_id, **owner)
    assert stored is not None
    assert stored.execution_events["items"][0]["kind"] == "created"

    await store.merge_meta(task.task_id, {"internal": False}, **owner)
    assert [
        item.task_id
        for item in await store.list(
            account_id=owner["account_id"],
            user_id=owner["user_id"],
            include_internal=False,
        )
    ] == [task.task_id]


async def _running_task(store, *, task_id, owner, work_ids):
    await store.create(TaskRecord(task_id=task_id, task_type="add_resource", **owner))
    await store.start_task(task_id, stage=None, **owner)
    for work_id in work_ids:
        await store.add_work(
            WorkRecord(task_id=task_id, work_id=work_id, queue_name="Embedding"), **owner
        )
    await store.record_outcome(
        task_id, result={"done": True}, error=None, resource_id=None, cancelled=False, **owner
    )


async def _insert_rows(store, model, rows):
    # Store engines belong to MySQLRuntime's loop; fixture provisioning uses
    # its own engine on the pytest loop.
    engine = create_async_engine(store._url, **store._engine_options)
    try:
        async with engine.begin() as connection:
            await connection.execute(model.__table__.insert(), rows)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
@pytest.mark.parametrize("operation", ["finish", "settle", "discard"])
async def test_mysql_concurrent_last_work_closes_task(
    mysql_stores, monkeypatch, cancelled, operation
):
    first, second = mysql_stores
    owner = {"account_id": "concurrent", "user_id": "alice"}
    task_id = "last-two"
    await _running_task(first, task_id=task_id, owner=owner, work_ids=["one", "two"])
    if cancelled:
        await first.request_cancellation(
            task_id, cancellable_types={"add_resource"}, allow_terminal=False, **owner
        )

    # Hold the first open-work observation until the other transaction reaches
    # the same point. Correct Task locking serializes it, so the bounded wait
    # expires; without that lock both transactions observe the other Work open.
    observations = []
    both_observed = asyncio.Event()
    original = MySQLTaskWorkStore._has_open_work

    async def observe(store, session, observed_task_id):
        result = await original(store, session, observed_task_id)
        observations.append(result)
        if len(observations) == 2:
            both_observed.set()
        try:
            await asyncio.wait_for(both_observed.wait(), timeout=0.3)
        except TimeoutError:
            pass
        return result

    monkeypatch.setattr(MySQLTaskWorkStore, "_has_open_work", observe)

    async def close_work(store, work_id):
        if operation == "finish":
            return await store.finish_work(
                task_id,
                work_id,
                WorkState.DONE,
                error=None,
                errors=None,
                context_count_delta=0,
                **owner,
            )
        if operation == "settle":
            return await store.settle_work(task_id, work_id, **owner)
        return await store.discard_work(task_id, work_id, **owner)

    results = await asyncio.wait_for(
        asyncio.gather(close_work(first, "one"), close_work(second, "two")), timeout=10
    )
    assert all(result.is_applied for result in results)
    stored = await second.get_task_with_events(task_id, **owner)
    assert stored.status is (TaskStatus.CANCELLED if cancelled else TaskStatus.COMPLETED)
    assert not await second.has_open_work(task_id, exclude_work_id=None, **owner)
    terminal_events = [
        item
        for item in stored.execution_events["items"]
        if item["kind"] == "status_changed" and item["status"] == stored.status.value
    ]
    assert len(terminal_events) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("registration", ["add_work", "reconcile_delivery"])
async def test_mysql_registration_during_last_completion_preserves_terminal_task(
    mysql_stores, registration
):
    first, second = mysql_stores
    owner = {"account_id": "lock-order", "user_id": "alice"}

    for index in range(12):
        task_id = f"registration-{index}"
        await _running_task(first, task_id=task_id, owner=owner, work_ids=["last"])
        results = await asyncio.wait_for(
            asyncio.gather(
                first.finish_work(
                    task_id,
                    "last",
                    WorkState.DONE,
                    error=None,
                    errors=None,
                    context_count_delta=0,
                    **owner,
                ),
                getattr(second, registration)(
                    WorkRecord(task_id=task_id, work_id="last", queue_name="Embedding"),
                    **owner,
                ),
            ),
            timeout=10,
        )
        assert results[0].is_applied
        assert (await second.get_task(task_id, **owner)).status is TaskStatus.COMPLETED
        assert (await second.get_work(task_id, "last", **owner)).state is WorkState.DONE


@pytest.mark.asyncio
async def test_mysql_bulk_deletion_blockers_and_tracker_cleanup(mysql_stores):
    first, second = mysql_stores
    alice = {"account_id": "delete-account", "user_id": "alice"}
    bob = {"account_id": "delete-account", "user_id": "bob"}
    outsider = {"account_id": "other-account", "user_id": "alice"}
    for task_id, owner, status in [
        ("active", alice, TaskStatus.RUNNING),
        ("open", bob, TaskStatus.COMPLETED),
        ("terminal", alice, TaskStatus.COMPLETED),
        ("outside", outsider, TaskStatus.COMPLETED),
    ]:
        await first.create(TaskRecord(task_id=task_id, task_type="cleanup", status=status, **owner))
    # A terminal record with open Work can exist after an interrupted migration.
    await _insert_rows(
        first,
        first._models.Work,
        [
            {
                "task_id": "open",
                "work_id": "pending",
                "queue_name": "Embedding",
                "state": WorkState.PENDING.value,
                "enqueued_at": time.time(),
                "updated_at": time.time(),
                **bob,
            }
        ],
    )
    blocked = await second.delete_tasks(account_id=alice["account_id"])
    assert not blocked.accepted and not blocked.deleted
    assert {item.task_id: item.code for item in blocked.blockers} == {
        "active": StoreOperationCode.ACTIVE_TASK,
        "open": StoreOperationCode.OPEN_WORK,
    }
    assert await first.get_task("terminal", **alice) is not None
    hidden = await first.delete_tasks("open", **alice)
    assert hidden.accepted and not hidden.deleted
    assert hidden.result.code is StoreOperationCode.NOT_FOUND
    empty = await first.delete_tasks(account_id="missing")
    assert empty.accepted and not empty.deleted

    tracker = TaskTracker(second)
    assert await tracker.delete_tasks(**alice, allow_active_types={"cleanup"}) == 2
    assert await first.get_task("open", **bob) is not None
    assert await tracker.delete_tasks(alice["account_id"], allow_active_types={"cleanup"}) == 1
    assert await first.get_work("open", "pending", **bob) is None
    assert await first.get_task_with_events("open", **bob) is None
    assert await first.get_task("outside", **outsider) is not None


@pytest.mark.asyncio
async def test_mysql_root_cursor_is_bounded_and_complete(mysql_stores):
    store, _ = mysql_stores
    Task = store._models.Task
    await _insert_rows(
        store,
        Task,
        [
            {
                "task_id": f"history-{index:05d}",
                "task_type": "history",
                "account_id": ("_system" if index % 3 == 0 else f"account-{index % 3}"),
                "user_id": ("root" if index % 3 == 0 else "alice"),
                "status": TaskStatus.COMPLETED.value,
                "created_at": float(index // 3),
                "updated_at": float(index // 3),
            }
            for index in range(1000)
        ],
    )
    queries = []

    def record_select(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith("SELECT") and Task.__tablename__ in statement:
            queries.append((statement, parameters))

    tracker = TaskTracker(store)
    event.listen(store._engine.sync_engine, "before_cursor_execute", record_select)
    try:
        page = await tracker.list_root_tasks(account_id="account-1", user_id="alice", limit=1)
        assert len(queries) == 3
        assert all(
            "LIMIT" in statement and parameters[-1] == 2 for statement, parameters in queries
        )
        assert page[0].task_id == "history-00999"
        queries.clear()
        before = (page[-1].created_at, page[-1].task_id)
        page = await tracker.list_root_tasks(
            account_id="account-1", user_id="alice", limit=1, before=before
        )
        assert len(queries) == 3
        assert all(parameters[-1] == 2 for _, parameters in queries)
        assert page[0].task_id == "history-00998"
    finally:
        event.remove(store._engine.sync_engine, "before_cursor_execute", record_select)
    seen = []
    before = None
    while page := await tracker.list_root_tasks(
        account_id="account-1", user_id="alice", limit=37, before=before
    ):
        seen.extend(task.task_id for task in page)
        before = (page[-1].created_at, page[-1].task_id)
    assert seen == [f"history-{index:05d}" for index in range(999, -1, -1)]


@pytest.mark.asyncio
async def test_mysql_service_shutdown_keeps_store_for_producers_and_queue(
    mysql_stores, monkeypatch
):
    store, observer = mysql_stores
    owner = {"account_id": "shutdown", "user_id": "alice"}
    await _running_task(store, task_id="draining", owner=owner, work_ids=["worker"])
    tracker = TaskTracker(store)
    events = []

    async def stop_producer():
        assert (await store.get_task("draining", **owner)).status is TaskStatus.RUNNING
        events.append("producer")

    loop = asyncio.get_running_loop()

    def drain_queue():
        result = asyncio.run_coroutine_threadsafe(
            store.finish_work(
                "draining",
                "worker",
                WorkState.DONE,
                error=None,
                errors=None,
                context_count_delta=0,
                **owner,
            ),
            loop,
        ).result(timeout=10)
        assert result.is_applied
        events.append("queue")

    service = OpenVikingService.__new__(OpenVikingService)
    service._resource_service = SimpleNamespace(close_background_tasks=stop_producer)
    service._expiry_runtime = None
    service._session_service = SimpleNamespace(set_session_expiry_adapter=lambda _: None)
    service._runtime_config_manager = SimpleNamespace(stop_refresh_loop=stop_producer)
    service._watch_scheduler = SimpleNamespace(stop=stop_producer)
    service._session_auto_commit_scheduler = SimpleNamespace(stop=stop_producer)
    service._queue_manager = SimpleNamespace(stop=drain_queue)
    service._vlm_resolver = None
    service._config = SimpleNamespace(vlm=SimpleNamespace(close=lambda: None))
    service._vikingdb_manager = None
    service._agfs_client = None
    service._embedder = None
    monkeypatch.setattr(service, "_release_data_dir_lock", lambda: None)
    monkeypatch.setattr("openviking.service.core.get_task_tracker", lambda: tracker)
    await asyncio.wait_for(service.close(), timeout=15)
    assert events == ["producer"] * 4 + ["queue"]
    assert store._sessions is None
    assert (await observer.get_task("draining", **owner)).status is TaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_mysql_shutdown_timeout_retains_dependencies_until_retry(mysql_stores, monkeypatch):
    store, observer = mysql_stores
    owner = {"account_id": "shutdown-timeout", "user_id": "alice"}
    await _running_task(store, task_id="late-worker", owner=owner, work_ids=["worker"])
    release = threading.Event()
    results = []

    def worker():
        release.wait()
        results.append(
            asyncio.run(
                store.finish_work(
                    "late-worker",
                    "worker",
                    WorkState.DONE,
                    error=None,
                    errors=None,
                    context_count_delta=0,
                    **owner,
                )
            )
        )

    thread = threading.Thread(target=worker)
    thread.start()
    join = thread.join
    monkeypatch.setattr(thread, "join", lambda timeout: join(timeout=0.05))
    queue = QueueManager.__new__(QueueManager)
    queue._started = True
    queue._queue_threads = {"Semantic": thread}
    queue._queue_stop_events = {"Semantic": threading.Event()}
    queue._embedding_worker_stopped = threading.Event()
    queue._agfs = object()
    queue._queues = {"Semantic": object()}

    async def stop_producer():
        pass

    service = OpenVikingService.__new__(OpenVikingService)
    service._resource_service = SimpleNamespace(close_background_tasks=stop_producer)
    service._expiry_runtime = None
    service._session_service = SimpleNamespace(set_session_expiry_adapter=lambda _: None)
    service._runtime_config_manager = None
    service._watch_scheduler = None
    service._session_auto_commit_scheduler = None
    service._queue_manager = queue
    service._vlm_resolver = None
    service._config = SimpleNamespace(vlm=SimpleNamespace(close=lambda: None))
    service._vikingdb_manager = None
    service._agfs_client = None
    service._embedder = None
    monkeypatch.setattr(service, "_release_data_dir_lock", lambda: None)
    monkeypatch.setattr("openviking.service.core.get_task_tracker", lambda: TaskTracker(store))
    try:
        with pytest.raises(RuntimeError, match="Queue workers did not stop: Semantic"):
            await service.close()
        assert store._sessions is not None
        assert service._queue_manager is queue and queue.is_running()
        assert queue._agfs is not None and "Semantic" in queue._queues
    finally:
        release.set()
        await asyncio.to_thread(join, 5.0)
    assert not thread.is_alive() and results[0].is_applied
    await service.close()
    assert store._sessions is None and not queue.is_running()
    assert (await observer.get_task("late-worker", **owner)).status is TaskStatus.COMPLETED
