# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Observable contract tests for Task/Work stores."""

import asyncio
import time
from copy import deepcopy

import pytest

from openviking.service.task_domain import (
    TaskRecord,
    TaskRetentionPolicy,
    TaskStatus,
    WorkRecord,
    WorkRecoveryMode,
    WorkState,
)
from openviking.service.task_store import (
    PersistentTaskStore,
    StoreOperationCode,
    StoreOperationOutcome,
    WorkRegistration,
)
from openviking.service.task_tracker import TaskTracker
from tests.test_task_tracker import _FakeAgfs

OWNER = {"account_id": "account", "user_id": "user"}


@pytest.fixture
def store() -> PersistentTaskStore:
    return PersistentTaskStore(_FakeAgfs())


async def create_running_task(
    store: PersistentTaskStore,
    task_id: str = "task",
    task_type: str = "content_write",
) -> TaskRecord:
    task = TaskRecord(task_id=task_id, task_type=task_type, **OWNER)
    await store.create(task)
    await store.start_task(task_id, stage=None, **OWNER)
    started = await store.get_task(task_id, **OWNER)
    assert started is not None
    return started


def test_file_store_declares_queue_snapshot_recovery(store):
    assert store.work_recovery_mode is WorkRecoveryMode.QUEUE_SNAPSHOT


@pytest.mark.asyncio
async def test_failed_task_write_does_not_mutate_live_cache():
    class FailingAgfs(_FakeAgfs):
        def write(self, path, data, *args, **kwargs):
            if b'"status": "running"' in data:
                raise OSError("write failed")
            return super().write(path, data, *args, **kwargs)

    store = PersistentTaskStore(FailingAgfs())
    task = TaskRecord(task_id="task", task_type="content_write", **OWNER)
    await store.create(task)

    with pytest.raises(OSError, match="write failed"):
        await store.start_task("task", stage="started", **OWNER)

    unchanged = await store.get_task("task", **OWNER)
    assert unchanged is not None
    assert unchanged.status is TaskStatus.PENDING
    assert unchanged.stage is None


@pytest.mark.asyncio
async def test_failed_finalization_write_does_not_publish_terminal_work():
    class FailingAgfs(_FakeAgfs):
        def write(self, path, data, *args, **kwargs):
            if b'"status": "completed"' in data:
                raise OSError("write failed")
            return super().write(path, data, *args, **kwargs)

    store = PersistentTaskStore(FailingAgfs())
    tracker = TaskTracker(store)
    await create_running_task(store)
    await tracker.register_work("task", "work", "Semantic", **OWNER)
    await tracker.complete("task", {"done": True}, **OWNER)

    with pytest.raises(OSError, match="write failed"):
        await tracker.mark_work_done("task", "work", **OWNER)

    task = await tracker.get("task", **OWNER)
    assert task is not None and task.status is TaskStatus.RUNNING
    assert await tracker.get_work_state("task", "work", **OWNER) is WorkState.PENDING
    assert await tracker.has_work("task", **OWNER)


@pytest.mark.asyncio
async def test_work_registration_is_idempotent_and_rejects_terminal_task(store):
    await create_running_task(store)
    work = WorkRecord(work_id="work", task_id="task", queue_name="Semantic")

    result = await store.add_work(work, **OWNER)
    assert result.outcome is StoreOperationOutcome.APPLIED
    assert result.code is StoreOperationCode.OK
    duplicate = await store.add_work(work, **OWNER)
    assert duplicate.outcome is StoreOperationOutcome.NOOP
    assert duplicate.code is StoreOperationCode.DUPLICATE

    await store.finish_work(
        "task",
        "work",
        WorkState.DONE,
        error=None,
        errors=None,
        context_count_delta=0,
        **OWNER,
    )
    task = await store.get_task("task", **OWNER)
    assert task is not None and task.status is TaskStatus.FAILED
    rejected = await store.add_work(
        WorkRecord(work_id="later", task_id="task", queue_name="Semantic"),
        **OWNER,
    )
    assert rejected.outcome is StoreOperationOutcome.REJECTED
    assert rejected.code is StoreOperationCode.ALREADY_TERMINAL


@pytest.mark.asyncio
async def test_cancellation_reports_whether_the_owned_task_exists(store):
    missing = await store.request_cancellation(
        "missing",
        cancellable_types={"content_write"},
        allow_terminal=False,
        **OWNER,
    )
    assert missing.outcome is StoreOperationOutcome.REJECTED
    assert missing.code is StoreOperationCode.NOT_FOUND
    await create_running_task(store)
    assert await store.get_task_status("task", **OWNER) is TaskStatus.RUNNING
    assert await store.get_task_status("task", account_id="account", user_id="other") is None
    accepted = await store.request_cancellation(
        "task",
        cancellable_types={"content_write"},
        allow_terminal=False,
        **OWNER,
    )
    assert accepted.outcome is StoreOperationOutcome.APPLIED
    repeated = await store.request_cancellation(
        "task",
        cancellable_types={"content_write"},
        allow_terminal=False,
        **OWNER,
    )
    assert repeated.outcome is StoreOperationOutcome.NOOP
    assert repeated.code is StoreOperationCode.ALREADY_CANCELLED
    assert await store.get_task_status("task", **OWNER) is TaskStatus.CANCELLED


@pytest.mark.asyncio
async def test_task_mutations_report_business_rejection_after_terminal(store):
    await create_running_task(store)
    await store.record_outcome(
        "task",
        result={"done": True},
        error=None,
        resource_id=None,
        cancelled=False,
        **OWNER,
    )

    rejected = await store.update_stage("task", "late-stage", meta=None, **OWNER)

    assert rejected.outcome is StoreOperationOutcome.REJECTED
    assert rejected.code is StoreOperationCode.ALREADY_TERMINAL
    task = await store.get_task("task", **OWNER)
    assert task is not None
    assert task.status is TaskStatus.COMPLETED
    assert task.stage == TaskStatus.COMPLETED.value


@pytest.mark.asyncio
async def test_wait_for_work_observes_only_explicit_ids(store):
    tracker = TaskTracker(store)
    await create_running_task(store)
    for work_id in ("first", "unrelated"):
        assert (
            await tracker.register_work("task", work_id, "Semantic", **OWNER)
            is WorkRegistration.CREATED
        )

    await tracker.mark_work_done("task", "first", **OWNER)
    await tracker.wait_for_work("task", ["first"], timeout=0, **OWNER)

    assert await tracker.has_work("task", **OWNER)


@pytest.mark.asyncio
async def test_wait_for_task_work_includes_restored_and_later_work(store):
    tracker = TaskTracker(store)
    await create_running_task(store)
    await tracker.reconcile_delivery("task", "semantic", "Semantic", **OWNER)

    waiting = asyncio.create_task(
        tracker.wait_for_task_work(
            "task",
            exclude_work_id="root",
            poll_interval=0.001,
            **OWNER,
        )
    )
    try:
        await asyncio.sleep(0)
        assert not waiting.done()
        await tracker.register_work("task", "embedding", "Embedding", **OWNER)
        await tracker.mark_work_done("task", "semantic", **OWNER)
        with pytest.raises(TimeoutError):
            await tracker.wait_for_task_work("task", exclude_work_id="root", timeout=0, **OWNER)
        await tracker.mark_work_failed("task", "embedding", errors=["embedding failed"], **OWNER)
        await asyncio.wait_for(waiting, timeout=1)
    finally:
        waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)

    status = await tracker.build_queue_status("task", **OWNER)
    assert status["Embedding"]["error_count"] == 1


@pytest.mark.asyncio
async def test_requeue_is_non_terminal_and_preserves_logical_work(store):
    tracker = TaskTracker(store)
    await create_running_task(store)
    await tracker.register_work("task", "work", "Semantic", **OWNER)
    await tracker.start_work("task", "work", **OWNER)

    await tracker.mark_work_requeued("task", "work", **OWNER)
    assert await tracker.has_work("task", **OWNER)
    assert await tracker.get_work_state("task", "work", **OWNER) is WorkState.PENDING

    await tracker.start_work("task", "work", **OWNER)
    await tracker.mark_work_done("task", "work", **OWNER)
    status = await tracker.build_queue_status("task", **OWNER)
    assert status["Semantic"] == {
        "processed": 1,
        "requeue_count": 1,
        "error_count": 0,
        "errors": [],
    }


@pytest.mark.asyncio
async def test_terminal_work_result_is_monotonic_and_counts_context(store):
    tracker = TaskTracker(store)
    await create_running_task(store)
    await tracker.register_work("task", "work", "Embedding", **OWNER)

    await tracker.mark_work_failed("task", "work", errors=["first", "second"], **OWNER)
    await tracker.mark_work_done("task", "work", context_count_delta=3, **OWNER)
    await tracker.mark_work_failed("task", "work", "later failure", **OWNER)

    status = await tracker.build_queue_status("task", **OWNER)
    assert status["Embedding"] == {
        "processed": 0,
        "requeue_count": 0,
        "error_count": 2,
        "errors": [{"message": "first"}, {"message": "second"}],
    }
    assert await tracker.get_context_count("task", **OWNER) == 0


@pytest.mark.asyncio
async def test_task_outcome_finalizes_after_all_work_settles(store):
    tracker = TaskTracker(store)
    await create_running_task(store)
    await tracker.register_work("task", "semantic", "Semantic", **OWNER)
    await tracker.register_work("task", "embedding", "Embedding", **OWNER)

    await tracker.complete("task", {"done": True}, **OWNER)
    active = await tracker.get("task", **OWNER)
    assert active is not None and active.status is TaskStatus.RUNNING

    await tracker.mark_work_done("task", "semantic", **OWNER)
    await tracker.mark_work_done("task", "embedding", vector_written=True, **OWNER)
    completed = await tracker.get("task", **OWNER)
    assert completed is not None and completed.status is TaskStatus.COMPLETED
    assert completed.result is not None
    assert completed.result["queue_status"]["Semantic"]["processed"] == 1
    assert await tracker.get_context_count("task", **OWNER) == 1


@pytest.mark.asyncio
async def test_persisted_queue_status_caps_error_details_without_losing_count(store):
    tracker = TaskTracker(store)
    await create_running_task(store)
    await tracker.register_work("task", "embedding", "Embedding", **OWNER)
    await tracker.complete("task", {"done": True}, **OWNER)

    await tracker.mark_work_failed(
        "task",
        "embedding",
        errors=[f"failure {index}" for index in range(150)],
        **OWNER,
    )

    completed = await tracker.get("task", **OWNER)
    assert completed is not None and completed.result is not None
    embedding = completed.result["queue_status"]["Embedding"]
    assert embedding["error_count"] == 150
    assert len(embedding["errors"]) == 100


@pytest.mark.asyncio
async def test_delete_expired_limits_reads_and_skips_cached_records():
    class CountingAgfs(_FakeAgfs):
        def __init__(self):
            super().__init__()
            self.read_count = 0

        def read(self, *args, **kwargs):
            value = super().read(*args, **kwargs)
            self.read_count += 1
            return value

    agfs = CountingAgfs()
    writer = PersistentTaskStore(agfs)
    for index in range(4):
        await writer.create(
            TaskRecord(
                task_id=f"task-{index}",
                task_type="content_write",
                **OWNER,
            )
        )

    store = PersistentTaskStore(agfs)
    for task_id in ("task-0", "task-1"):
        assert await store.get_task(task_id, **OWNER) is not None
    agfs.read_count = 0
    policy = TaskRetentionPolicy(completed_seconds=3600, failed_seconds=3600)

    first = await store.delete_expired(policy, now=time.time(), limit=2)
    assert first.scanned == 2
    assert agfs.read_count == 0

    second = await store.delete_expired(policy, now=time.time(), limit=2)
    assert second.scanned == 2
    assert agfs.read_count == 2


@pytest.mark.asyncio
async def test_deleting_task_releases_work_projection(store):
    tracker = TaskTracker(store)
    await create_running_task(store)
    await tracker.register_work("task", "work", "Semantic", **OWNER)
    await tracker.mark_work_done("task", "work", **OWNER)

    assert await tracker.delete_tasks(**OWNER) == 1
    assert not await tracker.has_work("task", **OWNER)
    assert await tracker.build_queue_status("task", **OWNER) == {
        "Semantic": {
            "processed": 0,
            "requeue_count": 0,
            "error_count": 0,
            "errors": [],
        },
        "Embedding": {
            "processed": 0,
            "requeue_count": 0,
            "error_count": 0,
            "errors": [],
        },
    }


@pytest.mark.asyncio
async def test_delete_tasks_does_not_preflight_list_before_store_delete():
    class CountingListStore(PersistentTaskStore):
        def __init__(self) -> None:
            super().__init__(_FakeAgfs())
            self.list_calls = 0

        async def list(self, *args, **kwargs):
            self.list_calls += 1
            return await super().list(*args, **kwargs)

    store = CountingListStore()
    tracker = TaskTracker(store)
    for task_id in ("task-0", "task-1"):
        task = await tracker.create("content_write", task_id=task_id, **OWNER)
        await tracker.complete(task.task_id, {"done": True}, **OWNER)
    store.list_calls = 0

    assert await tracker.delete_tasks(**OWNER) == 2

    assert store.list_calls == 1


@pytest.mark.asyncio
async def test_delete_tasks_raises_store_business_rejection_message(store):
    tracker = TaskTracker(store)
    await create_running_task(store, task_id="task-a")
    await create_running_task(store, task_id="task-b")

    with pytest.raises(RuntimeError) as exc_info:
        await tracker.delete_tasks(**OWNER)

    message = str(exc_info.value)
    assert message.startswith("Cannot delete active task records: ")
    assert "task-a(content_write)" in message
    assert "task-b(content_write)" in message
    assert await tracker.get("task-a", **OWNER) is not None
    assert await tracker.get("task-b", **OWNER) is not None


@pytest.mark.asyncio
async def test_cancelled_persisted_mutation_updates_read_view_after_write_finishes():
    class DelayedWriteStore(PersistentTaskStore):
        def __init__(self) -> None:
            super().__init__(_FakeAgfs())
            self.delay_writes = False
            self.entered_write = asyncio.Event()
            self.release_write = asyncio.Event()

        async def _write_task(self, task: TaskRecord) -> None:
            if self.delay_writes:
                self.entered_write.set()
                await self.release_write.wait()
            await super()._write_task(task)

    store = DelayedWriteStore()
    tracker = TaskTracker(store)
    await create_running_task(store)
    store.delay_writes = True

    completing = asyncio.create_task(tracker.complete("task", {"done": True}, **OWNER))
    await asyncio.wait_for(store.entered_write.wait(), timeout=1)
    completing.cancel()
    store.release_write.set()

    with pytest.raises(asyncio.CancelledError):
        await completing

    task = await tracker.get("task", **OWNER)
    assert task is not None
    assert task.status is TaskStatus.COMPLETED
    assert task.result == {"done": True}


@pytest.mark.asyncio
async def test_delete_tasks_rechecks_liveness_under_record_lock(monkeypatch, store):
    tracker = TaskTracker(store)
    running = await create_running_task(store)
    stale_terminal = deepcopy(running)
    stale_terminal.status = TaskStatus.COMPLETED
    stale_terminal.stage = TaskStatus.COMPLETED.value
    stale_terminal.result = {"done": True}

    async def stale_list(*args, **kwargs):
        return [deepcopy(stale_terminal)]

    monkeypatch.setattr(store, "list", stale_list)

    with pytest.raises(RuntimeError, match="Cannot delete active task record"):
        await tracker.delete_tasks(**OWNER)

    task = await tracker.get("task", **OWNER)
    assert task is not None
    assert task.status is TaskStatus.RUNNING


@pytest.mark.asyncio
async def test_delete_tasks_removes_all_users_in_account_only(store):
    tracker = TaskTracker(store)
    owners = [
        {"account_id": "account", "user_id": "alice"},
        {"account_id": "account", "user_id": "bob"},
        {"account_id": "other", "user_id": "alice"},
    ]
    for index, owner in enumerate(owners):
        task = await tracker.create("content_write", task_id=f"task-{index}", **owner)
        await tracker.complete(task.task_id, {"done": True}, **owner)

    assert await tracker.delete_tasks("account") == 2
    assert await tracker.get("task-0", **owners[0]) is None
    assert await tracker.get("task-1", **owners[1]) is None
    assert await tracker.get("task-2", **owners[2]) is not None
