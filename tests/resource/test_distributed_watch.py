"""Behavioral contracts for the distributed timer/observer orchestration."""

import asyncio
import time
from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from openviking.resource.distributed_uri_coordinator import DistributedUriCoordinator
from openviking.resource.distributed_watch_manager import DistributedWatchManager
from openviking.resource.distributed_watch_scheduler import DistributedWatchScheduler
from openviking.resource.distributed_watch_types import WatchProjection, WatchState
from openviking.resource.watch_manager import WatchTask
from openviking.scheduling.expiry import CallbackRequest, CallbackStatus
from openviking.service.watch_expiry import (
    WatchTimerHandler,
    WatchTimers,
    observer_identity,
    watch_identity,
)
from openviking.storage.mysql_runtime import MySQLRuntime, mysql_runtime_store


def run_state(*, active=True, deleted=False, submitted=True, owner="node-a", initial=False):
    task = WatchTask(
        task_id="watch-a", path="https://example.org/doc", to_uri="viking://resources/a",
        account_id="account-a", user_id="user-a", is_active=active,
    )
    return WatchState(
        task=task, current_run_id="run-a", run_started_at=100,
        submitted=submitted, deleted=deleted, revision=1, now=101,
        lease_owner=owner, lease_until=110, initial_run=initial,
    )


def scheduler_fixture(state):
    store = SimpleNamespace(
        get_state=AsyncMock(return_value=state), mark_submitted=AsyncMock(),
        acquire_submission=AsyncMock(return_value=False),
        get_projection=AsyncMock(return_value=None),
        release_submission=AsyncMock(),
        renew_submission=AsyncMock(return_value=True),
    )
    resource = SimpleNamespace(refresh_resource=AsyncMock())
    scheduler = DistributedWatchScheduler(resource, store)
    scheduler._running = True
    scheduler._loop = asyncio.get_running_loop()
    scheduler._timers = SimpleNamespace(
        arm_observer=AsyncMock(), arm=AsyncMock(), arm_identity=AsyncMock(),
        record_failure=lambda *args: None,
    )
    scheduler._watch_manager.finish_run = AsyncMock()
    return scheduler, store, resource


@pytest.mark.parametrize("active,deleted", [(False, False), (False, True), (True, False)])
async def test_observer_settles_occupancy_even_paused_or_deleted(monkeypatch, active, deleted):
    state = run_state(active=active, deleted=deleted)
    scheduler, _, _ = scheduler_fixture(state)
    record = SimpleNamespace(
        is_terminal=lambda: True, result={}, status=SimpleNamespace(value="completed"),
        error=None, task_id="run-a", created_at=100,
    )
    tracker = SimpleNamespace(get=AsyncMock(return_value=record))
    monkeypatch.setattr("openviking.resource.distributed_watch_scheduler.get_task_tracker", lambda: tracker)
    await scheduler.handle_observer("watch-a", "run-a")
    claim, result = scheduler.watch_manager.finish_run.call_args.args
    assert claim.run_id == "run-a"
    assert result.status == "completed"
    assert result.started_at == 100


async def test_observer_ignores_an_old_run(monkeypatch):
    scheduler, _, _ = scheduler_fixture(run_state())
    tracker = SimpleNamespace(get=AsyncMock())
    monkeypatch.setattr("openviking.resource.distributed_watch_scheduler.get_task_tracker", lambda: tracker)
    await scheduler.handle_observer("watch-a", "run-old")
    tracker.get.assert_not_awaited()
    scheduler.watch_manager.finish_run.assert_not_awaited()


async def test_missing_submitted_task_does_not_release_occupancy(monkeypatch):
    scheduler, store, resource = scheduler_fixture(run_state(deleted=True, active=False))
    tracker = SimpleNamespace(get=AsyncMock(return_value=None))
    monkeypatch.setattr("openviking.resource.distributed_watch_scheduler.get_task_tracker", lambda: tracker)
    await scheduler.handle_observer("watch-a", "run-a")
    scheduler.watch_manager.finish_run.assert_not_awaited()
    store.acquire_submission.assert_not_awaited()
    resource.refresh_resource.assert_not_awaited()
    scheduler._timers.arm_observer.assert_awaited_once()


async def test_timeout_requests_cancellation_and_keeps_occupancy(monkeypatch):
    scheduler, _, _ = scheduler_fixture(run_state())
    record = SimpleNamespace(is_terminal=lambda: False, created_at=0)
    tracker = SimpleNamespace(get=AsyncMock(return_value=record), cancel=AsyncMock())
    monkeypatch.setattr("openviking.resource.distributed_watch_scheduler.get_task_tracker", lambda: tracker)
    await scheduler.handle_observer("watch-a", "run-a")
    tracker.cancel.assert_awaited_once()
    scheduler.watch_manager.finish_run.assert_not_awaited()


async def test_manual_trigger_passes_manual_condition_for_paused_watch():
    state = run_state(active=False, submitted=False)
    scheduler, store, _ = scheduler_fixture(state)
    claim = state.claim()
    store.claim = AsyncMock(return_value=claim)
    scheduler.watch_manager.sync_timer = AsyncMock()
    scheduler._submit = AsyncMock()
    assert await scheduler.schedule_task("watch-a")
    assert store.claim.call_args.kwargs["manual"] is True
    scheduler._submit.assert_awaited_once_with(claim)
    store.claim.return_value = None
    assert not await scheduler.schedule_task("watch-a")
    assert scheduler._submit.await_count == 1


async def test_stale_submit_owner_never_calls_resource():
    state = run_state(submitted=False, owner="another-node")
    scheduler, _, resource = scheduler_fixture(state)
    await scheduler._submit(state.claim())
    resource.refresh_resource.assert_not_awaited()


async def test_manual_trigger_waits_for_submission_slot(monkeypatch):
    state = run_state(active=False, submitted=False)
    scheduler, store, _ = scheduler_fixture(state)
    scheduler._owner = state.lease_owner
    scheduler._submission_slots = asyncio.Semaphore(0)
    store.claim = AsyncMock(return_value=state.claim())
    scheduler.watch_manager.sync_timer = AsyncMock()
    scheduler._submit_resource = AsyncMock()
    tracker = SimpleNamespace(get=AsyncMock(return_value=None))
    monkeypatch.setattr("openviking.resource.distributed_watch_scheduler.get_task_tracker", lambda: tracker)
    observer_armed = asyncio.Event()
    scheduler._timers.arm_observer.side_effect = lambda *args: observer_armed.set()
    trigger = asyncio.create_task(scheduler.schedule_task(state.task.task_id))
    try:
        await asyncio.wait_for(observer_armed.wait(), 1)
        assert not trigger.done()
        scheduler._submit_resource.assert_not_awaited()
        scheduler._submission_slots.release()
        assert await asyncio.wait_for(trigger, 1)
        scheduler._submit_resource.assert_awaited_once_with(state.claim(), state)
    finally:
        trigger.cancel()
        await asyncio.gather(trigger, return_exceptions=True)


async def test_waiting_submission_rechecks_run_after_slot_is_available(monkeypatch):
    state = run_state(submitted=False)
    scheduler, store, resource = scheduler_fixture(state)
    scheduler._owner = state.lease_owner
    scheduler._submission_slots = asyncio.Semaphore(0)
    store.renew_submission = AsyncMock(return_value=True)
    entered = asyncio.Event()

    async def read_state(*args):
        entered.set()
        return state

    store.get_state.side_effect = read_state
    tracker = SimpleNamespace(get=AsyncMock(return_value=None))
    monkeypatch.setattr("openviking.resource.distributed_watch_scheduler.get_task_tracker", lambda: tracker)
    submission = asyncio.create_task(scheduler._submit(state.claim()))
    await asyncio.wait_for(entered.wait(), 1)
    assert not submission.done()
    store.get_state.side_effect = None
    store.get_state.return_value = replace(state, current_run_id="new-run")
    scheduler._submission_slots.release()
    await asyncio.wait_for(submission, 1)
    resource.refresh_resource.assert_not_awaited()
    scheduler.watch_manager.finish_run.assert_not_awaited()


async def test_submission_exception_after_task_commit_keeps_run_for_observer(monkeypatch):
    state = run_state(submitted=False)
    scheduler, store, _ = scheduler_fixture(state)
    scheduler._owner = state.lease_owner
    store.renew_submission = AsyncMock(return_value=True)
    scheduler._submit_resource = AsyncMock(side_effect=ConnectionError("response lost"))
    record = SimpleNamespace(is_terminal=lambda: False)
    tracker = SimpleNamespace(get=AsyncMock(side_effect=[None, record]))
    monkeypatch.setattr("openviking.resource.distributed_watch_scheduler.get_task_tracker", lambda: tracker)
    await scheduler._submit(state.claim())
    store.mark_submitted.assert_awaited_once_with(state.claim())
    scheduler.watch_manager.finish_run.assert_not_awaited()


async def test_unsubmitted_run_is_recovered_using_the_same_run_id(monkeypatch):
    state = run_state(submitted=False, owner="dead-node")
    scheduler, store, _ = scheduler_fixture(state)
    store.acquire_submission.return_value = True
    scheduler._submit = AsyncMock()
    tracker = SimpleNamespace(get=AsyncMock(return_value=None))
    monkeypatch.setattr("openviking.resource.distributed_watch_scheduler.get_task_tracker", lambda: tracker)
    await scheduler.handle_observer(state.task.task_id, state.current_run_id)
    store.acquire_submission.assert_awaited_once_with(
        state.task.task_id, state.current_run_id, owner=scheduler._owner,
    )
    scheduler._submit.assert_awaited_once_with(state.claim())
    scheduler.watch_manager.finish_run.assert_not_awaited()


def submission_callback_fixture(monkeypatch, kind):
    state = run_state(submitted=False)
    state.task.next_execution_time = datetime.fromtimestamp(100)
    scheduler, store, resource = scheduler_fixture(state)
    scheduler._owner = state.lease_owner
    tracker = SimpleNamespace(get=AsyncMock(return_value=None), cancel=AsyncMock())
    monkeypatch.setattr("openviking.resource.distributed_watch_scheduler.get_task_tracker", lambda: tracker)
    if kind == "timer":
        store.get_state.return_value = replace(state, current_run_id=None)

        async def claim(*args, **kwargs):
            store.get_state.return_value = state
            return state.claim()

        store.claim = AsyncMock(side_effect=claim)
        identity = watch_identity(state.task)
    else:
        store.acquire_submission.return_value = True
        identity = observer_identity(state.task, state.current_run_id)
    handler = WatchTimerHandler(scheduler, scheduler._timers, error_backoff=60)
    return state, scheduler, store, resource, tracker, handler, CallbackRequest(identity)


@pytest.mark.parametrize("kind", ["timer", "observer"])
async def test_callback_waits_for_submission_but_not_ingestion_completion(monkeypatch, kind):
    state, scheduler, store, resource, tracker, handler, request = submission_callback_fixture(
        monkeypatch, kind,
    )
    entered, release = asyncio.Event(), asyncio.Event()
    pending_record = SimpleNamespace(is_terminal=lambda: False, created_at=time.time())

    async def submit_resource(**kwargs):
        assert kwargs["task_id"] == state.current_run_id
        entered.set()
        await release.wait()
        tracker.get.return_value = pending_record
        store.get_state.return_value = replace(state, submitted=True, lease_owner=None)
        return {"task_id": state.current_run_id}

    resource.refresh_resource.side_effect = submit_resource
    callback = asyncio.create_task(handler.handle(request))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        assert not callback.done(), "callback must not ACK while submission is in progress"
        if kind == "timer":
            scheduler._timers.arm_observer.assert_awaited_once_with(state.task, state.current_run_id)
        release.set()
        result = await asyncio.wait_for(callback, 1)
        assert result.status is CallbackStatus.COMPLETED
        assert result.reason == "advanced"
        assert not pending_record.is_terminal()
        store.mark_submitted.assert_awaited_once_with(state.claim())
        scheduler.watch_manager.finish_run.assert_not_awaited()
    finally:
        callback.cancel()
        await asyncio.gather(callback, return_exceptions=True)


async def test_callback_cancellation_stops_submission_and_preserves_run(monkeypatch):
    state, scheduler, store, resource, _, handler, request = submission_callback_fixture(
        monkeypatch, "timer",
    )
    entered = asyncio.Event()
    stopped = asyncio.Event()

    async def submit_resource(**kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    resource.refresh_resource.side_effect = submit_resource
    callback = asyncio.create_task(handler.handle(request))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        callback.cancel()
        with pytest.raises(asyncio.CancelledError):
            await callback
        assert stopped.is_set()
        assert store.get_state.return_value.current_run_id == state.current_run_id
        store.release_submission.assert_awaited_once_with(state.claim(), scheduler._owner)
        store.mark_submitted.assert_not_awaited()
        scheduler.watch_manager.finish_run.assert_not_awaited()
    finally:
        callback.cancel()
        await asyncio.gather(callback, return_exceptions=True)


@pytest.mark.parametrize("redis_failure", [False, True])
async def test_submit_failure_retries_same_run_before_ack(monkeypatch, redis_failure):
    state, scheduler, store, _, _, handler, request = submission_callback_fixture(
        monkeypatch, "observer",
    )
    scheduler._submit_resource = AsyncMock(side_effect=ConnectionError("dispatch unavailable"))
    if redis_failure:
        scheduler._timers.arm_identity.side_effect = ConnectionError("Redis unavailable")
    result = await handler.handle(request)
    assert result.status is (CallbackStatus.RETRY if redis_failure else CallbackStatus.COMPLETED)
    assert result.reason == ("backoff_report_failed" if redis_failure else "backoff")
    scheduler._timers.arm_identity.assert_awaited_once()
    assert scheduler._timers.arm_identity.call_args.args[0] == request.identity
    assert store.get_state.return_value.current_run_id == state.current_run_id
    store.mark_submitted.assert_not_awaited()
    store.release_submission.assert_awaited_once_with(state.claim(), scheduler._owner)
    scheduler.watch_manager.finish_run.assert_not_awaited()


async def test_failed_timer_projection_remains_dirty():
    projection = WatchProjection(
        "watch-a", "account-a", "user-a", True, False, 1, 200, None, 2, 1, 100,
    )
    store = SimpleNamespace(
        get_projection=AsyncMock(return_value=projection), acknowledge_timer=AsyncMock(),
    )
    manager = DistributedWatchManager(store)
    timers = SimpleNamespace(sync=AsyncMock(side_effect=ConnectionError("Redis unavailable")))
    manager.set_timer_adapter(timers)
    await manager.sync_timer("watch-a")
    store.acknowledge_timer.assert_not_awaited()


async def test_stale_projection_is_re_read_before_ack():
    first = WatchProjection(
        "watch-a", "account-a", "user-a", True, False, 1, 200, None, 2, 1, 100,
    )
    second = replace(first, enabled=False, next_run_at=None, revision=3)
    store = SimpleNamespace(
        get_projection=AsyncMock(side_effect=[first, second]),
        acknowledge_timer=AsyncMock(side_effect=[False, True]),
    )
    manager = DistributedWatchManager(store)
    timers = SimpleNamespace(sync=AsyncMock())
    manager.set_timer_adapter(timers)
    await manager.sync_timer("watch-a")
    assert [call.args[0] for call in timers.sync.await_args_list] == [first, second]
    assert store.acknowledge_timer.await_args_list[-1].args == ("watch-a", 3)


@pytest.mark.parametrize("deleted", [False, True])
async def test_paused_or_deleted_projection_registers_observer(deleted):
    projection = WatchProjection(
        "watch-a", "account-a", "user-a", False, deleted, 1, None, "run-a", 2, 1, 100,
    )
    expiry = SimpleNamespace(
        report=AsyncMock(return_value=SimpleNamespace(accepted_due_at_ms=100_000)),
        unregister=AsyncMock(),
    )
    await WatchTimers(expiry).sync(projection)
    registration = expiry.report.call_args.args[0]
    assert registration.identity.item_type == "watch_observer"
    assert registration.identity.local_id == '["watch-a","run-a"]'
    assert registration.due_at_ms == 100_000
    expiry.unregister.assert_awaited_once()


async def test_cancelled_mutation_wait_cleans_its_token():
    attempted = asyncio.Event()

    async def blocked(*args):
        attempted.set()
        return False

    store = SimpleNamespace(acquire_mutation=AsyncMock(side_effect=blocked), release_mutation=AsyncMock())
    coordinator = DistributedUriCoordinator(store)

    async def mutate():
        async with coordinator.mutation("account-a", ["viking://resources/a"]):
            pytest.fail("occupied mutation entered")

    operation = asyncio.create_task(mutate())
    await asyncio.wait_for(attempted.wait(), 1)
    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation
    store.release_mutation.assert_awaited_once()


async def test_first_callback_waits_for_terminal_task(monkeypatch):
    state = run_state()
    store = SimpleNamespace(get_state=AsyncMock(return_value=state))
    manager = DistributedWatchManager(store)
    manager.finish_run = AsyncMock()
    record = SimpleNamespace(is_terminal=lambda: False)
    tracker = SimpleNamespace(get=AsyncMock(return_value=record))
    monkeypatch.setattr("openviking.service.task_tracker.get_task_tracker", lambda: tracker)
    await manager.record_execution("watch-a", status="completed", execution_task_id="run-a")
    manager.finish_run.assert_not_awaited()


async def test_identity_delete_settles_watch_before_task_evidence_is_removed(monkeypatch):
    state = run_state()
    scheduler, store, _ = scheduler_fixture(state)
    store.delete_tasks = AsyncMock(return_value=[state])
    store.get_state.return_value = replace(state, deleted=True, current_run_id=None)
    scheduler.watch_manager.sync_timer = AsyncMock()
    record = SimpleNamespace(
        is_terminal=lambda: True, result={}, status=SimpleNamespace(value="cancelled"),
        error=None, task_id=state.current_run_id, created_at=100,
    )
    tracker = SimpleNamespace(get=AsyncMock(return_value=record), cancel=AsyncMock())
    monkeypatch.setattr("openviking.resource.distributed_watch_scheduler.get_task_tracker", lambda: tracker)
    await scheduler.delete_tasks(state.task.account_id, state.task.user_id)
    _, result = scheduler.watch_manager.finish_run.call_args.args
    assert result.status == "cancelled"
    tracker.cancel.assert_not_awaited()
    scheduler.watch_manager.sync_timer.assert_awaited_once_with(state.task.task_id)


async def test_concurrent_store_initialize_acquires_only_one_runtime_lease():
    @mysql_runtime_store
    class Store:
        def __init__(self):
            self._mysql_runtime = MySQLRuntime()
            self.ready = False
            self.acquires = 0

        async def initialize(self):
            if self.ready:
                return
            await self._mysql_runtime.acquire()
            self.acquires += 1
            await asyncio.sleep(0)
            self.ready = True

        async def close(self):
            if self.ready:
                self.ready = False
                await self._mysql_runtime.release()

    store = Store()
    try:
        await asyncio.gather(store.initialize(), store.initialize())
        assert store.acquires == 1
    finally:
        await store.close()
