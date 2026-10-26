# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""File Watch store persistence over real local RAGFS."""

import json
from datetime import datetime, timedelta

import pytest

from openviking.resource.watch_store_file import FileWatchStore
from openviking.resource.watch_types import WatchAuthority, WatchRunResult, WatchTask
from openviking.server.identity import RequestContext, Role
from openviking_cli.session.user_id import UserIdentifier


def _ctx() -> RequestContext:
    return RequestContext(user=UserIdentifier.the_default_user(), role=Role.ROOT)


async def _write(viking_fs, uri, data) -> None:
    await viking_fs.write_file(uri, json.dumps(data), ctx=_ctx())


async def _read(viking_fs, uri) -> dict:
    return json.loads(await viking_fs.read_file(uri, ctx=_ctx()))


async def _open(viking_fs) -> FileWatchStore:
    store = FileWatchStore(viking_fs)
    await store.initialize()
    return store


def _task(**values) -> WatchTask:
    values.setdefault("path", "https://example.org/doc")
    values.setdefault("to_uri", "viking://resources/doc")
    task = WatchTask(watch_interval=5, **values)
    task.next_execution_time = task.calculate_next_execution_time() if task.is_active else None
    return task


@pytest.mark.asyncio
async def test_loads_and_normalizes_a_file_written_by_earlier_releases(watch_vfs):
    created = datetime.now() - timedelta(hours=1)
    unscheduled = WatchTask(
        path="https://example.org/a", to_uri="viking://resources/a", created_at=created
    )
    paused = WatchTask(
        path="https://example.org/b",
        to_uri="viking://resources/b",
        is_active=False,
        next_execution_time=datetime.now(),
    )
    await _write(
        watch_vfs,
        FileWatchStore.STORAGE_URI,
        {"tasks": [unscheduled.to_storage_dict(), paused.to_storage_dict()]},
    )

    store = await _open(watch_vfs)

    loaded = await store.get_task(unscheduled.task_id)
    assert loaded.next_execution_time == created + timedelta(minutes=unscheduled.watch_interval)
    assert (await store.get_task(paused.task_id)).next_execution_time is None
    assert (await store.get_state(unscheduled.task_id)).current_run_id is None
    persisted = await _read(watch_vfs, FileWatchStore.STORAGE_URI)
    assert {item["task_id"] for item in persisted["tasks"]} == {
        unscheduled.task_id,
        paused.task_id,
    }


@pytest.mark.asyncio
async def test_recovers_from_backup_when_the_file_is_corrupt(watch_vfs):
    task = _task()
    await _write(watch_vfs, FileWatchStore.STORAGE_BAK_URI, {"tasks": [task.to_storage_dict()]})
    await watch_vfs.write_file(FileWatchStore.STORAGE_URI, "{not json", ctx=_ctx())

    store = await _open(watch_vfs)

    assert (await store.get_task(task.task_id)).to_uri == task.to_uri
    persisted = await _read(watch_vfs, FileWatchStore.STORAGE_URI)
    assert [item["task_id"] for item in persisted["tasks"]] == [task.task_id]


@pytest.mark.asyncio
async def test_run_occupancy_and_tombstones_survive_restart(watch_vfs):
    store = await _open(watch_vfs)
    busy = _task(to_uri="viking://resources/busy")
    idle = _task(to_uri="viking://resources/idle")
    await store.create_task(busy, initial_run_id="run-1")
    await store.create_task(idle)
    await store.delete_task(busy.task_id, WatchAuthority())

    reopened = await _open(watch_vfs)

    state = await reopened.get_state(busy.task_id)
    assert state.deleted and state.current_run_id == "run-1"
    assert state.run_started_at is not None
    assert await reopened.has_running_overlap("default", ["viking://resources"])
    assert await reopened.finish_run(busy.task_id, "run-1", WatchRunResult(status="completed"))
    assert await reopened.get_state(busy.task_id) is None

    third = await _open(watch_vfs)
    assert await third.get_state(busy.task_id) is None
    assert (await third.get_state(idle.task_id)).current_run_id is None


@pytest.mark.asyncio
async def test_idle_tombstones_are_dropped_on_load(watch_vfs):
    task = _task()
    await _write(
        watch_vfs,
        FileWatchStore.STORAGE_URI,
        {"tasks": [{**task.to_storage_dict(), "deleted": True}]},
    )

    store = await _open(watch_vfs)

    assert await store.get_state(task.task_id) is None
    assert (await _read(watch_vfs, FileWatchStore.STORAGE_URI))["tasks"] == []


@pytest.mark.asyncio
async def test_writes_while_a_tree_lock_covers_the_resources_root(watch_vfs):
    store = await _open(watch_vfs)
    root = watch_vfs._uri_to_path("viking://resources", ctx=_ctx())
    await watch_vfs.mkdir("viking://resources", ctx=_ctx(), exist_ok=True)
    lease = await watch_vfs._async_agfs.pathlock_acquire_tree(root, timeout_secs=0)
    try:
        task = _task()
        await store.create_task(task, initial_run_id="run-1")
        await store.finish_run(task.task_id, "run-1", WatchRunResult(status="completed"))
    finally:
        await watch_vfs._async_agfs.pathlock_release(lease)

    reopened = await _open(watch_vfs)
    assert (await reopened.get_task(task.task_id)).last_status == "completed"


class _CountingFs:
    def __init__(self, viking_fs):
        self._inner = viking_fs
        self.reads = 0
        self.writes = 0

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def read_file(self, *args, **kwargs):
        self.reads += 1
        return await self._inner.read_file(*args, **kwargs)

    async def write_file(self, *args, **kwargs):
        self.writes += 1
        return await self._inner.write_file(*args, **kwargs)


@pytest.mark.asyncio
async def test_reads_come_from_memory_and_a_scheduled_run_writes_once(watch_vfs):
    fs = _CountingFs(watch_vfs)
    store = await _open(fs)
    task = _task()
    await store.create_task(task)
    reads, writes = fs.reads, fs.writes

    assert (await store.get_task(task.task_id)).task_id == task.task_id
    assert (
        len(
            await store.list_tasks(
                WatchAuthority(), target_uri=task.to_uri, scope_account_id="default"
            )
        )
        == 1
    )
    assert await store.due(10) == []
    claim = await store.claim(task.task_id, target_uri=task.to_uri, manual=True)
    assert await store.has_running_overlap("default", [task.to_uri])
    assert fs.reads == reads and fs.writes == writes

    assert await store.finish_run(task.task_id, claim.run_id, WatchRunResult(status="completed"))
    assert fs.reads == reads and fs.writes == writes + 1
    assert not await store.has_running_overlap("default", [task.to_uri])


@pytest.mark.asyncio
async def test_target_index_follows_rewrites_and_deletes(watch_vfs):
    store = await _open(watch_vfs)
    task = _task(to_uri="viking://resources/a/doc")
    await store.create_task(task)

    await store.rewrite_target_prefix("default", "viking://resources/a", "viking://resources/b")

    def on(uri):
        return store.list_tasks(WatchAuthority(), scope_account_id="default", target_uri=uri)

    assert await on("viking://resources/a/doc") == []
    assert [t.task_id for t in await on("viking://resources/b/doc")] == [task.task_id]
    await store.create_task(_task(to_uri="viking://resources/a/doc"))

    await store.delete_task(task.task_id, WatchAuthority())
    assert await on("viking://resources/b/doc") == []
    await store.create_task(_task(to_uri="viking://resources/b/doc"))
