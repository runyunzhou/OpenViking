# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Watch test fixtures: the default file Watch store over real local RAGFS.

Import the fixtures a module needs, e.g.
``from tests.utils.watch_fixtures import watch_manager, watch_store, watch_vfs``.
"""

from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator

import pytest_asyncio

from openviking.resource.watch_manager import WatchManager, WatchPathLocks
from openviking.resource.watch_store import WatchStore
from openviking.resource.watch_store_file import FileWatchStore


def local_viking_fs(root: Path):
    from openviking.storage.viking_fs import VikingFS
    from openviking.utils.agfs_utils import RagfsBindingConfig, create_agfs_client
    from openviking_cli.utils.config.agfs_config import AGFSConfig

    agfs = create_agfs_client(RagfsBindingConfig(agfs=AGFSConfig(path=str(root), backend="local")))
    return VikingFS(agfs=agfs)


async def shape_watch(store: WatchStore, task_id: str, **values: Any) -> None:
    """Set ``next_run_at``, ``current_run_id`` or ``run_started_at`` (epoch) directly."""
    unknown = values.keys() - {"next_run_at", "current_run_id", "run_started_at"}
    if unknown:
        raise ValueError(f"unsupported fields: {sorted(unknown)}")
    if isinstance(store, FileWatchStore):
        row = store._rows[task_id]
        task = row.task
        if "next_run_at" in values:
            at = values.pop("next_run_at")
            task = task.model_copy(
                update={"next_execution_time": datetime.fromtimestamp(at) if at else None}
            )
        await store._commit({task_id: replace(row, task=task, **values)})
        return
    from sqlalchemy import update

    async def write() -> None:
        async with store._transaction() as session:
            await session.execute(
                update(store._tasks).where(store._tasks.c.task_id == task_id).values(**values)
            )

    await store._mysql_runtime.run(write)


@pytest_asyncio.fixture
async def watch_vfs(tmp_path: Path):
    return local_viking_fs(tmp_path / "agfs")


@pytest_asyncio.fixture
async def watch_store(watch_vfs) -> AsyncIterator[FileWatchStore]:
    store = FileWatchStore(watch_vfs)
    await store.initialize()
    yield store
    await store.close()


@pytest_asyncio.fixture
async def watch_manager(watch_store: WatchStore, watch_vfs) -> WatchManager:
    return WatchManager(watch_store, WatchPathLocks(watch_vfs))
