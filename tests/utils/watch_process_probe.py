# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Bounded subprocess probe used only by Watch persistence tests."""

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

from openviking.resource.watch_manager import WatchManager
from openviking.resource.watch_runtime import WatchRuntime
from openviking.resource.watch_store_file import FileWatchStore
from openviking.resource.watch_store_mysql import MySQLWatchStore
from openviking.service.task_store import PersistentTaskStore
from openviking.service.task_tracker import TaskTracker
from tests.utils.watch_fixtures import local_viking_fs

TARGET = "viking://resources/probe"
OWNER = {"account_id": "default", "user_id": "default"}


async def main():
    mode, material = sys.argv[1:3]
    if mode == "mysql":
        config = json.loads(material)
        store = MySQLWatchStore(service_name=config["service_name"], params=config["params"])
        await store.initialize()
        try:
            won = await store.begin_run(
                config["watch_id"],
                target_uri=config["target"],
            )
            print(json.dumps({"won": won is not None}))
        finally:
            await store.close()
        return

    root = Path(material)
    fs = local_viking_fs(root / "agfs")
    tracker = TaskTracker(PersistentTaskStore(fs._async_agfs))
    seen = []

    async def refresh(**kwargs):
        task = await tracker.create("add_resource", resource_id=TARGET, **OWNER)
        seen.append(task.task_id)
        await kwargs["on_task_created"](task)
        return {"status": "success", "task_id": task.task_id}

    service = WatchManager(
        FileWatchStore(fs),
        viking_fs=fs,
        tracker=tracker,
        resource_service=SimpleNamespace(refresh_resource=refresh, _connector=None),
    )
    await service.initialize()
    if mode == "seed":
        await fs.mkdir(TARGET, exist_ok=True)
        task = await service.create_task(
            path="https://example.org/probe",
            to_uri=TARGET,
            watch_interval=1,
        )
        receipt = await service.trigger(task.task_id)
        print(json.dumps({"watch_id": task.task_id, "task_id": receipt.task_id}))
    else:
        tasks = await service.get_all_tasks("default", "default", "root")
        state = await service.store.get_state(tasks[0].task_id)
        if state.current_run_id:
            await tracker.complete(state.current_run_id, {"root_uri": TARGET}, **OWNER)
        runtime = WatchRuntime(service, leader_enabled=False)
        await runtime.start()
        try:
            await runtime.sweep()
            tasks = await service.get_all_tasks("default", "default", "root")
            state = await service.store.get_state(tasks[0].task_id)
            print(
                json.dumps(
                    {
                        "submitted": seen,
                        "current_run_id": state.current_run_id,
                        "status": state.task.last_status,
                    }
                )
            )
        finally:
            await runtime.stop()
    await service.close()


if __name__ == "__main__":
    asyncio.run(main())
