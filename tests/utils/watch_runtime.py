# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Test assembly for original scheduling scenarios against Manager/Runtime.

The convenience trigger waits for submission only. There are no per-run Task
followers: tests must explicitly sweep after making a business Task terminal.
"""

import uuid
from types import SimpleNamespace

from openviking.resource.watch_manager import WatchManager
from openviking.resource.watch_runtime import WatchRuntime


class RuntimeHarness(WatchRuntime):
    def __init__(self, resources, store, *, viking_fs, tracker=None, **kwargs):
        if isinstance(resources, SimpleNamespace):
            fake_resources = resources

            async def refresh(**values):
                task_tracker = manager.tracker
                task_id = str(uuid.uuid4())
                record = await task_tracker.create(
                    "add_resource",
                    task_id=task_id,
                    resource_id=values["to"],
                    account_id=values["ctx"].account_id,
                    user_id=values["ctx"].user.user_id,
                )
                await values.pop("on_task_created")(record)
                return await fake_resources.refresh_resource(task_id=task_id, **values)

            resources = SimpleNamespace(
                refresh_resource=refresh, _connector=fake_resources._connector
            )
        runtime_config = kwargs.pop("runtime_config_manager", None)
        kwargs.pop("cancel_grace", None)
        manager = WatchManager(
            store,
            viking_fs=viking_fs,
            tracker=tracker,
            resource_service=resources,
            runtime_config_manager=runtime_config,
        )
        super().__init__(manager, **kwargs)

    @property
    def _viking_fs(self):
        return self.watch_manager._viking_fs

    @property
    def _background(self):
        return set(self._submissions.values())

    async def start(self):
        await self.watch_manager.initialize()
        await super().start()

    async def schedule_task(self, task_id):
        receipt = await self.watch_manager.trigger(task_id)
        if receipt is None or receipt.reused:
            return False
        await self.sweep()
        return True
