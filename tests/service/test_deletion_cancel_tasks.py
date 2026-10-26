# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Identity cleanup stopping the identity's Tasks."""

import pytest

from openviking.service import deletion as deletion_module
from openviking.service.deletion import DeletionService
from openviking.service.task_store import PersistentTaskStore
from openviking.service.task_tracker import TaskStatus, TaskTracker, set_task_tracker
from tests.test_task_tracker import _FakeAgfs

OWNER = {"account_id": "acme", "user_id": "alice"}


@pytest.fixture
def tracker():
    tracker = TaskTracker(store=PersistentTaskStore(_FakeAgfs()))
    set_task_tracker(tracker)
    yield tracker
    set_task_tracker(None)


@pytest.mark.asyncio
async def test_cancel_tasks_fails_connector_imports_instead_of_waiting(tracker, monkeypatch):
    monkeypatch.setattr(deletion_module, "_CANCEL_WAIT_SECONDS", 1)
    task = await tracker.create("connector_import", **OWNER)
    await tracker.start(task.task_id, **OWNER)

    await DeletionService._cancel_tasks(object.__new__(DeletionService), "acme", "alice")

    record = await tracker.get(task.task_id, **OWNER)
    assert record.status == TaskStatus.FAILED
    assert record.error == "Identity deleted"
