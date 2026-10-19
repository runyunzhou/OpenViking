# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""ExternalTask recovery through persistent queue deliveries and task stores."""

import asyncio
import json
import os
from pathlib import Path
from uuid import uuid4

import pytest
import ragfs_python

from openviking.server.identity import RequestContext, Role
from openviking.service.external_task_service import ExternalTaskService, ExternalTaskSnapshot
from openviking.service.task_domain import TaskStatus, WorkRecoveryMode
from openviking.service.task_store import PersistentTaskStore
from openviking.service.task_store_mysql import MySQLTaskWorkStore
from openviking.service.task_tracker import TaskTracker, set_task_tracker
from openviking.service.task_work_index import bind_task_context
from openviking.storage.queuefs.external_task_processor import ExternalTaskProcessor
from openviking.storage.queuefs.queue_manager import QueueManager
from openviking_cli.session.user_id import UserIdentifier


@pytest.fixture(params=["file", "mysql"])
async def recovery_environment(request, tmp_path):
    params = None
    admin = None
    metadata = None
    if request.param == "mysql":
        if not (raw := os.environ.get("OV_TEST_MYSQL")):
            pytest.skip("Set OV_TEST_MYSQL for external task recovery integration")
        from sqlalchemy.ext.asyncio import create_async_engine

        params = {**json.loads(raw), "table": f"ov_external_{uuid4().hex[:12]}"}
        provision = MySQLTaskWorkStore(params=params)
        metadata = provision._models.Base.metadata
        admin = create_async_engine(provision._url, **provision._engine_options)
        sql_path = Path(__file__).parents[2] / "openviking/service/sql/task_store_init.sql"
        async with admin.begin() as connection:
            for statement in sql_path.read_text().replace("ov_tasks", params["table"]).split(";"):
                if "CREATE TABLE" in statement:
                    await connection.exec_driver_sql(statement)

    clients = []
    stores = []
    (tmp_path / "fs").mkdir()

    async def open_environment(service):
        client = ragfs_python.RAGFSBindingClient()
        client.mount("localfs", "/local", {"local_dir": str(tmp_path / "fs")})
        client.mount(
            "queuefs", "/queue", {"backend": "sqlite", "db_path": str(tmp_path / "queue.db")}
        )
        store = MySQLTaskWorkStore(params=params) if params else PersistentTaskStore(client)
        tracker = TaskTracker(store)
        await tracker.load()
        set_task_tracker(tracker)
        manager = QueueManager(client)
        queue = manager.get_queue(
            manager.EXTERNAL_TASK, ExternalTaskProcessor(service), allow_create=True
        )
        clients.append(client)
        stores.append(store)
        return client, tracker, manager, queue

    try:
        yield open_environment
    finally:
        set_task_tracker(None)
        for store in stores:
            closer = getattr(store, "close", None)
            if closer:
                await closer()
        for client in clients:
            client.close()
        if admin:
            async with admin.begin() as connection:
                await connection.run_sync(metadata.drop_all)
            await admin.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["polling", "submitting", "runtime_queued", "cancelling"])
async def test_recovery_reserves_delivered_task_before_pending_target_work(
    recovery_environment, monkeypatch, phase
):
    entered = asyncio.Event()
    release = asyncio.Event()
    submissions = []
    resumes = []
    first_id = None

    class Provider:
        task_type = "compile"
        task_id_prefix = "cmp_"
        poll_max_attempts = 1
        poll_interval_seconds = 0.001
        queued = False

        def serialization_key(self, payload):
            return payload["to"]

        async def submit(self, task_id, payload, private_payload, connection):
            submissions.append(task_id)
            if task_id == first_id and phase == "submitting" and not release.is_set():
                entered.set()
                await release.wait()
            return task_id

        async def get(self, task_id, connection, **kwargs):
            if task_id == first_id:
                if not release.is_set():
                    if phase == "runtime_queued" and not self.queued:
                        self.queued = True
                        return ExternalTaskSnapshot(status="pending", stage="queued")
                    entered.set()
                    await release.wait()
                resumes.append(task_id)
            return ExternalTaskSnapshot(status="completed", result={"output": "ready"})

        async def cancel(self, task_id, connection, **kwargs):
            resumes.append(task_id)
            return ExternalTaskSnapshot(status="cancelled")

    provider = Provider()
    service = ExternalTaskService()
    service.register(provider)
    client, tracker, manager, queue = await recovery_environment(service)
    monkeypatch.setattr(
        "openviking.service.external_task_service.get_queue_manager", lambda: manager
    )
    monkeypatch.setattr(
        "openviking.storage.queuefs.external_task_processor.get_queue_manager", lambda: manager
    )
    alice = RequestContext(user=UserIdentifier("account", "alice"), role=Role.USER)
    bob = RequestContext(user=UserIdentifier("account", "bob"), role=Role.USER)
    target = "viking://resources/output"

    async def create(ctx, to=target):
        return await service.create(
            "compile",
            resource_id="viking://resources/source",
            payload={"to": to, "skill": "viking://agent/skills/wiki"},
            connection={},
            ctx=ctx,
        )

    first = await create(alice)
    first_id = first.task_id
    waiting = await create(bob)
    independent = await create(bob, "viking://resources/independent")

    # These running records must not become target owners during recovery.
    for task_id, skill in (("orphan-external", "wiki"), ("orphan-memory", "memory")):
        await tracker.create(
            "compile",
            task_id=task_id,
            account_id="account",
            user_id="alice",
            meta={"request": {"to": target, "skill": skill}},
        )
        await tracker.start(task_id, account_id="account", user_id="alice")
    other = manager.get_queue("Other", allow_create=True)
    with bind_task_context("orphan-memory", "account", "alice"):
        await other.enqueue({"task_id": "orphan-memory"})

    worker = asyncio.create_task(queue.dequeue())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        if phase == "cancelling":
            await tracker.cancel(first_id, account_id="account", user_id="alice")
        await tracker.close()
        client.close()

        service = ExternalTaskService()
        service.register(provider)
        _, tracker, manager, queue = await recovery_environment(service)
        await manager.prepare_task_tracking(tracker)
        candidates = await manager.get_queued_tasks(tracker, manager.EXTERNAL_TASK)
        assert {task.task_id for task in candidates} == {
            first_id,
            waiting.task_id,
            independent.task_id,
        }
        await service.restore_tasks(candidates)

        # Force B to be processed first regardless of backend recovery ordering.
        deliveries = {}
        while message := await queue.dequeue_raw():
            payload = message["data"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            deliveries[payload["task_id"]] = message

        async def consume(task_id):
            message = deliveries[task_id]
            await asyncio.wait_for(queue.process_dequeued(message), 5)
            await queue.ack(message["id"], message)

        await consume(waiting.task_id)
        assert waiting.task_id not in submissions
        assert (
            await tracker.get(waiting.task_id, account_id="account", user_id="bob")
        ).status is TaskStatus.PENDING
        await consume(independent.task_id)
        assert (
            await tracker.get(independent.task_id, account_id="account", user_id="bob")
        ).status is TaskStatus.COMPLETED
        release.set()
        await consume(first_id)
        terminal = await tracker.wait(first_id, account_id="account", user_id="alice", timeout=5)
        assert terminal.status is (
            TaskStatus.CANCELLED if phase == "cancelling" else TaskStatus.COMPLETED
        )
        assert resumes == [first_id]
        assert submissions.count(first_id) == (2 if phase == "submitting" else 1)
        await queue.dequeue()
        assert (
            await tracker.get(waiting.task_id, account_id="account", user_id="bob")
        ).status is TaskStatus.COMPLETED
        assert await queue.snapshot() == []
        assert (
            await tracker.get("orphan-external", account_id="account", user_id="alice")
        ).status is TaskStatus.RUNNING
    finally:
        release.set()
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.asyncio
async def test_queued_task_lookup_uses_delivery_owner_and_deduplicates(
    recovery_environment,
):
    service = ExternalTaskService()
    _, tracker, manager, queue = await recovery_environment(service)
    task = await tracker.create("compile", account_id="account", user_id="alice", task_id="owned")
    await queue.snapshot()
    # A legacy message can omit work_id; its queue envelope supplies the ID.
    for _ in range(2):
        await queue._async_agfs.write(
            queue.path + "/enqueue",
            json.dumps(
                {"task_id": task.task_id, "account_id": "account", "user_id": "alice"}
            ).encode(),
        )
    for payload in (
        {"task_id": "owned", "account_id": "account", "user_id": "bob"},
        {"task_id": "missing", "account_id": "account", "user_id": "alice"},
        {"task_id": "owned"},
    ):
        await queue._async_agfs.write(queue.path + "/enqueue", json.dumps(payload).encode())
    assert [
        item.task_id for item in await manager.get_queued_tasks(tracker, manager.EXTERNAL_TASK)
    ] == ["owned"]
    assert await manager.get_queued_tasks(tracker, "Unregistered") == []
    if tracker.work_recovery_mode is WorkRecoveryMode.DURABLE_STORE:
        assert await tracker.has_work("owned", account_id="account", user_id="alice") is False
