# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Real MySQL coverage for task duration projections and schema name limits."""

import json
import os
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import event, update
from sqlalchemy.ext.asyncio import create_async_engine

from openviking.service import task_store_mysql
from openviking.service.task_domain import TaskRecord, TaskStatus, WorkRecord, WorkState
from openviking.service.task_store_mysql import MySQLTaskWorkStore
from openviking.service.task_store_mysql_schema import make_task_store_models


@pytest.fixture
async def projection_store():
    raw = os.environ.get("OV_TEST_MYSQL")
    if not raw:
        pytest.skip("Set OV_TEST_MYSQL to exercise task projections on MySQL")
    params = {**json.loads(raw), "table": f"ov_projection_{uuid4().hex[:12]}"}
    store = MySQLTaskWorkStore(params=params)
    admin = create_async_engine(store._url, **store._engine_options)
    try:
        sql_path = Path(task_store_mysql.__file__).parent / "sql/task_store_init.sql"
        sql = sql_path.read_text().replace("ov_tasks", params["table"])
        async with admin.begin() as connection:
            for statement in sql.split(";"):
                if "CREATE TABLE" in statement:
                    await connection.exec_driver_sql(statement)
        await store.load()
        yield store
    finally:
        await store.close()
        async with admin.begin() as connection:
            await connection.run_sync(store._models.Base.metadata.drop_all)
        await admin.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("read", ["get", "events", "list", "search", "cursor", "unbounded"])
async def test_active_duration_uses_work_interval_union(projection_store, monkeypatch, read):
    store = projection_store
    owner = {"account_id": "account", "user_id": "alice"}
    for task_id in ("running", "cancelling", "pending"):
        await store.create(TaskRecord(task_id=task_id, task_type="compile", **owner))
        if task_id != "pending":
            await store.start_task(task_id, stage="processing", **owner)
            for work_id, start, finish in (
                ("overlap-a", 10, 15),
                ("overlap-b", 12, 18),
                ("separate", 40, 43),
                ("live", 90, None),
                ("unstarted", None, None),
            ):
                await store.add_work(
                    WorkRecord(
                        task_id=task_id,
                        work_id=work_id,
                        queue_name="Embedding",
                        state=WorkState.DONE if finish is not None else WorkState.RUNNING,
                        started_at=start,
                        finished_at=finish,
                        enqueued_at=1,
                    ),
                    **owner,
                )
    await store.request_cancellation(
        "cancelling", cancellable_types=["compile"], allow_terminal=False, **owner
    )
    await store.create(
        TaskRecord(
            task_id="terminal",
            task_type="compile",
            status=TaskStatus.COMPLETED,
            processing_seconds=37,
            **owner,
        )
    )
    monkeypatch.setattr(task_store_mysql, "time", SimpleNamespace(time=lambda: 100))
    expected = {"running": 21, "cancelling": 21, "pending": None, "terminal": 37}
    if read in {"get", "events"}:
        getter = store.get_task if read == "get" else store.get_task_with_events
        tasks = [await getter(task_id, **owner) for task_id in expected]
        assert await getter("running", account_id="account", user_id="bob") is None
        assert await getter("missing", **owner) is None
    elif read == "cursor":
        tasks = []
        before = None
        while page := await store.list(**owner, limit=2, before=before):
            assert not {task.task_id for task in page} & {task.task_id for task in tasks}
            tasks.extend(page)
            before = (page[-1].created_at, page[-1].task_id)
    else:
        options = {"limit": None if read == "unbounded" else 10}
        if read == "search":
            options["q"] = "ing"
            expected = {key: value for key, value in expected.items() if "ing" in key}
        tasks = await store.list(**owner, **options)
    assert {task.task_id: task.processing_seconds for task in tasks} == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("search", [False, True])
async def test_duration_query_is_bounded_to_returned_page(projection_store, search):
    store = projection_store
    owner = {"account_id": "account", "user_id": "alice"}
    for index in range(12):
        task_id = f"match-{index:02d}"
        await store.create(TaskRecord(task_id=task_id, task_type="compile", **owner))
        await store.add_work(
            WorkRecord(
                task_id=task_id,
                work_id="work",
                queue_name="Embedding",
                started_at=10,
                finished_at=20,
                state=WorkState.DONE,
            ),
            **owner,
        )
    queries = []

    def capture(_connection, _cursor, statement, parameters, _context, _many):
        if store._models.Work.__tablename__ in statement and statement.lstrip().startswith(
            "SELECT"
        ):
            queries.append((statement, parameters))

    engine = store._engine.sync_engine
    event.listen(engine, "before_cursor_execute", capture)
    try:
        for size in (1, 5):
            queries.clear()
            page = await store.list(**owner, limit=size, q="match" if search else None)
            assert len(page) == size
            assert all(task.processing_seconds == 10 for task in page)
            assert len(queries) == 1
            _, parameters = queries[0]
            assert set(parameters) == {task.task_id for task in page}
        queries.clear()
        assert await store.list(**owner, limit=0) == []
        assert await store.list(**owner, q="no-match") == []
        assert queries == []

        async def finish_rows():
            async with store._engine.begin() as connection:
                await connection.execute(
                    update(store._models.Task).values(status="completed", processing_seconds=10)
                )

        await store._mysql_runtime.run(finish_rows)
        queries.clear()
        assert all(task.processing_seconds == 10 for task in await store.list(**owner))
        assert queries == []
    finally:
        event.remove(engine, "before_cursor_execute", capture)


@pytest.mark.asyncio
async def test_maximum_prefix_provisions_documented_schema(projection_store):
    params = json.loads(os.environ["OV_TEST_MYSQL"])
    prefix = "p" + uuid4().hex + "abcde"
    assert len(prefix) == 38
    models = make_task_store_models(prefix)
    for invalid in (prefix + "x", "a" * 50, "0prefix", "_prefix", "préfix", "bad-name", ""):
        with pytest.raises(ValueError):
            MySQLTaskWorkStore(params={**params, "table": invalid})

    store = MySQLTaskWorkStore(params={**params, "table": prefix})
    admin = create_async_engine(store._url, **store._engine_options)
    sql_path = Path(task_store_mysql.__file__).parent / "sql/task_store_init.sql"
    sql = sql_path.read_text().replace("ov_tasks", prefix)
    try:
        async with admin.begin() as connection:
            for statement in sql.split(";"):
                if "CREATE TABLE" in statement:
                    await connection.exec_driver_sql(statement)
        await store.load()
        await store.create(
            TaskRecord(task_id="boundary", task_type="compile", account_id="a", user_id="u")
        )
        assert (await store.get_task("boundary", account_id="a", user_id="u")).task_id == "boundary"
    finally:
        await store.close()
        async with admin.begin() as connection:
            await connection.run_sync(models.Base.metadata.drop_all)
        await admin.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("schema", ["sql", "orm"])
async def test_schema_preserves_long_resource_uri_and_epoch_intervals(schema):
    raw = os.environ.get("OV_TEST_MYSQL")
    if not raw:
        pytest.skip("Set OV_TEST_MYSQL to exercise task schema contracts")
    params = {**json.loads(raw), "table": f"ov_schema_{uuid4().hex[:12]}"}
    store = MySQLTaskWorkStore(params=params)
    admin = create_async_engine(store._url, **store._engine_options)
    sql_root = Path(task_store_mysql.__file__).parent / "sql"
    try:
        async with admin.begin() as connection:
            if schema == "orm":
                await connection.run_sync(store._models.Base.metadata.create_all)
            else:
                sql = (sql_root / "task_store_init.sql").read_text()
                for statement in sql.replace("ov_tasks", params["table"]).split(";"):
                    if "CREATE TABLE" in statement:
                        await connection.exec_driver_sql(statement)
        await store.load()
        owner = {"account_id": "account", "user_id": "alice"}
        uri = "viking://resources/" + "/".join("directory" + "x" * 32 for _ in range(8))
        start = 1792310400.25
        await store.create(
            TaskRecord(
                task_id="schema-contract",
                task_type="probe",
                resource_id=uri,
                created_at=start,
                updated_at=start + 0.5,
                **owner,
            )
        )
        await store.add_work(
            WorkRecord(
                task_id="schema-contract",
                work_id="interval",
                queue_name="Embedding",
                state=WorkState.DONE,
                enqueued_at=start,
                started_at=start,
                finished_at=start + 20,
                updated_at=start + 20,
            ),
            **owner,
        )
        task = await store.get_task("schema-contract", **owner)
        work = await store.get_work("schema-contract", "interval", **owner)
        assert task.resource_id == uri
        assert task.created_at == start and task.updated_at == start + 0.5
        assert task.processing_seconds == 20
        assert work.started_at == start and work.finished_at == start + 20
        await store.record_outcome(
            task.task_id,
            result={"done": True},
            resource_id=uri + "/result.md",
            error=None,
            cancelled=False,
            **owner,
        )
        task = await store.get_task(task.task_id, **owner)
        assert task.status is TaskStatus.COMPLETED
        assert task.resource_id == uri + "/result.md"
        assert task.processing_seconds == 20
    finally:
        await store.close()
        async with admin.begin() as connection:
            await connection.run_sync(store._models.Base.metadata.drop_all)
        await admin.dispose()
