# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Watch store contracts on the file store and, when configured, MySQL.

In file mode every simulated node shares the one process's store. Set
OPENVIKING_WATCH_MYSQL_PARAMS to a JSON object of connection parameters for
an isolated MySQL database; the fixture provisions randomly named tables from
openviking/service/sql/001_watch_tasks.sql and drops them afterwards.
"""

import asyncio
import base64
import json
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import pytest_asyncio

from openviking.resource.watch_store import WatchStore
from openviking.resource.watch_store_file import FileWatchStore
from openviking.resource.watch_store_mysql import MySQLWatchStore
from openviking.resource.watch_types import (
    PermissionDeniedError,
    WatchAuthority,
    WatchPatch,
    WatchRunResult,
    WatchTask,
)
from openviking_cli.exceptions import ConflictError
from tests.utils.watch_fixtures import local_viking_fs

MYSQL_PARAMS = os.environ.get("OPENVIKING_WATCH_MYSQL_PARAMS")
SCHEMA = Path(__file__).parents[2] / "openviking/service/sql/001_watch_tasks.sql"
ACCOUNT = "account-a"
OWNER = WatchAuthority(ACCOUNT, "user-a", "user")
CONNECTOR_AUTH = {"provider": "connector_encrypted", "ciphertext": "unused"}
ENCRYPTION_KEY = base64.urlsafe_b64encode(b"k" * 32).decode()


def _mysql_connection(params):
    import pymysql

    values = {key: value for key, value in params.items() if key != "maxsize"}
    return pymysql.connect(**values, charset="utf8mb4", autocommit=True)


@pytest_asyncio.fixture(params=["file", "mysql"])
async def make_store(request, tmp_path):
    """Return a factory for stores sharing one database (one per simulated node)."""
    created: list[WatchStore] = []
    cleanup = None
    if request.param == "file":
        viking_fs = local_viking_fs(tmp_path / "agfs")
    else:
        if not MYSQL_PARAMS:
            pytest.skip("OPENVIKING_WATCH_MYSQL_PARAMS is not configured")
        base = json.loads(MYSQL_PARAMS)
        table = "ov_test_watch_" + uuid.uuid4().hex[:12]
        params = {**base, "table": table, "credential_encryption_key": ENCRYPTION_KEY}

        def provision():
            connection = _mysql_connection(base)
            sql = SCHEMA.read_text()
            sql = sql[sql.index("CREATE TABLE") :].replace("ov_watch_tasks", table)
            with connection.cursor() as cursor:
                for statement in sql.split(";"):
                    if statement.strip():
                        cursor.execute(statement)
            return connection

        connection = await asyncio.to_thread(provision)

        def cleanup():
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE `{table}_leader`")
                cursor.execute(f"DROP TABLE `{table}`")
            connection.close()

    async def factory(service_name: str = "watch-test", **overrides) -> WatchStore:
        if request.param == "file":
            if created:
                return created[0]
            store: WatchStore = FileWatchStore(viking_fs)
        else:
            store = MySQLWatchStore(service_name=service_name, params={**params, **overrides})
        await store.initialize()
        created.append(store)
        return store

    try:
        yield factory
    finally:
        for store in created:
            await store.close()
        if cleanup is not None:
            await asyncio.to_thread(cleanup)


@pytest_asyncio.fixture
async def store(make_store):
    return await make_store()


async def create(store, uri="viking://resources/a", *, initial_run_id=None, due=False, **values):
    values.setdefault("account_id", ACCOUNT)
    values.setdefault("user_id", "user-a")
    if due:
        values["created_at"] = datetime.now() - timedelta(minutes=5)
    task = WatchTask(path="https://example.org/doc", to_uri=uri, watch_interval=1, **values)
    task.next_execution_time = task.calculate_next_execution_time() if task.is_active else None
    await store.create_task(task, initial_run_id=initial_run_id)
    return task


@pytest.mark.asyncio
async def test_claim_is_exclusive_across_nodes(make_store):
    first, second = await make_store(), await make_store()
    task = await create(first, due=True)

    claims = await asyncio.gather(
        *[node.claim(task.task_id, target_uri=task.to_uri) for node in (first, second) * 3]
    )

    winners = [claim for claim in claims if claim is not None]
    assert len(winners) == 1
    state = await second.get_state(task.task_id)
    assert state.current_run_id == winners[0].run_id
    assert state.run_started_at is not None
    assert await first.due(10) == []
    assert [s.current_run_id for s in await second.running()] == [winners[0].run_id]


@pytest.mark.asyncio
async def test_claim_requires_due_enabled_and_unmoved_target(store):
    not_due = await create(store, "viking://resources/later")
    task = await create(store, due=True)

    assert await store.claim(not_due.task_id, target_uri=not_due.to_uri) is None
    assert (
        await store.claim(task.task_id, target_uri="viking://resources/moved", manual=True) is None
    )
    await store.patch_task(task.task_id, WatchPatch({"is_active": False}))
    assert await store.claim(task.task_id, target_uri=task.to_uri) is None

    manual = await store.claim(task.task_id, target_uri=task.to_uri, manual=True)
    assert manual is not None and manual.task.task_id == task.task_id


@pytest.mark.asyncio
async def test_deactivate_idle_skips_occupied_and_moved_watches(store):
    occupied = await create(store, "viking://resources/busy", initial_run_id="run-1")
    task = await create(store, due=True)

    assert not await store.deactivate_idle(
        occupied.task_id, target_uri=occupied.to_uri, error="gone"
    )
    assert not await store.deactivate_idle(
        task.task_id, target_uri="viking://resources/moved", error="gone"
    )
    assert await store.deactivate_idle(task.task_id, target_uri=task.to_uri, error="gone")

    state = await store.get_state(task.task_id)
    assert state.current_run_id is None
    assert not state.task.is_active and state.task.next_execution_time is None
    assert (state.task.last_status, state.task.last_error) == ("failed", "gone")
    assert state.task.last_execution_time is not None
    assert (await store.get_state(occupied.task_id)).current_run_id == "run-1"
    assert await store.due(10) == []


@pytest.mark.asyncio
async def test_finish_run_is_a_cas_on_the_run_id(store):
    task = await create(store, initial_run_id="run-1")

    assert not await store.finish_run(task.task_id, "run-2", WatchRunResult(status="completed"))
    assert await store.finish_run(
        task.task_id,
        "run-1",
        WatchRunResult(status="completed", execution_task_id="run-1"),
    )
    assert not await store.finish_run(task.task_id, "run-1", WatchRunResult(status="failed"))

    state = await store.get_state(task.task_id)
    assert state.current_run_id is None and state.run_started_at is None
    assert state.task.last_status == "completed"
    assert state.task.last_task_id == "run-1"
    expected = state.task.last_execution_time.timestamp() + 60
    assert abs(state.task.next_execution_time.timestamp() - expected) < 1


@pytest.mark.asyncio
async def test_finish_run_deactivates_and_stores_connector_states(store):
    native = await create(store, initial_run_id="run-1")
    connector = await create(
        store,
        "viking://resources/c",
        initial_run_id="run-2",
        auth_state=dict(CONNECTOR_AUTH),
        connector_states={"cursor": 1},
    )

    await store.finish_run(
        native.task_id, "run-1", WatchRunResult(status="failed", error="gone", deactivate=True)
    )
    await store.finish_run(
        connector.task_id,
        "run-2",
        WatchRunResult(
            status="completed", connector_states={"cursor": 2}, has_connector_states=True
        ),
    )

    native_state = await store.get_state(native.task_id)
    assert native_state.task.is_active is False
    assert native_state.task.next_execution_time is None
    assert (await store.get_task(connector.task_id)).connector_states == {"cursor": 2}


@pytest.mark.asyncio
async def test_paused_run_settles_without_rescheduling(store):
    task = await create(store, initial_run_id="run-1")
    await store.patch_task(task.task_id, WatchPatch({"is_active": False}), OWNER)

    await store.finish_run(task.task_id, "run-1", WatchRunResult(status="completed"))

    state = await store.get_state(task.task_id)
    assert state.task.is_active is False
    assert state.task.next_execution_time is None


@pytest.mark.asyncio
async def test_run_scoped_patch_requires_occupancy(store):
    task = await create(store, initial_run_id="run-1")

    assert (
        await store.patch_task(task.task_id, WatchPatch({"auth_state": {"a": 1}}), run_id="other")
        is None
    )
    patched = await store.patch_task(
        task.task_id, WatchPatch({"auth_state": {"a": 2}}), run_id="run-1"
    )
    assert patched.auth_state == {"a": 2}


@pytest.mark.asyncio
async def test_permissions(store):
    task = await create(store)
    intruder = WatchAuthority(ACCOUNT, "user-b", "admin")

    assert await store.get_task(task.task_id, intruder) is None
    with pytest.raises(PermissionDeniedError):
        await store.patch_task(task.task_id, WatchPatch({"reason": "x"}), intruder)
    with pytest.raises(PermissionDeniedError):
        await store.delete_task(task.task_id, intruder)
    assert (await store.patch_task(task.task_id, WatchPatch({"reason": "x"}), OWNER)).reason == "x"


@pytest.mark.asyncio
async def test_target_conflicts_are_per_account_and_ignore_tombstones(store):
    first = await create(store)
    with pytest.raises(ConflictError, match="already being monitored"):
        await create(store)
    await create(store, account_id="account-b")
    with pytest.raises(ConflictError, match="already exists"):
        await store.create_task(first.model_copy(update={"to_uri": "viking://resources/other"}))

    await store.delete_task(first.task_id, OWNER)
    await create(store)


@pytest.mark.asyncio
async def test_occupied_tombstone_is_hidden_and_purged_on_settlement(store):
    task = await create(store, initial_run_id="run-1")

    assert await store.delete_task(task.task_id, OWNER)
    assert await store.get_task(task.task_id) is None
    assert await store.list_tasks(OWNER) == []
    assert await store.claim(task.task_id, target_uri=task.to_uri, manual=True) is None
    assert await store.has_running_overlap(ACCOUNT, [task.to_uri])

    await store.finish_run(task.task_id, "run-1", WatchRunResult(status="cancelled"))
    assert await store.get_state(task.task_id) is None


@pytest.mark.asyncio
async def test_delete_tasks_returns_runs_to_release(store):
    idle = await create(store, "viking://resources/idle")
    busy = await create(store, "viking://resources/busy", initial_run_id="run-1")
    other = await create(store, "viking://resources/other", user_id="user-b")

    running = await store.delete_tasks(ACCOUNT, "user-a")

    assert [(state.task.task_id, state.current_run_id) for state in running] == [
        (busy.task_id, "run-1")
    ]
    assert await store.get_state(idle.task_id) is None
    assert (await store.get_state(busy.task_id)).deleted
    assert await store.get_task(other.task_id) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target", "uris", "expected"),
    [
        ("viking://resources/a/b", ["viking://resources/a/b"], True),
        ("viking://resources/a/b", ["viking://resources/a"], True),
        ("viking://resources/a/b", ["viking://resources/a/b/c.md"], True),
        ("viking://resources/a/b", ["viking://resources/a/bc"], False),
        ("viking://resources/a/b", ["viking://resources/x", "viking://resources/a/"], True),
        ("viking://resources/a_b", ["viking://resources/a%b"], False),
    ],
)
async def test_running_overlap_uses_path_boundaries(store, target, uris, expected):
    await create(store, target, initial_run_id="run-1")

    assert await store.has_running_overlap(ACCOUNT, uris) is expected
    assert await store.has_running_overlap("account-b", uris) is False


@pytest.mark.asyncio
async def test_idle_watch_does_not_overlap(store):
    await create(store)
    assert await store.has_running_overlap(ACCOUNT, ["viking://resources"]) is False


@pytest.mark.asyncio
async def test_running_pages_by_task_id(store):
    ids = sorted(
        [
            (await create(store, f"viking://resources/{i}", initial_run_id=f"run-{i}")).task_id
            for i in range(5)
        ]
    )
    await create(store, "viking://resources/idle")

    first = await store.running(limit=2)
    rest = await store.running(after=first[-1].task.task_id, limit=10)

    assert [state.task.task_id for state in first + rest] == ids


@pytest.mark.asyncio
async def test_due_orders_and_limits(store):
    tasks = [await create(store, f"viking://resources/{i}", due=True) for i in range(3)]
    await create(store, "viking://resources/later")

    due = await store.due(2)

    assert len(due) == 2
    assert {state.task.task_id for state in due} <= {task.task_id for task in tasks}


@pytest.mark.asyncio
async def test_rewrite_escapes_nothing_and_respects_boundaries(store):
    exact = await create(store, "viking://resources/a_b")
    child = await create(store, "viking://resources/a_b/c")
    lookalike = await create(store, "viking://resources/aXb")
    sibling = await create(store, "viking://resources/a_bc")

    moved = await store.rewrite_target_prefix(
        ACCOUNT, "viking://resources/a_b", "viking://resources/z"
    )

    assert {task.task_id for task in moved} == {exact.task_id, child.task_id}
    assert (await store.get_task(child.task_id)).to_uri == "viking://resources/z/c"
    assert (await store.get_task(lookalike.task_id)).to_uri == "viking://resources/aXb"
    assert (await store.get_task(sibling.task_id)).to_uri == "viking://resources/a_bc"


@pytest.mark.asyncio
async def test_leader_lease_epochs(make_store):
    first, second = await make_store(), await make_store()

    epoch = await first.acquire_leadership("node-a", 30)
    assert epoch == 1
    assert await second.acquire_leadership("node-b", 30) is None
    assert await first.acquire_leadership("node-a", 30) == epoch
    assert await first.renew_leadership("node-a", epoch, 30)
    assert not await second.renew_leadership("node-b", epoch, 30)

    await first.release_leadership("node-a", epoch)
    assert not await first.renew_leadership("node-a", epoch, 30)
    taken = await second.acquire_leadership("node-b", 30)
    assert taken == epoch + 1
    assert not await first.renew_leadership("node-a", epoch, 30)


@pytest.mark.asyncio
async def test_leader_lease_expires(make_store):
    first, second = await make_store(), await make_store()

    assert await first.acquire_leadership("node-a", 0.2) == 1
    await asyncio.sleep(0.5)

    assert await second.acquire_leadership("node-b", 30) == 2
    assert not await first.renew_leadership("node-a", 1, 30)


@pytest.mark.asyncio
async def test_concurrent_first_leader_insert(make_store):
    nodes = [await make_store() for _ in range(4)]

    epochs = await asyncio.gather(
        *[node.acquire_leadership(f"node-{i}", 30) for i, node in enumerate(nodes)]
    )

    assert sorted(epoch for epoch in epochs if epoch is not None) == [1]


@pytest.mark.asyncio
async def test_service_names_partition_rows_and_leadership(make_store):
    first = await make_store()
    if first.provider == "file":
        pytest.skip("the file store belongs to one deployment")
    other = await make_store("other-service")
    task = await create(first, initial_run_id="run-1")

    assert await other.get_state(task.task_id) is None
    assert await other.running() == []
    assert not await other.has_running_overlap(ACCOUNT, [task.to_uri])
    await create(other)
    assert await first.acquire_leadership("node-a", 30) == 1
    assert await other.acquire_leadership("node-b", 30) == 1


@pytest.mark.asyncio
async def test_list_order(store):
    now = datetime.now()
    created = [
        await create(store, f"viking://resources/t{index}", created_at=now + timedelta(seconds=index))
        for index in range(3)
    ]
    expected = [task.task_id for task in created]
    if store.provider == "mysql":
        expected.reverse()

    assert [task.task_id for task in await store.list_tasks(OWNER)] == expected


async def _raw_secrets(store):
    from sqlalchemy import select

    c = store._tasks.c

    async def read():
        async with store._transaction() as session:
            rows = await session.execute(select(c.task_id, c.auth_state, c.connector_states))
            return {row["task_id"]: dict(row) for row in rows.mappings().all()}

    return await store._mysql_runtime.run(read)


async def _set_raw(store, task_id, **values):
    from sqlalchemy import update

    async def write():
        async with store._transaction() as session:
            await session.execute(
                update(store._tasks).where(store._tasks.c.task_id == task_id).values(**values)
            )

    await store._mysql_runtime.run(write)


@pytest.mark.asyncio
async def test_mysql_private_state_is_encrypted_at_rest(make_store):
    store = await make_store()
    if store.provider != "mysql":
        pytest.skip("the file store relies on VikingFS encryption")
    auth = {"provider": "git_http", "token": "secret-token"}
    task = await create(store, auth_state=auth, connector_states={"cursor": "secret-cursor"})
    await store.patch_task(task.task_id, WatchPatch({"auth_state": {**auth, "token": "rotated"}}))

    raw = (await _raw_secrets(store))[task.task_id]
    for column in ("auth_state", "connector_states"):
        assert raw[column].startswith("aesgcm:v1:")
        assert "secret" not in raw[column] and "rotated" not in raw[column]
    loaded = await store.get_task(task.task_id)
    assert loaded.auth_state["token"] == "rotated"
    assert loaded.connector_states == {"cursor": "secret-cursor"}

    claim = await store.claim(task.task_id, target_uri=task.to_uri, manual=True)
    await store.finish_run(
        task.task_id,
        claim.run_id,
        WatchRunResult(status="completed", connector_states={"c": 2}, has_connector_states=True),
    )
    assert (await _raw_secrets(store))[task.task_id]["connector_states"].startswith("aesgcm:v1:")
    assert (await store.get_task(task.task_id)).connector_states == {"c": 2}


@pytest.mark.asyncio
async def test_mysql_private_state_is_bound_to_key_and_row(make_store):
    store = await make_store()
    if store.provider != "mysql":
        pytest.skip("the file store relies on VikingFS encryption")
    task = await create(store, auth_state={"token": "a"})
    other = await create(store, "viking://resources/other", auth_state={"token": "b"})

    other_key = base64.urlsafe_b64encode(b"x" * 32).decode()
    rekeyed = await make_store(credential_encryption_key=other_key)
    with pytest.raises(RuntimeError, match="credential_encryption_key"):
        await rekeyed.get_task(task.task_id)

    swapped = (await _raw_secrets(store))[other.task_id]["auth_state"]
    await _set_raw(store, task.task_id, auth_state=swapped)
    with pytest.raises(RuntimeError, match="Cannot decrypt auth_state"):
        await store.get_task(task.task_id)


@pytest.mark.parametrize("key", [None, "not base64!", base64.b64encode(b"short").decode()])
def test_mysql_store_requires_a_valid_encryption_key(key):
    params = {"user": "u", "password": "p", "database": "d"}
    if key is not None:
        params["credential_encryption_key"] = key
    with pytest.raises(ValueError, match="credential_encryption_key"):
        MySQLWatchStore(service_name="watch-test", params=params)


@pytest.mark.asyncio
async def test_connector_flag_follows_auth_state(make_store):
    store = await make_store()
    first = await create(store, "viking://resources/shared", auth_state=dict(CONNECTOR_AUTH))
    await create(store, "viking://resources/shared", auth_state=dict(CONNECTOR_AUTH))

    await store.patch_task(first.task_id, WatchPatch({"auth_state": None}))

    with pytest.raises(ConflictError, match="already being monitored"):
        await create(store, "viking://resources/shared", auth_state=dict(CONNECTOR_AUTH))
