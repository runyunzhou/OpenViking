"""Real MySQL 5.7.44 Store contracts.

Set OPENVIKING_WATCH_MYSQL57_PARAMS to a JSON object containing connection
parameters for an isolated test database. Only randomly named tables owned
by this fixture are created/dropped. This is Store integration, not OV-node
acceptance; that acceptance additionally needs real API servers and Redis.
"""

import asyncio
import json
import os
import threading
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pymysql
import pytest
import pytest_asyncio

from openviking.resource.distributed_watch_manager import DistributedWatchManager
from openviking.resource.distributed_watch_store import MySQLWatchStore
from openviking.resource.distributed_watch_types import WatchAuthority, WatchPatch, WatchRunResult
from openviking.resource.watch_manager import PermissionDeniedError
from openviking_cli.exceptions import ConflictError

PARAMS = os.environ.get("OPENVIKING_WATCH_MYSQL57_PARAMS")
pytestmark = pytest.mark.skipif(not PARAMS, reason="isolated MySQL 5.7.44 connection not configured")


@pytest_asyncio.fixture
async def stores():
    params = json.loads(PARAMS)
    table = "ov_test_watch_" + uuid.uuid4().hex[:12]

    def provision():
        connection_params = {key: value for key, value in params.items() if key != "maxsize"}
        connection = pymysql.connect(**connection_params, charset="utf8mb4", autocommit=True)
        with connection.cursor() as cursor:
            cursor.execute("SELECT VERSION()")
            assert cursor.fetchone()[0] == "5.7.44", "acceptance requires exact MySQL 5.7.44"
            sql = (
                Path(__file__).parents[2] / "openviking/service/sql/001_schedule_provider.sql"
            ).read_text()
            sql = sql[sql.index("CREATE TABLE"):].replace("ov_watch_tasks", table)
            for statement in sql.split(";"):
                if statement.strip():
                    cursor.execute(statement)
        return connection

    connection = await asyncio.to_thread(provision)
    first = MySQLWatchStore(service_name="watch-test", params={**params, "table": table})
    second = MySQLWatchStore(service_name="watch-test", params={**params, "table": table})
    try:
        await asyncio.gather(first.initialize(), first.initialize(), second.initialize())
        yield first, second, connection, table, params
    finally:
        await first.close()
        await second.close()

        def cleanup():
            with connection.cursor() as cursor:
                for suffix in ("_uri_mutations", "_uri_accounts", ""):
                    cursor.execute(f"DROP TABLE `{table}{suffix}`")
            connection.close()
        await asyncio.to_thread(cleanup)


async def create(manager, *, uri="viking://resources/a", **values):
    return await manager.create_task(
        path="https://example.org/doc", to_uri=uri,
        account_id="account-a", user_id="user-a", watch_interval=1, **values,
    )


async def test_pause_delete_retain_occupancy_until_matching_run(stores):
    first, second, *_ = stores
    manager = DistributedWatchManager(first)
    watch = await create(manager)
    claim = await first.claim(watch.task_id, owner="node-a", manual=True)
    assert claim
    authority = WatchAuthority("account-a", "user-a", "user")
    await second.patch_task(watch.task_id, WatchPatch({"is_active": False}), authority)
    assert (await first.get_state(watch.task_id)).current_run_id == claim.run_id
    assert await second.delete_task(watch.task_id, authority)
    assert await first.get_task(watch.task_id) is None
    assert not await first.acquire_mutation("account-a", [watch.to_uri], "waiting")
    old = type(claim)(
        schedule_kind="watch", schedule_id=claim.schedule_id, account_id=claim.account_id,
        run_id="obsolete", revision=claim.revision, payload=claim.payload,
    )
    assert not await first.finish_run(old, WatchRunResult(status="completed"))
    assert await second.finish_run(claim, WatchRunResult(status="completed"))
    assert await first.acquire_mutation("account-a", [watch.to_uri], "waiting")
    await first.release_mutation("waiting")
    state = await first.get_state(watch.task_id)
    assert state.deleted and state.current_run_id is None
    assert await second.acknowledge_timer(watch.task_id, state.revision)
    assert await first.get_state(watch.task_id) is None


async def test_cross_connection_scope_and_registration_race(stores):
    first, second, *_ = stores
    manager = DistributedWatchManager(first)
    watch = await create(manager)
    assert await first.acquire_mutation("account-a", ["viking://resources"], "move-a")
    assert await second.claim(watch.task_id, owner="node-b", manual=True) is None
    # Unrelated scopes/account must progress while the mutation is held.
    assert await second.acquire_mutation("account-a", ["viking://other"], "move-b")
    assert await second.acquire_mutation("other-account", ["viking://resources"], "move-c")
    await second.release_mutation("move-b")
    await second.release_mutation("move-c")
    await first.release_mutation("move-a")
    claims = await asyncio.gather(
        first.claim(watch.task_id, owner="node-a", manual=True),
        second.claim(watch.task_id, owner="node-b", manual=True),
    )
    assert sum(claim is not None for claim in claims) == 1
    assert not await second.acquire_mutation("account-a", ["viking://resources/a/child"], "move-d")
    assert not await second.acquire_mutation("account-a", ["viking://resources"], "move-e")


async def test_claim_sees_mutation_committed_after_its_initial_snapshot(stores, monkeypatch):
    first, second, *_ = stores
    watch = await create(DistributedWatchManager(first))
    read_account = threading.Event()
    continue_claim = threading.Event()
    original_gate = first._gate

    async def delayed_gate(session, account_id):
        read_account.set()
        assert await asyncio.to_thread(continue_claim.wait, 5)
        await original_gate(session, account_id)

    monkeypatch.setattr(first, "_gate", delayed_gate)
    claim = asyncio.create_task(first.claim(watch.task_id, owner="node-a", manual=True))
    try:
        assert await asyncio.to_thread(read_account.wait, 5)
        assert await second.acquire_mutation("account-a", [watch.to_uri], "move-after-snapshot")
    finally:
        continue_claim.set()
    assert await claim is None
    await second.release_mutation("move-after-snapshot")


async def test_move_rebind_is_batch_and_preserves_paused_watch(stores):
    first, second, *_ = stores
    manager = DistributedWatchManager(first)
    watch = await create(manager, uri="viking://resources/old/a", parent_uri="viking://resources/old")
    paused = await create(manager, uri="viking://resources/old/b", is_active=False, parent_uri="viking://custom")
    await second.validate_target_prefix_rewrite(
        "account-a", "viking://resources/old", "viking://resources/new",
    )
    moved = await second.rewrite_target_prefix(
        "account-a", "viking://resources/old", "viking://resources/new",
    )
    assert {task.task_id for task in moved} == {watch.task_id, paused.task_id}
    current = await first.get_task(watch.task_id)
    assert current.to_uri == "viking://resources/new/a"
    assert current.parent_uri == "viking://resources/new"
    current = await first.get_task(paused.task_id)
    assert current.to_uri == "viking://resources/new/b"
    assert current.parent_uri == "viking://custom"
    assert not current.is_active
    assert await first.claim(paused.task_id, owner="node-a", manual=False) is None
    assert await first.claim(paused.task_id, owner="node-a", manual=True)


async def test_conflicting_creates_are_atomic(stores):
    first, second, *_ = stores
    outcomes = await asyncio.gather(
        create(DistributedWatchManager(first)), create(DistributedWatchManager(second)),
        return_exceptions=True,
    )
    assert sum(isinstance(item, ConflictError) for item in outcomes) == 1
    tasks, count = await first.list_tasks(WatchAuthority())
    assert len(tasks) == count == 1


async def test_permissions_service_partition_and_timer_cas(stores):
    first, second, _, table, params = stores
    manager = DistributedWatchManager(first)
    watch = await create(manager)
    wrong_user = WatchAuthority("account-a", "user-other", "admin")
    assert await second.get_task(watch.task_id, wrong_user) is None
    with pytest.raises(PermissionDeniedError):
        await second.patch_task(watch.task_id, WatchPatch({"is_active": False}), wrong_user)
    projection = await first.get_projection(watch.task_id)
    await second.patch_task(watch.task_id, WatchPatch({"reason": "changed"}))
    assert not await first.acknowledge_timer(watch.task_id, projection.revision)
    other = MySQLWatchStore(service_name="other-service", params={**params, "table": table})
    await other.initialize()
    try:
        assert await other.get_state(watch.task_id) is None
        assert await other.acquire_mutation("account-a", [watch.to_uri], "other-move")
        await other.release_mutation("other-move")
    finally:
        await other.close()


async def test_resume_keeps_main_due_time(stores):
    first, second, *_ = stores
    watch = await create(DistributedWatchManager(first), is_active=False)
    await second.patch_task(watch.task_id, WatchPatch({"is_active": True}))
    state = await first.get_state(watch.task_id)
    assert state.task.next_execution_time == watch.created_at + timedelta(minutes=1)
    assert isinstance(state.task.created_at, datetime)


async def test_expired_submission_lease_never_releases_uri_occupancy(stores):
    first, second, *_ = stores
    watch = await create(DistributedWatchManager(first))
    claim = await first.claim(watch.task_id, owner="node-a", manual=True)
    await first.release_submission(claim, "node-a")
    assert not await second.acquire_mutation("account-a", [watch.to_uri], "move")
    assert await second.acquire_submission(watch.task_id, claim.run_id, owner="node-b")
    assert not await first.acquire_submission(watch.task_id, claim.run_id, owner="node-c")
    state = await first.get_state(watch.task_id)
    assert state.current_run_id == claim.run_id
    assert state.lease_owner == "node-b"
    # An old owner cannot release the new owner's lease.
    await first.release_submission(claim, "node-a")
    assert (await second.get_state(watch.task_id)).lease_until > state.now
    assert await second.finish_run(claim, WatchRunResult(status="completed"))
    assert await first.acquire_mutation("account-a", [watch.to_uri], "move")
    await first.release_mutation("move")
