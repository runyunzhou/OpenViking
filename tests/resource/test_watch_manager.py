# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Unit tests for WatchTask and WatchManager over the SQLite Watch store."""

import asyncio
from datetime import datetime, timedelta

import pytest

from openviking.resource.watch_manager import PermissionDeniedError, WatchManager, WatchTask
from openviking.resource.watch_store_file import FileWatchStore
from openviking.resource.watch_types import WatchRunResult
from openviking.storage.errors import ResourceBusyError
from openviking_cli.exceptions import ConflictError

TEST_ACCOUNT_ID = "default"
TEST_USER_ID = "default"
TEST_ROLE = "ROOT"
_CONNECTOR_AUTH = {"provider": "connector_encrypted", "ciphertext": "unused"}


class TestWatchTask:
    """Tests for WatchTask data model."""

    def test_create_task_with_defaults(self):
        """Test creating a task with default values."""
        task = WatchTask(path="/test/path")

        assert task.path == "/test/path"
        assert task.source_type is None
        assert task.task_id is not None
        assert task.to_uri is None
        assert task.parent_uri is None
        assert task.reason == ""
        assert task.instruction == ""
        assert task.watch_interval == 60.0
        assert task.is_active is True
        assert task.created_at is not None
        assert task.last_execution_time is None
        assert task.next_execution_time is None
        assert task.processing_mode == "semantic_and_vectors"

    def test_create_task_with_all_fields(self):
        """Test creating a task with all fields specified."""
        now = datetime.now()
        task = WatchTask(
            task_id="test-task-id",
            path="/test/path",
            source_type="feishu_project",
            to_uri="viking://resources/test",
            parent_uri="viking://resources",
            reason="Test reason",
            instruction="Test instruction",
            watch_interval=30.0,
            processing_mode="vectors_only",
            created_at=now,
            last_execution_time=now,
            next_execution_time=now + timedelta(minutes=30),
            is_active=False,
        )

        assert task.task_id == "test-task-id"
        assert task.path == "/test/path"
        assert task.source_type == "feishu_project"
        assert task.to_uri == "viking://resources/test"
        assert task.parent_uri == "viking://resources"
        assert task.reason == "Test reason"
        assert task.instruction == "Test instruction"
        assert task.watch_interval == 30.0
        assert task.processing_mode == "vectors_only"
        assert task.is_active is False
        assert task.created_at == now
        assert task.last_execution_time == now

    def test_to_dict(self):
        """Test converting task to dictionary."""
        now = datetime.now()
        task = WatchTask(
            task_id="test-id",
            path="/test/path",
            source_type="url",
            to_uri="viking://test",
            auth_state={
                "provider": "feishu",
                "access_token": "u-test",
                "refresh_token": "r-test",
                "expires_at": None,
            },
            created_at=now,
        )

        data = task.to_dict()

        assert data["task_id"] == "test-id"
        assert data["path"] == "/test/path"
        assert data["source_type"] == "url"
        assert data["to_uri"] == "viking://test"
        assert data["created_at"] == now.isoformat()
        assert data["is_active"] is True
        assert data["processing_mode"] == "semantic_and_vectors"
        assert "auth_state" not in data

    def test_from_dict(self):
        """Test creating task from dictionary."""
        now = datetime.now()
        data = {
            "task_id": "test-id",
            "path": "/test/path",
            "source_type": "git",
            "to_uri": "viking://test",
            "parent_uri": "viking://parent",
            "reason": "Test",
            "instruction": "Instruction",
            "watch_interval": 45.0,
            "processing_mode": "vectors_only",
            "created_at": now.isoformat(),
            "last_execution_time": now.isoformat(),
            "next_execution_time": (now + timedelta(minutes=45)).isoformat(),
            "is_active": False,
        }

        task = WatchTask.from_dict(data)

        assert task.task_id == "test-id"
        assert task.path == "/test/path"
        assert task.source_type == "git"
        assert task.to_uri == "viking://test"
        assert task.watch_interval == 45.0
        assert task.processing_mode == "vectors_only"
        assert task.is_active is False
        assert task.created_at == now
        assert task.last_execution_time == now

    def test_from_dict_defaults_legacy_processing_mode(self):
        task = WatchTask.from_dict({"path": "/test/path"})

        assert task.processing_mode == "semantic_and_vectors"
        assert task.to_is_directory is None
        assert task.source_type is None

    def test_calculate_next_execution_time(self):
        """Test calculating next execution time."""
        now = datetime.now()
        task = WatchTask(
            path="/test",
            watch_interval=30.0,
            created_at=now,
        )

        next_time = task.calculate_next_execution_time()

        expected = now + timedelta(minutes=30.0)
        assert abs((next_time - expected).total_seconds()) < 1

    def test_calculate_next_execution_time_with_last_execution(self):
        """Test calculating next execution time based on last execution."""
        now = datetime.now()
        last_exec = now - timedelta(minutes=10)
        task = WatchTask(
            path="/test",
            watch_interval=30.0,
            created_at=now - timedelta(hours=1),
            last_execution_time=last_exec,
        )

        next_time = task.calculate_next_execution_time()

        expected = last_exec + timedelta(minutes=30.0)
        assert abs((next_time - expected).total_seconds()) < 1


async def _create(manager: WatchManager, uri: str = "viking://resources/test", **kwargs):
    kwargs.setdefault("path", "/test/path")
    return await manager.create_task(to_uri=uri, **kwargs)


class TestWatchManager:
    @pytest.mark.asyncio
    async def test_create_task(self, watch_manager: WatchManager):
        task = await _create(watch_manager, reason="Test task", watch_interval=30.0)

        assert task.to_uri == "viking://resources/test"
        assert task.is_active is True
        assert task.next_execution_time is not None
        stored = await watch_manager.get_task(task.task_id)
        assert stored.reason == "Test task"
        assert stored.watch_interval == 30.0
        state = await watch_manager.store.get_state(task.task_id)
        assert state.current_run_id is None

    @pytest.mark.asyncio
    async def test_create_inactive_task_has_no_next_execution(self, watch_manager: WatchManager):
        task = await _create(watch_manager, watch_interval=30.0, is_active=False)

        assert task.next_execution_time is None
        listed = await watch_manager.get_all_tasks(TEST_ACCOUNT_ID, TEST_USER_ID, TEST_ROLE)
        assert [item.task_id for item in listed] == [task.task_id]
        assert (
            await watch_manager.get_all_tasks(
                TEST_ACCOUNT_ID, TEST_USER_ID, TEST_ROLE, active_only=True
            )
            == []
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"path": ""}, "Path is required"),
            ({"to_uri": None}, "target URI"),
            ({"watch_interval": 0}, "watch_interval must be > 0"),
        ],
    )
    async def test_create_task_validation(self, watch_manager: WatchManager, kwargs, message):
        values = {"path": "/test/path", "to_uri": "viking://resources/test", **kwargs}
        with pytest.raises(ValueError, match=message):
            await watch_manager.create_task(**values)

    @pytest.mark.asyncio
    async def test_initial_run_occupies_watch_until_released(self, watch_manager: WatchManager):
        task = await _create(watch_manager, initial_run_id="ingest-1")

        state = await watch_manager.store.get_state(task.task_id)
        assert state.current_run_id == "ingest-1"
        assert await watch_manager.has_running_overlap(TEST_ACCOUNT_ID, ["viking://resources"])

        await watch_manager.fail_initial_submission(task.task_id, "other-run", "ignored")
        assert (await watch_manager.store.get_state(task.task_id)).current_run_id == "ingest-1"

        await watch_manager.fail_initial_submission(task.task_id, "ingest-1", "submit failed")
        released = await watch_manager.get_task(task.task_id)
        assert (await watch_manager.store.get_state(task.task_id)).current_run_id is None
        assert released.last_status == "failed"
        assert released.last_error == "submit failed"

    @pytest.mark.asyncio
    async def test_create_requires_target_tree_lock(self, watch_manager: WatchManager, watch_vfs):
        locks = watch_manager.locks
        async with locks.tree(TEST_ACCOUNT_ID, "viking://resources") as lease:
            with pytest.raises(ResourceBusyError):
                await _create(watch_manager, "viking://resources/test")
            task = await _create(watch_manager, "viking://resources/test", lease_ref=lease)
        assert await watch_manager.get_task(task.task_id) is not None

    @pytest.mark.asyncio
    async def test_create_for_missing_target_locks_existing_ancestor(
        self, watch_manager: WatchManager, watch_vfs, tmp_path
    ):
        await _create(watch_manager, "viking://resources/pending/doc.md")

        assert not await watch_vfs.exists("viking://resources/pending")
        assert not list((tmp_path / "agfs").rglob("pending"))

        await watch_vfs.mkdir("viking://resources/held")
        async with watch_manager.locks.tree(TEST_ACCOUNT_ID, "viking://resources/held"):
            with pytest.raises(ResourceBusyError):
                await _create(watch_manager, "viking://resources/held/new/doc.md")

    @pytest.mark.asyncio
    async def test_finish_run_records_sanitized_result(self, watch_vfs, watch_manager):
        task = await _create(watch_manager, initial_run_id="ingest-task-1")

        applied = await watch_manager.finish_run(
            task.task_id,
            "ingest-task-1",
            WatchRunResult(
                status="failed",
                execution_task_id="ingest-task-1",
                error="pull failed with Bearer secret-token",
            ),
        )

        assert applied is True
        assert not await watch_manager.finish_run(
            task.task_id, "ingest-task-1", WatchRunResult(status="completed")
        )
        reopened = FileWatchStore(watch_vfs)
        await reopened.initialize()
        try:
            loaded = await reopened.get_task(task.task_id)
        finally:
            await reopened.close()
        assert loaded.last_task_id == "ingest-task-1"
        assert loaded.last_status == "failed"
        assert loaded.last_error == "pull failed with Bearer [REDACTED]"
        assert loaded.last_execution_time is not None
        assert loaded.to_dict()["last_error"] == loaded.last_error

    @pytest.mark.asyncio
    async def test_secret_state_persisted_and_hidden_from_public_dict(
        self, watch_vfs, watch_manager
    ):
        states = {"documents": {"cursor": {"sync_checkpoint": "2026-09-01T00:00:00+00:00"}}}
        task = await _create(
            watch_manager,
            path="tos://bucket/docs/",
            auth_state=dict(_CONNECTOR_AUTH),
            connector_states=states,
        )

        reopened = FileWatchStore(watch_vfs)
        await reopened.initialize()
        try:
            loaded = await reopened.get_task(task.task_id)
        finally:
            await reopened.close()
        assert loaded.auth_state == _CONNECTOR_AUTH
        assert loaded.connector_states == states
        public = loaded.to_dict()
        assert "auth_state" not in public
        assert "connector_states" not in public

    @pytest.mark.asyncio
    async def test_create_task_with_conflicting_uri(self, watch_manager: WatchManager):
        await _create(watch_manager, path="/test/path1")

        with pytest.raises(ConflictError, match="already being monitored"):
            await _create(watch_manager, path="/test/path2")
        await _create(watch_manager, path="/test/path3", account_id="other-account")

    @pytest.mark.asyncio
    async def test_update_task(self, watch_manager: WatchManager):
        task = await _create(watch_manager, reason="Original reason")

        updated = await watch_manager.update_task(
            task.task_id,
            TEST_ACCOUNT_ID,
            TEST_USER_ID,
            TEST_ROLE,
            reason="Updated reason",
            watch_interval=45.0,
            is_active=False,
        )

        assert updated.reason == "Updated reason"
        assert updated.watch_interval == 45.0
        assert updated.is_active is False
        assert updated.next_execution_time is None

        resumed = await watch_manager.update_task(
            task.task_id, TEST_ACCOUNT_ID, TEST_USER_ID, TEST_ROLE, is_active=True
        )
        expected = resumed.created_at + timedelta(minutes=45)
        assert abs((resumed.next_execution_time - expected).total_seconds()) < 1

    @pytest.mark.asyncio
    async def test_non_positive_interval_pauses(self, watch_manager: WatchManager):
        task = await _create(watch_manager, watch_interval=30.0)

        updated = await watch_manager.update_task(
            task.task_id, TEST_ACCOUNT_ID, TEST_USER_ID, TEST_ROLE, watch_interval=0
        )

        assert updated.is_active is False
        assert updated.watch_interval == 30.0
        with pytest.raises(ValueError, match="must be > 0 for active tasks"):
            await watch_manager.update_task(
                task.task_id,
                TEST_ACCOUNT_ID,
                TEST_USER_ID,
                TEST_ROLE,
                watch_interval=0,
                is_active=True,
            )

    @pytest.mark.asyncio
    async def test_update_task_not_found_and_unsupported_fields(self, watch_manager):
        with pytest.raises(ValueError, match="not found"):
            await watch_manager.update_task(
                "non-existent-id", TEST_ACCOUNT_ID, TEST_USER_ID, TEST_ROLE, reason="Updated"
            )
        task = await _create(watch_manager)
        with pytest.raises(ValueError, match="Unsupported Watch update fields"):
            await watch_manager.update_task(
                task.task_id, TEST_ACCOUNT_ID, TEST_USER_ID, TEST_ROLE, to_uri="viking://x"
            )

    @pytest.mark.asyncio
    async def test_other_user_cannot_update_or_delete(self, watch_manager: WatchManager):
        task = await _create(watch_manager, user_id="alice")

        with pytest.raises(PermissionDeniedError):
            await watch_manager.update_task(
                task.task_id, TEST_ACCOUNT_ID, "bob", "user", reason="x"
            )
        with pytest.raises(PermissionDeniedError):
            await watch_manager.delete_task(task.task_id, TEST_ACCOUNT_ID, "bob", "admin")
        assert await watch_manager.get_task(task.task_id, TEST_ACCOUNT_ID, "bob", "user") is None
        assert await watch_manager.delete_task(task.task_id, TEST_ACCOUNT_ID, "root", "root")

    @pytest.mark.asyncio
    async def test_delete_task(self, watch_manager: WatchManager):
        task = await _create(watch_manager)

        assert await watch_manager.delete_task(
            task.task_id, TEST_ACCOUNT_ID, TEST_USER_ID, TEST_ROLE
        )
        assert await watch_manager.get_task(task.task_id) is None
        assert (
            await watch_manager.get_task_by_uri(
                "viking://resources/test", TEST_ACCOUNT_ID, TEST_USER_ID, TEST_ROLE
            )
            is None
        )
        assert await watch_manager.store.get_state(task.task_id) is None
        assert not await watch_manager.delete_task(
            task.task_id, TEST_ACCOUNT_ID, TEST_USER_ID, TEST_ROLE
        )
        await _create(watch_manager, path="/replacement")

    @pytest.mark.asyncio
    async def test_delete_occupied_task_is_purged_when_run_settles(self, watch_manager):
        task = await _create(watch_manager, initial_run_id="run-1")

        assert await watch_manager.delete_task(
            task.task_id, TEST_ACCOUNT_ID, TEST_USER_ID, TEST_ROLE
        )
        state = await watch_manager.store.get_state(task.task_id)
        assert state.deleted and state.current_run_id == "run-1"
        assert await watch_manager.get_task(task.task_id) is None
        assert await watch_manager.has_running_overlap(TEST_ACCOUNT_ID, ["viking://resources"])

        await watch_manager.finish_run(task.task_id, "run-1", WatchRunResult(status="completed"))
        assert await watch_manager.store.get_state(task.task_id) is None

    @pytest.mark.asyncio
    async def test_get_all_tasks_keeps_file_order_across_updates(self, watch_manager: WatchManager):
        tasks = [
            await _create(watch_manager, f"viking://resources/t{index}", path=f"/test/path{index}")
            for index in range(1, 4)
        ]
        await watch_manager.update_task(
            tasks[0].task_id, TEST_ACCOUNT_ID, TEST_USER_ID, TEST_ROLE, path="/test/renamed"
        )

        listed = await watch_manager.get_all_tasks(TEST_ACCOUNT_ID, TEST_USER_ID, TEST_ROLE)

        assert [task.task_id for task in listed] == [task.task_id for task in tasks]

    @pytest.mark.asyncio
    async def test_visibility_by_role(self, watch_manager: WatchManager):
        await _create(watch_manager, "viking://resources/a1", user_id="alice")
        await _create(watch_manager, "viking://resources/b1", user_id="bob")
        await _create(watch_manager, "viking://resources/a2", user_id="alice")
        await _create(watch_manager, "viking://resources/o1", account_id="other", user_id="alice")

        root = await watch_manager.get_all_tasks(TEST_ACCOUNT_ID, "root", "root")
        admin = await watch_manager.get_all_tasks(TEST_ACCOUNT_ID, "alice", "admin")
        user = await watch_manager.get_all_tasks(TEST_ACCOUNT_ID, "bob", "user")

        assert len(root) == 4
        assert {task.user_id for task in admin} == {"alice"} and len(admin) == 2
        assert [task.user_id for task in user] == ["bob"]

    @pytest.mark.asyncio
    async def test_get_task_by_uri(self, watch_manager: WatchManager):
        task = await _create(watch_manager)

        found = await watch_manager.get_task_by_uri(
            "viking://resources/test", TEST_ACCOUNT_ID, TEST_USER_ID, TEST_ROLE
        )

        assert found.task_id == task.task_id
        assert (
            await watch_manager.get_task_by_uri(
                "viking://nonexistent", TEST_ACCOUNT_ID, TEST_USER_ID, TEST_ROLE
            )
            is None
        )

    @pytest.mark.asyncio
    async def test_pause_wins_over_concurrent_run_settlement(self, watch_manager: WatchManager):
        task = await _create(watch_manager, initial_run_id="run-1")

        await asyncio.gather(
            watch_manager.update_task(
                task.task_id, TEST_ACCOUNT_ID, TEST_USER_ID, TEST_ROLE, is_active=False
            ),
            watch_manager.finish_run(task.task_id, "run-1", WatchRunResult(status="completed")),
        )

        final = await watch_manager.get_task(task.task_id)
        assert final.is_active is False
        assert final.last_status == "completed"
        assert final.next_execution_time is None


class TestTargetRewrites:
    @pytest.mark.asyncio
    async def test_deactivate_tasks_under_uri_matches_subtree(self, watch_manager: WatchManager):
        root = await _create(watch_manager, "viking://resources/codeask/wiki")
        child = await _create(watch_manager, "viking://resources/codeask/wiki/page")
        sibling = await _create(watch_manager, "viking://resources/codeask/wiki-renamed")

        deactivated = await watch_manager.deactivate_tasks_under_uri_internal(
            "viking://resources/codeask/wiki", TEST_ACCOUNT_ID
        )

        assert {task.task_id for task in deactivated} == {root.task_id, child.task_id}
        assert (await watch_manager.get_task(root.task_id)).is_active is False
        assert (await watch_manager.get_task(child.task_id)).is_active is False
        assert (await watch_manager.get_task(sibling.task_id)).is_active is True

    @pytest.mark.asyncio
    async def test_rewrite_moves_subtree_and_parent_uri(self, watch_manager: WatchManager):
        root = await _create(
            watch_manager,
            "viking://resources/codeask/wiki",
            parent_uri="viking://resources/codeask",
        )
        child = await _create(watch_manager, "viking://resources/codeask/wiki/page")

        moved = await watch_manager.rewrite_target_prefix_internal(
            "viking://resources/codeask/wiki", "viking://resources/docs/wiki", TEST_ACCOUNT_ID
        )

        assert {task.task_id for task in moved} == {root.task_id, child.task_id}
        root_after = await watch_manager.get_task(root.task_id)
        assert root_after.to_uri == "viking://resources/docs/wiki"
        assert root_after.parent_uri == "viking://resources/docs"
        child_after = await watch_manager.get_task(child.task_id)
        assert child_after.to_uri == "viking://resources/docs/wiki/page"

    @pytest.mark.asyncio
    async def test_move_and_deactivate_are_account_scoped(self, watch_manager: WatchManager):
        uri = "viking://resources/shared"
        task_a = await _create(watch_manager, uri, path="/a", account_id="account-a")
        task_b = await _create(watch_manager, uri, path="/b", account_id="account-b")

        await watch_manager.validate_target_prefix_rewrite_internal(
            uri, f"{uri}-moved", "account-a"
        )
        await watch_manager.rewrite_target_prefix_internal(uri, f"{uri}-moved", "account-a")
        deactivated = await watch_manager.deactivate_tasks_under_uri_internal(uri, "account-b")

        moved = await watch_manager.get_task_by_uri(f"{uri}-moved", "account-a", "default", "root")
        assert moved.task_id == task_a.task_id and moved.is_active is True
        assert [task.task_id for task in deactivated] == [task_b.task_id]
        stayed = await watch_manager.get_task_by_uri(uri, "account-b", "default", "root")
        assert stayed.task_id == task_b.task_id and stayed.is_active is False

    @pytest.mark.asyncio
    async def test_rewrite_onto_native_target_is_refused_without_changes(self, watch_manager):
        await _create(watch_manager, "viking://resources/target")
        moving = await _create(watch_manager, "viking://resources/source")

        with pytest.raises(ConflictError, match="already being monitored"):
            await watch_manager.validate_target_prefix_rewrite_internal(
                "viking://resources/source", "viking://resources/target", TEST_ACCOUNT_ID
            )
        with pytest.raises(ConflictError, match="already being monitored"):
            await watch_manager.rewrite_target_prefix_internal(
                "viking://resources/source", "viking://resources/target", TEST_ACCOUNT_ID
            )
        assert (await watch_manager.get_task(moving.task_id)).to_uri == "viking://resources/source"


class TestSharedConnectorTargets:
    """Connector watches may share a target; native watches are exclusive."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("user_id", "role", "visible_indexes"),
        [
            ("carol", "user", ()),
            ("bob", "user", (2,)),
            ("alice", "user", (0, 1)),
            ("bob", "admin", (2,)),
            ("bob", "root", (0, 1, 2)),
        ],
    )
    async def test_uri_lookup_only_considers_visible_tasks(
        self, watch_manager: WatchManager, user_id, role, visible_indexes
    ):
        to_uri = "viking://resources/shared"
        tasks = []
        for index, owner in enumerate(("alice", "alice", "bob")):
            tasks.append(
                await _create(
                    watch_manager,
                    to_uri,
                    path=f"tos://bucket/{index}/",
                    user_id=owner,
                    auth_state=dict(_CONNECTOR_AUTH),
                )
            )

        if len(visible_indexes) > 1:
            with pytest.raises(
                ConflictError, match=f"has {len(visible_indexes)} watch tasks"
            ) as exc_info:
                await watch_manager.get_task_by_uri(to_uri, TEST_ACCOUNT_ID, user_id, role)
            for index, task in enumerate(tasks):
                assert (task.task_id in str(exc_info.value)) == (index in visible_indexes)
        else:
            task = await watch_manager.get_task_by_uri(to_uri, TEST_ACCOUNT_ID, user_id, role)
            expected = tasks[visible_indexes[0]].task_id if visible_indexes else None
            assert (task.task_id if task else None) == expected

    @pytest.mark.asyncio
    async def test_connector_watches_coexist_and_native_is_refused(self, watch_manager):
        to_uri = "viking://resources/shared"
        first = await _create(
            watch_manager, to_uri, path="tos://bucket/a/", auth_state=dict(_CONNECTOR_AUTH)
        )
        second = await _create(
            watch_manager, to_uri, path="tos://bucket/b/", auth_state=dict(_CONNECTOR_AUTH)
        )
        assert first.task_id != second.task_id

        with pytest.raises(ConflictError, match="already being monitored"):
            await _create(watch_manager, to_uri, path="/local/doc")

        await watch_manager.delete_task(first.task_id, "default", "default", "user")
        remaining = await watch_manager.get_task_by_uri(to_uri, "default", "default", "user")
        assert remaining.task_id == second.task_id

    @pytest.mark.asyncio
    async def test_native_watch_blocks_connector_watch(self, watch_manager: WatchManager):
        to_uri = "viking://resources/native"
        await _create(watch_manager, to_uri, path="/local/doc")
        with pytest.raises(ConflictError, match="already being monitored"):
            await _create(
                watch_manager, to_uri, path="tos://bucket/a/", auth_state=dict(_CONNECTOR_AUTH)
            )

    @pytest.mark.asyncio
    async def test_connector_rewrite_may_land_on_connector_target(self, watch_manager):
        auth = dict(_CONNECTOR_AUTH)
        await _create(watch_manager, "viking://resources/target", path="tos://a/", auth_state=auth)
        moving = await _create(
            watch_manager, "viking://resources/source", path="tos://b/", auth_state=auth
        )

        await watch_manager.rewrite_target_prefix_internal(
            "viking://resources/source", "viking://resources/target", TEST_ACCOUNT_ID
        )

        assert (await watch_manager.get_task(moving.task_id)).to_uri == "viking://resources/target"
