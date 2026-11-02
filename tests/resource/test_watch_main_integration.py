# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Main's Git gate and credential lifecycle on both distributed Watch stores."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from openviking.connector.delegate import ConnectorDelegate
from openviking.resource.watch_manager import WatchManager
from openviking.resource.watch_types import WatchPatch, WatchRunResult, WatchTask
from tests.resource.test_watch_scheduler import FakeTracker
from tests.resource.test_watch_store import (
    OWNER,
    create,
)
from tests.resource.test_watch_store import (
    make_store as make_store,
)
from tests.resource.test_watch_store import (
    store as store,
)

REPO = "https://github.com/volcengine/OpenViking.git"
SHA = "a" * 40
CHECKPOINT = {"kind": "git", "head_sha": SHA, "instruction": ""}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario",
    [
        "unchanged",
        "commit",
        "token",
        "instruction",
        "changed",
        "error",
        "timeout",
        "non_git",
        "sync",
        "failed",
    ],
)
async def test_git_gate(store, watch_vfs, monkeypatch, scenario):
    tracker = FakeTracker()
    imported_sha = "c" * 40
    probe = AsyncMock(return_value=("", SHA))
    if scenario == "changed":
        probe.return_value = ("refs/heads/main", "b" * 40)
    elif scenario == "error":
        probe.side_effect = RuntimeError("probe failed")
    elif scenario == "timeout":

        async def blocked(*args, **kwargs):
            await asyncio.Event().wait()

        probe.side_effect = blocked
    monkeypatch.setattr(
        "openviking.parse.accessors.git_accessor.GitAccessor.probe_git_remote_head", probe
    )

    async def refresh(**kwargs):
        if scenario == "sync":
            return {"status": "completed", "meta": {"resolved_head_sha": imported_sha}}
        record = await tracker.create("add_resource", task_id="refresh-1", resource_id=kwargs["to"])
        await kwargs["on_task_created"](record)
        return {"status": "success", "task_id": record.task_id}

    resources = SimpleNamespace(
        refresh_resource=AsyncMock(side_effect=refresh),
        _resource_processor=SimpleNamespace(github_token_for=AsyncMock(return_value=None)),
    )
    if scenario == "token":
        resources._resource_processor.github_token_for.return_value = "account-token"
    manager = WatchManager(store, viking_fs=watch_vfs, tracker=tracker, resource_service=resources)
    manager.DEFAULT_GIT_PROBE_TIMEOUT = 0.01 if scenario == "timeout" else 30
    target = "viking://resources/git"
    task = WatchTask(
        account_id=OWNER.account_id,
        user_id=OWNER.user_id,
        to_uri=target,
        path=REPO if scenario != "non_git" else "https://example.org/doc",
        source_checkpoint=None if scenario in {"sync", "failed"} else CHECKPOINT,
        last_task_id="previous-import",
        instruction="new" if scenario == "instruction" else "",
        processor_kwargs={"commit": SHA} if scenario == "commit" else {},
    )
    task.next_execution_time = task.calculate_next_execution_time()
    await watch_vfs.mkdir(target, ctx=manager.context(task), exist_ok=True)
    await store.create_task(task)
    receipt = await manager.trigger(task.task_id)
    state = await store.get_state(task.task_id)
    if scenario in {"unchanged", "commit", "token"}:
        assert receipt is None
        resources.refresh_resource.assert_not_awaited()
        assert state.task.last_task_id == "previous-import"
        assert state.task.last_status == "completed"
        assert state.run_started_at is None
        assert state.task.next_execution_time is not None
    elif scenario == "sync":
        assert receipt is None
        assert state.task.last_status == "completed"
        assert state.task.source_checkpoint == {
            "kind": "git",
            "head_sha": imported_sha,
            "instruction": "",
        }
        assert state.run_started_at is None
    else:
        assert receipt.task_id == "refresh-1"
        assert state.task.source_checkpoint == task.source_checkpoint
        assert state.current_run_id == receipt.task_id
        tracker.put(
            receipt.task_id,
            "failed" if scenario == "failed" else "completed",
            result=None if scenario == "failed" else {"meta": {"resolved_head_sha": imported_sha}},
            error="import failed" if scenario == "failed" else None,
        )
        assert await manager.settle(task.task_id, receipt.task_id)
        assert not await manager.settle(task.task_id, receipt.task_id)
        state = await store.get_state(task.task_id)
        assert state.task.source_checkpoint == (
            None
            if scenario == "failed"
            else {
                "kind": "git",
                "head_sha": imported_sha,
                "instruction": task.instruction,
            }
        )
        kwargs = resources.refresh_resource.await_args.kwargs
        if scenario in {"instruction", "changed"}:
            assert kwargs["git_head_sha_hint"] == probe.return_value[1]
        elif scenario != "failed":
            assert "git_head_sha_hint" not in kwargs
    if scenario == "non_git":
        probe.assert_not_awaited()
    elif scenario == "token":
        probe.assert_awaited_once_with(REPO, auth_config={"token": "account-token"})
        assert "auth_config" not in state.task.processor_kwargs
    elif scenario == "commit":
        probe.assert_awaited_once_with(REPO, commit=SHA)
    assert "source_checkpoint" not in state.task.to_dict()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalidated", [False, True])
async def test_checkpoint_settlement_is_fenced(store, invalidated):
    task = await create(store, initial_run_id="first")
    outcome = WatchRunResult(status="completed", source_checkpoint=CHECKPOINT)
    if invalidated:
        await store.patch_task(task.task_id, WatchPatch({}, invalidate_source_checkpoint=True))
    assert not await store.finish_run(task.task_id, "stale", outcome)
    assert await store.finish_run(task.task_id, "first", outcome)
    assert (await store.get_task(task.task_id)).source_checkpoint == (
        None if invalidated else CHECKPOINT
    )
    run = await store.begin_run(task.task_id, run_id="second", target_uri=task.to_uri, manual=True)
    assert not await store.finish_run(task.task_id, "first", outcome)
    assert await store.finish_run(task.task_id, "second", outcome, started_at=run.started_at)
    assert (await store.get_task(task.task_id)).source_checkpoint == CHECKPOINT


def credential_state(version=8, authorized=False, **values):
    return {
        "account_id": "volcano-account",
        "resource_id": "resource",
        "user_id": "volcano-user",
        "ov_account_id": OWNER.account_id,
        "ov_user_id": OWNER.user_id,
        "type": "oauth",
        "platform": "dingtalk",
        "version": version,
        "authorized": authorized,
        **values,
    }


async def connector_task(store, watch_vfs, *, active=True, encrypted=False, legacy=False):
    manager = WatchManager(store, viking_fs=watch_vfs)
    account_id = "default" if legacy else OWNER.account_id
    reference = credential_state(ov_account_id=account_id)
    del reference["version"], reference["authorized"]
    reference["binding_version"] = 3
    if legacy:
        reference.pop("ov_account_id")
    request = {
        "api_key": "test-key",
        "account_id": account_id,
        "add_type": "dingtalk",
        "path": "dingtalk://node",
        "connector_args": {},
        "connector_auth_config": {"openviking_credential_ref": reference},
    }
    auth = {"provider": "connector_plaintext", "request": request}
    if encrypted:

        class Encryptor:
            async def encrypt(self, account_id, plaintext):
                return b"OVE1" + plaintext[::-1]

            async def decrypt(self, account_id, ciphertext):
                return ciphertext[4:][::-1]

        watch_vfs._encryptor = Encryptor()
        delegate = ConnectorDelegate(
            viking_fs=watch_vfs, background_tasks=set(), link_reason_memory=AsyncMock()
        )
        auth = await delegate.create_watch_auth_state(**request)
    task = await create(
        store,
        account_id=account_id,
        auth_state=auth,
        is_active=active,
        last_status="failed",
        last_error="previous",
    )
    task = await store.patch_task(task.task_id, WatchPatch({"path": request["path"]}))
    return manager, task


async def notify(manager, task, state):
    return await manager.update_task(
        task.task_id, task.account_id, task.user_id, OWNER.role, credential_state=state
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["plaintext", "encrypted", "legacy"])
async def test_credential_lifecycle_and_stale_events(store, watch_vfs, mode):
    manager, task = await connector_task(
        store, watch_vfs, encrypted=mode == "encrypted", legacy=mode == "legacy"
    )

    def state(version=8, authorized=False):
        return credential_state(version, authorized, ov_account_id=task.account_id)

    revoked = await notify(manager, task, state())
    assert not revoked.is_active and revoked.credential_paused
    assert revoked.next_execution_time is None
    await store.close()
    await store.initialize()
    persisted_pause = await store.get_task(task.task_id)
    assert persisted_pause.credential_paused and persisted_pause.credential_version == 8
    resumed = await notify(manager, task, state(10, True))
    assert resumed.is_active and not resumed.credential_paused
    assert resumed.next_execution_time is not None
    await asyncio.gather(
        *(
            notify(manager, task, state(version, version % 2 == 0))
            for version in [11, 14, 12, 13, 14]
        )
    )
    saved = await store.get_task(task.task_id)
    assert saved.is_active and saved.credential_version == 14
    assert saved.auth_state == task.auth_state
    assert (saved.last_status, saved.last_error) == ("failed", "previous")
    persisted = WatchTask.from_dict(saved.to_storage_dict())
    assert persisted.credential_version == 14
    assert "credential_version" not in persisted.to_dict()
    assert "credential_paused" not in persisted.to_dict()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    [
        "account_id",
        "resource_id",
        "user_id",
        "ov_account_id",
        "ov_user_id",
        "type",
        "platform",
    ],
)
async def test_credential_owner_fence(store, watch_vfs, field):
    manager, task = await connector_task(store, watch_vfs)
    result = await notify(manager, task, credential_state(**{field: "other"}))
    assert result.is_active and result.credential_version == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("already_paused", [False, True])
async def test_user_pause_survives_reauthorization(store, watch_vfs, already_paused):
    manager, task = await connector_task(store, watch_vfs, active=not already_paused)
    await notify(manager, task, credential_state())
    if not already_paused:
        await manager.update_task(
            task.task_id, OWNER.account_id, OWNER.user_id, OWNER.role, is_active=False
        )
    result = await notify(manager, task, credential_state(10, True))
    assert not result.is_active and not result.credential_paused
    assert result.next_execution_time is None


@pytest.mark.asyncio
async def test_credential_reference_change_during_notification(store, watch_vfs, monkeypatch):
    manager, task = await connector_task(store, watch_vfs)
    original_patch = store.patch_task

    async def replace_reference(task_id, patch, *args, **kwargs):
        await original_patch(task_id, WatchPatch({"auth_state": None}))
        return await original_patch(task_id, patch, *args, **kwargs)

    monkeypatch.setattr(store, "patch_task", replace_reference)
    result = await notify(manager, task, credential_state())
    assert result.is_active and result.credential_version == 0
