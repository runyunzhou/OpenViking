# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Redis execution timers and run observers backed by MySQL Watch authority."""

from __future__ import annotations

import asyncio
import time
import uuid
from pathlib import Path
from typing import Any, Optional

from openviking.connector.auth import is_external_feishu_auth, restore_feishu_request
from openviking.connector.delegate import ConnectorDelegate
from openviking.resource.distributed_watch_manager import DistributedWatchManager
from openviking.resource.distributed_watch_store import MySQLWatchStore
from openviking.resource.distributed_watch_types import WatchRunResult, WatchState
from openviking.resource.feishu_watch_auth import (
    FeishuOAuthClient,
    FeishuTokenRefreshError,
    apply_feishu_refreshed_token,
    feishu_auth_state_needs_refresh,
    is_feishu_auth_state,
)
from openviking.resource.git_watch_auth import (
    git_http_auth_config_from_state,
    is_git_http_auth_state,
)
from openviking.resource.watch_manager import WatchTask
from openviking.scheduling.provider import ScheduleClaim
from openviking.server.error_mapping import is_not_found_error
from openviking.server.identity import RequestContext, Role
from openviking.service.task_tracker import get_task_tracker
from openviking.storage.mysql_runtime import wait_mysql_cleanup
from openviking.utils.git_auth import GIT_AUTH_FAILED
from openviking_cli.session.user_id import UserIdentifier
from openviking_cli.utils import get_logger

logger = get_logger(__name__)
TERMINAL = {"completed", "failed", "cancelled", "error"}


class DistributedWatchScheduler:
    def __init__(
        self, resource_service: Any, store: MySQLWatchStore, *,
        viking_fs: Any = None, runtime_config_manager: Any = None,
        max_concurrency: int = 4, check_interval: float = 60,
        task_timeout: float = 3 * 60 * 60,
    ) -> None:
        if max_concurrency <= 0 or check_interval <= 0 or task_timeout <= 0:
            raise ValueError("Watch scheduling limits must be positive")
        self._resource_service = resource_service
        self._store = store
        self._watch_manager = DistributedWatchManager(store)
        self._viking_fs = viking_fs
        self._runtime_config_manager = runtime_config_manager
        self._submission_slots = asyncio.Semaphore(max_concurrency)
        self._check_interval = check_interval
        self._task_timeout = task_timeout
        self._owner = str(uuid.uuid4())
        self._timers: Any = None
        self._running = False
        self._loop: Any = None
        self._repair_task: Optional[asyncio.Task] = None
        self._repair_cursor = ""
        self._full_repair_cursor = ""

    @property
    def backend(self) -> str:
        return "redis"

    @property
    def watch_manager(self) -> DistributedWatchManager:
        return self._watch_manager

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def check_interval(self) -> float:
        return self._check_interval

    def set_timer_adapter(self, timers: Any) -> None:
        self._timers = timers
        self._watch_manager.set_timer_adapter(timers)

    async def start(self) -> None:
        if self._running:
            return
        self._loop = asyncio.get_running_loop()
        await self._watch_manager.initialize()
        self._running = True
        self._repair_task = asyncio.create_task(self._repair_timers())

    async def stop(self) -> None:
        self._running = False
        # ExpiryRuntime drains its callbacks before the service stops Watch.
        if self._repair_task:
            self._repair_task.cancel()
            await asyncio.gather(self._repair_task, return_exceptions=True)
        self._repair_task = None
        await self._watch_manager.close()

    def _require_running(self) -> None:
        if not self._running or self._timers is None:
            raise RuntimeError("Distributed Watch scheduler is not running")
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Distributed Watch scheduler must run on its owner event loop")

    async def _repair_timers(self) -> None:
        """Repair registrations, including Redis loss, without executing SQL due rows."""
        while self._running:
            try:
                if self._timers is not None:
                    dirty = await self._store.timer_candidates(after=self._repair_cursor)
                    for state in dirty:
                        await self._watch_manager.sync_timer(state.task_id)
                    self._repair_cursor = dirty[-1].task_id if len(dirty) == 100 else ""
                    # One bounded page also repairs a Redis restart after a
                    # previously acknowledged registration. Full scans rotate.
                    page = await self._store.timer_candidates(
                        after=self._full_repair_cursor, dirty_only=False,
                    )
                    for state in page:
                        await self._watch_manager.sync_timer(state.task_id)
                    self._full_repair_cursor = page[-1].task_id if len(page) == 100 else ""
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Watch timer registration repair failed")
            await asyncio.sleep(self._check_interval)

    async def schedule_task(self, task_id: str) -> bool:
        self._require_running()
        claim = await self._store.claim(task_id, owner=self._owner, manual=True)
        if claim is None:
            return False
        await self._watch_manager.sync_timer(task_id)
        # Require observer registration before submitting; the dirty row
        # recovers registration if Redis failed after the claim.
        await self._timers.arm_observer(claim.payload, claim.run_id)
        await self._submit(claim)
        return True

    async def handle_timer(self, task_id: str) -> None:
        self._require_running()
        state = await self._store.get_state(task_id)
        if state is None:
            return
        if state.current_run_id:
            await self._timers.arm_observer(state.task, state.current_run_id)
            return
        if not state.enabled or state.next_run_at is None:
            return
        if state.next_run_at > state.now:
            await self._timers.arm(state.task, state.next_run_at)
            return
        claim = await self._store.claim(task_id, owner=self._owner)
        if claim is None:
            latest = await self._store.get_state(task_id)
            if latest and latest.enabled and not latest.current_run_id:
                # Mutation contention must not consume the sole execution timer.
                await self._timers.arm(latest.task, latest.now + 1)
            else:
                await self._watch_manager.sync_timer(task_id)
            return
        await self._timers.arm_observer(claim.payload, claim.run_id)
        await self._submit(claim)

    async def handle_observer(self, task_id: str, run_id: str) -> None:
        self._require_running()
        state = await self._store.get_state(task_id)
        if state is None or (state.current_run_id and state.current_run_id != run_id):
            return
        if state.current_run_id == run_id:
            await self._observe(state)
        # Settlement may have committed just before a failed next registration.
        latest = await self._store.get_state(task_id)
        if latest and latest.current_run_id == run_id:
            await self._timers.arm_observer(latest.task, run_id)
        else:
            await self._watch_manager.sync_timer(task_id)

    async def _observe(self, state: WatchState) -> None:
        claim = state.claim()
        task = state.task
        record = await get_task_tracker().get(
            claim.run_id, account_id=task.account_id, user_id=task.user_id,
        )
        if record is None:
            if state.submitted:
                # Absence is not proof that the worker stopped. Retain occupancy
                # and retry observation instead of allowing a conflicting move.
                logger.warning("Watch submitted Task missing watch=%s run=%s", task.task_id, claim.run_id)
                return
            if await self._store.acquire_submission(task.task_id, claim.run_id, owner=self._owner):
                await self._submit(claim)
            return
        if not state.submitted:
            await self._store.mark_submitted(claim)
        if not record.is_terminal():
            if time.time() - record.created_at > self._task_timeout:
                await get_task_tracker().cancel(
                    claim.run_id, account_id=task.account_id, user_id=task.user_id,
                )
            return
        result = record.result or {}
        status = record.status.value
        connector = ConnectorDelegate.is_watch_auth_state(task.auth_state)
        states = result.get("connector_states")
        has_states = connector and status == "completed" and "connector_states" in result
        error = record.error
        if has_states and not isinstance(states, dict):
            status, error, has_states = "failed", "connector_states must be an object", False
        await self._watch_manager.finish_run(claim, WatchRunResult(
            status=status, execution_task_id=record.task_id, error=error,
            deactivate=result.get("code") == GIT_AUTH_FAILED,
            connector_states=states, has_connector_states=has_states,
            started_at=None if state.initial_run else state.run_started_at,
        ))

    async def _submit(self, claim: ScheduleClaim) -> None:
        state = await self._store.get_state(claim.schedule_id)
        if state is None or state.current_run_id != claim.run_id or state.submitted:
            return
        # A delayed callback cannot submit a lease already reclaimed elsewhere.
        if state.lease_owner != self._owner:
            return
        task = state.task
        execution = asyncio.current_task()

        async def heartbeat() -> None:
            try:
                while True:
                    await asyncio.sleep(30)
                    if not await self._store.renew_submission(claim, self._owner):
                        return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Watch submission heartbeat failed run=%s", claim.run_id)
                if execution:
                    execution.cancel()

        renewal = asyncio.create_task(heartbeat())
        try:
            # Manual triggers wait for a slot just as the local scheduler does.
            # Keep renewing the submission lease while waiting.
            async with self._submission_slots:
                state = await self._store.get_state(claim.schedule_id)
                if (
                    state is None or state.current_run_id != claim.run_id
                    or state.submitted or state.lease_owner != self._owner
                ):
                    return
                # Recover a committed Task before calling the submission API again.
                record = await get_task_tracker().get(
                    claim.run_id, account_id=task.account_id, user_id=task.user_id,
                )
                if record is not None:
                    await self._store.mark_submitted(claim)
                    return
                await self._submit_resource(claim, state)
        except asyncio.CancelledError:
            raise
        except Exception:
            # A submit exception can occur after the Task was committed.
            record = await get_task_tracker().get(
                claim.run_id, account_id=task.account_id, user_id=task.user_id,
            )
            if record is not None:
                await self._store.mark_submitted(claim)
            else:
                # Keep the same run recoverable. The awaiting callback handles
                # retry/backoff; a local submission failure is not Task outcome.
                raise
        finally:
            renewal.cancel()
            await asyncio.gather(renewal, return_exceptions=True)
            await wait_mysql_cleanup(self._store.release_submission(claim, self._owner))

    async def _submit_resource(self, claim: ScheduleClaim, state: WatchState) -> None:
        task = state.task
        auth = task.auth_state
        connector = ConnectorDelegate.is_watch_auth_state(auth)
        ctx = self._context(task)
        if not connector and not task.path.startswith(("http://", "https://", "git@", "ssh://", "git://")):
            if not Path(task.path).exists():
                await self._deactivate(claim, state, f"Resource path does not exist: {task.path}")
                return
        if task.to_uri and not connector and not state.initial_run and self._viking_fs:
            try:
                await self._viking_fs.stat(task.to_uri, ctx=ctx, skip_count=True)
            except Exception as exc:
                if is_not_found_error(exc):
                    await self._deactivate(claim, state, f"Watched target URI does not exist: {task.to_uri}")
                    return
                raise
        kwargs = dict(task.processor_kwargs or {})
        kwargs.pop("build_index", None)
        kwargs.pop("summarize", None)
        if is_external_feishu_auth(auth):
            ctx.api_key, kwargs["args"] = await restore_feishu_request(
                self._resource_service._connector, auth, path=task.path, ctx=ctx,
            )
        elif is_feishu_auth_state(auth):
            try:
                if feishu_auth_state_needs_refresh(auth):
                    from openviking.config.feishu import get_effective_feishu_config
                    config = await get_effective_feishu_config(self._runtime_config_manager, task.account_id)
                    refreshed = await FeishuOAuthClient.from_auth_state(
                        auth, config=config,
                    ).refresh_user_access_token(auth.get("refresh_token"))
                    auth = apply_feishu_refreshed_token(auth, refreshed)
                    await self._watch_manager.update_auth_state(task.task_id, auth, run_id=claim.run_id)
                kwargs["feishu_access_token"] = auth["access_token"]
            except FeishuTokenRefreshError as exc:
                if not exc.permanent:
                    raise
                await self._deactivate(claim, state, str(exc))
                return
        elif is_git_http_auth_state(auth):
            kwargs["auth_config"] = git_http_auth_config_from_state(auth, task.path)
        elif connector:
            ctx.api_key, kwargs["add_type"], kwargs["args"] = (
                await self._resource_service._connector.restore_watch_request(
                    auth, account_id=task.account_id, path=task.path,
                )
            )
            kwargs["connector_states"] = task.connector_states
        result = await self._resource_service.refresh_resource(
            path=task.path, ctx=ctx, to=task.to_uri, to_is_directory=task.to_is_directory,
            parent=task.parent_uri, reason=task.reason, instruction=task.instruction,
            build_index=task.build_index, summarize=task.summarize,
            processing_mode=task.processing_mode, watch_interval=task.watch_interval,
            enforce_public_remote_targets=True, wait_for_completion=False,
            task_id=claim.run_id, schedule_kind="watch", schedule_id=task.task_id,
            run_id=claim.run_id, **kwargs,
        )
        if result.get("task_id"):
            await self._store.mark_submitted(claim)
            # Even an inline terminal response is settled from the authority.
            latest = await self._store.get_state(task.task_id)
            if latest and latest.current_run_id == claim.run_id:
                await self._observe(latest)
            return
        status = str(result.get("status") or "completed").lower()
        states = result.get("connector_states")
        has_states = connector and status == "completed" and "connector_states" in result
        error = result.get("error")
        if status not in TERMINAL:
            status = "completed"
        if has_states and not isinstance(states, dict):
            status, error, has_states = "failed", "connector_states must be an object", False
        await self._watch_manager.finish_run(claim, WatchRunResult(
            status=status, error=error, deactivate=result.get("code") == GIT_AUTH_FAILED,
            connector_states=states, has_connector_states=has_states,
            started_at=None if state.initial_run else state.run_started_at,
        ))

    async def _deactivate(self, claim: ScheduleClaim, state: WatchState, error: str) -> None:
        await self._watch_manager.finish_run(claim, WatchRunResult(
            status="failed", error=error, deactivate=True,
            started_at=None if state.initial_run else state.run_started_at,
        ))

    @staticmethod
    def _context(task: WatchTask) -> RequestContext:
        try:
            role = Role(task.original_role)
        except ValueError:
            role = Role.USER
        return RequestContext(
            user=UserIdentifier(task.account_id, task.user_id), role=role, bypass_acl=True,
        )

    async def hold_execution(self, task_id: str) -> bool:
        state = await self._store.get_state(task_id)
        return bool(state and state.current_run_id)

    async def release_execution(self, task_id: str) -> None:
        # A callback exiting is not terminal Task evidence.
        await self._watch_manager.sync_timer(task_id)

    async def delete_tasks(self, account_id: str, user_id: Optional[str] = None) -> None:
        states = await self._store.delete_tasks(account_id, user_id)
        for state in states:
            # The caller deletes this identity's Task records next. Settle
            # Watch occupancy here, while terminal Task evidence still exists.
            while state.current_run_id:
                record = await get_task_tracker().get(
                    state.current_run_id,
                    account_id=state.task.account_id, user_id=state.task.user_id,
                )
                if record is not None:
                    if not record.is_terminal():
                        await get_task_tracker().cancel(
                            state.current_run_id,
                            account_id=state.task.account_id, user_id=state.task.user_id,
                        )
                    await self._observe(state)
                elif not state.submitted and await self._store.acquire_submission(
                    state.task.task_id, state.current_run_id, owner=self._owner,
                ):
                    # No committed Task and no live submission lease remains.
                    await self._watch_manager.finish_run(
                        state.claim(), WatchRunResult(status="cancelled"),
                    )
                latest = await self._store.get_state(state.task.task_id)
                if latest is None or latest.current_run_id is None:
                    break
                state = latest
                await asyncio.sleep(0.1)
            await self._watch_manager.sync_timer(state.task.task_id)
