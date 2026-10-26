# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Watch API orchestration; permission and runtime conditions live in SQL."""

import asyncio
from typing import Any, Optional

from openviking.observability.http_error_context import sanitize_public_http_error
from openviking.resource.distributed_watch_store import MySQLWatchStore
from openviking.resource.distributed_watch_types import (
    MUTABLE_FIELDS,
    WatchAuthority,
    WatchPatch,
    WatchRunResult,
    WatchTimerAdapter,
)
from openviking.resource.watch_manager import WatchTask
from openviking.scheduling.provider import ScheduleClaim
from openviking_cli.exceptions import ConflictError
from openviking_cli.utils import get_logger

logger = get_logger(__name__)


class DistributedWatchManager:
    def __init__(self, store: MySQLWatchStore) -> None:
        self._store = store
        self._timers: Optional[WatchTimerAdapter] = None

    async def initialize(self) -> None:
        await self._store.initialize()

    async def close(self) -> None:
        await self._store.close()

    @property
    def schedule_provider(self) -> "DistributedWatchManager":
        return self

    def set_timer_adapter(self, timers: WatchTimerAdapter) -> None:
        self._timers = timers

    async def sync_timer(self, task_id: str) -> None:
        """CAS the acknowledgement; a stale write forces another projection."""
        if self._timers is None:
            return
        for _ in range(3):
            state = await self._store.get_projection(task_id)
            if state is None:
                return
            try:
                await self._timers.sync(state)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Watch timer sync pending repair task_id=%s", task_id)
                return
            if await self._store.acknowledge_timer(task_id, state.revision):
                return

    async def create_task(self, *, initial_run_id: Optional[str] = None, **values: Any) -> WatchTask:
        if not values.get("path"):
            raise ValueError("Path is required")
        if values.get("watch_interval", 60) <= 0:
            raise ValueError("watch_interval must be > 0")
        task = WatchTask(**values)
        task.next_execution_time = task.calculate_next_execution_time() if task.is_active else None
        while not await self._store.create_task(task, initial_run_id=initial_run_id):
            await asyncio.sleep(0.1)
        await self.sync_timer(task.task_id)
        return task

    async def update_task(
        self, task_id: str, account_id: str, user_id: str, role: str, **values: Any,
    ) -> WatchTask:
        # Existing internal callers omit optional None values. Auth/state None
        # explicitly clears private state, matching the main manager.
        values = {
            key: value for key, value in values.items()
            if value is not None or key in {"auth_state", "connector_states"}
        }
        unknown = values.keys() - MUTABLE_FIELDS
        if unknown:
            raise ValueError(f"Unsupported Watch update fields: {sorted(unknown)}")
        interval = values.get("watch_interval")
        if interval is not None and interval <= 0:
            if values.get("is_active") is True:
                raise ValueError("watch_interval must be > 0 for active tasks")
            values.pop("watch_interval")
            values["is_active"] = False
        task = await self._store.patch_task(
            task_id, WatchPatch(values), WatchAuthority(account_id, user_id, role),
        )
        if task is None:
            raise ValueError(f"Task {task_id} not found")
        await self.sync_timer(task_id)
        return task

    async def get_task(
        self, task_id: str, account_id: str = "default",
        user_id: str = "default", role: str = "root",
    ) -> Optional[WatchTask]:
        return await self._store.get_task(
            task_id, WatchAuthority(account_id, user_id, role), private=False,
        )

    async def get_all_tasks(
        self, account_id: str, user_id: str, role: str, active_only: bool = False,
        scope_account_id: Optional[str] = None,
    ) -> list[WatchTask]:
        tasks, _ = await self._store.list_tasks(
            WatchAuthority(account_id, user_id, role),
            active_only=active_only, scope_account_id=scope_account_id,
        )
        return tasks

    async def get_task_by_uri(
        self, to_uri: str, account_id: str, user_id: str, role: str,
    ) -> Optional[WatchTask]:
        tasks, total = await self._store.list_tasks(
            WatchAuthority(account_id, user_id, role),
            target_uri=to_uri, scope_account_id=account_id,
        )
        if total > 1:
            raise ConflictError(
                f"Target URI '{to_uri}' has {total} watch tasks; address one by task_id.",
                resource=to_uri,
            )
        return tasks[0] if tasks else None

    async def delete_task(self, task_id: str, account_id: str, user_id: str, role: str) -> bool:
        deleted = await self._store.delete_task(task_id, WatchAuthority(account_id, user_id, role))
        if deleted:
            await self.sync_timer(task_id)
        return deleted

    async def update_auth_state(
        self, task_id: str, auth_state: Optional[dict[str, Any]], *, run_id: Optional[str] = None,
    ) -> None:
        await self._store.patch_task(task_id, WatchPatch({"auth_state": auth_state}), run_id=run_id)

    async def update_connector_states(self, task_id: str, connector_states: dict[str, Any]) -> None:
        await self._store.patch_task(task_id, WatchPatch({"connector_states": connector_states}))

    async def mark_submitted(self, claim: ScheduleClaim) -> bool:
        return await self._store.mark_submitted(claim)

    async def finish_run(self, claim: ScheduleClaim, result: WatchRunResult) -> bool:
        if result.error:
            from dataclasses import replace
            result = replace(result, error=sanitize_public_http_error(
                code="WATCH_EXECUTION_FAILED", message=result.error,
            ).message)
        applied = await self._store.finish_run(claim, result)
        await self.sync_timer(claim.schedule_id)
        return applied

    async def record_execution(
        self, task_id: str, *, status: str, execution_task_id: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        from openviking.service.task_tracker import get_task_tracker
        state = await self._store.get_state(task_id)
        if state is not None and state.current_run_id == execution_task_id:
            record = await get_task_tracker().get(
                execution_task_id, account_id=state.task.account_id, user_id=state.task.user_id,
            )
            if record is None or not record.is_terminal():
                return
            await self.finish_run(state.claim(), WatchRunResult(
                status=record.status.value, execution_task_id=execution_task_id, error=record.error,
            ))

    async def fail_initial_submission(self, task_id: str, run_id: str, error: str) -> None:
        state = await self._store.get_state(task_id)
        if state and state.current_run_id == run_id and not state.submitted:
            await self.finish_run(state.claim(), WatchRunResult(status="failed", error=error))

    async def validate_target_prefix_rewrite_internal(
        self, old_uri: str, new_uri: str, account_id: str = "default",
    ) -> None:
        await self._store.validate_target_prefix_rewrite(account_id, old_uri, new_uri)

    async def rewrite_target_prefix_internal(
        self, old_uri: str, new_uri: str, account_id: str = "default",
    ) -> list[WatchTask]:
        tasks = await self._store.rewrite_target_prefix(account_id, old_uri, new_uri)
        for task in tasks:
            await self.sync_timer(task.task_id)
        return tasks

    async def deactivate_tasks_under_uri_internal(
        self, uri: str, account_id: str = "default",
    ) -> list[WatchTask]:
        tasks = await self._store.deactivate_under_uri(account_id, uri)
        for task in tasks:
            await self.sync_timer(task.task_id)
        return tasks
