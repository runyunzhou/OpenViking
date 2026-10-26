# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Leader-elected Watch scheduler.

Every node serves the Watch API and may start manual runs. Eligible nodes
compete for the Watch store's leader lease; the leader polls due Watches and sweeps runs
whose Tasks have finished, disappeared or exceeded the run timeout.

Leadership only avoids duplicated polling. Exclusion comes from the claim:
the target's Tree PathLock is held while ``current_run_id`` is set by a
compare-and-set, and the run ID is the ingestion Task ID, so a run is
submitted, observed and settled by that ID from any node.

Occupancy is released only once the run's Task is terminal, or once it is
certain the Task will never exist: a submission may create its Task only
while its row is still occupied by it, not deleted and not fenced, and
whoever releases a run without a Task first fences or deletes the row.
"""

import asyncio
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Set

from openviking.connector.auth import is_external_feishu_auth, restore_feishu_request
from openviking.connector.delegate import ConnectorDelegate
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
from openviking.resource.watch_manager import WatchManager, WatchPathLocks
from openviking.resource.watch_store import WatchStore
from openviking.resource.watch_types import (
    TERMINAL_RUN_STATUSES,
    WatchClaim,
    WatchRunResult,
    WatchState,
    WatchTask,
)
from openviking.server.error_mapping import is_not_found_error
from openviking.server.identity import RequestContext, Role
from openviking.service.task_domain import TaskStatus
from openviking.service.task_tracker import get_task_tracker, task_admission
from openviking.service.task_tracker_concurrency import run_to_completion
from openviking.storage.errors import LockAcquisitionError
from openviking.utils.git_auth import GIT_AUTH_FAILED
from openviking_cli.session.user_id import UserIdentifier
from openviking_cli.utils import get_logger

logger = get_logger(__name__)


class WatchScheduler:
    POLL_INTERVAL = 15.0
    LEADER_TTL = 45.0
    DUE_BATCH = 100
    # A live submission past this is fenced off: it can no longer create its
    # Task, so releasing it never leaves a running Task unprotected.
    TASK_MISSING_GRACE = 60 * 60.0
    RUN_TIMEOUT = 3 * 60 * 60.0
    CANCEL_GRACE = 30 * 60.0

    def __init__(
        self,
        resource_service: Any,
        store: WatchStore,
        *,
        viking_fs: Any,
        runtime_config_manager: Any = None,
        leader_enabled: bool = True,
        max_concurrency: int = 4,
        poll_interval: float = POLL_INTERVAL,
        leader_ttl: float = LEADER_TTL,
        task_missing_grace: float = TASK_MISSING_GRACE,
        run_timeout: float = RUN_TIMEOUT,
        cancel_grace: float = CANCEL_GRACE,
        locks: Optional[WatchPathLocks] = None,
    ) -> None:
        if (
            min(
                max_concurrency,
                poll_interval,
                leader_ttl,
                task_missing_grace,
                run_timeout,
                cancel_grace,
            )
            <= 0
        ):
            raise ValueError("Watch scheduling limits must be positive")
        if leader_ttl <= poll_interval:
            raise ValueError("leader_ttl must exceed poll_interval")
        self._resource_service = resource_service
        self._store = store
        self._viking_fs = viking_fs
        self._watch_manager = WatchManager(store, locks or WatchPathLocks(viking_fs))
        self._runtime_config_manager = runtime_config_manager
        self._leader_enabled = leader_enabled
        self._slots = asyncio.Semaphore(max_concurrency)
        self._max_concurrency = max_concurrency
        self._poll_interval = poll_interval
        self._leader_ttl = leader_ttl
        self._task_missing_grace = task_missing_grace
        self._run_timeout = run_timeout
        self._cancel_grace = cancel_grace
        self._owner = str(uuid.uuid4())
        self._epoch: Optional[int] = None
        self._running = False
        self._loop_task: Optional[asyncio.Task] = None
        self._background: Set[asyncio.Task] = set()
        self._submissions: Dict[str, asyncio.Task] = {}
        self._cancel_requested: Set[str] = set()
        self._dispatching = 0

    @property
    def watch_manager(self) -> WatchManager:
        return self._watch_manager

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def is_leader(self) -> bool:
        return self._epoch is not None

    async def start(self) -> None:
        if self._running:
            return
        await self._watch_manager.initialize()
        self._running = True
        if self._leader_enabled:
            self._loop_task = asyncio.create_task(self._leader_loop())
        logger.info(
            "[WatchScheduler] Started store=%s leader_candidate=%s",
            self._store.provider,
            self._leader_enabled,
        )

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        tasks = [
            task
            for task in (self._loop_task, *self._background, *self._submissions.values())
            if task is not None
        ]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._loop_task = None
        self._background.clear()
        if self._epoch is not None:
            epoch, self._epoch = self._epoch, None
            try:
                await self._store.release_leadership(self._owner, epoch)
            except Exception:
                logger.exception("[WatchScheduler] Failed to release leadership")
        await self._watch_manager.close()
        logger.info("[WatchScheduler] Stopped")

    def _spawn(self, coroutine: Any) -> asyncio.Task:
        task = asyncio.create_task(coroutine)
        self._background.add(task)
        task.add_done_callback(self._background.discard)
        return task

    # ── Leader loop ─────────────────────────────────────────────────────

    async def _leader_loop(self) -> None:
        while self._running:
            try:
                if await self._hold_leadership():
                    await self.sweep()
                    await self.dispatch_due()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[WatchScheduler] Leader round failed")
            await asyncio.sleep(self._poll_interval)

    async def _hold_leadership(self) -> bool:
        if self._epoch is not None and await self._store.renew_leadership(
            self._owner,
            self._epoch,
            self._leader_ttl,
        ):
            return True
        previous, self._epoch = (
            self._epoch,
            await self._store.acquire_leadership(
                self._owner,
                self._leader_ttl,
            ),
        )
        if previous != self._epoch:
            logger.info("[WatchScheduler] Leadership epoch=%s owner=%s", self._epoch, self._owner)
        return self._epoch is not None

    async def dispatch_due(self) -> int:
        """Claim due Watches up to free submission capacity; returns runs started.

        A Watch that cannot be claimed (its target is busy) stays due, so the
        scan pages past it instead of letting it hold the capacity.
        """
        capacity = self._max_concurrency - self._dispatching
        started = skipped = 0
        while started < capacity:
            page = await self._store.due(self.DUE_BATCH, offset=skipped)
            for state in page:
                if started >= capacity:
                    break
                try:
                    claim = await self._claim(state.task)
                except Exception:
                    logger.exception(
                        "[WatchScheduler] Failed to claim Watch %s", state.task.task_id
                    )
                    claim = None
                if claim is None:
                    skipped += 1
                    continue
                started += 1
                self._dispatching += 1
                self._spawn(self._run(claim))
            if len(page) < self.DUE_BATCH:
                break
        return started

    async def _claim(self, task: WatchTask, *, manual: bool = False) -> Optional[WatchClaim]:
        # Locking a missing path materializes it on some PathLock backends, so a
        # native Watch whose target is gone is retired before any lock is taken.
        if task.to_uri and not ConnectorDelegate.is_watch_auth_state(task.auth_state):
            if not await self._target_exists(task):
                error = f"Watched target URI does not exist: {task.to_uri}"
                if await self._watch_manager.deactivate_idle(
                    task.task_id, target_uri=task.to_uri, error=error
                ):
                    logger.warning(
                        "[WatchScheduler] Deactivating Watch %s: %s", task.task_id, error
                    )
                return None
        try:
            async with self._watch_manager.locks.tree(task.account_id, task.to_uri):
                return await self._store.claim(task.task_id, target_uri=task.to_uri, manual=manual)
        except LockAcquisitionError:
            logger.info(
                "[WatchScheduler] Target busy; Watch %s waits for the next round",
                task.task_id,
            )
            return None

    async def _run(self, claim: WatchClaim) -> None:
        try:
            await self._submit(claim)
        finally:
            self._dispatching -= 1

    async def schedule_task(self, task_id: str) -> bool:
        """Start a run now, from any node; False when the Watch is occupied or busy."""
        if not self._running:
            raise RuntimeError("Watch scheduler is not running")
        state = await self._store.get_state(task_id)
        if state is None or state.deleted or state.current_run_id:
            return False
        claim = await self._claim(state.task, manual=True)
        if claim is None:
            return False
        await self._submit(claim)
        return True

    # ── Submission ──────────────────────────────────────────────────────

    async def _submit(self, claim: WatchClaim) -> None:
        """Submit a claimed run, tracked so stop and identity deletion can cancel it."""
        task = claim.task
        self._submissions[claim.run_id] = asyncio.current_task()
        try:
            async with self._slots:
                with task_admission(claim.run_id, lambda: self._admits(claim)):
                    result = await self._submit_resource(claim)
        except asyncio.CancelledError:
            await run_to_completion(
                lambda: self._finish_unsubmitted(
                    claim, WatchRunResult(status="cancelled", error="Watch submission cancelled")
                )
            )
            raise
        except Exception as exc:
            failure = WatchRunResult(
                status="failed",
                error=str(exc) or type(exc).__name__,
                deactivate=isinstance(exc, FileNotFoundError)
                or getattr(exc, "code", None) == GIT_AUTH_FAILED,
            )
            if await run_to_completion(lambda: self._finish_unsubmitted(claim, failure)):
                logger.warning("[WatchScheduler] Watch %s submission failed: %s", task.task_id, exc)
                return
            result = {"task_id": claim.run_id}
        finally:
            self._submissions.pop(claim.run_id, None)
        if result is None:
            return
        if result.get("task_id"):
            self._spawn(self._await_settlement(claim))
            return
        status = str(result.get("status") or "completed").lower()
        if status not in TERMINAL_RUN_STATUSES:
            status = "completed"
        await run_to_completion(
            lambda: self._watch_manager.finish_run(
                claim.task_id,
                claim.run_id,
                self._result(task, status, result, result.get("error")),
            )
        )

    async def _admits(self, claim: WatchClaim) -> bool:
        return await self._watch_manager.admits_run(claim.task_id, claim.run_id)

    async def _finish_unsubmitted(self, claim: WatchClaim, result: WatchRunResult) -> bool:
        """Settle a run whose Task was never committed; False when the Task exists."""
        return await self._release_without_task(claim.task, claim.run_id, result)

    async def _release_without_task(
        self, task: WatchTask, run_id: str, result: WatchRunResult
    ) -> bool:
        if not await self._store.fence_run(task.task_id, run_id):
            return False
        record = await get_task_tracker().get(
            run_id, account_id=task.account_id, user_id=task.user_id
        )
        if record is not None:
            return False
        return await self._watch_manager.finish_run(task.task_id, run_id, result)

    async def _submit_resource(self, claim: WatchClaim) -> Optional[dict[str, Any]]:
        task = claim.task
        auth = task.auth_state
        connector = ConnectorDelegate.is_watch_auth_state(auth)
        ctx = self._context(task)
        if (
            not connector
            and not task.path.startswith(("http://", "https://", "git@", "ssh://", "git://"))
            and not Path(task.path).exists()
        ):
            await self._deactivate(claim, f"Resource path does not exist: {task.path}")
            return None
        kwargs = dict(task.processor_kwargs or {})
        kwargs.pop("build_index", None)
        kwargs.pop("summarize", None)
        if is_external_feishu_auth(auth):
            ctx.api_key, kwargs["args"] = await restore_feishu_request(
                self._resource_service._connector,
                auth,
                path=task.path,
                ctx=ctx,
            )
        elif is_feishu_auth_state(auth):
            try:
                if feishu_auth_state_needs_refresh(auth):
                    from openviking.config.feishu import get_effective_feishu_config

                    if self._runtime_config_manager is None:
                        raise RuntimeError("Runtime config manager is not initialized")
                    config = await get_effective_feishu_config(
                        self._runtime_config_manager,
                        task.account_id,
                    )
                    refreshed = await FeishuOAuthClient.from_auth_state(
                        auth,
                        config=config,
                    ).refresh_user_access_token(auth.get("refresh_token"))
                    auth = apply_feishu_refreshed_token(auth, refreshed)
                    await self._watch_manager.update_auth_state(
                        task.task_id,
                        auth,
                        run_id=claim.run_id,
                    )
                kwargs["feishu_access_token"] = auth["access_token"]
            except FeishuTokenRefreshError as exc:
                if not exc.permanent:
                    raise
                await self._deactivate(claim, str(exc))
                return None
        elif is_git_http_auth_state(auth):
            kwargs["auth_config"] = git_http_auth_config_from_state(auth, task.path)
        elif connector:
            (
                ctx.api_key,
                kwargs["add_type"],
                kwargs["args"],
            ) = await self._resource_service._connector.restore_watch_request(
                auth,
                account_id=task.account_id,
                path=task.path,
            )
            kwargs["connector_states"] = task.connector_states
        return await self._resource_service.refresh_resource(
            path=task.path,
            ctx=ctx,
            to=task.to_uri,
            to_is_directory=task.to_is_directory,
            parent=task.parent_uri,
            reason=task.reason,
            instruction=task.instruction,
            build_index=task.build_index,
            summarize=task.summarize,
            processing_mode=task.processing_mode,
            watch_interval=task.watch_interval,
            enforce_public_remote_targets=True,
            wait_for_completion=False,
            task_id=claim.run_id,
            **kwargs,
        )

    async def _target_exists(self, task: WatchTask) -> bool:
        # Connector targets are rebuilt by the plugin's own writes; only native
        # Watches are tied to their target's existence.
        try:
            await self._viking_fs.stat(task.to_uri, ctx=self._context(task), skip_count=True)
        except Exception as exc:
            if not is_not_found_error(exc):
                raise
            return False
        return True

    async def _deactivate(self, claim: WatchClaim, error: str) -> None:
        logger.warning("[WatchScheduler] Deactivating Watch %s: %s", claim.task_id, error)
        await run_to_completion(
            lambda: self._watch_manager.finish_run(
                claim.task_id,
                claim.run_id,
                WatchRunResult(status="failed", error=error, deactivate=True),
            )
        )

    @staticmethod
    def _context(task: WatchTask) -> RequestContext:
        try:
            role = Role(task.original_role)
        except ValueError:
            role = Role.USER
        return RequestContext(
            user=UserIdentifier(task.account_id, task.user_id),
            role=role,
            bypass_acl=True,
        )

    # ── Settlement ──────────────────────────────────────────────────────

    @staticmethod
    def _result(
        task: WatchTask,
        status: str,
        result: Optional[dict[str, Any]],
        error: Optional[str],
    ) -> WatchRunResult:
        result = result or {}
        connector = ConnectorDelegate.is_watch_auth_state(task.auth_state)
        states = result.get("connector_states")
        has_states = connector and status == "completed" and "connector_states" in result
        if has_states and not isinstance(states, dict):
            status, error, has_states = "failed", "connector_states must be an object", False
        if status in {"failed", "error"} and not error:
            errors = result.get("errors")
            error = "; ".join(str(item) for item in errors) if errors else "watch ingestion failed"
        return WatchRunResult(
            status=status,
            execution_task_id=result.get("task_id"),
            error=error,
            deactivate=result.get("code") == GIT_AUTH_FAILED,
            connector_states=states if has_states else None,
            has_connector_states=has_states,
        )

    async def _await_settlement(self, claim: WatchClaim) -> None:
        """Settle promptly on the submitting node; the leader sweep covers any gap."""
        task = claim.task
        try:
            await get_task_tracker().wait(
                claim.run_id,
                account_id=task.account_id,
                user_id=task.user_id,
                timeout=self._run_timeout,
            )
        except (KeyError, TimeoutError, asyncio.TimeoutError):
            return
        await self.settle(claim.task_id, claim.run_id)

    async def settle(self, task_id: str, run_id: str) -> bool:
        """Record a terminal run Task; idempotent and safe on every node."""
        state = await self._store.get_state(task_id)
        if state is None or state.current_run_id != run_id:
            return False
        task = state.task
        record = await get_task_tracker().get(
            run_id, account_id=task.account_id, user_id=task.user_id
        )
        if record is None or not record.is_terminal():
            return False
        result = dict(record.result or {})
        result["task_id"] = record.task_id
        return await run_to_completion(
            lambda: self._watch_manager.finish_run(
                task_id,
                run_id,
                self._result(task, record.status.value, result, record.error),
            )
        )

    async def sweep(self) -> None:
        """Settle finished runs and force-settle missing or timed-out ones."""
        tracker = get_task_tracker()
        after = ""
        seen: Set[str] = set()
        while True:
            page = await self._store.running(after=after, limit=self.DUE_BATCH)
            for state in page:
                seen.add(state.current_run_id or "")
                try:
                    await self._sweep_one(tracker, state)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "[WatchScheduler] Sweep failed for Watch %s", state.task.task_id
                    )
            if len(page) < self.DUE_BATCH:
                self._cancel_requested &= seen
                return
            after = page[-1].task.task_id

    async def _sweep_one(self, tracker: Any, state: WatchState) -> None:
        task, run_id = state.task, state.current_run_id
        age = state.now - (state.run_started_at or state.now)
        record = await tracker.get(run_id, account_id=task.account_id, user_id=task.user_id)
        if record is None:
            if age > self._task_missing_grace:
                await self._release_without_task(
                    task,
                    run_id,
                    WatchRunResult(
                        status="error",
                        execution_task_id=run_id,
                        error="Watch run Task is missing",
                    ),
                )
            return
        if record.is_terminal():
            self._cancel_requested.discard(run_id)
            await self.settle(task.task_id, run_id)
            return
        # Request cancellation even when the first sweep arrives after the
        # release deadline. The deadline remains relative to the run's start.
        if (
            age > self._run_timeout
            and record.status.value != TaskStatus.CANCELLING.value
            and run_id not in self._cancel_requested
        ):
            self._cancel_requested.add(run_id)
            logger.warning(
                "[WatchScheduler] Watch %s run %s exceeded %gs; cancelling its Task",
                task.task_id,
                run_id,
                self._run_timeout,
            )
            await self._cancel_run_task(tracker, task, run_id)
        if age > self._run_timeout + self._cancel_grace:
            self._cancel_requested.discard(run_id)
            # Cancellation may have made the Task terminal; preserve its result.
            if await self.settle(task.task_id, run_id):
                return
            logger.warning(
                "[WatchScheduler] Watch %s run %s exceeded the %gs release deadline; "
                "releasing it after requesting cancellation",
                task.task_id,
                run_id,
                self._run_timeout + self._cancel_grace,
            )
            await self._watch_manager.finish_run(
                task.task_id,
                run_id,
                WatchRunResult(
                    status="failed",
                    execution_task_id=run_id,
                    error=f"Watch run timed out after {self._run_timeout:g}s",
                ),
            )

    async def delete_tasks(self, account_id: str, user_id: Optional[str] = None) -> None:
        """Remove an identity's Watches and release the runs they still occupy.

        Rows are tombstoned before their Tasks are looked up, and a submission
        may only create its Task while its row is not deleted, so a Task
        created by a submission on any node is either found here or withdrawn
        by its creator, also after the rows are purged. Local submissions are
        cancelled and awaited first so they stop promptly.
        """
        tracker = get_task_tracker()
        states = await self._store.delete_tasks(account_id, user_id)
        local = [
            submission
            for state in states
            if (submission := self._submissions.get(state.current_run_id or "")) is not None
            and submission is not asyncio.current_task()
        ]
        for submission in local:
            submission.cancel()
        if local:
            await asyncio.gather(*local, return_exceptions=True)
        for state in states:
            task, run_id = state.task, state.current_run_id
            record = await tracker.get(run_id, account_id=task.account_id, user_id=task.user_id)
            if record is not None and not record.is_terminal():
                await self._stop_run_task(tracker, task, run_id)
            await self._watch_manager.finish_run(
                task.task_id,
                run_id,
                WatchRunResult(status="cancelled", execution_task_id=run_id),
            )

    @staticmethod
    async def _stop_run_task(tracker: Any, task: WatchTask, run_id: str) -> None:
        """Cancel the run's Task, or fail one that cannot be cancelled.

        A Connector import runs remotely and cannot be cancelled; failing its
        Task ends local monitoring, so identity cleanup need not wait for it.
        """
        owner = {"account_id": task.account_id, "user_id": task.user_id}
        try:
            await tracker.cancel(run_id, **owner)
        except ValueError:
            try:
                await tracker.fail(run_id, "Watch deleted", **owner)
            except Exception:
                logger.exception(
                    "[WatchScheduler] Failed to stop Task %s of Watch %s", run_id, task.task_id
                )
        except Exception:
            logger.exception(
                "[WatchScheduler] Failed to cancel Task %s of Watch %s", run_id, task.task_id
            )

    @staticmethod
    async def _cancel_run_task(tracker: Any, task: WatchTask, run_id: str) -> None:
        try:
            await tracker.cancel(run_id, account_id=task.account_id, user_id=task.user_id)
        except Exception:
            logger.exception(
                "[WatchScheduler] Failed to cancel Task %s of Watch %s", run_id, task.task_id
            )
