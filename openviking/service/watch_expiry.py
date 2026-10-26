# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Watch business adapter for distributed expiry scheduling.

Idle watches own an execution timer; periodic runs hand off to an observer
bound to (watch, run). The MySQL row remains the source of truth. Callbacks
await submission; subsequent ingestion completion is observed separately.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import TYPE_CHECKING

from openviking.scheduling.expiry import (
    WATCH_ITEM_TYPE,
    WATCH_OBSERVER_ITEM_TYPE,
    CallbackRequest,
    CallbackResult,
    CallbackStatus,
    ExpiryMode,
    ExpiryRegistration,
    ExpiryScheduler,
    HandlerOptions,
    ItemIdentity,
)
from openviking_cli.utils import get_logger

if TYPE_CHECKING:
    from openviking.resource.distributed_watch_scheduler import DistributedWatchScheduler
    from openviking.resource.distributed_watch_types import WatchProjection
    from openviking.resource.watch_manager import WatchTask

logger = get_logger(__name__)


def install_watch_expiry(
    scheduler: ExpiryScheduler,
    watch_scheduler: "DistributedWatchScheduler",
    *,
    concurrency: int = 4,
    error_backoff: float = 60.0,
) -> "WatchTimers":
    """Register the watch timer handler; must run before expiry ``start_roles``."""
    timers = WatchTimers(scheduler)
    scheduler.register_handler(
        WATCH_ITEM_TYPE,
        WatchTimerHandler(watch_scheduler, timers, error_backoff=error_backoff),
        options=HandlerOptions(
            concurrency=concurrency,
            max_attempts=3,
            metadata_enabled=False,
        ),
    )
    scheduler.register_handler(
        WATCH_OBSERVER_ITEM_TYPE,
        WatchTimerHandler(watch_scheduler, timers, error_backoff=error_backoff),
        options=HandlerOptions(
            concurrency=concurrency,
            max_attempts=3,
            metadata_enabled=False,
        ),
    )
    watch_scheduler.set_timer_adapter(timers)
    return timers


def watch_identity(task: "WatchTask") -> ItemIdentity:
    return ItemIdentity(WATCH_ITEM_TYPE, task.account_id, task.user_id, task.task_id)


def projection_identity(state: "WatchProjection") -> ItemIdentity:
    return ItemIdentity(WATCH_ITEM_TYPE, state.account_id, state.user_id, state.task_id)


def observer_identity(task: "WatchTask", run_id: str) -> ItemIdentity:
    return ItemIdentity(
        WATCH_OBSERVER_ITEM_TYPE,
        task.account_id,
        task.user_id,
        json.dumps([task.task_id, run_id], separators=(",", ":")),
    )


class WatchTimers:
    """Arms and disarms per-watch timers.

    ``arm`` raises so callback paths can back off; write-path hooks
    (``sync_after_write`` / ``disarm``) are best effort and only log, leaving
    committed Watch data intact for operational re-registration.
    """

    def __init__(self, scheduler: ExpiryScheduler) -> None:
        self._scheduler = scheduler

    async def arm(self, task: "WatchTask", due_at: float) -> None:
        await self.arm_identity(
            watch_identity(task),
            due_at,
            timeout_seconds=max(1, int(task.watch_interval * 60)),
        )

    async def arm_observer(self, task: "WatchTask", run_id: str) -> None:
        """Observe independently of the business interval (at most 60 seconds)."""
        delay = min(task.watch_interval * 60, 60.0)
        await self.arm_identity(
            observer_identity(task, run_id),
            time.time() + delay,
            timeout_seconds=max(1, int(delay)),
        )

    async def arm_identity(
        self,
        identity: ItemIdentity,
        due_at: float,
        *,
        timeout_seconds: int = 60,
    ) -> None:
        # The store clamps past due times to "now"; metadata is disabled, so the
        # timeout only has to be a valid positive value.
        due_at_ms = max(0, int(due_at * 1000))
        try:
            registration = await self._scheduler.report(
                ExpiryRegistration(
                    identity=identity,
                    due_at_ms=due_at_ms,
                    timeout_seconds=timeout_seconds,
                    mode=ExpiryMode.IDLE_ONESHOT,
                )
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.record_failure("report_failed", identity.local_id, exc)
            raise
        logger.info(
            "Watch timer registered item_type=%s local_id=%s account_id=%s user_id=%s due_at_ms=%s",
            identity.item_type,
            identity.local_id,
            identity.account_id,
            identity.user_id,
            registration.accepted_due_at_ms,
        )

    async def disarm(self, task: "WatchTask") -> None:
        await self._scheduler.unregister(watch_identity(task))

    async def sync(self, state: "WatchProjection") -> None:
        """Raise on failure so MySQL's projection version remains dirty."""
        identity = projection_identity(state)
        timeout_seconds = max(1, int(state.watch_interval * 60))
        if state.current_run_id:
            # Paused/deleted runs still own their observer and URI occupancy.
            await self.arm_identity(
                ItemIdentity(
                    WATCH_OBSERVER_ITEM_TYPE, state.account_id, state.user_id,
                    json.dumps([state.task_id, state.current_run_id], separators=(",", ":")),
                ),
                # Repair/write sync must not continually postpone a live OB.
                state.now,
            )
            await self._scheduler.unregister(identity)
        elif state.enabled and not state.deleted and state.next_run_at is not None:
            await self.arm_identity(identity, state.next_run_at, timeout_seconds=timeout_seconds)
        else:
            await self._scheduler.unregister(identity)

    def record_failure(self, reason: str, watch_task_id: str, exc: Exception | None) -> None:
        logger.warning(
            "Watch expiry operation failed reason=%s watch_task_id=%s error=%s",
            reason,
            watch_task_id,
            type(exc).__name__ if exc is not None else "unknown",
            exc_info=exc is not None,
        )


class WatchTimerHandler:
    def __init__(
        self,
        watch_scheduler: "DistributedWatchScheduler",
        timers: WatchTimers,
        *,
        error_backoff: float,
    ) -> None:
        self._watch_scheduler = watch_scheduler
        self._timers = timers
        self._error_backoff = error_backoff

    async def handle(self, request: CallbackRequest) -> CallbackResult:
        watch_task_id = request.identity.local_id
        try:
            if request.identity.item_type == WATCH_OBSERVER_ITEM_TYPE:
                watch_task_id, run_id = json.loads(request.identity.local_id)
                await self._watch_scheduler.handle_observer(watch_task_id, run_id)
            else:
                await self._watch_scheduler.handle_timer(watch_task_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "Watch timer callback failed watch_task_id=%s error=%s; backing off %ss",
                watch_task_id,
                type(exc).__name__,
                self._error_backoff,
                exc_info=True,
            )
            # Re-arm the same stable timer with backoff instead of the consumer's
            # immediate retries; only fall back to RETRY when Redis itself fails.
            try:
                await self._timers.arm_identity(request.identity, time.time() + self._error_backoff)
            except asyncio.CancelledError:
                raise
            except Exception as report_exc:
                self._timers.record_failure("backoff_report_failed", watch_task_id, report_exc)
                return CallbackResult(CallbackStatus.RETRY, "backoff_report_failed")
            return CallbackResult(CallbackStatus.COMPLETED, "backoff")
        return CallbackResult(CallbackStatus.COMPLETED, "advanced")
