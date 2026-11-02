# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Leader-polled Watch triggers and observation of existing resource Tasks."""

import asyncio
import uuid
from functools import partial
from typing import Any, Optional

from openviking.resource.watch_manager import WatchManager
from openviking.resource.watch_types import WatchRunResult, WatchState
from openviking.service.task_tracker_concurrency import OwnerLoopDispatcher
from openviking_cli.utils import get_logger

logger = get_logger(__name__)


class WatchRuntime:
    POLL_INTERVAL = 15.0
    LEADER_TTL = 45.0
    DUE_BATCH = 100
    TASK_MISSING_GRACE = 3600.0
    RUN_TIMEOUT = 10800.0

    def __init__(
        self,
        watch_manager: WatchManager,
        *,
        leader_enabled: bool = True,
        max_concurrency: int = 4,
        poll_interval: float = POLL_INTERVAL,
        leader_ttl: float = LEADER_TTL,
        task_missing_grace: float = TASK_MISSING_GRACE,
        run_timeout: float = RUN_TIMEOUT,
        leadership_candidate: Any = None,
    ) -> None:
        if (
            min(max_concurrency, poll_interval, leader_ttl, task_missing_grace, run_timeout) <= 0
            or leader_ttl <= poll_interval
        ):
            raise ValueError("Watch scheduling limits must be positive; ttl must exceed poll")
        self.watch_manager = watch_manager
        self._leader_enabled = leader_enabled
        self._max_concurrency = max_concurrency
        self._poll_interval = poll_interval
        self._run_timeout = run_timeout
        self._task_missing_grace = task_missing_grace
        self._dispatcher = OwnerLoopDispatcher()
        self._leadership_candidate = leadership_candidate
        if leader_enabled and leadership_candidate is None:
            self._leadership_candidate = watch_manager._viking_fs._async_agfs.leader_candidate(
                "watch-scheduler",
                str(uuid.uuid4()),
                ttl_secs=leader_ttl,
                renew_interval_secs=poll_interval,
                retry_interval_secs=poll_interval,
                mode="local" if watch_manager.store.provider == "file" else "distributed",
            )
        self._running = False
        self._is_leader = False
        self._loop_task: Optional[asyncio.Task] = None
        self._submissions: dict[str, asyncio.Task] = {}
        self._due_offset = 0

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    async def start(self) -> None:
        if self._running:
            return
        self._dispatcher.bind_current_loop()
        self._running = True
        if self._leader_enabled:
            self._loop_task = asyncio.create_task(self._leader_loop())

    async def stop(self, *, timeout: Optional[float] = None) -> None:
        """Keep dependencies open if a submission cannot finish before timeout."""
        await self._dispatcher.run(lambda: self._stop(timeout))

    async def _stop(self, timeout: Optional[float]) -> None:
        self._running = False
        tasks = set(self._submissions.values())
        if self._loop_task is not None:
            tasks.add(self._loop_task)
        for task in tasks:
            task.cancel()
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=timeout)
            for task in done:
                if not task.cancelled():
                    task.exception()
            if pending:
                raise TimeoutError(
                    "Watch Runtime coroutines have not stopped; keep dependencies open"
                )
        self._loop_task = None
        self._is_leader = False

    async def _leader_loop(self) -> None:
        try:
            async with self._leadership_candidate as candidate:
                async for leadership in candidate:
                    if not self._running:
                        return
                    self._is_leader = True
                    try:
                        while self._continue(leadership.stop_event):
                            try:
                                await self._sweep(leadership.stop_event)
                                await self._dispatch_due(leadership.stop_event)
                            except asyncio.CancelledError:
                                raise
                            except Exception:
                                logger.exception("Watch Runtime round failed")
                            try:
                                await asyncio.wait_for(
                                    leadership.stop_event.wait(), self._poll_interval
                                )
                            except asyncio.TimeoutError:
                                pass
                    finally:
                        self._is_leader = False
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Watch leader candidate failed")

    def _continue(self, stop_event: Optional[asyncio.Event]) -> bool:
        return self._running and (stop_event is None or not stop_event.is_set())

    async def dispatch_due(self, stop_event: Optional[asyncio.Event] = None) -> int:
        return await self._dispatcher.run(lambda: self._dispatch_due(stop_event))

    async def _dispatch_due(self, stop_event: Optional[asyncio.Event]) -> int:
        capacity = self._max_concurrency - len(self._submissions)
        started, offset = 0, self._due_offset
        while self._continue(stop_event) and started < capacity:
            page = await self.watch_manager.store.due(self.DUE_BATCH, offset=offset)
            if not page and offset:
                # Completed submissions disappear from due. Restart the scan
                # after reaching its end; a busy prefix cannot monopolize polls.
                offset = 0
                if started:
                    break
                page = await self.watch_manager.store.due(self.DUE_BATCH)
            for state in page:
                if not self._continue(stop_event) or started >= capacity:
                    break
                task_id = state.task.task_id
                # Already scheduled coroutines may not have started occupancy yet.
                # Advancing the offset lets unrelated due Watches make progress.
                offset += 1
                if task_id in self._submissions:
                    continue
                runner = asyncio.create_task(self.watch_manager.trigger(task_id, manual=False))
                self._submissions[task_id] = runner
                runner.add_done_callback(partial(self._finished, task_id))
                started += 1
            if len(page) < self.DUE_BATCH and started < capacity:
                offset = 0
                break
        self._due_offset = offset
        return started

    def _finished(self, task_id: str, task: asyncio.Task) -> None:
        self._submissions.pop(task_id, None)
        if not task.cancelled() and task.exception() is not None:
            logger.error("Watch trigger %s failed", task_id, exc_info=task.exception())

    async def sweep(self, stop_event: Optional[asyncio.Event] = None) -> None:
        await self._dispatcher.run(lambda: self._sweep(stop_event))

    async def _sweep(self, stop_event: Optional[asyncio.Event]) -> None:
        after = ""
        while self._continue(stop_event):
            page = await self.watch_manager.store.running(after=after, limit=self.DUE_BATCH)
            for state in page:
                if not self._continue(stop_event):
                    return
                try:
                    await self._sweep_one(state)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Watch sweep failed for %s", state.task.task_id)
            if len(page) < self.DUE_BATCH:
                return
            after = page[-1].task.task_id

    async def _sweep_one(self, state: WatchState) -> None:
        run = self.watch_manager.run(state)
        owner = {"account_id": run.task.account_id, "user_id": run.task.user_id}
        record = await self.watch_manager.tracker.get(run.run_id, **owner) if run.run_id else None
        if record is not None and record.is_terminal():
            await self.watch_manager.settle_record(run, record)
            return
        age = state.now - (state.run_started_at or state.now)
        if record is None and age > self._task_missing_grace:
            await self.watch_manager.finish_run(
                run.task_id,
                run.run_id,
                WatchRunResult(status="error", error="Watch run Task is missing"),
                started_at=run.started_at,
            )
        elif record is not None and age > self._run_timeout:
            try:
                await self.watch_manager.tracker.cancel(run.run_id, **owner)
            except ValueError:
                # Remote non-cancellable execution remains occupied until terminal.
                return
            record = await self.watch_manager.tracker.get(run.run_id, **owner)
            await self.watch_manager.settle_record(run, record)
