# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Shared contracts for durable schedule producers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Protocol

from openviking.service.task_domain import TaskRecord


@dataclass(frozen=True)
class ScheduleClaim:
    """A short-lived right to submit one scheduled business run."""

    schedule_kind: str
    schedule_id: str
    account_id: str
    run_id: str
    revision: int
    payload: Any = None


class ScheduleProvider(Protocol):
    """Storage-owned schedule state and claim transitions.

    A claim lease covers only business Task submission. Long-running execution
    is owned by the Task/Work layer. The provider settles its schedule through
    :meth:`finish_run`, committing the result and releasing the run together.
    """

    async def claim_due(
        self,
        *,
        owner: str,
        now: Optional[float] = None,
        limit: int = 1,
        lease_seconds: float = 60.0,
    ) -> list[ScheduleClaim]: ...

    async def mark_submitted(self, claim: ScheduleClaim) -> bool: ...

    async def finish_run(self, claim: ScheduleClaim, result: Any) -> bool:
        """Atomically commit a provider-specific result if the run is current."""
        ...

    async def trigger_now(self, *, account_id: str, subject_id: str) -> bool: ...


class ScheduleHandler(Protocol):
    """Business adapter for one schedule kind."""

    async def submit(self, claim: ScheduleClaim) -> Optional[str]: ...

    async def on_terminal(
        self,
        claim: ScheduleClaim,
        task: TaskRecord,
    ) -> Any:
        """Build a provider-specific result for finish_run without persisting it."""
        ...
