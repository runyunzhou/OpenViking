# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Contracts shared by distributed Watch storage, management and scheduling."""

from dataclasses import dataclass
from typing import Any, Optional, Protocol

from openviking.resource.processing_mode import DEFAULT_PROCESSING_MODE
from openviking.resource.watch_manager import WatchTask
from openviking.scheduling.provider import ScheduleClaim

MUTABLE_FIELDS = frozenset(
    {
        "path", "source_type", "reason", "instruction", "watch_interval", "build_index",
        "summarize", "processing_mode", "processor_kwargs", "auth_state",
        "connector_states", "is_active",
    }
)
PAYLOAD_FIELDS = MUTABLE_FIELDS - {
    "watch_interval", "is_active", "auth_state", "connector_states",
}


@dataclass(frozen=True)
class WatchAuthority:
    account_id: str = "default"
    user_id: str = "default"
    role: str = "root"

    @property
    def root(self) -> bool:
        return self.role.lower() == "root"


@dataclass(frozen=True)
class WatchProjection:
    """Small timer projection; no credentials or request payload."""

    task_id: str
    account_id: str
    user_id: str
    enabled: bool
    deleted: bool
    watch_interval: float
    next_run_at: Optional[float]
    current_run_id: Optional[str]
    revision: int
    timer_revision: int
    now: float


@dataclass(frozen=True)
class WatchPatch:
    """Explicit field updates; omitted fields are never replaced."""

    values: dict[str, Any]

    def normalized(self) -> dict[str, Any]:
        unknown = self.values.keys() - MUTABLE_FIELDS
        if unknown:
            raise ValueError(f"Unsupported Watch update fields: {sorted(unknown)}")
        values = dict(self.values)
        if "path" in values and not values["path"]:
            raise ValueError("path cannot be cleared")
        if "watch_interval" in values:
            interval = values["watch_interval"]
            if interval is None or interval <= 0:
                raise ValueError("watch_interval must be > 0")
        for name in ("auth_state", "connector_states", "processor_kwargs"):
            if name in values and values[name] is not None and not isinstance(values[name], dict):
                raise ValueError(f"{name} must be an object")
        defaults = {
            "reason": "", "instruction": "", "build_index": False, "summarize": False,
            "processing_mode": DEFAULT_PROCESSING_MODE, "processor_kwargs": {},
        }
        for name, default in defaults.items():
            if name in values and values[name] is None:
                values[name] = default
        return values


@dataclass(frozen=True)
class WatchState:
    """One authoritative row; a run remains occupied even when paused/deleted."""

    task: WatchTask
    current_run_id: Optional[str]
    run_started_at: Optional[float]
    submitted: bool
    deleted: bool
    revision: int
    now: float
    lease_owner: Optional[str] = None
    lease_until: float = 0.0
    initial_run: bool = False

    @property
    def enabled(self) -> bool:
        return self.task.is_active and not self.deleted

    @property
    def next_run_at(self) -> Optional[float]:
        value = self.task.next_execution_time
        return value.timestamp() if value else None

    def claim(self) -> ScheduleClaim:
        if not self.current_run_id:
            raise ValueError("Watch has no current run")
        return ScheduleClaim(
            schedule_kind="watch", schedule_id=self.task.task_id,
            account_id=self.task.account_id, run_id=self.current_run_id,
            revision=self.revision, payload=self.task,
        )


@dataclass(frozen=True)
class WatchRunResult:
    """Fields atomically settled by matching the occupying run ID."""

    status: str
    execution_task_id: Optional[str] = None
    error: Optional[str] = None
    deactivate: bool = False
    connector_states: Optional[dict[str, Any]] = None
    has_connector_states: bool = False
    started_at: Optional[float] = None
    retry_immediately: bool = False

    def __post_init__(self) -> None:
        if self.status not in {"completed", "failed", "cancelled", "error"}:
            raise ValueError("Watch run result must be terminal")
        if self.has_connector_states and not isinstance(self.connector_states, dict):
            raise ValueError("connector_states must be an object")


class WatchTimerAdapter(Protocol):
    async def sync(self, state: WatchProjection) -> None: ...

    async def arm(self, task: WatchTask, due_at: float) -> None: ...

    async def arm_observer(self, task: WatchTask, run_id: str) -> None: ...
