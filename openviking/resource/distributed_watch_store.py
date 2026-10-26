# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""MySQL authority for distributed Watch operations (including MySQL 5.7).

The account gate serializes only short registration transactions. Business
execution is represented by current_run_id, never by an expiring lock.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncIterator, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from openviking.resource.distributed_watch_types import (
    PAYLOAD_FIELDS,
    WatchAuthority,
    WatchPatch,
    WatchProjection,
    WatchRunResult,
    WatchState,
)
from openviking.resource.uri_mutation_coordinator import uri_matches_prefix
from openviking.resource.watch_manager import PermissionDeniedError, WatchTask
from openviking.scheduling.provider import ScheduleClaim
from openviking.storage.mysql_runtime import (
    MySQLRuntime,
    get_shared_mysql_runtime,
    mysql_runtime_store,
    wait_mysql_cleanup,
)
from openviking_cli.exceptions import ConflictError
from openviking_cli.utils.config.config_utils import normalize_service_name

DB_NOW = "UNIX_TIMESTAMP(NOW(6))"
SUBMIT_LEASE_SECONDS = 30 * 60.0
MUTATION_LEASE_SECONDS = 120.0


def _json(value: Any) -> Optional[str]:
    return json.dumps(value, ensure_ascii=False) if value is not None else None


def _decoded(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _epoch(value: Optional[datetime]) -> Optional[float]:
    return value.timestamp() if value else None


def _hash(value: Optional[str]) -> str:
    return hashlib.sha256((value or "").encode()).hexdigest()


def _prefix_sql(column: str, prefix: str, params: dict[str, Any], name: str) -> str:
    """LIKE escapes %, _ and backslash; equality keeps the path boundary."""
    params[name] = prefix.rstrip("/")
    escaped = params[name].replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    params[name + "_children"] = escaped + "/%"
    return f"({column}=:{name} OR {column} LIKE :{name}_children)"


@mysql_runtime_store
class MySQLWatchStore:
    def __init__(
        self, *, service_name: str, params: Optional[dict[str, Any]] = None,
        runtime: Optional[MySQLRuntime] = None,
    ) -> None:
        from openviking.service.task_store_mysql import MySQLTaskWorkStore
        from openviking.service.task_store_mysql_schema import validate_table_prefix

        values = dict(params or {})
        self._service_name = normalize_service_name(service_name)
        self._table = validate_table_prefix(values.pop("table", "ov_watch_tasks"))
        self._accounts = validate_table_prefix(self._table + "_uri_accounts")
        self._mutations = validate_table_prefix(self._table + "_uri_mutations")
        self._url, self._engine_options = MySQLTaskWorkStore._connection_options(values)
        self._mysql_runtime = runtime or get_shared_mysql_runtime()
        self._engine: Any = None
        self._sessions: Any = None
        self._mysql_lease = False

    async def initialize(self) -> None:
        if self._engine is not None:
            return
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        from openviking.storage.mysql_session import MySQLAsyncSession

        await self._mysql_runtime.acquire()
        self._mysql_lease = True
        engine = None
        try:
            engine = create_async_engine(self._url, **self._engine_options)
            sessions = async_sessionmaker(
                engine, class_=MySQLAsyncSession, expire_on_commit=False,
            )
            async with sessions() as session:
                await session.execute(text(
                    f"SELECT deleted, run_started_at, initial_run, timer_revision "
                    f"FROM `{self._table}` LIMIT 1"
                ))
                await session.execute(text(f"SELECT account_hash FROM `{self._accounts}` LIMIT 1"))
                await session.execute(text(f"SELECT token FROM `{self._mutations}` LIMIT 1"))
        except BaseException:
            async def cleanup() -> None:
                if engine is not None:
                    await engine.dispose()
                self._mysql_lease = False
                await self._mysql_runtime.release()
            await wait_mysql_cleanup(cleanup())
            raise
        self._engine, self._sessions = engine, sessions

    async def close(self) -> None:
        async def cleanup() -> None:
            if self._engine is not None:
                await self._engine.dispose()
            self._engine = self._sessions = None
            if self._mysql_lease:
                self._mysql_lease = False
                await self._mysql_runtime.release()
        await wait_mysql_cleanup(cleanup())

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[Any]:
        if self._sessions is None:
            raise RuntimeError("MySQLWatchStore must be initialized")
        async with self._sessions() as session:
            async with session.begin():
                yield session

    def _params(self, **values: Any) -> dict[str, Any]:
        return {"service_name": self._service_name, **values}

    async def _execute(self, session: Any, sql: str, **params: Any) -> Any:
        return await session.execute(text(sql), self._params(**params))

    @staticmethod
    def _authority(authority: WatchAuthority, params: dict[str, Any]) -> str:
        if authority.root:
            return "1=1"
        params.update(auth_account=authority.account_id, auth_user=authority.user_id)
        return "account_id=:auth_account AND user_id=:auth_user"

    async def _check_write_failure(self, session: Any, task_id: str) -> None:
        row = (await self._execute(
            session,
            f"SELECT task_id FROM `{self._table}` "
            "WHERE service_name=:service_name AND task_id=:task_id AND deleted=0",
            task_id=task_id,
        )).first()
        if row:
            raise PermissionDeniedError(f"No permission to update Watch {task_id}")

    @staticmethod
    def _task(row: Any) -> WatchTask:
        data = dict(_decoded(row["payload"]))
        for name in ("task_id", "account_id", "user_id", "original_role"):
            data[name] = row[name]
        data["to_uri"] = row["target_uri"]
        data["is_active"] = bool(row["enabled"])
        data["watch_interval"] = float(row["watch_interval"])
        for column, field in (
            ("created_at", "created_at"), ("next_run_at", "next_execution_time"),
            ("last_execution_time", "last_execution_time"),
        ):
            data[field] = datetime.fromtimestamp(row[column]) if row[column] is not None else None
        for name in ("last_task_id", "last_status", "last_error"):
            data[name] = row[name]
        for name in ("auth_state", "connector_states"):
            if name in row:
                data[name] = _decoded(row[name])
        return WatchTask.from_dict(data)

    @classmethod
    def _state(cls, row: Any) -> WatchState:
        return WatchState(
            task=cls._task(row), current_run_id=row["current_run_id"],
            run_started_at=row["run_started_at"], submitted=bool(row["submitted"]),
            deleted=bool(row["deleted"]), revision=int(row["revision"]), now=float(row["db_now"]),
            lease_owner=row["lease_owner"], lease_until=float(row["lease_until"]),
            initial_run=bool(row["initial_run"]),
        )

    _PUBLIC_COLUMNS = (
        "task_id, account_id, user_id, original_role, target_uri, enabled, watch_interval, "
        "created_at, next_run_at, last_execution_time, last_task_id, last_status, last_error, payload"
    )
    _STATE_COLUMNS = (
        _PUBLIC_COLUMNS + ", auth_state, connector_states, current_run_id, run_started_at, "
        "submitted, deleted, revision, lease_owner, lease_until, initial_run, "
        + DB_NOW + " AS db_now"
    )
    _PROJECTION_COLUMNS = (
        "task_id, account_id, user_id, enabled, deleted, watch_interval, next_run_at, "
        "current_run_id, revision, timer_revision, " + DB_NOW + " AS db_now"
    )

    @staticmethod
    def _projection(row: Any) -> WatchProjection:
        return WatchProjection(
            task_id=row["task_id"], account_id=row["account_id"], user_id=row["user_id"],
            enabled=bool(row["enabled"]), deleted=bool(row["deleted"]),
            watch_interval=float(row["watch_interval"]), next_run_at=row["next_run_at"],
            current_run_id=row["current_run_id"], revision=int(row["revision"]),
            timer_revision=int(row["timer_revision"]), now=float(row["db_now"]),
        )

    async def get_state(self, task_id: str) -> Optional[WatchState]:
        async with self._transaction() as session:
            row = (await self._execute(
                session, f"SELECT {self._STATE_COLUMNS} FROM `{self._table}` "
                "WHERE service_name=:service_name AND task_id=:task_id", task_id=task_id,
            )).mappings().first()
            return self._state(row) if row else None

    async def get_projection(self, task_id: str) -> Optional[WatchProjection]:
        async with self._transaction() as session:
            row = (await self._execute(
                session, f"SELECT {self._PROJECTION_COLUMNS} FROM `{self._table}` "
                "WHERE service_name=:service_name AND task_id=:task_id", task_id=task_id,
            )).mappings().first()
            return self._projection(row) if row else None

    async def get_task(
        self, task_id: str, authority: WatchAuthority = WatchAuthority(),
        *, private: bool = True,
    ) -> Optional[WatchTask]:
        params: dict[str, Any] = {"task_id": task_id}
        permission = self._authority(authority, params)
        columns = self._PUBLIC_COLUMNS + (", auth_state, connector_states" if private else "")
        async with self._transaction() as session:
            row = (await self._execute(
                session, f"SELECT {columns} FROM `{self._table}` "
                f"WHERE service_name=:service_name AND task_id=:task_id AND deleted=0 AND {permission}",
                **params,
            )).mappings().first()
            return self._task(row) if row else None

    async def list_tasks(
        self, authority: WatchAuthority, *, active_only: bool = False,
        scope_account_id: Optional[str] = None, offset: int = 0, limit: Optional[int] = None,
        target_uri: Optional[str] = None,
    ) -> tuple[list[WatchTask], int]:
        params: dict[str, Any] = {}
        clauses = ["service_name=:service_name", "deleted=0", self._authority(authority, params)]
        if active_only:
            clauses.append("enabled=1")
        if scope_account_id:
            clauses.append("account_id=:scope_account")
            params["scope_account"] = scope_account_id
        if target_uri is not None:
            clauses.extend(["target_uri_hash=:target_hash", "target_uri=:target_uri"])
            params.update(target_hash=_hash(target_uri), target_uri=target_uri)
        where = " AND ".join(clauses)
        paging = ""
        if limit is not None:
            if offset < 0 or limit < 0:
                raise ValueError("Pagination must be nonnegative")
            paging = " LIMIT :page_limit OFFSET :page_offset"
            params.update(page_limit=limit, page_offset=offset)
        async with self._transaction() as session:
            total = (await self._execute(
                session, f"SELECT COUNT(*) FROM `{self._table}` WHERE {where}", **params,
            )).scalar_one()
            rows = (await self._execute(
                session, f"SELECT {self._PUBLIC_COLUMNS} FROM `{self._table}` WHERE {where} "
                f"ORDER BY created_at DESC, task_id DESC{paging}", **params,
            )).mappings().all()
            return [self._task(row) for row in rows], int(total)

    async def _gate(self, session: Any, account_id: str) -> None:
        params = {"account_hash": _hash(account_id)}
        await self._execute(
            session, f"INSERT IGNORE INTO `{self._accounts}` (service_name, account_hash) "
            "VALUES (:service_name, :account_hash)", **params,
        )
        await self._execute(
            session, f"SELECT account_hash FROM `{self._accounts}` "
            "WHERE service_name=:service_name AND account_hash=:account_hash FOR UPDATE", **params,
        )

    async def _has_mutation(self, session: Any, account_id: str, uris: Sequence[str]) -> bool:
        # claim() reads the account before waiting for the account gate. Under
        # REPEATABLE READ its snapshot can predate a mutation's commit, so this
        # must be a current read after acquiring that gate.
        rows = (await self._execute(
            session, f"SELECT uri FROM `{self._mutations}` WHERE service_name=:service_name "
            f"AND account_hash=:account_hash AND expires_at>{DB_NOW} FOR UPDATE",
            account_hash=_hash(account_id),
        )).scalars().all()
        return any(
            uri_matches_prefix(left, right) or uri_matches_prefix(right, left)
            for left in uris for right in rows
        )

    async def create_task(
        self, task: WatchTask, *, initial_run_id: Optional[str] = None,
    ) -> bool:
        """False means a mutation owns the scope; the manager retries outside SQL."""
        from openviking.connector.delegate import ConnectorDelegate

        payload = self._payload(task)
        async with self._transaction() as session:
            await self._gate(session, task.account_id)
            if task.to_uri and await self._has_mutation(session, task.account_id, [task.to_uri]):
                return False
            others = (await self._execute(
                session, f"SELECT task_id, auth_state FROM `{self._table}` "
                "WHERE service_name=:service_name AND account_id=:account_id AND deleted=0 "
                "AND target_uri_hash=:target_hash AND target_uri <=> :target_uri",
                account_id=task.account_id, target_hash=_hash(task.to_uri), target_uri=task.to_uri,
            )).mappings().all()
            if others and not (
                ConnectorDelegate.is_watch_auth_state(task.auth_state)
                and all(ConnectorDelegate.is_watch_auth_state(_decoded(r["auth_state"])) for r in others)
            ):
                raise ConflictError(f"Target URI '{task.to_uri}' has an incompatible Watch", resource=task.to_uri)
            try:
                await self._execute(
                    session, f"INSERT INTO `{self._table}` (service_name, task_id, account_id, "
                    "user_id, original_role, target_uri_hash, target_uri_prefix, target_uri, "
                    "enabled, watch_interval, next_run_at, current_run_id, run_started_at, initial_run, "
                    "lease_until, payload, auth_state, connector_states, created_at, updated_at) "
                    "VALUES (:service_name, :task_id, :account_id, :user_id, :original_role, "
                    ":target_hash, :target_prefix, :target_uri, :enabled, :interval, :next_at, "
                    f":run_id, IF(:run_id IS NULL, NULL, {DB_NOW}), :initial_run, "
                    f"IF(:run_id IS NULL, 0, {DB_NOW}+:submit_lease), :payload, :auth_state, "
                    f":connector_states, :created_at, {DB_NOW})",
                    task_id=task.task_id, account_id=task.account_id, user_id=task.user_id,
                    original_role=task.original_role, target_hash=_hash(task.to_uri),
                    target_prefix=(task.to_uri or "")[:500], target_uri=task.to_uri,
                    enabled=task.is_active, interval=task.watch_interval,
                    next_at=_epoch(task.next_execution_time), run_id=initial_run_id,
                    initial_run=initial_run_id is not None, submit_lease=SUBMIT_LEASE_SECONDS,
                    payload=_json(payload), auth_state=_json(task.auth_state),
                    connector_states=_json(task.connector_states), created_at=_epoch(task.created_at),
                )
            except IntegrityError as exc:
                raise ConflictError(f"Watch '{task.task_id}' already exists", resource=task.task_id) from exc
        return True

    @staticmethod
    def _payload(task: WatchTask) -> dict[str, Any]:
        payload = task.to_storage_dict()
        for name in (
            "task_id", "account_id", "user_id", "original_role", "to_uri",
            "is_active", "watch_interval", "created_at", "next_execution_time",
            "last_execution_time", "last_task_id", "last_status", "last_error",
            "auth_state", "connector_states",
        ):
            payload.pop(name, None)
        return payload

    async def patch_task(
        self, task_id: str, patch: WatchPatch, authority: WatchAuthority = WatchAuthority(),
        *, run_id: Optional[str] = None,
    ) -> Optional[WatchTask]:
        values = patch.normalized()
        params: dict[str, Any] = {"task_id": task_id}
        permission = self._authority(authority, params)
        sets: list[str] = []
        payload = "payload"
        for name in PAYLOAD_FIELDS & values.keys():
            params[name] = _json(values[name])
            payload = f"JSON_SET({payload}, '$.{name}', CAST(:{name} AS JSON))"
        if payload != "payload":
            sets.append(f"payload={payload}")
        for name in ("auth_state", "connector_states"):
            if name in values:
                params[name] = _json(values[name])
                sets.append(f"{name}=:{name}")
        active = "is_active" in values
        interval = "watch_interval" in values
        if active:
            params["enabled"] = bool(values["is_active"])
        if interval:
            params["interval"] = values["watch_interval"]
        # Evaluate next_run_at before updating enabled / interval: MySQL SET
        # evaluates from left to right, rather than from the original row.
        if active or interval:
            enabled_expr = ":enabled" if active else "enabled"
            interval_expr = ":interval" if interval else "watch_interval"
            next_expr = f"COALESCE(last_execution_time, created_at)+{interval_expr}*60"
            if not interval:
                next_expr = f"COALESCE(next_run_at, {next_expr})"
            sets.append(f"next_run_at=IF({enabled_expr}, {next_expr}, NULL)")
        if active:
            sets.append("enabled=:enabled")
        if interval:
            sets.append("watch_interval=:interval")
        where = f"service_name=:service_name AND task_id=:task_id AND {permission}"
        where += " AND deleted=0" if run_id is None else " AND current_run_id=:run_id"
        if run_id is not None:
            params["run_id"] = run_id
        async with self._transaction() as session:
            if sets:
                sets.extend(["revision=revision+1", f"updated_at={DB_NOW}"])
                changed = (await self._execute(
                    session, f"UPDATE `{self._table}` SET {', '.join(sets)} WHERE {where}", **params,
                )).rowcount
                if not changed:
                    if run_id is None:
                        await self._check_write_failure(session, task_id)
                    return None
            row = (await self._execute(
                session, f"SELECT {self._PUBLIC_COLUMNS}, auth_state, connector_states "
                f"FROM `{self._table}` WHERE {where}", **params,
            )).mappings().first()
            return self._task(row) if row else None

    async def delete_task(self, task_id: str, authority: WatchAuthority) -> bool:
        params: dict[str, Any] = {"task_id": task_id}
        permission = self._authority(authority, params)
        async with self._transaction() as session:
            changed = (await self._execute(
                session, f"UPDATE `{self._table}` "
                f"SET deleted=1, enabled=0, next_run_at=NULL, revision=revision+1, updated_at={DB_NOW} "
                f"WHERE service_name=:service_name AND task_id=:task_id AND deleted=0 AND {permission}",
                **params,
            )).rowcount
            if not changed:
                await self._check_write_failure(session, task_id)
            return bool(changed)

    async def delete_tasks(self, account_id: str, user_id: Optional[str] = None) -> list[WatchState]:
        params: dict[str, Any] = {"account_id": account_id}
        # Identity deletion is resumable. Previously tombstoned runs must still
        # be returned so their Tasks settle before the deletion removes Tasks.
        where = "service_name=:service_name AND account_id=:account_id"
        if user_id is not None:
            where += " AND user_id=:user_id"
            params["user_id"] = user_id
        async with self._transaction() as session:
            rows = (await self._execute(
                session, f"SELECT {self._STATE_COLUMNS} FROM `{self._table}` "
                f"WHERE {where} ORDER BY task_id FOR UPDATE", **params,
            )).mappings().all()
            await self._execute(
                session, f"UPDATE `{self._table}` SET deleted=1, enabled=0, next_run_at=NULL, "
                f"revision=revision+1, updated_at={DB_NOW} WHERE {where}", **params,
            )
            return [self._state(row) for row in rows]

    async def claim(
        self, task_id: str, *, owner: str, manual: bool = False,
    ) -> Optional[ScheduleClaim]:
        """Register occupancy while holding the same gate as URI mutations."""
        async with self._transaction() as session:
            account = (await self._execute(
                session, f"SELECT account_id FROM `{self._table}` "
                "WHERE service_name=:service_name AND task_id=:task_id AND deleted=0",
                task_id=task_id,
            )).scalar_one_or_none()
            if account is None:
                return None
            await self._gate(session, account)
            row = (await self._execute(
                session, f"SELECT {self._STATE_COLUMNS} FROM `{self._table}` "
                "WHERE service_name=:service_name AND task_id=:task_id FOR UPDATE", task_id=task_id,
            )).mappings().first()
            if not row or row["deleted"] or row["current_run_id"]:
                return None
            if not manual and (
                not row["enabled"] or row["next_run_at"] is None or row["next_run_at"] > row["db_now"]
            ):
                return None
            uri = row["target_uri"]
            if uri and await self._has_mutation(session, account, [uri]):
                return None
            run_id = str(uuid.uuid4())
            await self._execute(
                session, f"UPDATE `{self._table}` SET current_run_id=:run_id, submitted=0, "
                f"lease_owner=:owner, lease_until={DB_NOW}+:submit_lease, run_started_at={DB_NOW}, "
                f"initial_run=0, revision=revision+1, updated_at={DB_NOW} "
                "WHERE service_name=:service_name AND task_id=:task_id",
                task_id=task_id, run_id=run_id, owner=owner, submit_lease=SUBMIT_LEASE_SECONDS,
            )
            return ScheduleClaim(
                schedule_kind="watch", schedule_id=task_id, account_id=account,
                run_id=run_id, revision=int(row["revision"]) + 1, payload=self._task(row),
            )

    async def acquire_submission(self, task_id: str, run_id: str, *, owner: str) -> bool:
        """Recover an unsubmitted run without ever clearing its URI occupancy."""
        async with self._transaction() as session:
            return bool((await self._execute(
                session, f"UPDATE `{self._table}` SET lease_owner=:owner, "
                f"lease_until={DB_NOW}+:submit_lease WHERE service_name=:service_name "
                "AND task_id=:task_id AND current_run_id=:run_id AND submitted=0 "
                f"AND lease_until<{DB_NOW}",
                task_id=task_id, run_id=run_id, owner=owner, submit_lease=SUBMIT_LEASE_SECONDS,
            )).rowcount)

    async def renew_submission(self, claim: ScheduleClaim, owner: str) -> bool:
        async with self._transaction() as session:
            return bool((await self._execute(
                session, f"UPDATE `{self._table}` SET lease_until={DB_NOW}+:submit_lease "
                "WHERE service_name=:service_name AND task_id=:task_id AND current_run_id=:run_id "
                "AND lease_owner=:owner AND submitted=0",
                task_id=claim.schedule_id, run_id=claim.run_id,
                owner=owner, submit_lease=SUBMIT_LEASE_SECONDS,
            )).rowcount)

    async def release_submission(self, claim: ScheduleClaim, owner: str) -> None:
        """A stopped submitter permits retry, but does not release occupancy."""
        async with self._transaction() as session:
            await self._execute(
                session, f"UPDATE `{self._table}` SET lease_until=0 "
                "WHERE service_name=:service_name AND task_id=:task_id AND current_run_id=:run_id "
                "AND lease_owner=:owner AND submitted=0",
                task_id=claim.schedule_id, run_id=claim.run_id, owner=owner,
            )

    async def mark_submitted(self, claim: ScheduleClaim) -> bool:
        async with self._transaction() as session:
            return bool((await self._execute(
                session, f"UPDATE `{self._table}` SET submitted=1, lease_owner=NULL, lease_until=0, "
                f"revision=revision+1, updated_at={DB_NOW} "
                "WHERE service_name=:service_name AND task_id=:task_id AND current_run_id=:run_id",
                task_id=claim.schedule_id, run_id=claim.run_id,
            )).rowcount)

    async def finish_run(self, claim: ScheduleClaim, result: WatchRunResult) -> bool:
        params: dict[str, Any] = {
            "task_id": claim.schedule_id, "run_id": claim.run_id,
            "status": result.status, "last_task_id": result.execution_task_id,
            "error": result.error, "deactivate": result.deactivate,
            "started_at": result.started_at, "retry": result.retry_immediately,
        }
        sets = [
            "next_run_at=IF(deleted=0 AND enabled=1 AND :deactivate=0, "
            f"IF(:retry, {DB_NOW}, COALESCE(:started_at, {DB_NOW})+watch_interval*60), NULL)",
            f"last_execution_time=COALESCE(:started_at, {DB_NOW})",
            "last_task_id=:last_task_id", "last_status=:status", "last_error=:error",
            "enabled=IF(:deactivate, 0, enabled)",
            "current_run_id=NULL", "submitted=0", "run_started_at=NULL", "initial_run=0",
            "lease_owner=NULL", "lease_until=0", "revision=revision+1", f"updated_at={DB_NOW}",
        ]
        if result.has_connector_states:
            sets.append("connector_states=:connector_states")
            params["connector_states"] = _json(result.connector_states)
        async with self._transaction() as session:
            return bool((await self._execute(
                session, f"UPDATE `{self._table}` SET {', '.join(sets)} "
                "WHERE service_name=:service_name AND task_id=:task_id AND current_run_id=:run_id",
                **params,
            )).rowcount)

    async def timer_candidates(
        self, *, after: str = "", limit: int = 100, dirty_only: bool = True,
    ) -> list[WatchProjection]:
        """Registration repair only; this operation never claims due work."""
        async with self._transaction() as session:
            rows = (await self._execute(
                session, f"SELECT {self._PROJECTION_COLUMNS} FROM `{self._table}` "
                "WHERE service_name=:service_name AND task_id>:after "
                + ("AND timer_revision<>revision " if dirty_only else "")
                + "ORDER BY task_id LIMIT :page_limit", after=after, page_limit=limit,
            )).mappings().all()
            return [self._projection(row) for row in rows]

    async def acknowledge_timer(self, task_id: str, revision: int) -> bool:
        async with self._transaction() as session:
            changed = (await self._execute(
                session, f"UPDATE `{self._table}` SET timer_revision=:revision "
                "WHERE service_name=:service_name AND task_id=:task_id AND revision=:revision",
                task_id=task_id, revision=revision,
            )).rowcount
            if changed:
                await self._execute(
                    session, f"DELETE FROM `{self._table}` WHERE service_name=:service_name "
                    "AND task_id=:task_id AND deleted=1 AND current_run_id IS NULL "
                    "AND timer_revision=revision", task_id=task_id,
                )
            return bool(changed)

    async def acquire_mutation(self, account_id: str, uris: Sequence[str], token: str) -> bool:
        uris = sorted({uri.rstrip("/") for uri in uris if uri})
        if not uris:
            return True
        async with self._transaction() as session:
            await self._gate(session, account_id)
            if await self._has_mutation(session, account_id, uris):
                return False
            params: dict[str, Any] = {"account_id": account_id}
            overlaps = []
            for i, uri in enumerate(uris):
                child = _prefix_sql("target_uri", uri, params, f"uri_{i}")
                # An ancestor is any prefix ending at a slash, plus equality.
                # Bind exact ancestors instead of LIKE on a column-derived pattern.
                ancestors = [uri[:j] for j, ch in enumerate(uri) if ch == "/" and j > len("viking://")]
                placeholders = []
                for j, ancestor in enumerate(ancestors):
                    name = f"ancestor_{i}_{j}"
                    params[name] = ancestor
                    placeholders.append(":" + name)
                overlaps.append(child + (f" OR target_uri IN ({','.join(placeholders)})" if placeholders else ""))
            busy = (await self._execute(
                session, f"SELECT task_id FROM `{self._table}` "
                "WHERE service_name=:service_name AND account_id=:account_id "
                f"AND current_run_id IS NOT NULL AND ({' OR '.join(overlaps)}) LIMIT 1", **params,
            )).first()
            if busy:
                return False
            await self._execute(
                session, f"DELETE FROM `{self._mutations}` WHERE service_name=:service_name "
                f"AND account_hash=:account_hash AND expires_at<={DB_NOW}", account_hash=_hash(account_id),
            )
            for i, uri in enumerate(uris):
                await self._execute(
                    session, f"INSERT INTO `{self._mutations}` "
                    "(service_name, account_hash, token, scope_index, uri, expires_at) "
                    f"VALUES (:service_name, :account_hash, :token, :scope_index, :uri, {DB_NOW}+:ttl)",
                    account_hash=_hash(account_id), token=token, scope_index=i, uri=uri,
                    ttl=MUTATION_LEASE_SECONDS,
                )
            return True

    async def renew_mutation(self, token: str) -> bool:
        async with self._transaction() as session:
            return bool((await self._execute(
                session, f"UPDATE `{self._mutations}` SET expires_at={DB_NOW}+:ttl "
                f"WHERE service_name=:service_name AND token=:token AND expires_at>{DB_NOW}",
                token=token, ttl=MUTATION_LEASE_SECONDS,
            )).rowcount)

    async def release_mutation(self, token: str) -> None:
        async with self._transaction() as session:
            await self._execute(
                session, f"DELETE FROM `{self._mutations}` "
                "WHERE service_name=:service_name AND token=:token", token=token,
            )

    async def _validate_rewrite(
        self, session: Any, account_id: str, old_uri: str, new_uri: str,
    ) -> None:
        params: dict[str, Any] = {"account_id": account_id, "new_uri": new_uri.rstrip("/")}
        source = _prefix_sql("m.target_uri", old_uri, params, "old_uri")
        destination_is_source = _prefix_sql("o.target_uri", old_uri, params, "old_uri")
        landing = "CONCAT(:new_uri, SUBSTRING(m.target_uri, CHAR_LENGTH(:old_uri)+1))"
        connector = (
            "COALESCE(JSON_UNQUOTE(JSON_EXTRACT({alias}.auth_state, '$.provider')), '') "
            "IN ('connector_encrypted', 'connector_plaintext')"
        )
        row = (await self._execute(
            session, f"SELECT m.task_id FROM `{self._table}` m JOIN `{self._table}` o "
            "ON o.service_name=m.service_name AND o.account_id=m.account_id AND o.deleted=0 "
            f"AND o.target_uri_hash=SHA2({landing},256) AND o.target_uri={landing} "
            "WHERE m.service_name=:service_name AND m.account_id=:account_id AND m.deleted=0 "
            f"AND {source} AND NOT {destination_is_source} AND NOT "
            f"({connector.format(alias='m')} AND {connector.format(alias='o')}) LIMIT 1", **params,
        )).first()
        if row:
            raise ConflictError(f"Target URI '{new_uri}' has an incompatible Watch", resource=new_uri)

    async def validate_target_prefix_rewrite(
        self, account_id: str, old_uri: str, new_uri: str,
    ) -> None:
        async with self._transaction() as session:
            await self._gate(session, account_id)
            await self._validate_rewrite(session, account_id, old_uri, new_uri)

    async def rewrite_target_prefix(
        self, account_id: str, old_uri: str, new_uri: str,
    ) -> list[WatchTask]:
        params: dict[str, Any] = {"account_id": account_id, "new_uri": new_uri.rstrip("/")}
        scope = _prefix_sql("target_uri", old_uri, params, "old_uri")
        landing = "CONCAT(:new_uri, SUBSTRING(target_uri, CHAR_LENGTH(:old_uri)+1))"
        old_parent = "LEFT(target_uri, CHAR_LENGTH(target_uri)-CHAR_LENGTH(SUBSTRING_INDEX(target_uri,'/',-1))-1)"
        new_parent = f"LEFT({landing}, CHAR_LENGTH({landing})-CHAR_LENGTH(SUBSTRING_INDEX({landing},'/',-1))-1)"
        async with self._transaction() as session:
            await self._gate(session, account_id)
            await self._validate_rewrite(session, account_id, old_uri, new_uri)
            # Capture IDs, not payloads, to return precisely the changed rows.
            ids = (await self._execute(
                session, f"SELECT task_id FROM `{self._table}` "
                f"WHERE service_name=:service_name AND account_id=:account_id AND deleted=0 AND {scope}",
                **params,
            )).scalars().all()
            if not ids:
                return []
            await self._execute(
                session, f"UPDATE `{self._table}` SET "
                "payload=IF(JSON_UNQUOTE(JSON_EXTRACT(payload,'$.parent_uri'))="
                f"{old_parent}, JSON_SET(payload,'$.parent_uri',{new_parent}),payload), "
                f"target_uri_hash=SHA2({landing},256), target_uri_prefix=LEFT({landing},500), "
                f"target_uri={landing}, revision=revision+1, updated_at={DB_NOW} "
                f"WHERE service_name=:service_name AND account_id=:account_id AND deleted=0 AND {scope}",
                **params,
            )
            names = []
            for i, task_id in enumerate(ids):
                params[f"id_{i}"] = task_id
                names.append(f":id_{i}")
            rows = (await self._execute(
                session, f"SELECT {self._PUBLIC_COLUMNS} FROM `{self._table}` "
                f"WHERE service_name=:service_name AND task_id IN ({','.join(names)})", **params,
            )).mappings().all()
            return [self._task(row) for row in rows]

    async def deactivate_under_uri(self, account_id: str, uri: str) -> list[WatchTask]:
        params: dict[str, Any] = {"account_id": account_id}
        scope = _prefix_sql("target_uri", uri, params, "uri")
        where = f"service_name=:service_name AND account_id=:account_id AND deleted=0 AND {scope}"
        async with self._transaction() as session:
            await self._execute(
                session, f"UPDATE `{self._table}` SET enabled=0, next_run_at=NULL, "
                f"revision=revision+1, updated_at={DB_NOW} WHERE {where}", **params,
            )
            rows = (await self._execute(
                session, f"SELECT {self._PUBLIC_COLUMNS} FROM `{self._table}` WHERE {where}", **params,
            )).mappings().all()
            return [self._task(row) for row in rows]
