#!/usr/bin/env python3
"""Migrate legacy .watch_tasks.json data into the MySQL Watch store."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import time
from datetime import datetime
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from openviking.resource.distributed_watch_store import MySQLWatchStore
from openviking.resource.watch_manager import WatchTask
from openviking.service.task_store_mysql import MySQLTaskWorkStore
from openviking.service.task_store_mysql_schema import validate_table_prefix
from openviking_cli.utils.config.config_utils import DEFAULT_SERVICE_NAME, normalize_service_name


def _dt_to_epoch(value: datetime | None) -> float | None:
    return value.timestamp() if value is not None else None


def _target_hash(to_uri: str | None) -> str:
    return hashlib.sha256((to_uri or "").encode()).hexdigest()


def _target_prefix(to_uri: str | None) -> str:
    return (to_uri or "")[:500]


def _load_tasks(path: Path) -> list[WatchTask]:
    data = json.loads(path.read_text())
    if not isinstance(data, dict):
        raise ValueError("input JSON must be an object with a tasks array")
    tasks = data.get("tasks", [])
    if not isinstance(tasks, list):
        raise ValueError("input JSON field 'tasks' must be an array")
    return [WatchTask.from_dict(item) for item in tasks if isinstance(item, dict)]


async def _run(args: argparse.Namespace) -> None:
    table = validate_table_prefix(args.table)
    service_name = normalize_service_name(args.service_name)
    params: dict[str, object] = {
        "user": args.user,
        "password": args.password,
        "database": args.database,
        "host": args.host,
        "port": args.port,
        "connect_timeout": args.connect_timeout,
    }
    if args.unix_socket:
        params["unix_socket"] = args.unix_socket
    url, engine_options = MySQLTaskWorkStore._connection_options(params)
    tasks = _load_tasks(args.input)
    if args.dry_run:
        print(
            f"Would migrate {len(tasks)} watch task(s) into `{table}` "
            f"for service_name={service_name!r}"
        )
        return

    query = text(
        f"""
        INSERT INTO `{table}` (
            service_name, task_id, account_id, user_id, original_role,
            target_uri_hash, target_uri_prefix, target_uri,
            enabled, watch_interval, next_run_at, current_run_id, submitted,
            lease_owner, lease_until, revision,
            last_execution_time, last_task_id, last_status, last_error,
            payload, auth_state, connector_states, created_at, updated_at
        ) VALUES (
            :service_name, :task_id, :account_id, :user_id, :original_role,
            :target_uri_hash, :target_uri_prefix, :target_uri,
            :enabled, :watch_interval, :next_run_at, NULL, 0,
            NULL, 0, 0,
            :last_execution_time, :last_task_id, :last_status, :last_error,
            :payload, :auth_state, :connector_states,
            :created_at, UNIX_TIMESTAMP(NOW(6))
        )
        ON DUPLICATE KEY UPDATE
            account_id=VALUES(account_id),
            user_id=VALUES(user_id),
            original_role=VALUES(original_role),
            target_uri_hash=VALUES(target_uri_hash),
            target_uri_prefix=VALUES(target_uri_prefix),
            target_uri=VALUES(target_uri),
            enabled=VALUES(enabled),
            watch_interval=VALUES(watch_interval),
            next_run_at=VALUES(next_run_at),
            last_execution_time=VALUES(last_execution_time),
            last_task_id=VALUES(last_task_id),
            last_status=VALUES(last_status),
            last_error=VALUES(last_error),
            payload=VALUES(payload),
            auth_state=VALUES(auth_state),
            connector_states=VALUES(connector_states),
            updated_at=UNIX_TIMESTAMP(NOW(6)),
            revision=revision + 1
        """
    )
    engine = create_async_engine(url, **engine_options)
    try:
        async with engine.begin() as conn:
            for task in tasks:
                await conn.execute(
                    query,
                    {
                        "service_name": service_name,
                        "task_id": task.task_id,
                        "account_id": task.account_id,
                        "user_id": task.user_id,
                        "original_role": task.original_role,
                        "target_uri_hash": _target_hash(task.to_uri),
                        "target_uri_prefix": _target_prefix(task.to_uri),
                        "target_uri": task.to_uri,
                        "enabled": 1 if task.is_active else 0,
                        "watch_interval": task.watch_interval,
                        "next_run_at": _dt_to_epoch(task.next_execution_time),
                        "last_execution_time": _dt_to_epoch(task.last_execution_time),
                        "last_task_id": task.last_task_id,
                        "last_status": task.last_status,
                        "last_error": task.last_error,
                        "payload": json.dumps(
                            MySQLWatchStore._payload(task),
                            ensure_ascii=False,
                        ),
                        "auth_state": (
                            json.dumps(task.auth_state, ensure_ascii=False)
                            if task.auth_state is not None
                            else None
                        ),
                        "connector_states": (
                            json.dumps(task.connector_states, ensure_ascii=False)
                            if task.connector_states is not None
                            else None
                        ),
                        "created_at": _dt_to_epoch(task.created_at) or time.time(),
                    },
                )
    finally:
        await engine.dispose()
    print(
        f"Migrated {len(tasks)} watch task(s) into `{table}` "
        f"for service_name={service_name!r}"
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Import legacy .watch_tasks.json into MySQL. Stop old file-mode writers "
            "before running without --dry-run."
        )
    )
    parser.add_argument("--input", type=Path, required=True, help="Path to .watch_tasks.json")
    parser.add_argument("--table", default="ov_watch_tasks")
    parser.add_argument(
        "--service-name",
        default=DEFAULT_SERVICE_NAME,
        help="Top-level OpenViking service_name partition to import into",
    )
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--connect-timeout", type=int, default=10)
    parser.add_argument("--unix-socket")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> None:
    asyncio.run(_run(_parser().parse_args()))


if __name__ == "__main__":
    main()
