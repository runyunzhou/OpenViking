# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Shared URI mutations; Watch occupancy is registered directly in its Store."""

import asyncio
import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional, Sequence

from openviking.resource.distributed_watch_store import MySQLWatchStore
from openviking.storage.mysql_runtime import wait_mysql_cleanup
from openviking_cli.utils import get_logger

logger = get_logger(__name__)


class DistributedUriCoordinator:
    def __init__(self, store: MySQLWatchStore, *, retry_interval: float = 5.0) -> None:
        self._store = store
        self._retry_interval = retry_interval

    @asynccontextmanager
    async def mutation(
        self, account_id: str, uris: Sequence[Optional[str]],
    ) -> AsyncIterator[None]:
        scopes = sorted({uri.rstrip("/") for uri in uris if uri})
        if not scopes:
            yield
            return
        token = str(uuid.uuid4())
        try:
            while not await self._store.acquire_mutation(account_id, scopes, token):
                await asyncio.sleep(self._retry_interval)
            operation = asyncio.current_task()

            async def heartbeat() -> None:
                try:
                    while True:
                        await asyncio.sleep(30)
                        if not await self._store.renew_mutation(token):
                            raise RuntimeError("URI mutation lease lost")
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("URI mutation heartbeat failed token=%s", token)
                    if operation is not None:
                        operation.cancel()

            renewal = asyncio.create_task(heartbeat())
            try:
                yield
            finally:
                renewal.cancel()
                await asyncio.gather(renewal, return_exceptions=True)
        finally:
            # Also covers cancellation delivered immediately after SQL commit.
            await wait_mysql_cleanup(self._store.release_mutation(token))
