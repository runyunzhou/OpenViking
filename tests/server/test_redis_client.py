# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

import pytest
from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster

from openviking.server.account_stores.redis_client import create_redis_client
from openviking_cli.exceptions import InvalidArgumentError


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "endpoints", "extra", "expected_type"),
    [
        ("standalone", ["redis://127.0.0.1:6379"], {}, Redis),
        (
            "cluster",
            ["redis://127.0.0.1:7000", "redis://127.0.0.1:7001"],
            {},
            RedisCluster,
        ),
    ],
)
async def test_create_redis_client_supports_direct_topologies(
    mode, endpoints, extra, expected_type
):
    handle = create_redis_client(
        {
            "mode": mode,
            "endpoints": endpoints,
            **extra,
        }
    )
    try:
        assert isinstance(handle.client, expected_type)
    finally:
        await handle.close()


@pytest.mark.asyncio
async def test_create_redis_client_supports_sentinel():
    handle = create_redis_client(
        {
            "mode": "sentinel",
            "endpoints": [
                "redis://127.0.0.1:26379",
                "redis://127.0.0.1:26380",
            ],
            "master_name": "openviking",
        }
    )
    try:
        assert isinstance(handle.client, Redis)
        assert len(handle.sentinel_clients) == 2
    finally:
        await handle.close()


def test_create_redis_client_rejects_invalid_topology_config():
    with pytest.raises(InvalidArgumentError, match="standalone mode requires exactly one"):
        create_redis_client(
            {
                "mode": "standalone",
                "endpoints": [
                    "redis://127.0.0.1:6379",
                    "redis://127.0.0.1:6380",
                ],
            }
        )

    with pytest.raises(InvalidArgumentError, match="master_name"):
        create_redis_client(
            {
                "mode": "sentinel",
                "endpoints": ["redis://127.0.0.1:26379"],
            }
        )
