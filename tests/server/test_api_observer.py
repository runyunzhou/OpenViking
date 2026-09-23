# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

"""Tests for observer endpoints (/api/v1/observer/*)."""

import asyncio

import httpx


async def test_observer_queue(client: httpx.AsyncClient):
    """GET /api/v1/observer/queue should return queue status."""
    resp = await client.get("/api/v1/observer/queue")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    result = body["result"]
    assert "name" in result
    assert "is_healthy" in result
    assert "has_errors" in result
    assert "status" in result


async def test_observer_queue_structured(client: httpx.AsyncClient):
    """GET /api/v1/observer/queue?format=json should return structured status."""
    resp = await client.get("/api/v1/observer/queue", params={"format": "json"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    result = body["result"]
    assert "status" in result
    assert isinstance(result["status"], dict)
    assert "queues" in result["status"]
    assert isinstance(result["status"]["queues"], list)
    assert "summary" in result["status"]
    assert isinstance(result["status"]["summary"], dict)


async def test_observer_vikingdb(client: httpx.AsyncClient):
    """GET /api/v1/observer/vikingdb should return VikingDB status."""
    resp = await client.get("/api/v1/observer/vikingdb")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    result = body["result"]
    assert "name" in result
    assert "is_healthy" in result


async def test_observer_models(client: httpx.AsyncClient):
    """GET /api/v1/observer/models should return models status."""
    resp = await client.get("/api/v1/observer/models")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    result = body["result"]
    assert "name" in result
    assert "is_healthy" in result


async def test_observer_models_structured(client: httpx.AsyncClient):
    resp = await client.get("/api/v1/observer/models", params={"format": "json"})

    assert resp.status_code == 200
    status = resp.json()["result"]["status"]
    assert isinstance(status, dict)
    assert {"account_id", "embedding_dimension", "vlm", "embedding", "rerank"} <= status.keys()


async def test_observer_system(client: httpx.AsyncClient):
    """GET /api/v1/observer/system should return full system status."""
    resp = await client.get("/api/v1/observer/system")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    result = body["result"]
    assert "is_healthy" in result
    assert "errors" in result
    assert "components" in result
    assert isinstance(result["components"], dict)


async def test_observer_system_structured(client: httpx.AsyncClient):
    """GET /api/v1/observer/system?format=json should include structured component payloads."""
    resp = await client.get("/api/v1/observer/system", params={"format": "json"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    result = body["result"]
    assert "components" in result
    queue = result["components"]["queue"]
    assert "status" in queue
    assert isinstance(queue["status"], dict)


async def test_async_observer_routes_do_not_use_sync_bridge(client, monkeypatch):
    def fail_sync_bridge(*args, **kwargs):
        raise AssertionError("async observer route used run_async")

    monkeypatch.setattr("openviking.service.debug_service.run_async", fail_sync_bridge)

    async with asyncio.timeout(10):
        responses = await asyncio.gather(
            client.get("/api/v1/observer/queue"),
            client.get("/api/v1/observer/lock"),
            client.get("/api/v1/observer/filesystem"),
            client.get("/api/v1/observer/system"),
        )

    assert all(response.status_code == 200 for response in responses)
