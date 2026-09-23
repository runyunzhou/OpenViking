# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

import asyncio

import pytest

from openviking.config.binding import manager_over_source
from openviking.config.source import MemoryConfigSource
from openviking.config.vlm import AccountBoundVLM, AccountVLMProvider
from openviking_cli.utils.config import set_openviking_config
from openviking_cli.utils.config.open_viking_config import (
    OpenVikingConfig,
    OpenVikingConfigSingleton,
)


class _ControlledVLM:
    def __init__(self, model):
        self.model = model
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.closed = 0
        self.calls = []

    async def get_completion_async(self, **kwargs):
        self.calls.append(kwargs)
        self.started.set()
        await self.release.wait()
        return self.model

    async def get_vision_completion_async(self, **kwargs):
        self.calls.append(kwargs)
        self.started.set()
        await self.release.wait()
        return self.model

    def close(self):
        self.closed += 1


@pytest.fixture
async def runtime(monkeypatch):
    base = OpenVikingConfig.from_dict(
        {"vlm": {"model": "cluster", "provider": "litellm"}}
    )
    set_openviking_config(base)
    manager = manager_over_source(MemoryConfigSource(), base_config=base)
    await manager.initialize()
    clients = []

    def create(config):
        client = _ControlledVLM(config["model"])
        clients.append(client)
        return client

    monkeypatch.setattr("openviking.models.vlm.VLMFactory.create", create)
    provider = AccountVLMProvider(manager)
    await manager.patch_account(
        "a",
        {
            "vlm": {
                "model": "account-v1",
                "credentials": [{"provider": "openai", "api_key": "key"}],
            }
        },
    )
    yield manager, provider, clients
    await provider.close()
    OpenVikingConfigSingleton.reset_instance()


@pytest.mark.asyncio
async def test_bound_vlm_switches_after_update_and_retires_after_inflight_call(runtime):
    manager, provider, clients = runtime
    bound = await provider.get_vlm("a")

    assert isinstance(bound, AccountBoundVLM)

    first = asyncio.create_task(bound.get_completion_async(prompt="first"))
    while not clients:
        await asyncio.sleep(0)
    old = clients[0]
    await old.started.wait()
    old_resource = provider._bindings[("a", "vlm")]

    await manager.patch_account("a", {"vlm": {"model": "account-v2"}})
    assert old_resource.retired
    assert not old_resource.closed
    assert old.closed == 0

    old.release.set()
    assert await first == "account-v1"
    assert old_resource.closed
    assert old.closed == 1

    second = asyncio.create_task(bound.get_completion_async(prompt="second"))
    while len(clients) < 2:
        await asyncio.sleep(0)
    new = clients[1]
    new.release.set()
    assert await second == "account-v2"
    assert bound.model == "account-v2"


@pytest.mark.asyncio
async def test_cancelled_call_releases_retired_vlm_resource(runtime):
    manager, provider, clients = runtime
    bound = await provider.get_vlm("a")
    call = asyncio.create_task(bound.get_completion_async(prompt="cancel"))
    while not clients:
        await asyncio.sleep(0)
    old = clients[0]
    await old.started.wait()

    await manager.patch_account("a", {"vlm": {"model": "account-v2"}})
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    assert old.closed == 1


@pytest.mark.asyncio
async def test_provider_close_waits_for_inflight_vlm(runtime):
    _, provider, clients = runtime
    bound = await provider.get_vlm("a")
    call = asyncio.create_task(bound.get_completion_async(prompt="close"))
    while not clients:
        await asyncio.sleep(0)
    client = clients[0]
    await client.started.wait()
    resource = provider._bindings[("a", "vlm")]

    closing = asyncio.create_task(provider.close())
    await asyncio.sleep(0)
    assert not closing.done()
    assert client.closed == 0

    client.release.set()
    assert await call == "account-v1"
    await closing
    assert resource.retired and resource.closed
    assert client.closed == 1


@pytest.mark.asyncio
async def test_bound_query_planner_tracks_role_precedence_changes(runtime):
    manager, provider, clients = runtime
    planner = await provider.get_query_planner("a")

    inherited = asyncio.create_task(planner.get_completion_async(prompt="inherited"))
    while not clients:
        await asyncio.sleep(0)
    clients[0].release.set()
    assert await inherited == "account-v1"

    await manager.patch_account(
        "a",
        {
            "query_planner": {
                "model": "planner-v1",
                "credentials": [{"provider": "openai", "api_key": "planner-key"}],
            }
        },
    )
    dedicated = asyncio.create_task(planner.get_completion_async(prompt="dedicated"))
    while len(clients) < 2:
        await asyncio.sleep(0)
    clients[1].release.set()
    assert await dedicated == "planner-v1"


@pytest.mark.asyncio
async def test_bound_vlm_forwards_per_call_max_tokens(runtime):
    _, provider, clients = runtime
    bound = await provider.get_vlm("a")
    call = asyncio.create_task(
        bound.get_completion_async(prompt="extract", max_tokens=321)
    )
    while not clients:
        await asyncio.sleep(0)
    clients[0].release.set()

    assert await call == "account-v1"
    assert clients[0].calls[0]["max_tokens"] == 321


@pytest.mark.asyncio
async def test_bound_vlm_forwards_vision_tool_choice(runtime):
    _, provider, clients = runtime
    bound = await provider.get_vlm("a")
    tool_choice = {"type": "function", "function": {"name": "describe"}}
    call = asyncio.create_task(
        bound.get_vision_completion_async(
            prompt="describe",
            tools=[{"type": "function"}],
            tool_choice=tool_choice,
        )
    )
    while not clients:
        await asyncio.sleep(0)
    clients[0].release.set()

    assert await call == "account-v1"
    assert clients[0].calls[0]["tool_choice"] == tool_choice


@pytest.mark.asyncio
async def test_cluster_fallback_vlm_and_planner_usage_is_account_scoped():
    base = OpenVikingConfig.from_dict(
        {
            "vlm": {"model": "cluster-vlm", "provider": "litellm"},
            "query_planner": {"model": "cluster-planner", "provider": "litellm"},
        }
    )
    set_openviking_config(base)
    manager = manager_over_source(MemoryConfigSource(), base_config=base)
    await manager.initialize()
    provider = AccountVLMProvider(manager)
    try:
        vlm_a = await provider.get_vlm("a")
        vlm_b = await provider.get_vlm("b")
        planner_a = await provider.get_query_planner("a")
        planner_b = await provider.get_query_planner("b")

        assert provider._bindings[("a", "vlm")] is not provider._bindings[("b", "vlm")]
        assert provider._bindings[("a", "query_planner")] is not provider._bindings[
            ("b", "query_planner")
        ]
        assert vlm_a.model == vlm_b.model == "cluster-vlm"
        assert planner_a.model == planner_b.model == "cluster-planner"

        vlm_a.token_tracker.update("cluster-vlm", "litellm", 100, 20)
        planner_b.token_tracker.update("cluster-planner", "litellm", 30, 10)

        assert provider.get_token_usage("a")["total_usage"]["total_tokens"] == 120
        assert provider.get_token_usage("b")["total_usage"]["total_tokens"] == 0
        assert provider.get_node_token_usage()["total_usage"]["total_tokens"] == 160
    finally:
        await provider.close()
        OpenVikingConfigSingleton.reset_instance()


@pytest.mark.asyncio
async def test_query_planner_vlm_fallback_counts_shared_tracker_once():
    base = OpenVikingConfig.from_dict(
        {"vlm": {"model": "cluster-vlm", "provider": "litellm"}}
    )
    set_openviking_config(base)
    manager = manager_over_source(MemoryConfigSource(), base_config=base)
    await manager.initialize()
    provider = AccountVLMProvider(manager)
    try:
        vlm = await provider.get_vlm("a")
        planner = await provider.get_query_planner("a")

        assert provider._bindings[("a", "vlm")] is provider._bindings[
            ("a", "query_planner")
        ]
        assert vlm.token_tracker is planner.token_tracker

        vlm.token_tracker.update("cluster-vlm", "litellm", 100, 20)
        assert provider.get_node_token_usage()["total_usage"]["total_tokens"] == 120
    finally:
        await provider.close()
        OpenVikingConfigSingleton.reset_instance()


@pytest.mark.asyncio
async def test_cluster_vlm_update_retires_account_fallback_resources():
    base = OpenVikingConfig.from_dict(
        {"vlm": {"model": "cluster-v1", "provider": "litellm"}}
    )
    set_openviking_config(base)
    manager = manager_over_source(MemoryConfigSource(), base_config=base)
    await manager.initialize()
    provider = AccountVLMProvider(manager)
    try:
        await provider.get_vlm("a")
        await provider.get_vlm("b")
        first_resource = provider._bindings[("a", "vlm")]
        second_resource = provider._bindings[("b", "vlm")]

        updated = OpenVikingConfig.from_dict(
            {"vlm": {"model": "cluster-v2", "provider": "litellm"}}
        )
        await manager.replace_base_config(updated)

        assert first_resource.retired and first_resource.closed
        assert second_resource.retired and second_resource.closed
        assert ("a", "vlm") not in provider._bindings
        assert ("b", "vlm") not in provider._bindings
        assert (await provider.get_vlm("a")).model == "cluster-v2"
    finally:
        await provider.close()
        OpenVikingConfigSingleton.reset_instance()


@pytest.mark.asyncio
async def test_bound_vlm_executes_on_caller_loop(monkeypatch):
    class _LoopVLM:
        async def get_completion_async(self, **_kwargs):
            return asyncio.get_running_loop()

        def close(self):
            return None

    base = OpenVikingConfig.from_dict(
        {"vlm": {"model": "cluster", "provider": "litellm"}}
    )
    set_openviking_config(base)
    manager = manager_over_source(MemoryConfigSource(), base_config=base)
    await manager.initialize()
    monkeypatch.setattr(
        "openviking.models.vlm.VLMFactory.create",
        lambda _config: _LoopVLM(),
    )
    provider = AccountVLMProvider(manager)
    try:
        bound = await provider.get_vlm("a")
        service_loop = asyncio.get_running_loop()
        call_loop = await asyncio.to_thread(
            lambda: asyncio.run(bound.get_completion_async(prompt="queue"))
        )
        assert call_loop is not service_loop
    finally:
        await provider.close()
        OpenVikingConfigSingleton.reset_instance()
