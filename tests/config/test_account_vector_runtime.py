# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from openviking.config.binding import manager_over_source
from openviking.config.embedding import AccountEmbeddingProvider
from openviking.config.source import MemoryConfigSource
from openviking.config.vector import AccountVectorConfigResolver
from openviking.models.embedder.base import (
    EmbedderBase,
    EmbedResult,
    embed_compat,
    query_embed_cache_scope,
)
from openviking.server.identity import RequestContext, Role
from openviking.storage.collection_schemas import init_context_collection
from openviking.storage.viking_vector_index_backend import (
    VikingVectorIndexBackend,
    _AsyncVectorAdapter,
)
from openviking.utils.circuit_breaker import CircuitBreakerOpen
from openviking_cli.session.user_id import UserIdentifier
from openviking_cli.utils.config import set_openviking_config
from openviking_cli.utils.config.embedding_config import EmbeddingConfig
from openviking_cli.utils.config.open_viking_config import (
    OpenVikingConfig,
    OpenVikingConfigSingleton,
)


class ControlledEmbedder(EmbedderBase):
    def __init__(self, config):
        super().__init__(config.dense.model, config.model_dump())
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()
        self.calls = 0
        self.closed = 0
        self.error = None
        self.dimension = config.dimension
        self.call_loops = []

    def embed(self, content, is_query=False):
        raise AssertionError("async embedding expected")

    async def embed_async(self, content, is_query=False):
        async def call():
            self.calls += 1
            self.call_loops.append(asyncio.get_running_loop())
            self.started.set()
            await self.release.wait()
            if self.error:
                raise self.error
            return EmbedResult(dense_vector=[0.5] * self.dimension)

        return await self._run_with_async_retry(call, operation_name="test")

    def close(self):
        self.closed += 1


@pytest.fixture
async def runtime(tmp_path, monkeypatch):
    config = OpenVikingConfig(
        storage={"workspace": str(tmp_path)},
        embedding={
            "dense": {
                "provider": "openai",
                "model": "cluster",
                "dimension": 4,
                "api_key": "test",
                "input": "text",
            },
            "max_concurrent": 1,
            "max_retries": 0,
            "circuit_breaker": {"failure_threshold": 1},
        },
    )
    set_openviking_config(config)
    manager = manager_over_source(MemoryConfigSource(), base_config=config)
    await manager.initialize()
    monkeypatch.setattr(EmbeddingConfig, "get_embedder", lambda config: ControlledEmbedder(config))
    resolver = AccountVectorConfigResolver(manager)
    provider = AccountEmbeddingProvider(resolver, manager)
    yield config, manager, provider
    await provider.close()
    OpenVikingConfigSingleton.reset_instance()


@pytest.mark.asyncio
async def test_same_account_creation_deduplicates_and_other_account_isolates(runtime):
    _, _, provider = runtime
    a, again, b = await asyncio.gather(
        provider._resource_for("a"), provider._resource_for("a"), provider._resource_for("b")
    )
    assert a is again and a is not b
    assert a.embedder is not b.embedder
    assert a.breaker is not b.breaker
    assert a.embedder._account_semaphore is not b.embedder._account_semaphore
    assert a.embedder._token_tracker is not b.embedder._token_tracker


@pytest.mark.asyncio
async def test_public_embedding_status_does_not_expose_client(runtime):
    _, _, provider = runtime

    status = await provider.get_status("a")

    assert status.dimension == 4
    assert status.borrowers == 0
    assert not hasattr(status, "embedder")
    assert not hasattr(provider, "get_resource")


@pytest.mark.asyncio
async def test_update_and_cancel_releases_inflight_resource(runtime):
    _, manager, provider = runtime
    await manager.patch_account(
        "a",
        {"embedding": {"dense": complete_account_dense()}},
        creating=True,
    )
    old = await provider._resource_for("a")
    old.embedder.release.clear()
    waiter = asyncio.create_task(provider.embed("a", "first"))
    await old.embedder.started.wait()
    await manager.patch_account("a", {"embedding": {"dense": {"credentials": [
        {"provider": "openai", "model": "new", "api_key": "test"},
    ]}}})
    new = await provider._resource_for("a")
    assert old is not new and old.retired and old.embedder.closed == 0
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    assert old.embedder.closed == 1
    assert (await provider.embed("a", "second")).dense_vector == [0.5] * 4
    await provider.close()
    assert new.embedder.closed == 1


@pytest.mark.asyncio
async def test_query_cache_keys_include_account_and_current_config(runtime):
    _, manager, provider = runtime
    await manager.patch_account(
        "a",
        {"embedding": {"dense": complete_account_dense()}},
        creating=True,
    )
    async with query_embed_cache_scope():
        a_embedder = provider.bind("a")
        await asyncio.gather(*(embed_compat(a_embedder, "same", is_query=True) for _ in range(4)))
        a = await provider._resource_for("a")
        assert a.embedder.calls == 1
        await embed_compat(provider.bind("b"), "same", is_query=True)
        assert (await provider._resource_for("b")).embedder.calls == 1
        await manager.patch_account("a", {"embedding": {"dense": {"credentials": [
            {"provider": "openai", "model": "new", "api_key": "test"},
        ]}}})
        await embed_compat(a_embedder, "same", is_query=True)
        assert (await provider._resource_for("a")).embedder.calls == 1


@pytest.mark.asyncio
async def test_failed_query_is_evicted_and_breaker_is_account_scoped(runtime):
    _, _, provider = runtime
    a = await provider._resource_for("a")
    a.embedder.error = RuntimeError("provider unavailable")
    async with query_embed_cache_scope():
        a_embedder = provider.bind("a")
        with pytest.raises(RuntimeError, match="provider unavailable"):
            await embed_compat(a_embedder, "same", is_query=True)
        with pytest.raises(CircuitBreakerOpen):
            await embed_compat(a_embedder, "same", is_query=True)
        await embed_compat(provider.bind("b"), "same", is_query=True)
        a.embedder.error = None
        a.breaker.record_success()
        await embed_compat(a_embedder, "same", is_query=True)
    assert a.embedder.calls == 2


@pytest.mark.asyncio
async def test_dimension_is_checked_before_result_leaves_provider(runtime):
    _, _, provider = runtime
    a = await provider._resource_for("a")
    a.embedder.dimension = 3
    with pytest.raises(ValueError, match="dimension mismatch"):
        await provider.embed("a", "wrong dimension")
    await provider.embed("b", "healthy")


@pytest.mark.asyncio
async def test_queue_event_loop_executes_embedding_on_caller_loop(runtime):
    _, _, provider = runtime
    a = await provider._resource_for("a")
    service_loop = asyncio.get_running_loop()
    result = await asyncio.to_thread(lambda: asyncio.run(provider.embed("a", "queue")))
    assert result.dense_vector == [0.5] * 4
    assert a.embedder.calls == 1
    assert a.embedder.call_loops[0] is not service_loop


@pytest.mark.asyncio
async def test_account_semaphore_blocks_only_same_account(runtime):
    _, _, provider = runtime
    a = await provider._resource_for("a")
    a.embedder.release.clear()
    first = asyncio.create_task(provider.embed("a", "first"))
    await a.embedder.started.wait()
    second = asyncio.create_task(provider.embed("a", "second"))
    await provider.embed("b", "free")
    assert a.embedder.calls == 1
    a.embedder.release.set()
    await asyncio.gather(first, second)
    assert a.embedder.calls == 2


def ctx(account, role=Role.USER):
    return RequestContext(user=UserIdentifier(account, "user"), role=role)


def complete_account_dense(model="account-model", api_key="account-key"):
    return {
        "model": model,
        "dimension": 4,
        "credentials": [{
            "provider": "openai",
            "model": model,
            "api_key": api_key,
        }],
    }


@pytest.mark.asyncio
async def test_account_local_vectordb_is_rejected(runtime):
    _, manager, _ = runtime
    with pytest.raises(ValueError):
        await manager.patch_account(
            "a",
            {
                "embedding": {
                    "dense": {
                        "model": "account-model",
                        "dimension": 4,
                        "credentials": [{
                            "provider": "openai",
                            "api_key": "account-key",
                        }],
                    }
                },
                "vectordb": {
                    "backend": "local",
                    "name": "account-context",
                    "dimension": 4,
                },
            },
            creating=True,
        )


@pytest.mark.asyncio
async def test_cluster_fallback_shares_connection_but_filters_accounts(runtime):
    config, manager, provider = runtime
    store = VikingVectorIndexBackend(config.storage.vectordb)
    store.set_vector_config_resolver(AccountVectorConfigResolver(manager))
    try:
        await init_context_collection(store)
        a, b = await asyncio.gather(store.get_account_backend("a"), store.get_account_backend("b"))
        assert a._adapter is b._adapter
        await store.upsert(
            {
                "id": "a",
                "account_id": "a",
                "vector": [0.5] * 4,
                "uri": "viking://resources/a",
                "context_type": "resource",
            },
            ctx=ctx("a"),
        )
        assert await a.count() == 1 and await b.count() == 0
        await store.release_account("b")
        assert await a.count() == 1
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_remote_account_never_bootstraps_or_falls_back(runtime, monkeypatch):
    config, manager, provider = runtime
    store = VikingVectorIndexBackend(config.storage.vectordb)
    store.set_vector_config_resolver(AccountVectorConfigResolver(manager))
    await manager.patch_account(
        "remote",
        {
            "vectordb": {
                "backend": "vikingdb",
                "name": "remote-context",
                "index_name": "default",
                "dimension": 4,
                "vikingdb": {"host": "https://remote.invalid"},
            }
        },
        creating=True,
    )
    adapter = Mock(mode="vikingdb", USE_CONTENT_FIELD=True)
    adapter.query.side_effect = RuntimeError("remote connection failed")
    monkeypatch.setattr(
        "openviking.storage.viking_vector_index_backend.create_collection_adapter",
        lambda config: adapter,
    )
    try:
        backend = await store.get_account_backend("remote")
        adapter.create_collection.assert_not_called()
        with pytest.raises(RuntimeError, match="remote connection"):
            await store.query(ctx=ctx("remote", Role.ROOT))
        assert backend._adapter is adapter
        await store.release_account("remote")
        adapter.drop_collection.assert_not_called()
        adapter.close.assert_called_once()
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_adapter_close_waits_for_cancelled_threaded_call():
    started, release = threading.Event(), threading.Event()
    adapter = Mock()

    def query():
        started.set()
        assert release.wait(5)
        return 1

    adapter.query.side_effect = query
    wrapper = _AsyncVectorAdapter(adapter)
    call = asyncio.create_task(wrapper.call("query"))
    await asyncio.to_thread(started.wait, 5)
    call.cancel()
    closing = asyncio.create_task(wrapper.call("close"))
    await asyncio.sleep(0)
    adapter.close.assert_not_called()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await call
    await closing
    adapter.close.assert_called_once()


@pytest.mark.asyncio
async def test_remote_random_query_uses_account_dimension(runtime):
    from openviking.storage.vectordb_adapters import create_collection_adapter
    from openviking_cli.utils.config.vectordb_config import VectorDBBackendConfig

    adapter = create_collection_adapter(
        VectorDBBackendConfig(
            backend="vikingdb",
            dimension=8,
            vikingdb={"host": "https://account.invalid"},
        )
    )
    collection = Mock()
    collection.search_by_vector.return_value = SimpleNamespace(data=[])
    adapter._collection = collection
    try:
        assert adapter.query() == []
        assert len(collection.search_by_vector.call_args.kwargs["dense_vector"]) == 8
    finally:
        adapter.close()


@pytest.mark.asyncio
async def test_account_observer_uses_account_collection(runtime):
    _, manager, _ = runtime
    await manager.patch_account(
        "a",
        {"vectordb": {
            "backend": "vikingdb",
            "name": "account_only",
            "index_name": "default",
            "dimension": 4,
            "vikingdb": {"host": "https://account.invalid"},
        }},
        creating=True,
    )
    settings = await AccountVectorConfigResolver(manager).resolve("a")
    assert settings.vectordb.name == "account_only"
    assert settings.dedicated_vectordb


@pytest.mark.asyncio
async def test_reindex_text_source_follows_target_account(runtime, monkeypatch):
    from openviking.service.reindex_executor import ReindexExecutor

    config, manager, provider = runtime
    await manager.patch_account("a", {"embedding": {"text_source": "summary_first"}}, creating=True)
    store = VikingVectorIndexBackend(config.storage.vectordb)
    store.set_vector_config_resolver(AccountVectorConfigResolver(manager))
    monkeypatch.setattr(
        "openviking.service.reindex_executor.get_viking_fs",
        lambda: SimpleNamespace(vector_store=store),
    )
    executor = ReindexExecutor(
        vector_config_resolver=AccountVectorConfigResolver(manager)
    )
    executor._fetch_existing_record = AsyncMock(return_value=None)
    try:
        for account, expected in (("a", "summary"), ("b", "body")):
            assert (
                await executor._best_resource_file_vector_text(
                    "viking://resources/file.txt", "summary", ctx(account), b"body"
                )
                == expected
            )
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_ovpack_metadata_and_restore_validation_follow_account(runtime):
    from openviking.storage.ovpack.vectors import (
        build_dense_snapshot_manifest,
        choose_vector_restore_action,
    )
    from openviking_cli.exceptions import InvalidArgumentError

    config, manager, provider = runtime
    await manager.patch_account("a", {"embedding": {"dense": {
        "model": "account-a",
        "dimension": 4,
        "credentials": [{"provider": "openai", "model": "account-a", "api_key": "test"}],
    }}}, creating=True)
    store = VikingVectorIndexBackend(config.storage.vectordb)
    resolver = AccountVectorConfigResolver(manager)
    store.set_vector_config_resolver(resolver)
    records = [{"record_id": "row", "vector": {"dense": {"offset": 0, "dimensions": 4}}}]
    try:
        await init_context_collection(store)
        settings = await resolver.resolve("a")
        _, dense = build_dense_snapshot_manifest(records, [0.5] * 4, settings.embedding)
        assert dense["embedding"]["model"] == "account-a"
        manifest = {"index": {"dense": dense}}
        arguments = {
            "vector_store": store,
            "vector_config_resolver": resolver,
            "vector_mode": "require",
        }
        assert (
            await choose_vector_restore_action(
                manifest, records, {"row": [0.5] * 4}, ctx=ctx("a"), **arguments
            )
            == "restore"
        )
        with pytest.raises(InvalidArgumentError, match="incompatible"):
            await choose_vector_restore_action(
                manifest, records, {"row": [0.5] * 4}, ctx=ctx("b"), **arguments
            )
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_close_during_initial_resolution_does_not_create_client(runtime, monkeypatch):
    _, _, provider = runtime
    started, finish = asyncio.Event(), asyncio.Event()
    original = provider._resolver

    async def resolve(account):
        started.set()
        await finish.wait()
        return await original.resolve(account)

    provider._resolver = SimpleNamespace(resolve=resolve)
    factory = Mock(side_effect=ControlledEmbedder)
    monkeypatch.setattr(EmbeddingConfig, "get_embedder", lambda config: factory(config))
    pending = asyncio.create_task(provider.embed("new", "text"))
    await started.wait()
    await provider.close()
    finish.set()
    with pytest.raises(RuntimeError, match="closed"):
        await pending
    factory.assert_not_called()
    assert not provider._cache


@pytest.mark.asyncio
async def test_embedding_snapshot_allows_concurrent_first_account(runtime, monkeypatch):
    _, _, provider = runtime
    resource = await provider._resource_for("a")
    tracker = resource.embedder._token_tracker
    tracker.update("model", "openai", 3, 0)
    entered, finish = threading.Event(), threading.Event()
    original = tracker.to_dict

    def snapshot():
        entered.set()
        assert finish.wait(5)
        return original()

    monkeypatch.setattr(tracker, "to_dict", snapshot)
    pending = asyncio.create_task(asyncio.to_thread(provider.get_total_token_usage))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        await provider._resource_for("b")
    finally:
        finish.set()
    usage = await pending
    assert usage["usage_by_model"]["model"]["usage_by_provider"]["openai"]["call_count"] == 1


@pytest.mark.asyncio
async def test_embedding_node_usage_preserves_observer_fields(runtime):
    _, _, provider = runtime
    for account, count in (("a", 3), ("b", 2)):
        resource = await provider._resource_for(account)
        for _ in range(count):
            resource.embedder._token_tracker.update("model", "openai", 3, 0)
    usage = provider.get_total_token_usage()
    counts = usage["usage_by_model"]["model"]["usage_by_provider"]["openai"]
    assert counts["call_count"] == 5
    assert counts["last_updated"]
    assert usage["total_usage"]["total_tokens"] == 15


@pytest.mark.asyncio
async def test_vectordb_refresh_replaces_cached_backend(runtime, monkeypatch):
    from openviking.config.scope import ConfigScope, ScopeKind
    from openviking.service.core import OpenVikingService

    config, manager, _ = runtime
    adapters = []

    def create_adapter(config):
        adapter = Mock(mode=config.backend, USE_CONTENT_FIELD=True)
        adapters.append(adapter)
        return adapter

    monkeypatch.setattr(
        "openviking.storage.viking_vector_index_backend.create_collection_adapter", create_adapter
    )
    store = VikingVectorIndexBackend(config.storage.vectordb)
    store.set_vector_config_resolver(AccountVectorConfigResolver(manager))
    service = SimpleNamespace(release_account_vector_resources=store.release_account)
    manager.add_update_consumer(
        scope=ScopeKind.ACCOUNT, sections={"vectordb", "embedding"},
        consumer=lambda event: OpenVikingService._on_account_vector_config_change(service, event),
    )
    settings = {"vectordb": {
        "backend": "vikingdb", "name": "old", "index_name": "default", "dimension": 4,
        "vikingdb": {"host": "https://account.invalid"},
    }}
    try:
        await manager.patch_account("a", settings, creating=True)
        old = await store.get_account_backend("a")
        settings["vectordb"]["name"] = "new"
        await manager._source.update(ConfigScope.account("a"), lambda _: settings)
        await manager.refresh_once()
        new = await store.get_account_backend("a")
        assert new is not old
        assert new.collection_name == "new"
        old._adapter.close.assert_called_once()
    finally:
        await store.close()
