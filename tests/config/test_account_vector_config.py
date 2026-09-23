# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

import pytest

from openviking.config.account_config import AccountConfig
from openviking.config.binding import manager_over_source
from openviking.config.scope import ConfigScope
from openviking.config.source import MemoryConfigSource
from openviking.config.validate import ConfigPatchError, validate_patch
from openviking.config.vector import (
    resolve_effective_embedding,
    resolve_effective_vectordb,
    resolve_vector_settings,
    validate_vector_settings,
)
from openviking_cli.utils.config import set_openviking_config
from openviking_cli.utils.config.open_viking_config import (
    OpenVikingConfig,
    OpenVikingConfigSingleton,
)
from openviking_cli.utils.config.vectordb_config import VectorDBBackendConfig
from tests.config.case_data import ACCOUNT_RUNTIME_CASES


@pytest.fixture
def vector_config(tmp_path):
    config = OpenVikingConfig(
        storage={"workspace": str(tmp_path)},
        embedding={
            "dense": {
                "provider": "openai",
                "model": "cluster-model",
                "dimension": 4,
                "api_key": "cluster-secret",
                "api_base": "https://cluster.invalid/v1",
            }
        },
    )
    set_openviking_config(config)
    yield config
    OpenVikingConfigSingleton.reset_instance()


def test_policy_only_embedding_override_uses_cluster_binding(vector_config):
    account = AccountConfig.model_validate({"embedding": {"max_retries": 5}})
    result = resolve_effective_embedding(vector_config.embedding, account.embedding)
    assert result.dense.api_key == "cluster-secret"
    assert result.dense.api_base == "https://cluster.invalid/v1"
    assert result.dense.model == "cluster-model"
    assert result.max_retries == 5


def test_empty_embedding_override_is_valid_noop():
    account = AccountConfig.model_validate({"embedding": {}})
    assert account.embedding is not None
    assert account.embedding.model_fields_set == set()


def test_partial_embedding_model_is_rejected():
    with pytest.raises(ValueError, match="Field required"):
        AccountConfig.model_validate({"embedding": {"dense": {"model": "new-model"}}})


def test_credentials_replace_binding_share_only_model_and_contract(vector_config):
    result = resolve_effective_embedding(
        vector_config.embedding,
        {
            "dense": {
                "model": "account-model",
                "dimension": 4,
                "credentials": [
                    {"provider": "openai", "api_base": "https://account.invalid/v1"}
                ],
            }
        },
    )
    assert result.dense.api_key is None
    assert result.dense.api_base is None
    assert result.dense.credentials[0].model is None
    assert result.dense._effective_model() == "account-model"
    assert result.dense.dimension == 4
    assert vector_config.embedding.dense.api_key == "cluster-secret"


@pytest.mark.parametrize(
    "binding",
    [
        {"api_base": "https://account.invalid/v1"},
        {"credentials": [{"api_key": "account-secret"}]},
        {"credentials": [{"provider": "openai"}]},
        {"credentials": [{"provider": "azure", "api_key": "account-secret"}]},
    ],
)
def test_incomplete_account_binding_is_rejected(vector_config, binding):
    with pytest.raises(ValueError):
        resolve_effective_embedding(vector_config.embedding, {"dense": binding})


def test_backend_change_does_not_inherit_connection(vector_config):
    cluster = VectorDBBackendConfig(
        backend="volcengine",
        name="cluster",
        dimension=4,
        volcengine={"api_key": "cluster-secret", "host": "cluster.invalid"},
    )
    with pytest.raises(ValueError):
        resolve_effective_vectordb(cluster, {"backend": "vikingdb"})
    result = resolve_effective_vectordb(
        cluster,
        {
            "backend": "vikingdb",
            "name": "account-context",
            "index_name": "default",
            "dimension": 4,
            "vikingdb": {"host": "account.invalid"},
            "project": "account",
        },
    )
    assert not result.volcengine.api_key
    assert result.vikingdb.host == "account.invalid"
    assert result.project_name == "account"
    assert result.dimension == 4


@pytest.mark.parametrize(
    "patch",
    ACCOUNT_RUNTIME_CASES["create_only_vector_patches"],
)
def test_create_only_fields_are_rejected(patch):
    with pytest.raises(ConfigPatchError):
        validate_patch(AccountConfig, patch)
    validate_patch(AccountConfig, patch, creating=True)


@pytest.mark.asyncio
async def test_dynamic_patch_persistence_reset_and_failure(vector_config):
    source = MemoryConfigSource()
    manager = manager_over_source(source, base_config=vector_config)
    await manager.initialize()
    await manager.patch_account("old-account", {"embedding": {"max_retries": 5}})
    scope = ConfigScope.account("old-account")
    assert await manager.get_settings(scope) == {"embedding": {"max_retries": 5}}
    account_embedding = await manager.resolve_account(
        "old-account",
        lambda view: view.account.embedding.model_dump(exclude_unset=True),
    )
    assert account_embedding == {"max_retries": 5}
    settings = await resolve_vector_settings(manager, "old-account")
    assert settings.embedding.dense.api_key == "cluster-secret"
    assert settings.embedding.max_retries == 5
    assert settings.vectordb.dimension == 4
    assert not settings.dedicated_vectordb
    with pytest.raises(ValueError):
        await manager.patch_account(
            "old-account",
            {
                "embedding": {
                    "dense": {
                        "credentials": [
                            {"provider": "azure", "api_key": "missing-endpoint"},
                        ]
                    }
                }
            },
        )
    assert (await resolve_vector_settings(manager, "old-account")).embedding.max_retries == 5
    await manager.patch_account("old-account", {"embedding": {"max_retries": None}})
    assert (await resolve_vector_settings(manager, "old-account")).embedding.max_retries == 3
    assert await manager.get_settings(scope) == {"embedding": {}}
    assert "cluster-secret" not in str(await manager.get_settings(scope))


@pytest.mark.asyncio
async def test_mode_and_inferred_dimension_cannot_change(vector_config):
    manager = manager_over_source(MemoryConfigSource(), base_config=vector_config)
    await manager.initialize()
    with pytest.raises(ValueError, match="mode"):
        await manager.patch_account(
            "a",
            {
                "embedding": {
                    "sparse": {
                        "credentials": [
                            {"provider": "volcengine", "model": "endpoint", "api_key": "key"}
                        ],
                    }
                }
            },
        )
    assert await manager.get_settings(ConfigScope.account("a")) == {}
    with pytest.raises(ValueError, match="dimension"):
        manager.validate_initial_settings("a", {"vectordb": {"dimension": 8}})


@pytest.mark.asyncio
async def test_new_mode_allowed_at_creation_and_loaded_on_another_manager(vector_config):
    source = MemoryConfigSource()
    manager = manager_over_source(source, base_config=vector_config)
    await manager.initialize()
    settings = {
        "embedding": {
            "sparse": {
                "model": "endpoint",
                "dimension": 4,
                "credentials": [{
                    "provider": "volcengine", "model": "endpoint", "api_key": "key",
                }],
            }
        },
        "vectordb": {
            "backend": "vikingdb",
            "name": "account-context",
            "index_name": "default",
            "dimension": 4,
            "sparse_weight": 0.5,
            "vikingdb": {"host": "https://account.invalid"},
        },
    }
    manager.validate_initial_settings("a", settings)
    await manager.patch_account("a", settings, creating=True)
    reader = manager_over_source(source, base_config=vector_config)
    await reader.initialize()
    assert (await resolve_vector_settings(reader, "a")).embedding.sparse is not None
    await manager.patch_account("a", {"embedding": {"max_retries": 7}})
    await reader.refresh_once()
    refreshed = await resolve_vector_settings(reader, "a")
    assert refreshed.embedding.max_retries == 7
    assert refreshed.embedding.sparse is not None


@pytest.mark.asyncio
async def test_refresh_reloads_create_only_account_fields_and_ignores_unknown(vector_config):
    source = MemoryConfigSource()
    scope = ConfigScope.account("a")
    await source.update(
        scope,
        lambda _: {
            "embedding": {
                "dense": {
                    "model": "persisted-model",
                    "dimension": 4,
                    "credentials": [{
                        "provider": "openai",
                        "api_base": "https://persisted.invalid/v1",
                    }],
                    "future_option": "ignored",
                }
            },
            "vectordb": {
                "backend": "vikingdb",
                "name": "persisted-collection",
                "index_name": "default",
                "dimension": 4,
                "vikingdb": {"host": "https://persisted.invalid"},
                "future_backend_option": True,
            },
            "future_section": {"value": "ignored"},
        },
    )
    manager = manager_over_source(source, base_config=vector_config)
    await manager.initialize()

    initial = await manager.resolve_account(
        "a",
        lambda view: (
            view.account.embedding.dense.model,
            view.account.vectordb.name,
        ),
    )
    assert initial == ("persisted-model", "persisted-collection")

    await source.update(
        scope,
        lambda _: {
            "embedding": {
                "dense": {
                    "model": "reloaded-model",
                    "dimension": 4,
                    "credentials": [{
                        "provider": "openai",
                        "api_base": "https://reloaded.invalid/v1",
                    }],
                    "future_option": "ignored-again",
                }
            },
            "vectordb": {
                "backend": "vikingdb",
                "name": "reloaded-collection",
                "index_name": "default",
                "dimension": 4,
                "vikingdb": {"host": "https://reloaded.invalid"},
            },
        },
    )
    await manager.refresh_once()
    reloaded = await manager.resolve_account(
        "a",
        lambda view: (
            view.account.embedding.dense.model,
            view.account.vectordb.name,
        ),
    )
    assert reloaded == ("reloaded-model", "reloaded-collection")


def test_joint_validation_rejects_dense_sparse_weight(vector_config):
    with pytest.raises(ValueError, match="sparse_weight"):
        validate_vector_settings(vector_config.embedding, VectorDBBackendConfig(sparse_weight=0.5))
    with pytest.raises(ValueError, match="distance_metric"):
        validate_vector_settings(
            vector_config.embedding, VectorDBBackendConfig(distance_metric="bad")
        )


@pytest.mark.parametrize("headers", [None, {}, {"X-Account": "account"}])
def test_same_backend_account_connection_never_inherits_cluster_headers(headers):
    cluster = VectorDBBackendConfig(
        backend="vikingdb",
        vikingdb={
            "host": "https://cluster.invalid",
            "headers": {"Authorization": "cluster-secret"},
        },
    )
    connection = {"host": "https://account.invalid"}
    if headers is not None:
        connection["headers"] = headers
    result = resolve_effective_vectordb(cluster, {
        "backend": "vikingdb", "name": "account", "index_name": "default",
        "dimension": 4, "vikingdb": connection,
    })
    assert result.vikingdb.headers == (headers or {})
    assert result.vikingdb.host == "https://account.invalid"
    assert cluster.vikingdb.headers == {"Authorization": "cluster-secret"}
    assert resolve_effective_vectordb(cluster, None).vikingdb.headers == cluster.vikingdb.headers


def test_same_backend_account_ak_sk_does_not_inherit_cluster_api_key():
    cluster = VectorDBBackendConfig(
        backend="volcengine",
        volcengine={"api_key": "cluster-secret", "host": "cluster.invalid"},
    )
    result = resolve_effective_vectordb(cluster, {
        "backend": "volcengine", "name": "account", "index_name": "default",
        "dimension": 4,
        "volcengine": {"ak": "account-ak", "sk": "account-sk", "region": "cn-beijing"},
    })
    assert not result.volcengine.api_key
    assert result.volcengine.ak == "account-ak"
    assert result.volcengine.host != "cluster.invalid"


@pytest.mark.parametrize(
    "patch",
    ACCOUNT_RUNTIME_CASES["non_isolation_vector_patches"],
)
def test_non_isolation_fields_are_not_on_account_surface(patch):
    with pytest.raises(ConfigPatchError, match="not modifiable"):
        validate_patch(AccountConfig, patch, creating=True)


@pytest.mark.asyncio
async def test_account_credentials_reject_non_allowlisted_fields(vector_config):
    manager = manager_over_source(MemoryConfigSource(), base_config=vector_config)
    await manager.initialize()
    with pytest.raises(ConfigPatchError, match="extra_body"):
        await manager.patch_account(
            "a",
            {"embedding": {"dense": {"credentials": [{
                "provider": "openai",
                "api_key": "account-secret",
                "extra_body": {"route": "untrusted"},
            }]}}},
        )
    assert await manager.get_settings(ConfigScope.account("a")) == {}


@pytest.mark.asyncio
async def test_old_node_preserves_unknown_stored_credential_fields(vector_config):
    source = MemoryConfigSource()
    scope = ConfigScope.account("a")
    await source.update(scope, lambda _: {"embedding": {"dense": {
        "model": "future-deployment",
        "dimension": 4,
        "credentials": [{
            "provider": "openai",
            "model": "future-deployment",
            "api_key": "future-key",
            "future_option": "kept",
        }],
    }}})
    manager = manager_over_source(source, base_config=vector_config)
    await manager.initialize()
    await manager.patch_account("a", {"embedding": {"max_retries": 7}})
    stored = await manager.get_settings(scope)
    assert stored["embedding"]["dense"]["credentials"][0]["future_option"] == "kept"
    assert stored["embedding"]["max_retries"] == 7


@pytest.mark.asyncio
async def test_credentials_null_cannot_break_complete_account_binding(vector_config):
    manager = manager_over_source(MemoryConfigSource(), base_config=vector_config)
    await manager.initialize()
    await manager.patch_account(
        "a",
        {
            "embedding": {
                "dense": {
                    "model": "account-model",
                    "dimension": 4,
                    "credentials": [{
                        "provider": "openai",
                        "api_base": "https://account.invalid/v1",
                    }],
                }
            }
        },
        creating=True,
    )
    assert (await resolve_vector_settings(manager, "a")).embedding.dense.api_key is None
    with pytest.raises(ValueError, match="Field required"):
        await manager.patch_account("a", {"embedding": {"dense": {"credentials": None}}})


def test_account_dimension_cannot_reinfer_shared_cluster_collection(vector_config):
    vector_config.storage.vectordb.dimension = 0
    manager = manager_over_source(MemoryConfigSource(), base_config=vector_config)
    with pytest.raises(ValueError, match="dimension"):
        manager.validate_initial_settings("a", {"embedding": {"dense": {
            "model": "account-model",
            "dimension": 8,
            "credentials": [{
                "provider": "openai",
                "api_base": "https://account.invalid/v1",
            }],
        }}})


@pytest.mark.asyncio
async def test_account_publication_does_not_materialize_cluster_vector_values(vector_config):
    manager = manager_over_source(MemoryConfigSource(), base_config=vector_config)
    await manager.initialize()
    await manager.patch_account(
        "a",
        {"vectordb": {
            "backend": "vikingdb",
            "name": "account-context",
            "index_name": "default",
            "dimension": 4,
            "vikingdb": {"host": "https://account.invalid"},
        }},
        creating=True,
    )
    account_vectordb = await manager.resolve_account(
        "a",
        lambda view: view.account.vectordb.model_dump(exclude_unset=True),
    )
    assert account_vectordb == {
        "backend": "vikingdb",
        "name": "account-context",
        "index_name": "default",
        "dimension": 4,
        "vikingdb": {"host": "https://account.invalid"},
    }
    effective = await resolve_vector_settings(manager, "a")
    assert effective.vectordb.dimension == 4
    assert effective.vectordb.name == "account-context"


@pytest.mark.asyncio
async def test_unrelated_patch_revalidates_previously_rejected_source(vector_config):
    source = MemoryConfigSource()
    manager = manager_over_source(source, base_config=vector_config)
    await manager.initialize()
    scope = ConfigScope.account("a")
    settings = {"embedding": {"dense": {
        "model": "account", "dimension": 4,
        "credentials": [{"provider": "openai", "api_key": "account-key"}],
    }}}
    await manager.patch_account("a", settings, creating=True)
    before = await manager.resolve_account("a", lambda view: view.account)
    settings["embedding"]["dense"]["dimension"] = 8
    await source.update(scope, lambda _: settings)
    await manager.refresh_once()
    assert await manager.resolve_account("a", lambda view: view.account) is before
    with pytest.raises(ValueError, match="dimension"):
        await manager.patch_account("a", {"acl": {"enabled": True}})
    assert await manager.resolve_account("a", lambda view: view.account) is before
    assert "acl" not in await source.load(scope)
