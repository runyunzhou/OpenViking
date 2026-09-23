# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Prevent account business paths from acquiring cluster vector configuration."""

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
# These are configuration construction, cluster startup/health, or offline tools.
CLUSTER_MODULES = {
    "openviking/config/account_config.py",
    "openviking/config/binding.py",
    "openviking/config/vector.py",
    "openviking/service/core.py",
    "openviking/server/routers/system.py",
}


def _cluster_accesses(source):
    tree = ast.parse(source)
    getters = {"get_openviking_config"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            getters.update(
                alias.asname or alias.name
                for alias in node.names
                if alias.name == "get_openviking_config"
            )
    aliases = {}

    def path(node):
        if isinstance(node, ast.Name):
            return aliases.get(node.id)
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id in getters:
                return ()
            if isinstance(node.func, ast.Attribute) and (
                node.func.attr == "get_openviking_config"
                or (
                    node.func.attr == "get_instance"
                    and ast.unparse(node.func.value).endswith("OpenVikingConfigSingleton")
                )
            ):
                return ()
        if isinstance(node, ast.Attribute):
            parent = path(node.value)
            if parent is not None:
                return (*parent, node.attr)
        return None

    protected = {("embedding",), ("storage", "vectordb")}
    violations = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            value = path(node.value)
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if value is not None:
                for target in targets:
                    if isinstance(target, ast.Name):
                        aliases[target.id] = value
        if isinstance(node, ast.Attribute) and path(node) in protected:
            violations.append(node.lineno)
    return sorted(set(violations))


def test_account_business_uses_vector_resolvers():
    violations = []
    for directory in ("service", "retrieve", "utils", "storage", "server"):
        for file in (ROOT / "openviking" / directory).rglob("*.py"):
            relative = file.relative_to(ROOT).as_posix()
            if relative in CLUSTER_MODULES:
                continue
            source = file.read_text()
            if relative == "openviking/storage/collection_schemas.py":
                # Schema bootstrap can use cluster config; queue workers cannot.
                source = ast.unparse(
                    next(
                        node
                        for node in ast.parse(source).body
                        if isinstance(node, ast.ClassDef) and node.name == "TextEmbeddingHandler"
                    )
                )
            violations.extend(f"{relative}:{line}" for line in _cluster_accesses(source))
    assert not violations, "Use account vector resolution:\n" + "\n".join(violations)


def test_guard_detects_direct_and_aliased_cluster_access():
    assert _cluster_accesses(
        "from openviking_cli.utils.config import get_openviking_config as config\n"
        "embedding = config().embedding\n"
        "base = config()\n"
        "storage = base.storage\n"
        "db = storage.vectordb\n"
    ) == [2, 5]


def test_storage_does_not_own_embedding_or_runtime_config_resolution():
    vector_store = (
        ROOT / "openviking/storage/viking_vector_index_backend.py"
    ).read_text()
    assert "embedding_provider" not in vector_store
    assert "_runtime_config_manager" not in vector_store

    forbidden = []
    for relative in (
        "openviking/service/reindex_executor.py",
        "openviking/storage/ovpack/operations.py",
        "openviking/storage/ovpack/vectors.py",
        "openviking/utils/embedding_utils.py",
    ):
        source = (ROOT / relative).read_text()
        if ".resolve_vector_settings(" in source:
            forbidden.append(relative)
    assert not forbidden, "Inject AccountVectorConfigResolver:\n" + "\n".join(forbidden)
