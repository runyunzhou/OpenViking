# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Architecture guard for account-aware VLM configuration access."""

from __future__ import annotations

import ast
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "openviking"
RAW_ACCESS_MODULE = SOURCE_ROOT / "config" / "vlm.py"
PROTECTED_ATTRIBUTES = {"vlm", "query_planner", "get_query_planner"}
CLUSTER_RESOLVER_MODULES = {
    "openviking/eval/ragas/__init__.py",
    "openviking/eval/ragas/pipeline.py",
    "openviking/eval/ragas/playback.py",
    "openviking/metrics/datasources/model_usage.py",
    "openviking/metrics/datasources/probes.py",
    "openviking/service/core.py",
}


def _is_config_getter_call(node: ast.AST, getter_names: set[str]) -> bool:
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name):
        return func.id in getter_names
    return isinstance(func, ast.Attribute) and func.attr == "get_openviking_config"


def _assigned_names(target: ast.AST) -> set[str]:
    if isinstance(target, ast.Name):
        return {target.id}
    if isinstance(target, (ast.Tuple, ast.List)):
        return {name for item in target.elts for name in _assigned_names(item)}
    return set()


class _RawVLMAccessVisitor(ast.NodeVisitor):
    def __init__(self, getter_names: set[str]) -> None:
        self.getter_names = getter_names
        self.config_names: list[set[str]] = [set()]
        self.violations: set[tuple[int, str]] = set()

    def _is_config_name(self, node: ast.AST) -> bool:
        return isinstance(node, ast.Name) and any(
            node.id in names for names in reversed(self.config_names)
        )

    def _visit_scope(self, node: ast.AST) -> None:
        self.config_names.append(set())
        self.generic_visit(node)
        self.config_names.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_scope(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_scope(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._visit_scope(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        if _is_config_getter_call(node.value, self.getter_names):
            for target in node.targets:
                self.config_names[-1].update(_assigned_names(target))
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None and _is_config_getter_call(node.value, self.getter_names):
            self.config_names[-1].update(_assigned_names(node.target))
        self.generic_visit(node)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        if _is_config_getter_call(node.value, self.getter_names):
            self.config_names[-1].update(_assigned_names(node.target))
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr in PROTECTED_ATTRIBUTES and (
            _is_config_getter_call(node.value, self.getter_names)
            or self._is_config_name(node.value)
            or (
                isinstance(node.value, ast.Attribute)
                and node.value.attr == "cluster"
            )
        ):
            self.violations.add((node.lineno, node.attr))
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if (
            isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and self._is_config_name(node.args[0])
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value in PROTECTED_ATTRIBUTES
        ):
            self.violations.add((node.lineno, str(node.args[1].value)))
        self.generic_visit(node)


def _raw_vlm_accesses(source: str) -> list[tuple[int, str]]:
    tree = ast.parse(source)
    getter_names = {"get_openviking_config"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == "get_openviking_config":
                    getter_names.add(alias.asname or alias.name)

    visitor = _RawVLMAccessVisitor(getter_names)
    visitor.visit(tree)
    return sorted(visitor.violations)


def _cluster_resolver_uses(source: str) -> list[int]:
    tree = ast.parse(source)
    resolver_names = {"ClusterVLMResolver"}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == "ClusterVLMResolver":
                    resolver_names.add(alias.asname or alias.name)

    return sorted(
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and (
            (isinstance(node.func, ast.Name) and node.func.id in resolver_names)
            or (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "ClusterVLMResolver"
            )
        )
    )


def test_raw_vlm_config_access_is_confined_to_resolvers():
    violations = []
    for path in SOURCE_ROOT.rglob("*.py"):
        if path == RAW_ACCESS_MODULE:
            continue
        for line, attribute in _raw_vlm_accesses(path.read_text(encoding="utf-8")):
            violations.append(
                f"{path.relative_to(PROJECT_ROOT)}:{line}: direct access to {attribute}"
            )

    assert not violations, (
        "Resolve VLM configuration through VLMResolver instead of reading the "
        "global config directly:\n" + "\n".join(violations)
    )


def test_cluster_vlm_resolver_is_confined_to_non_account_composition_roots():
    violations = []
    for path in SOURCE_ROOT.rglob("*.py"):
        relative = path.relative_to(PROJECT_ROOT).as_posix()
        if relative in CLUSTER_RESOLVER_MODULES or path == RAW_ACCESS_MODULE:
            continue
        for line in _cluster_resolver_uses(path.read_text(encoding="utf-8")):
            violations.append(f"{relative}:{line}: ClusterVLMResolver")

    assert not violations, (
        "ClusterVLMResolver is restricted to explicit startup, diagnostics, and "
        "offline evaluation composition roots:\n" + "\n".join(violations)
    )


def test_guard_detects_direct_aliased_and_indirect_access():
    source = """
from openviking_cli.utils.config import get_openviking_config as get_config

direct = get_config().vlm
config = get_config()
planner = config.get_query_planner()
dynamic = getattr(config, "query_planner")
"""

    assert _raw_vlm_accesses(source) == [
        (4, "vlm"),
        (6, "get_query_planner"),
        (7, "query_planner"),
    ]
