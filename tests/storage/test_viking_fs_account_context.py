# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Account context requirements for account-scoped VikingFS resources."""

import contextvars
from unittest.mock import Mock

import pytest

from openviking.server.identity import RequestContext, Role
from openviking.storage.viking_fs import VikingFS
from openviking_cli.session.user_id import UserIdentifier


def _context(account_id: str) -> RequestContext:
    return RequestContext(
        user=UserIdentifier(account_id, "user"),
        role=Role.USER,
    )


def _viking_fs(provider: Mock) -> VikingFS:
    fs = VikingFS.__new__(VikingFS)
    fs._bound_ctx = contextvars.ContextVar("vikingfs_account_context_test", default=None)
    fs._embedding_provider = provider
    return fs


def test_get_embedder_rejects_missing_account_context() -> None:
    fs = _viking_fs(Mock())

    with pytest.raises(RuntimeError, match="Account request context is required"):
        fs._get_embedder()


def test_get_embedder_uses_bound_account_context() -> None:
    provider = Mock()
    embedder = object()
    provider.bind.return_value = embedder
    fs = _viking_fs(provider)
    ctx = _context("account-a")

    with fs.bind_request_context(ctx):
        assert fs._get_embedder() is embedder

    provider.bind.assert_called_once_with("account-a")
