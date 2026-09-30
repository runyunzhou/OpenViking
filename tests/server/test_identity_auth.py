# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from openviking.server.api_keys import APIKeyManager
from openviking.server.auth import (
    get_api_key_manager_or_raise,
    get_request_context,
    get_session_request_context,
    get_upload_request_context,
)
from openviking.server.auth.plugin import AuthPlugin
from openviking.server.auth.plugins.api_key import ApiKeyAuthPlugin
from openviking.server.auth.plugins.trusted import TrustedAuthPlugin
from openviking.server.config import ServerConfig
from openviking.server.identity import ResolvedIdentity, Role
from openviking.server.store_assembly import build_api_key_manager
from openviking.server.upload_token_store import upload_token_store
from openviking_cli.exceptions import (
    FailedPreconditionError,
    InvalidArgumentError,
    PermissionDeniedError,
    UnauthenticatedError,
)
from tests.server.account_store_fakes import InMemoryAGFS


@pytest.fixture
async def runtime():
    manager = build_api_key_manager(
        "root",
        SimpleNamespace(agfs=InMemoryAGFS()),
    )
    await manager.load()
    await manager.create_account("acme", "alice")
    yield manager
    await manager.close()


async def make_request(
    runtime,
    mode="api_key",
    root="root",
    headers=(),
    path="/api/v1/content/read",
    trusted_flush_interval=0,
):
    config = ServerConfig(
        auth_mode=mode,
        root_api_key=root,
        trusted_identity_flush_interval_seconds=trusted_flush_interval,
    )
    plugin = TrustedAuthPlugin() if mode == "trusted" else ApiKeyAuthPlugin()
    state = SimpleNamespace(config=config, api_key_manager=runtime, auth_plugin=plugin)
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": list(headers),
            "query_string": b"",
            "app": SimpleNamespace(state=state),
        }
    )
    if mode == "trusted":
        plugin._api_key_manager = runtime
        plugin._flush_interval_seconds = trusted_flush_interval
        if root is None:
            request.app.state.api_key_manager = None
    return request


async def test_key_identity_and_groups_are_resolved_independently(runtime):
    key = await runtime.register_user("acme", "bob")
    await runtime.create_group("acme", "team")
    await runtime.add_group_member("acme", "team", "bob")
    request = await make_request(runtime, headers=[(b"x-openviking-user", b"spoof")])
    plugin = request.app.state.auth_plugin
    identity = await plugin.resolve_identity(
        request, api_key=key, x_openviking_user="spoof", x_openviking_account="spoof"
    )
    ctx = await get_request_context(request, identity, "peer", key, None)
    assert (ctx.user.user_id, ctx.role, ctx.group_ids) == ("bob", Role.USER, ("team",))
    assert ctx.actor_peer_id == "peer"
    assert ctx.api_key == key
    assert "x-openviking-user" not in request.headers
    session = await get_session_request_context(request, identity, key, None)
    assert session.actor_peer_id is None


async def test_key_store_error_and_unknown_identity_fail_closed():
    store = AsyncMock()
    store.verify_api_key.return_value = ("acme", "missing")
    store.get_user.return_value = None
    manager = APIKeyManager("root", store)
    request = await make_request(manager)
    with pytest.raises(UnauthenticatedError, match="Unknown"):
        await request.app.state.auth_plugin.resolve_identity(request, api_key="verified-key")
    store.get_user.side_effect = OSError("database offline")
    with pytest.raises(OSError, match="database offline"):
        await request.app.state.auth_plugin.resolve_identity(request, api_key="verified-key")


async def test_root_key_route_restriction(runtime):
    request = await make_request(runtime)
    identity = await request.app.state.auth_plugin.resolve_identity(request, api_key="root")
    assert identity.role == Role.ROOT
    with pytest.raises(PermissionDeniedError, match="tenant-scoped"):
        await get_request_context(request, identity, None, "root", None)


@pytest.mark.parametrize("root", [None, "root"])
async def test_trusted_role_and_rootless_management_rules(runtime, root):
    request = await make_request(runtime, "trusted", root)
    plugin = request.app.state.auth_plugin
    identity = await plugin.resolve_identity(
        request, api_key=root, x_openviking_account="acme", x_openviking_user="alice"
    )
    assert identity.role == Role.ADMIN
    if root is None:
        with pytest.raises(PermissionDeniedError):
            get_api_key_manager_or_raise(request)
    else:
        assert get_api_key_manager_or_raise(request) is runtime
    asserted = await make_request(
        runtime, "trusted", root, headers=[(b"x-openviking-role", b"user")]
    )
    if root is None:
        with pytest.raises(InvalidArgumentError, match="Root API Key"):
            await plugin.resolve_identity(
                asserted, x_openviking_account="acme", x_openviking_user="alice"
            )
    else:
        identity = await plugin.resolve_identity(
            asserted, api_key=root, x_openviking_account="acme", x_openviking_user="alice"
        )
        assert identity.role == Role.USER


async def test_trusted_unknown_user_cannot_bypass_account_deletion(runtime):
    """[defect-probing] Account fence also covers not-yet-registered trusted users."""
    await runtime.begin_deletion(
        "acme", None, task_id="delete", owner_account_id="system", owner_user_id="system"
    )
    request = await make_request(runtime, "trusted")
    identity = await request.app.state.auth_plugin.resolve_identity(
        request, api_key="root", x_openviking_account="acme", x_openviking_user="newcomer"
    )
    with pytest.raises(FailedPreconditionError) as exc:
        await get_request_context(request, identity, None, "root", None)
    assert exc.value.details["task_id"] == "delete"


@pytest.mark.parametrize("mode", ["oidc", "ldap", "dev", "external"])
async def test_external_context_preserves_role_without_local_gate(runtime, mode):
    class ExternalPlugin(AuthPlugin):
        auth_mode = mode

        async def initialize(self, app, service, config):
            self.initialized = True

        def validate_config(self, config):
            pass

        async def resolve_identity(self, request, **kwargs):
            return ResolvedIdentity(role=Role.USER, account_id="external", user_id="subject")

        def get_request_context_checks(self, path, identity):
            assert self.initialized

        async def shutdown(self):
            self.initialized = False

    request = await make_request(runtime, mode)
    plugin = ExternalPlugin()
    request.app.state.auth_plugin = plugin
    await plugin.initialize(request.app, None, request.app.state.config)
    identity = await plugin.resolve_identity(request)
    ctx = await get_request_context(request, identity, None, None, None)
    assert (ctx.role, ctx.account_id, ctx.group_ids) == (Role.USER, "external", ())
    await plugin.shutdown()
    assert not plugin.initialized


async def test_signed_upload_uses_bound_identity_and_burns_token(runtime):
    request = await make_request(runtime)
    token, _ = upload_token_store.issue("unregistered", "subject", 60, actor_peer_id="peer")
    ctx = await get_upload_request_context(
        request,
        token=token,
        x_api_key=None,
        authorization=None,
        x_openviking_account="spoof",
        x_openviking_user="spoof",
    )
    assert (ctx.account_id, ctx.user.user_id, ctx.actor_peer_id) == (
        "unregistered",
        "subject",
        "peer",
    )
    with pytest.raises(HTTPException) as exc:
        await get_upload_request_context(request, token=token, x_api_key=None, authorization=None)
    assert exc.value.status_code == 401


async def test_trusted_cancelled_flush_requeues_batch_and_shutdown_preserves_store(
    runtime, monkeypatch
):
    request = await make_request(runtime, "trusted", trusted_flush_interval=300)
    plugin = request.app.state.auth_plugin
    await plugin.resolve_identity(
        request,
        api_key="root",
        x_openviking_account="acme",
        x_openviking_user="bob",
    )
    entered = asyncio.Event()
    original = runtime.ensure_trusted_identities

    async def blocked_batch(identities):
        entered.set()
        await asyncio.Future()

    monkeypatch.setattr(runtime, "ensure_trusted_identities", blocked_batch)
    flush = asyncio.create_task(plugin.flush_trusted_identities())
    await entered.wait()
    flush.cancel()
    with pytest.raises(asyncio.CancelledError):
        await flush
    monkeypatch.setattr(runtime, "ensure_trusted_identities", original)
    await plugin.shutdown()
    assert await runtime.has_user("acme", "bob")
    assert await runtime.get_registered_user_role("acme", "alice") == Role.ADMIN


@pytest.mark.parametrize(
    "mode,custom", [("oidc", False), ("oidc", True), ("ldap", False), ("ldap", True)]
)
async def test_builtin_external_mapping_preserves_identity_and_user_role(
    runtime, mode, custom, monkeypatch
):
    from openviking.server.auth.identity_mapping import IdentityMapper
    from openviking.server.auth.ldap_config import LDAPConfig
    from openviking.server.auth.oidc_config import OIDCConfig
    from openviking.server.auth.plugins import ldap as ldap_module
    from openviking.server.auth.plugins import oidc as oidc_module

    if mode == "oidc":
        plugin = oidc_module.OIDCAuthPlugin()
        config = OIDCConfig(issuer="https://issuer.test")
        headers = [(b"authorization", b"Bearer external.token.value")]
        monkeypatch.setattr(oidc_module, "_check_jwt_available", lambda: True)
        monkeypatch.setattr(
            plugin,
            "_validate_token",
            AsyncMock(return_value={"sub": "auth0|123", "org": "Team", "preferred": "Alice"}),
        )
        expected = ("default", "auth0_123")
        if custom:
            config.identity.account_id.source = "claim"
            config.identity.account_id.claim = "org"
            config.identity.account_id.normalize = "lowercase"
            config.identity.user_id.claims = ["preferred", "sub"]
            config.identity.user_id.prefix = "ext-"
            expected = ("team", "ext-Alice")
    else:
        plugin = ldap_module.LDAPAuthPlugin()
        config = LDAPConfig(host="directory.test", base_dn="dc=test")
        headers = [(b"authorization", b"Basic YWxpY2U6cGFzc3dvcmQ=")]
        monkeypatch.setattr(ldap_module, "LDAP_AVAILABLE", True)
        monkeypatch.setattr(
            plugin,
            "_authenticate_ldap",
            lambda *args: {
                "uid": [b"directory-user"],
                "dn": [b"cn=alice,ou=Team,dc=test"],
            },
        )
        expected = ("default", "directory-user")
        if custom:
            config.identity.user_id.source = "dn_attribute"
            config.identity.user_id.attribute = "cn"
            config.identity.account_id.source = "dn_attribute"
            config.identity.account_id.attribute = "ou"
            expected = ("Team", "alice")
    config.identity.role.source = "fixed"
    config.identity.role.value = "root"
    plugin._config = config
    plugin._mapper = IdentityMapper(config.identity)
    request = await make_request(runtime, mode, headers=headers)
    request.app.state.auth_plugin = plugin
    monkeypatch.setattr(
        runtime,
        "get_user",
        AsyncMock(side_effect=AssertionError("External identities must not need registration")),
    )
    identity = await plugin.resolve_identity(request)
    assert (identity.account_id, identity.user_id) == expected
    assert identity.role == Role.USER
    ctx = await get_request_context(request, identity, None, None, None)
    assert ctx.role == Role.USER
