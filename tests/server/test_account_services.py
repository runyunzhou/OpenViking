# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

import asyncio
import base64
import json
import os
import threading
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import and_, delete, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine

from openviking.server.account_stores import mysql as mysql_store_module
from openviking.server.account_stores.mysql import MySQLAccountStore
from openviking.server.account_stores.mysql_schema import Account, Base, Credential, User
from openviking.server.api_keys import APIKeyManager
from openviking.server.api_keys.legacy import FileStore
from openviking.server.identity import Role
from openviking.server.store_assembly import build_api_key_manager
from openviking_cli.exceptions import (
    FailedPreconditionError,
    InvalidArgumentError,
    NotFoundError,
    UnauthenticatedError,
)
from tests.server.account_store_fakes import InMemoryAGFS


def _file_store() -> FileStore:
    return FileStore(SimpleNamespace(agfs=InMemoryAGFS()))


@pytest.mark.parametrize(
    "account_id,user_id",
    [
        pytest.param("acme", "alice", id="normal"),
        pytest.param("a" * 1024, "alice", id="long-existing-account"),
        pytest.param("acme", "u" * 1024, id="long-existing-user"),
    ],
)
async def test_legacy_user_without_role_defaults_to_user_during_authentication(account_id, user_id):
    agfs = InMemoryAGFS()
    agfs.write(
        "/local/_system/accounts.json",
        json.dumps({"accounts": {account_id: {"created_at": "legacy"}}}).encode(),
    )
    agfs.write(
        f"/local/{account_id}/_system/users.json",
        json.dumps({"users": {user_id: {"key": "legacy-key"}}}).encode(),
    )
    manager = build_api_key_manager("root", SimpleNamespace(agfs=agfs))
    await manager.load()
    try:
        assert await manager.ensure_trusted_identities({account_id: {user_id}}) == {
            "created_accounts": 0,
            "created_users": 0,
        }
        identity = await manager.resolve_identity("legacy-key")
        assert (identity.account_id, identity.user_id, identity.role) == (
            account_id,
            user_id,
            Role.USER,
        )
    finally:
        await manager.close()


async def test_file_provider_keeps_existing_registry_paths():
    fs = SimpleNamespace(agfs=InMemoryAGFS())
    manager = build_api_key_manager("root", fs)
    await manager.load()
    try:
        key = await manager.create_account("acme", "alice")
        await manager.register_user("acme", "bob")
        assert await manager.resolve_identity(key)
        paths = set(fs.agfs._files)
        assert "/local/_system/accounts.json" in paths
        assert "/local/acme/_system/users.json" in paths
        assert "/local/acme/_system/groups.json" in paths
        assert not any("identities-v2" in path or "api-keys-v2" in path for path in paths)
    finally:
        await manager.close()


@pytest.mark.parametrize("issue_user_keys", [True, False], ids=["api-key", "trusted"])
@pytest.mark.parametrize("length", [255, 256, 1024])
async def test_file_account_creation_respects_path_component_limit(issue_user_keys, length):
    agfs = InMemoryAGFS()
    manager = build_api_key_manager(
        "root", SimpleNamespace(agfs=agfs), issue_user_keys=issue_user_keys
    )
    await manager.load()
    account_id = "a" * length
    admin_user_id = "admin"
    try:
        if length <= 255:
            key = await manager.create_account(account_id, admin_user_id)
            assert await manager.get_registered_user_role(account_id, admin_user_id) == Role.ADMIN
            if issue_user_keys:
                assert (await manager.resolve_identity(key)).account_id == account_id
            else:
                assert key is None
        else:
            before = dict(agfs._files)
            with pytest.raises(InvalidArgumentError, match="account_id.*255.*UTF-8 bytes"):
                await manager.create_account(account_id, admin_user_id)
            assert agfs._files == before
            assert await manager.get_account(account_id) is None
            assert await manager.get_user(account_id, admin_user_id) is None
    finally:
        await manager.close()


@pytest.mark.parametrize("issue_user_keys", [True, False], ids=["api-key", "trusted"])
@pytest.mark.parametrize("length", [255, 256, 1024])
async def test_file_account_creation_respects_admin_user_id_limit(
    issue_user_keys,
    length,
):
    agfs = InMemoryAGFS()
    manager = build_api_key_manager(
        "root", SimpleNamespace(agfs=agfs), issue_user_keys=issue_user_keys
    )
    await manager.load()
    account_id = "acme"
    admin_user_id = "u" * length
    try:
        if length <= 255:
            key = await manager.create_account(account_id, admin_user_id)
            assert await manager.get_registered_user_role(account_id, admin_user_id) == Role.ADMIN
            if issue_user_keys:
                assert (await manager.resolve_identity(key)).user_id == admin_user_id
        else:
            with pytest.raises(InvalidArgumentError, match="user_id.*255"):
                await manager.create_account(account_id, admin_user_id)
            assert await manager.get_account(account_id) is None
            assert await manager.get_user(account_id, admin_user_id) is None
    finally:
        await manager.close()


@pytest.mark.parametrize("issue_user_keys", [True, False], ids=["api-key", "trusted"])
@pytest.mark.parametrize("length", [255, 256, 1024])
async def test_file_user_creation_respects_user_id_limit(issue_user_keys, length):
    agfs = InMemoryAGFS()
    manager = build_api_key_manager(
        "root", SimpleNamespace(agfs=agfs), issue_user_keys=issue_user_keys
    )
    await manager.load()
    user_id = "u" * length
    try:
        await manager.create_account("acme", "alice")
        if length <= 255:
            key = await manager.register_user("acme", user_id)
            assert await manager.get_registered_user_role("acme", user_id) == Role.USER
            if issue_user_keys:
                assert (await manager.resolve_identity(key)).user_id == user_id
            else:
                assert key is None
        else:
            with pytest.raises(InvalidArgumentError, match="user_id.*255"):
                await manager.register_user("acme", user_id)
            assert await manager.get_user("acme", user_id) is None
    finally:
        await manager.close()


async def test_file_trusted_batch_rejects_oversized_account_before_any_write():
    agfs = InMemoryAGFS()
    manager = build_api_key_manager(
        "root",
        SimpleNamespace(agfs=agfs),
        issue_user_keys=False,
    )
    await manager.load()
    oversized_account_id = "a" * 256
    before = dict(agfs._files)
    try:
        with pytest.raises(InvalidArgumentError, match="account_id.*255.*UTF-8 bytes"):
            await manager.ensure_trusted_identities(
                {
                    "valid-account": {"alice"},
                    oversized_account_id: {"bob"},
                }
            )

        assert agfs._files == before
        assert await manager.get_account("valid-account") is None
        assert await manager.get_account(oversized_account_id) is None
    finally:
        await manager.close()


@pytest.mark.parametrize("operation", ["account", "user"])
async def test_file_creation_is_atomic_when_credential_write_fails(operation):
    class FailingAGFS(InMemoryAGFS):
        fail_user_write = False

        def write(self, path, content, ctx=None):
            if self.fail_user_write and path == "/local/acme/_system/users.json":
                raise OSError("credential store unavailable")
            return super().write(path, content, ctx)

    agfs = FailingAGFS()
    store = FileStore(SimpleNamespace(agfs=agfs))
    manager = APIKeyManager("root", store)
    await manager.load()
    if operation == "user":
        admin_key = await manager.create_account("acme", "alice")
    agfs.fail_user_write = True

    try:
        with pytest.raises(OSError, match="credential store unavailable"):
            if operation == "account":
                await manager.create_account("acme", "alice")
            else:
                await manager.register_user("acme", "bob")
        assert await manager.get_user("acme", "bob") is None
        if operation == "account":
            assert await manager.get_account("acme") is None
        else:
            assert (await manager.resolve_identity(admin_key)).user_id == "alice"
    finally:
        await manager.close()


async def test_regenerate_key_preserves_management_not_found_errors():
    manager = build_api_key_manager("root", SimpleNamespace(agfs=InMemoryAGFS()))
    await manager.load()
    await manager.create_account("acme", "alice")

    with pytest.raises(NotFoundError, match="Account not found"):
        await manager.regenerate_key("missing", "alice")
    with pytest.raises(NotFoundError, match="User not found"):
        await manager.regenerate_key("acme", "missing")


async def test_file_hashing_never_persists_plaintext_credentials():
    agfs = InMemoryAGFS()
    manager = build_api_key_manager(
        "root",
        SimpleNamespace(agfs=agfs),
        api_key_hashing_enabled=True,
    )
    await manager.load()
    account_key = await manager.create_account("acme", "alice")
    user_key = await manager.register_user("acme", "bob")
    rotated_key = await manager.regenerate_key("acme", "bob")

    persisted = agfs.read("/local/acme/_system/users.json")
    for key in (account_key, user_key, rotated_key):
        assert key.encode() not in persisted
    assert (await manager.resolve_identity(account_key)).user_id == "alice"
    assert (await manager.resolve_identity(rotated_key)).user_id == "bob"
    with pytest.raises(UnauthenticatedError):
        await manager.resolve_identity(user_key)


async def test_file_load_migrates_plaintext_credential_to_argon2():
    agfs = InMemoryAGFS()
    plaintext = build_api_key_manager("root", SimpleNamespace(agfs=agfs))
    await plaintext.load()
    key = await plaintext.create_account("acme", "alice")

    hashed = build_api_key_manager(
        "root",
        SimpleNamespace(agfs=agfs),
        api_key_hashing_enabled=True,
    )
    await hashed.load()

    assert key.encode() not in agfs.read("/local/acme/_system/users.json")
    assert (await hashed.resolve_identity(key)).user_id == "alice"


async def test_file_management_user_read_refreshes_only_explicitly():
    fs = SimpleNamespace(agfs=InMemoryAGFS())
    writer = build_api_key_manager("root", fs)
    reader = build_api_key_manager("root", fs)
    await writer.load()
    await reader.load()
    try:
        await writer.create_account("acme", "alice")

        assert not await reader.has_user("acme", "alice")
        assert await reader.get_user_for_management("acme", "alice") == {
            "user_id": "alice",
            "role": Role.ADMIN,
        }
        assert await reader.has_user("acme", "alice")
    finally:
        await reader.close()
        await writer.close()


async def test_file_user_deletion_fence_survives_concurrent_role_update():
    class PausingAGFS(InMemoryAGFS):
        def __init__(self):
            super().__init__()
            self.pause_next_user_read = False
            self.user_read_started = threading.Event()
            self.allow_user_read = threading.Event()

        def read(self, path, ctx=None):
            if path == "/local/acme/_system/users.json" and self.pause_next_user_read:
                self.pause_next_user_read = False
                self.user_read_started.set()
                self.allow_user_read.wait()
            return super().read(path, ctx)

    agfs = PausingAGFS()
    store = FileStore(SimpleNamespace(agfs=agfs))
    await store.load()
    await store.create_account_with_api_key("acme", "alice", "alice-key")
    await store.create_user("acme", "bob", "user")

    agfs.pause_next_user_read = True
    set_role = asyncio.create_task(store.set_role("acme", "bob", "admin"))
    await asyncio.wait_for(asyncio.to_thread(agfs.user_read_started.wait), timeout=1)
    deleting = asyncio.create_task(
        store.begin_deletion(
            "acme",
            "bob",
            task_id="delete",
            owner_account_id="system",
            owner_user_id="system",
        )
    )
    await asyncio.sleep(0)
    agfs.allow_user_read.set()
    try:
        await set_role
        deletion, created = await deleting
    finally:
        agfs.allow_user_read.set()

    assert created
    assert deletion["task_id"] == "delete"
    assert (await store.get_deletion("acme", "bob"))["task_id"] == "delete"


def test_mysql_uses_service_name_instead_of_resource_id_env(monkeypatch):
    monkeypatch.setenv("OV_RESOURCE_ID", "ignored-resource")
    store = MySQLAccountStore(
        service_name="account-cluster",
        params={"user": "unused", "password": "unused", "database": "unused"},
    )
    assert store._service_name == "account-cluster"


def test_mysql_service_name_must_fit_schema():
    params = {"user": "unused", "password": "unused", "database": "unused"}
    MySQLAccountStore(service_name="r" * 255, params=params)

    with pytest.raises(InvalidArgumentError, match="service_name must not exceed 255"):
        MySQLAccountStore(service_name="r" * 256, params=params)


@pytest.mark.parametrize(
    "operation",
    [
        pytest.param(
            lambda store, value: store.create_account_with_api_key(value, "admin", "key"),
            id="account",
        ),
        pytest.param(
            lambda store, value: store.create_account_with_api_key("account", value, "key"),
            id="account-admin",
        ),
        pytest.param(
            lambda store, value: store.create_user("account", value, "user"),
            id="user",
        ),
        pytest.param(
            lambda store, value: store.ensure_trusted_identities({value: {"user"}}),
            id="trusted-account",
        ),
        pytest.param(
            lambda store, value: store.ensure_trusted_identities({"account": {value}}),
            id="trusted-user",
        ),
        pytest.param(
            lambda store, value: store.create_group("account", value),
            id="group",
        ),
        pytest.param(
            lambda store, value: store.begin_deletion(
                "account",
                None,
                task_id="task",
                owner_account_id=value,
                owner_user_id="owner",
            ),
            id="deletion-owner-account",
        ),
        pytest.param(
            lambda store, value: store.begin_deletion(
                "account",
                None,
                task_id="task",
                owner_account_id="owner",
                owner_user_id=value,
            ),
            id="deletion-owner-user",
        ),
    ],
)
async def test_mysql_identifiers_must_fit_schema(operation):
    store = MySQLAccountStore(
        service_name="test-service",
        params={"user": "unused", "password": "unused", "database": "unused"},
    )

    with pytest.raises(InvalidArgumentError, match="must not exceed 255 characters"):
        await operation(store, "a" * 256)


def test_mysql_rejects_removed_table_parameter():
    with pytest.raises(InvalidArgumentError, match="Unknown MySQL account store parameters: table"):
        MySQLAccountStore(
            service_name="test-service",
            params={
                "user": "unused",
                "password": "unused",
                "database": "unused",
                "table": "tenant_accounts",
            },
        )


async def test_mysql_load_rejects_incomplete_schema_and_disposes_engine(monkeypatch):
    store = MySQLAccountStore(
        service_name="test-service",
        params={"user": "unused", "password": "unused", "database": "unused"},
    )

    class FakeEngine:
        disposed = False

        async def dispose(self):
            self.disposed = True

    engine = FakeEngine()

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def execute(self, statement):
            raise OperationalError(str(statement), {}, Exception(1146, "Table doesn't exist"))

    monkeypatch.setattr(mysql_store_module, "create_async_engine", lambda *args, **kwargs: engine)
    monkeypatch.setattr(
        mysql_store_module,
        "async_sessionmaker",
        lambda *args, **kwargs: FakeSession,
    )

    with pytest.raises(RuntimeError, match="schema is missing or incompatible"):
        await store.load()

    assert engine.disposed


async def test_file_store_rejects_deleting_nonempty_group():
    store = _file_store()
    await store.load()
    await store.create_account_with_api_key("acme", "alice", "alice-key")
    await store.create_group("acme", "engineering")
    await store.add_group_member("acme", "engineering", "alice")

    with pytest.raises(FailedPreconditionError, match="Group must be empty"):
        await store.delete_group("acme", "engineering")
    assert await store.get_group_members("acme", "engineering") == ["alice"]


async def test_mysql_managers_share_authoritative_state():
    raw = os.environ.get("OV_TEST_MYSQL")
    if not raw:
        pytest.skip("Set OV_TEST_MYSQL to connection JSON for the MySQL test")
    service_name = f"test-{uuid4().hex}"
    encryption_key = base64.urlsafe_b64encode(b"k" * 32).decode()
    params = {
        **json.loads(raw),
        "credential_encryption_key": encryption_key,
    }
    first = build_api_key_manager(
        "root",
        None,
        service_name=service_name,
        account_store_provider="mysql",
        account_store_params={**params, "credential_storage": "encrypted"},
    )
    second = build_api_key_manager(
        "root",
        None,
        service_name=service_name,
        account_store_provider="mysql",
        account_store_params={**params, "credential_storage": "hash"},
    )
    isolated_service_name = f"test-{uuid4().hex}"
    isolated = build_api_key_manager(
        "root",
        None,
        service_name=isolated_service_name,
        account_store_provider="mysql",
        account_store_params={**params, "credential_storage": "hash"},
    )
    setup_engine = create_async_engine(first._store._url, **first._store._engine_options)
    async with setup_engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
        await connection.execute(
            delete(Account).where(Account.service_name.in_([service_name, isolated_service_name]))
        )
    await setup_engine.dispose()
    await first.load()
    await second.load()
    await isolated.load()
    try:
        trusted_results = await asyncio.gather(
            first.ensure_trusted_identities({"trusted": {"alice", "bob"}}),
            second.ensure_trusted_identities({"trusted": {"bob", "carol"}}),
        )
        assert sum(result["created_accounts"] for result in trusted_results) == 1
        assert sum(result["created_users"] for result in trusted_results) == 3
        assert await first.get_registered_user_role("trusted", "alice") == Role.USER
        assert await second.get_registered_user_role("trusted", "bob") == Role.USER
        assert await first.get_registered_user_role("trusted", "carol") == Role.USER
        assert (await first.get_account("trusted"))["user_count"] == 3
        repeat = await second.ensure_trusted_identities({"trusted": {"alice", "bob", "carol"}})
        assert repeat == {"created_accounts": 0, "created_users": 0}

        deletion, created = await first.begin_deletion(
            "trusted",
            "carol",
            task_id="delete-carol",
            owner_account_id="system",
            owner_user_id="system",
        )
        assert created
        assert await second.get_deletion("trusted", "carol") == deletion
        account_deletion, account_created = await first.begin_deletion(
            "trusted",
            None,
            task_id="delete-trusted",
            owner_account_id="system",
            owner_user_id="system",
        )
        assert account_created
        assert await second.get_deletion("trusted", "carol") == account_deletion
        blocked = await second.ensure_trusted_identities({"trusted": {"late-user"}})
        assert blocked == {"created_accounts": 0, "created_users": 0}
        assert await first.get_registered_user_role("trusted", "late-user") is None

        key = await first.create_account("acme", "alice")
        assert (await second.resolve_identity(key)).role == Role.ADMIN
        user_key = await first.register_user("acme", "dave")
        assert (await second.resolve_identity(user_key)).role == Role.USER
        hashed_key = await second.create_account("hashed", "bob")
        assert (await first.resolve_identity(hashed_key)).role == Role.ADMIN
        rotated = await first.regenerate_key("acme", "alice")
        assert (await second.resolve_identity(rotated)).role == Role.ADMIN
        with pytest.raises(UnauthenticatedError):
            await second.resolve_identity(key)
        rotation_keys = [key, rotated]
        for _ in range(11):
            rotation_keys.append(await first.regenerate_key("acme", "alice"))
        assert (await second.resolve_identity(rotation_keys[-1])).role == Role.ADMIN
        async with first._store._session_factory() as session:
            credentials = list(
                (
                    await session.scalars(
                        select(Credential)
                        .join(
                            User,
                            and_(
                                User.service_name == Credential.service_name,
                                User.user_pk == Credential.user_pk,
                            ),
                        )
                        .join(
                            Account,
                            and_(
                                Account.service_name == User.service_name,
                                Account.account_pk == User.account_pk,
                            ),
                        )
                        .where(
                            Credential.service_name == service_name,
                            Account.account_id == "acme",
                            User.user_id == "alice",
                        )
                        .order_by(Credential.sequence)
                    )
                ).all()
            )
        assert len(credentials) == 11
        assert [credential.sequence for credential in credentials] == list(range(3, 14))
        assert sum(not credential.revoked for credential in credentials) == 1
        with pytest.raises(UnauthenticatedError):
            await second.resolve_identity(rotation_keys[1])
        await first.create_group("acme", "engineering")
        await first.add_group_member("acme", "engineering", "alice")
        assert await second.get_user_group_ids("acme", "alice") == ("engineering",)

        assert await isolated.get_account("acme") is None
        isolated_key = await isolated.create_account("acme", "alice")
        assert (await isolated.resolve_identity(isolated_key)).role == Role.ADMIN
        with pytest.raises(UnauthenticatedError):
            await isolated.resolve_identity(rotated)
        with pytest.raises(UnauthenticatedError):
            await first.resolve_identity(isolated_key)
        await isolated.register_user("acme", "dave")
        await isolated.set_role("acme", "dave", Role.ADMIN)
        assert await isolated.get_registered_user_role("acme", "dave") == Role.ADMIN
        assert await first.get_registered_user_role("acme", "dave") == Role.USER
    finally:
        async with first._store._engine.begin() as connection:
            await connection.execute(
                delete(Account).where(
                    Account.service_name.in_([service_name, isolated_service_name])
                )
            )
        await isolated.close()
        await second.close()
        await first.close()
