# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

import asyncio
import base64

import pytest

from openviking.server.account_stores.redis_mysql import RedisMySQLAccountStore
from openviking.server.api_keys.new import APIKeyManager
from openviking.server.identity import Role
from openviking_cli.exceptions import InvalidArgumentError, UnauthenticatedError


class FakeRedis:
    def __init__(self):
        self.values: dict[str, str] = {}
        self.zsets: dict[str, dict[str, int]] = {}
        self.now_ms = 0

    async def ping(self):
        return True

    async def get(self, key: str):
        return self.values.get(key)

    async def set(self, key: str, value, *, ex=None, nx=False, px=None):
        del ex, px
        if nx and key in self.values:
            return False
        self.values[key] = str(value)
        return True

    async def delete(self, *keys: str):
        removed = 0
        for key in keys:
            removed += self.values.pop(key, None) is not None
            removed += self.zsets.pop(key, None) is not None
        return removed

    async def zrange(self, key: str, start: int, end: int):
        members = sorted(self.zsets.get(key, {}).items(), key=lambda item: (item[1], item[0]))
        if end == -1:
            return [member for member, _ in members[start:]]
        return [member for member, _ in members[start : end + 1]]

    def _prune_zset(self, key: str):
        members = self.zsets.get(key, {})
        for member, score in tuple(members.items()):
            if score <= self.now_ms:
                del members[member]
        if not members:
            self.zsets.pop(key, None)

    async def eval(self, script: str, numkeys: int, *args):
        keys = args[:numkeys]
        values = args[numkeys:]
        if "-- release-lock" in script:
            if self.values.get(keys[0]) == values[0]:
                return await self.delete(keys[0])
            return 0
        if "-- get-if-locked" in script:
            if self.values.get(keys[0]) != values[0]:
                return [0]
            value = self.values.get(keys[1])
            return [1] if value is None else [2, value]
        if "-- set-if-locked" in script:
            if self.values.get(keys[0]) != values[0]:
                return 0
            self.values[keys[1]] = values[1]
            if str(values[4]) == "1":
                self._prune_zset(keys[2])
                self.zsets.setdefault(keys[2], {})[keys[1]] = self.now_ms + int(values[2]) * 1000
            return 1
        if "-- delete-and-unindex" in script:
            await self.delete(*keys[1:])
            members = self.zsets.get(keys[0], {})
            for key in keys[1:]:
                members.pop(key, None)
            if not members:
                self.zsets.pop(keys[0], None)
            return len(keys) - 1
        if "-- prune-cache-index" in script:
            self._prune_zset(keys[0])
            return 0
        raise AssertionError("Unknown Lua script")


class FakeMySQLAuthority:
    def __init__(self, api_key: str, resource_id: str = "test-resource"):
        self._resource_id = resource_id
        self.api_key = api_key
        self.account_exists = True
        self.user_exists = True
        self.user = {"user_id": "alice", "role": Role.ADMIN}
        self.group_ids = ("engineering",)
        self.account_deletion = None
        self.user_deletion = None

    async def load(self):
        return None

    async def close(self):
        return None

    async def verify_api_key(
        self,
        api_key: str,
        *,
        account_id_hint: str | None = None,
        user_id_hint: str | None = None,
    ):
        return (
            ("acme", "alice")
            if (
                self.account_exists
                and self.user_exists
                and api_key == self.api_key
                and account_id_hint in (None, "acme")
                and user_id_hint in (None, "alice")
            )
            else None
        )

    async def get_user(self, account_id: str, user_id: str):
        if (
            not self.account_exists
            or not self.user_exists
            or (account_id, user_id) != ("acme", "alice")
        ):
            return None
        return dict(self.user)

    async def get_user_group_ids(self, account_id: str, user_id: str):
        return (
            self.group_ids
            if self.account_exists and (account_id, user_id) == ("acme", "alice")
            else ()
        )

    async def get_deletion(self, account_id: str, user_id: str | None = None):
        if not self.account_exists or account_id != "acme":
            return None
        if user_id is None:
            return self.account_deletion
        return self.account_deletion or self.user_deletion

    async def replace_active_user_api_key(self, account_id: str, user_id: str, api_key: str):
        assert (account_id, user_id) == ("acme", "alice")
        self.api_key = api_key

    async def create_account_with_api_key(
        self,
        account_id: str,
        admin_user_id: str,
        api_key: str,
    ):
        assert (account_id, admin_user_id) == ("acme", "alice")
        self.account_exists = True
        self.user_exists = True
        self.api_key = api_key
        self.user = {"user_id": "alice", "role": Role.ADMIN}
        self.group_ids = ()
        self.account_deletion = None
        self.user_deletion = None

    async def create_user_with_api_key(
        self,
        account_id: str,
        user_id: str,
        role: str,
        api_key: str,
    ):
        assert (account_id, user_id) == ("acme", "alice")
        self.user_exists = True
        self.api_key = api_key
        self.user = {"user_id": user_id, "role": role}

    async def delete_account(self, account_id: str):
        assert account_id == "acme"
        self.account_exists = False

    async def revoke_active_api_keys(self, account_id: str, user_id: str | None = None):
        assert account_id == "acme"
        self.api_key = ""

    async def set_role(self, account_id: str, user_id: str, role: str):
        assert (account_id, user_id) == ("acme", "alice")
        self.user["role"] = role

    async def add_group_member(self, account_id: str, group_id: str, user_id: str):
        assert (account_id, user_id) == ("acme", "alice")
        self.group_ids = tuple(sorted(set(self.group_ids) | {group_id}))
        return True

    async def begin_deletion(self, account_id: str, user_id: str | None, **_kwargs):
        assert (account_id, user_id) == ("acme", "alice")
        self.user_deletion = {"task_id": "delete-1"}
        return self.user_deletion, True


def _store(
    api_key: str, *, redis_client=None, lock_wait_seconds: int = 30, resource_id="test-resource"
) -> tuple[RedisMySQLAccountStore, FakeMySQLAuthority]:
    mysql = FakeMySQLAuthority(api_key, resource_id)
    store = RedisMySQLAccountStore(
        params={
            "redis": {
                "url": "redis://unused",
                "ttl_seconds": 60,
                "lock_ttl_seconds": 60,
                "lock_wait_seconds": lock_wait_seconds,
            }
        },
        mysql_store=mysql,
        redis_client=redis_client if redis_client is not None else FakeRedis(),
    )
    return store, mysql


def _api_key(secret: str = "secret") -> str:
    encoded_account = base64.urlsafe_b64encode(b"acme").decode().rstrip("=")
    encoded_user = base64.urlsafe_b64encode(b"alice").decode().rstrip("=")
    encoded_secret = base64.urlsafe_b64encode(secret.encode()).decode().rstrip("=")
    return f"{encoded_account}.{encoded_user}.{encoded_secret}"


def test_redis_timeouts_must_be_positive_integers():
    mysql = FakeMySQLAuthority(_api_key())
    for name, value in (
        ("ttl_seconds", 1.5),
        ("lock_ttl_seconds", 1.5),
        ("lock_wait_seconds", 1.5),
        ("ttl_seconds", True),
        ("lock_ttl_seconds", 0),
        ("lock_wait_seconds", -1),
    ):
        with pytest.raises(InvalidArgumentError, match=f"redis.{name} must be a positive integer"):
            RedisMySQLAccountStore(
                params={"redis": {"url": "redis://unused", name: value}},
                mysql_store=mysql,
                redis_client=FakeRedis(),
            )


async def test_resources_do_not_share_cached_identities():
    redis = FakeRedis()
    first_key, second_key = _api_key("first"), _api_key("second")
    first, _ = _store(first_key, redis_client=redis, resource_id="first")
    second, _ = _store(second_key, redis_client=redis, resource_id="second")
    await first.load()
    await second.load()
    try:
        assert await first.verify_api_key(first_key, account_id_hint="acme") == ("acme", "alice")
        assert await second.verify_api_key(first_key, account_id_hint="acme") is None
        assert await second.verify_api_key(second_key, account_id_hint="acme") == ("acme", "alice")
        await first.get_user("acme", "alice")
        await second.set_role("acme", "alice", Role.USER)
        assert (await first.get_user("acme", "alice"))["role"] == Role.ADMIN
        assert (await second.get_user("acme", "alice"))["role"] == Role.USER
    finally:
        await first.close()
        await second.close()


async def test_cached_binding_respects_user_id_hint():
    api_key = _api_key()
    store, _ = _store(api_key)
    await store.load()

    assert await store.verify_api_key(
        api_key,
        account_id_hint="acme",
        user_id_hint="alice",
    ) == ("acme", "alice")
    assert (
        await store.verify_api_key(
            api_key,
            account_id_hint="acme",
            user_id_hint="bob",
        )
        is None
    )
    assert await store.verify_api_key(
        api_key,
        account_id_hint="acme",
        user_id_hint="alice",
    ) == ("acme", "alice")


async def test_cached_identity_survives_authority_read_failure(monkeypatch):
    api_key = _api_key()
    store, mysql = _store(api_key)
    await store.load()
    manager = APIKeyManager("root", store)

    first = await manager.resolve_identity(api_key)

    async def unavailable(*_args, **_kwargs):
        raise ConnectionError("MySQL unavailable")

    for method in ("verify_api_key", "get_user", "get_deletion"):
        monkeypatch.setattr(mysql, method, unavailable)
    second = await manager.resolve_identity(api_key)

    assert (first.account_id, first.user_id, first.role) == ("acme", "alice", Role.ADMIN)
    assert second == first


async def test_recreated_account_does_not_inherit_cached_identity():
    api_key = _api_key()
    store, _ = _store(api_key)
    await store.load()
    manager = APIKeyManager("root", store)

    assert (await manager.resolve_identity(api_key)).role == Role.ADMIN
    assert await store.get_user_group_ids("acme", "alice") == ("engineering",)
    await store.begin_deletion(
        "acme", "alice", task_id="delete-1", owner_account_id="acme", owner_user_id="alice"
    )
    with pytest.raises(UnauthenticatedError):
        await manager.resolve_identity(api_key)

    await store.delete_account("acme")
    assert await store.get_user("acme", "alice") is None
    with pytest.raises(UnauthenticatedError):
        await manager.resolve_identity(api_key)

    new_key = _api_key("recreated")
    await store.create_account_with_api_key("acme", "alice", new_key)
    assert (await manager.resolve_identity(new_key)).role == Role.ADMIN
    assert await store.get_user_group_ids("acme", "alice") == ()
    assert await store.get_deletion("acme", "alice") is None
    with pytest.raises(UnauthenticatedError):
        await manager.resolve_identity(api_key)


async def test_user_group_and_deletion_reads_are_cached_and_invalidated():
    api_key = _api_key()
    store, _ = _store(api_key)
    await store.load()

    assert await store.get_user("acme", "alice") == {"user_id": "alice", "role": Role.ADMIN}
    await store.set_role("acme", "alice", Role.USER)
    assert await store.get_user("acme", "alice") == {"user_id": "alice", "role": Role.USER}

    assert await store.get_user_group_ids("acme", "alice") == ("engineering",)
    await store.add_group_member("acme", "product", "alice")
    assert await store.get_user_group_ids("acme", "alice") == ("engineering", "product")

    assert await store.get_deletion("acme", "alice") is None
    await store.begin_deletion(
        "acme",
        "alice",
        task_id="delete-1",
        owner_account_id="acme",
        owner_user_id="alice",
    )
    assert await store.get_deletion("acme", "alice") == {"task_id": "delete-1"}


async def test_deleting_recreated_account_rejects_key_after_delayed_cleanup():
    api_key = _api_key()
    redis = FakeRedis()
    store, _ = _store(api_key, redis_client=redis, lock_wait_seconds=1)
    await store.load()
    original_zrange = redis.zrange
    saw_empty = asyncio.Event()
    resume_clear = asyncio.Event()
    paused = False

    async def delayed_empty_reply(*args):
        nonlocal paused
        result = await original_zrange(*args)
        if not result and not paused:
            paused = True
            saw_empty.set()
            await resume_clear.wait()
        return result

    redis.zrange = delayed_empty_reply
    old_clear = asyncio.create_task(store.delete_account("acme"))
    try:
        await asyncio.wait_for(saw_empty.wait(), 2)
        await asyncio.wait_for(store.create_account_with_api_key("acme", "alice", api_key), 5)
        manager = APIKeyManager("root", store)
        assert (await manager.resolve_identity(api_key)).role == Role.ADMIN

        resume_clear.set()
        await asyncio.wait_for(old_clear, 2)

        await store.delete_account("acme")
        with pytest.raises(UnauthenticatedError, match="Invalid API key"):
            await manager.resolve_identity(api_key)
    finally:
        resume_clear.set()
        await asyncio.gather(old_clear, return_exceptions=True)
        await store.close()


@pytest.mark.parametrize("operation", ["create_account", "create_user", "replace_key"])
@pytest.mark.parametrize("user_hint", [None, "alice"])
async def test_provisioned_key_is_immediately_usable(operation, user_hint):
    old_key = _api_key("old")
    new_key = _api_key("new")
    store, mysql = _store(old_key)
    if operation == "create_account":
        mysql.account_exists = False
        mysql.user_exists = False
    elif operation == "create_user":
        mysql.user_exists = False
    await store.load()
    try:
        if operation == "replace_key":
            assert await store.verify_api_key(
                old_key, account_id_hint="acme", user_id_hint=user_hint
            ) == ("acme", "alice")
        assert (
            await store.verify_api_key(new_key, account_id_hint="acme", user_id_hint=user_hint)
            is None
        )
        if operation == "create_account":
            await store.create_account_with_api_key("acme", "alice", new_key)
        elif operation == "create_user":
            await store.create_user_with_api_key("acme", "alice", Role.USER, new_key)
        else:
            await store.replace_active_user_api_key("acme", "alice", new_key)
        assert await store.verify_api_key(
            new_key, account_id_hint="acme", user_id_hint=user_hint
        ) == ("acme", "alice")
        assert (
            await store.verify_api_key(old_key, account_id_hint="acme", user_id_hint=user_hint)
            is None
        )
    finally:
        await store.close()


async def test_key_rotation_rejects_old_key_after_inflight_verification():
    old_key, new_key = _api_key("old"), _api_key("new")
    store, mysql = _store(old_key, lock_wait_seconds=1)
    await store.load()
    read_started = asyncio.Event()
    resume_read = asyncio.Event()
    original_verify = mysql.verify_api_key

    async def delayed_verification(api_key, **hints):
        result = await original_verify(api_key, **hints)
        if api_key == old_key and not read_started.is_set():
            read_started.set()
            await resume_read.wait()
        return result

    mysql.verify_api_key = delayed_verification
    reader = asyncio.create_task(
        store.verify_api_key(old_key, account_id_hint="acme", user_id_hint="alice")
    )
    try:
        await asyncio.wait_for(read_started.wait(), 2)
        await asyncio.wait_for(store.replace_active_user_api_key("acme", "alice", new_key), 5)
        assert await store.verify_api_key(
            new_key, account_id_hint="acme", user_id_hint="alice"
        ) == ("acme", "alice")
        resume_read.set()
        # After the earlier request finishes, only the new key remains valid.
        await asyncio.wait_for(reader, 2)
        assert (
            await store.verify_api_key(old_key, account_id_hint="acme", user_id_hint="alice")
            is None
        )
        assert await store.verify_api_key(
            new_key, account_id_hint="acme", user_id_hint="alice"
        ) == ("acme", "alice")
    finally:
        resume_read.set()
        await asyncio.gather(reader, return_exceptions=True)
        await store.close()


@pytest.mark.parametrize("error", [ConnectionError, TimeoutError])
async def test_redis_failure_allows_mysql_reads_and_writes(error):
    redis = FakeRedis()
    store, _ = _store(_api_key(), redis_client=redis)
    await store.load()

    async def timeout(*_args, **_kwargs):
        raise error("Redis did not respond")

    redis.get = timeout
    redis.set = timeout
    redis.delete = timeout
    redis.eval = timeout
    try:
        await store.set_role("acme", "alice", Role.USER)
        assert await store.get_user("acme", "alice") == {"user_id": "alice", "role": Role.USER}
    finally:
        await store.close()
