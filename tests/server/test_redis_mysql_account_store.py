# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0

import asyncio
import base64

import pytest

from openviking.server.account_stores.redis_mysql import RedisMySQLAccountStore
from openviking.server.api_keys.new import APIKeyManager
from openviking.server.identity import Role
from openviking_cli.exceptions import InvalidArgumentError


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
                self.zsets.setdefault(keys[2], {})[keys[1]] = (
                    self.now_ms + int(values[2]) * 1000
                )
            return 1
        if "-- delete-and-unindex" in script:
            await self.delete(*keys[1:])
            members = self.zsets.get(keys[0], {})
            for key in keys[1:]:
                members.pop(key, None)
            return len(keys) - 1
        if "-- prune-cache-index" in script:
            self._prune_zset(keys[0])
            return 0
        raise AssertionError("Unknown Lua script")


class FakeMySQLAuthority:
    _resource_id = "test-resource"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.account_exists = True
        self.user_exists = True
        self.user = {"user_id": "alice", "role": Role.ADMIN}
        self.group_ids = ("engineering",)
        self.account_deletion = None
        self.user_deletion = None
        self.calls = {
            "verify": 0,
            "user": 0,
            "groups": 0,
            "account_deletion": 0,
            "user_deletion": 0,
        }

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
        self.calls["verify"] += 1
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
        self.calls["user"] += 1
        if (
            not self.account_exists
            or not self.user_exists
            or (account_id, user_id) != ("acme", "alice")
        ):
            return None
        return dict(self.user)

    async def get_user_group_ids(self, account_id: str, user_id: str):
        self.calls["groups"] += 1
        return (
            self.group_ids
            if self.account_exists and (account_id, user_id) == ("acme", "alice")
            else ()
        )

    async def get_deletion(self, account_id: str, user_id: str | None = None):
        if not self.account_exists or account_id != "acme":
            return None
        if user_id is None:
            self.calls["account_deletion"] += 1
            return self.account_deletion
        self.calls["user_deletion"] += 1
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


def _store(api_key: str) -> tuple[RedisMySQLAccountStore, FakeMySQLAuthority]:
    mysql = FakeMySQLAuthority(api_key)
    store = RedisMySQLAccountStore(
        params={
            "redis": {
                "url": "redis://unused",
                "ttl_seconds": 60,
                "lock_ttl_seconds": 60,
            }
        },
        mysql_store=mysql,
        redis_client=FakeRedis(),
    )
    return store, mysql


def _api_key(secret: str = "secret") -> str:
    encoded_account = base64.urlsafe_b64encode(b"acme").decode().rstrip("=")
    encoded_user = base64.urlsafe_b64encode(b"alice").decode().rstrip("=")
    encoded_secret = base64.urlsafe_b64encode(secret.encode()).decode().rstrip("=")
    return f"{encoded_account}.{encoded_user}.{encoded_secret}"


def test_account_keys_include_resource_in_hash_tag():
    store, _ = _store(_api_key())

    assert "{dGVzdC1yZXNvdXJjZQBhY21l}" in store._user_key("acme", "alice")
    assert "{dGVzdC1yZXNvdXJjZQBhY21l}" in store._binding_key("acme", _api_key())
    assert store._lock_wait_seconds == 30


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


async def test_verify_api_key_caches_public_binding_read():
    api_key = _api_key()
    store, mysql = _store(api_key)
    await store.load()

    assert await store.verify_api_key(api_key, account_id_hint="acme", user_id_hint="alice") == (
        "acme",
        "alice",
    )
    assert await store.verify_api_key(api_key, account_id_hint="acme", user_id_hint="alice") == (
        "acme",
        "alice",
    )

    assert mysql.calls["verify"] == 1


async def test_cached_binding_respects_user_id_hint():
    api_key = _api_key()
    store, mysql = _store(api_key)
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
    assert mysql.calls["verify"] == 2


async def test_concurrent_user_cache_misses_query_mysql_once():
    store, mysql = _store(_api_key())
    await store.load()
    read_started = asyncio.Event()
    allow_read = asyncio.Event()

    async def delayed_get_user(account_id: str, user_id: str):
        mysql.calls["user"] += 1
        read_started.set()
        await allow_read.wait()
        return {"user_id": user_id, "role": Role.ADMIN}

    mysql.get_user = delayed_get_user
    reads = [
        asyncio.create_task(store.get_user("acme", "alice"))
        for _ in range(5)
    ]
    await read_started.wait()
    await asyncio.sleep(0.05)
    assert mysql.calls["user"] == 1

    allow_read.set()
    assert await asyncio.gather(*reads) == [
        {"user_id": "alice", "role": Role.ADMIN}
    ] * 5
    assert mysql.calls["user"] == 1


async def test_expired_cache_lease_cannot_refill_cache():
    store, _ = _store(_api_key())
    await store.load()
    redis = store._redis
    key = store._user_key("acme", "alice")
    redis.values[store._lock_key("acme")] = "new-owner"

    written = await store._set_if_locked(
        "acme",
        "expired-owner",
        key,
        {"found": True, "value": {"user_id": "alice", "role": Role.ADMIN}},
    )

    assert written is False
    assert key not in redis.values


async def test_write_lock_timeout_revokes_stale_lease_before_invalidation():
    store, mysql = _store(_api_key())
    store._lock_wait_seconds = 0.05
    await store.load()
    assert await store.get_user("acme", "alice") is not None
    redis = store._redis
    redis.values[store._lock_key("acme")] = "other-owner"

    mutation = asyncio.create_task(store.set_role("acme", "alice", Role.USER))
    await asyncio.sleep(0.01)
    assert mysql.user["role"] == Role.USER
    assert not mutation.done()
    await mutation

    assert store._lock_key("acme") not in redis.values
    assert store._user_key("acme", "alice") not in redis.values
    assert not await store._set_if_locked(
        "acme",
        "other-owner",
        store._user_key("acme", "alice"),
        {"found": True, "value": {"user_id": "alice", "role": Role.ADMIN}},
    )


async def test_invalidation_failure_does_not_block_mysql_mutation():
    store, mysql = _store(_api_key())
    await store.load()
    assert await store.get_user("acme", "alice") is not None

    async def fail_invalidation(*_args):
        raise ConnectionError("redis unavailable")

    store._delete_and_unindex = fail_invalidation

    await store.set_role("acme", "alice", Role.USER)

    assert mysql.user["role"] == Role.USER
    assert store._user_key("acme", "alice") in store._redis.values


async def test_redis_outage_does_not_block_mysql_mutation():
    store, mysql = _store(_api_key())
    await store.load()

    async def unavailable(*_args, **_kwargs):
        raise ConnectionError("redis unavailable")

    store._redis.set = unavailable
    store._redis.delete = unavailable
    store._redis.eval = unavailable

    await store.set_role("acme", "alice", Role.USER)

    assert mysql.user["role"] == Role.USER


async def test_api_key_manager_uses_cached_public_store_reads():
    api_key = _api_key()
    store, mysql = _store(api_key)
    await store.load()
    manager = APIKeyManager("root", store)

    first = await manager.resolve_identity(api_key)
    second = await manager.resolve_identity(api_key)

    assert (first.account_id, first.user_id, first.role) == ("acme", "alice", Role.ADMIN)
    assert second == first
    assert mysql.calls == {
        "verify": 1,
        "user": 1,
        "groups": 0,
        "account_deletion": 1,
        "user_deletion": 1,
    }


async def test_replacing_key_invalidates_cached_binding_and_prior_negative():
    old_key = _api_key("old")
    new_key = _api_key("new")
    store, mysql = _store(old_key)
    await store.load()
    assert await store.verify_api_key(old_key, account_id_hint="acme", user_id_hint="alice")
    assert await store.verify_api_key(new_key, account_id_hint="acme", user_id_hint="alice") is None
    assert await store.verify_api_key(new_key, account_id_hint="acme", user_id_hint="alice") is None
    assert mysql.calls["verify"] == 2
    assert store._binding_key("acme", new_key) not in await store._redis.zrange(
        store._cache_keys_index_key("acme"),
        0,
        -1,
    )

    await store.replace_active_user_api_key("acme", "alice", new_key)

    assert await store.verify_api_key(old_key, account_id_hint="acme", user_id_hint="alice") is None
    assert await store.verify_api_key(new_key, account_id_hint="acme", user_id_hint="alice") == (
        "acme",
        "alice",
    )
    assert mysql.calls["verify"] == 4


async def test_creating_account_invalidates_prior_negative_key_binding():
    api_key = _api_key("new-account")
    store, mysql = _store(api_key)
    mysql.account_exists = False
    mysql.user_exists = False
    await store.load()

    assert await store.verify_api_key(
        api_key,
        account_id_hint="acme",
        user_id_hint="alice",
    ) is None

    await store.create_account_with_api_key("acme", "alice", api_key)

    assert await store.verify_api_key(
        api_key,
        account_id_hint="acme",
        user_id_hint="alice",
    ) == ("acme", "alice")


async def test_creating_user_invalidates_prior_negative_key_binding():
    api_key = _api_key("new-user")
    store, mysql = _store(api_key)
    mysql.user_exists = False
    await store.load()

    assert await store.verify_api_key(
        api_key,
        account_id_hint="acme",
        user_id_hint="alice",
    ) is None

    await store.create_user_with_api_key("acme", "alice", Role.USER, api_key)

    assert await store.verify_api_key(
        api_key,
        account_id_hint="acme",
        user_id_hint="alice",
    ) == ("acme", "alice")


async def test_account_delete_and_recreate_clear_all_indexed_cache_keys():
    api_key = _api_key()
    store, mysql = _store(api_key)
    await store.load()
    redis = store._redis

    assert await store.get_user("acme", "alice")
    assert await store.get_user_group_ids("acme", "alice") == ("engineering",)
    assert await store.get_deletion("acme", "alice") is None
    assert await store.verify_api_key(
        api_key,
        account_id_hint="acme",
        user_id_hint="alice",
    )
    index_key = store._cache_keys_index_key("acme")
    indexed_keys = set(await redis.zrange(index_key, 0, -1))
    assert store._user_key("acme", "alice") in indexed_keys
    assert store._group_ids_key("acme", "alice") in indexed_keys
    assert store._account_deletion_key("acme") in indexed_keys
    assert store._user_deletion_key("acme", "alice") in indexed_keys
    assert store._binding_key("acme", api_key) in indexed_keys
    assert store._account_credential_fence_key("acme") in indexed_keys
    assert store._user_credential_fence_key("acme", "alice") in indexed_keys

    await store.delete_account("acme")

    assert index_key not in redis.zsets
    assert not indexed_keys.intersection(redis.values)
    assert await store.get_user("acme", "alice") is None

    await store.create_account_with_api_key("acme", "alice", api_key)

    assert await store.get_user("acme", "alice") == {
        "user_id": "alice",
        "role": Role.ADMIN,
    }


async def test_refill_prunes_expired_cache_index_members():
    store, _ = _store(_api_key())
    await store.load()
    redis = store._redis
    index_key = store._cache_keys_index_key("acme")
    user_key = store._user_key("acme", "alice")
    group_key = store._group_ids_key("acme", "alice")

    assert await store.get_user("acme", "alice")
    redis.now_ms += 60_001
    assert await store.get_user_group_ids("acme", "alice") == ("engineering",)

    assert await redis.zrange(index_key, 0, -1) == [group_key]
    assert user_key not in redis.zsets[index_key]


async def test_targeted_invalidation_removes_cache_index_member():
    store, _ = _store(_api_key())
    await store.load()
    redis = store._redis
    index_key = store._cache_keys_index_key("acme")
    user_key = store._user_key("acme", "alice")

    assert await store.get_user("acme", "alice")
    assert user_key in await redis.zrange(index_key, 0, -1)

    await store.set_role("acme", "alice", Role.USER)

    assert user_key not in await redis.zrange(index_key, 0, -1)


async def test_user_group_and_deletion_reads_are_cached_and_invalidated():
    api_key = _api_key()
    store, mysql = _store(api_key)
    await store.load()

    assert await store.get_user("acme", "alice") == {"user_id": "alice", "role": Role.ADMIN}
    assert await store.get_user("acme", "alice") == {"user_id": "alice", "role": Role.ADMIN}
    await store.set_role("acme", "alice", Role.USER)
    assert await store.get_user("acme", "alice") == {"user_id": "alice", "role": Role.USER}

    assert await store.get_user_group_ids("acme", "alice") == ("engineering",)
    assert await store.get_user_group_ids("acme", "alice") == ("engineering",)
    await store.add_group_member("acme", "product", "alice")
    assert await store.get_user_group_ids("acme", "alice") == ("engineering", "product")

    assert await store.get_deletion("acme", "alice") is None
    assert await store.get_deletion("acme", "alice") is None
    await store.begin_deletion(
        "acme",
        "alice",
        task_id="delete-1",
        owner_account_id="acme",
        owner_user_id="alice",
    )
    assert await store.get_deletion("acme", "alice") == {"task_id": "delete-1"}

    assert mysql.calls == {
        "verify": 0,
        "user": 2,
        "groups": 2,
        "account_deletion": 1,
        "user_deletion": 2,
    }
