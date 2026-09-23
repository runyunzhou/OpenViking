import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from openviking.server.identity import RequestContext, Role
from openviking.storage.viking_vector_index_backend import VikingVectorIndexBackend
from openviking_cli.session.user_id import UserIdentifier
from openviking_cli.utils.config.vectordb_config import VectorDBBackendConfig


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["upsert", "upsert_many", "delete"])
@pytest.mark.parametrize("shutdown", [False, True])
async def test_release_waits_for_complete_backend_operation(monkeypatch, operation, shutdown):
    entered, finish = threading.Event(), threading.Event()
    shared = Mock(mode="local", USE_CONTENT_FIELD=False)
    dedicated = Mock(mode="vikingdb", USE_CONTENT_FIELD=True)
    config = VectorDBBackendConfig(backend="local", dimension=2)
    remote = VectorDBBackendConfig(
        backend="vikingdb", dimension=2, vikingdb={"host": "https://account.invalid"}
    )

    def meta():
        entered.set()
        assert finish.wait(5)
        return {"Fields": [{"FieldName": name} for name in ("id", "vector", "account_id")]}

    def get(ids):
        meta()
        return [{"id": value, "account_id": "a"} for value in ids]

    dedicated.get_collection.return_value.get_meta_data.side_effect = meta
    dedicated.get.side_effect = get
    dedicated.upsert.side_effect = lambda data: (
        [row["id"] for row in data] if isinstance(data, list) else [data["id"]]
    )
    dedicated.delete.return_value = 1
    factory = Mock(side_effect=[shared, dedicated])
    monkeypatch.setattr(
        "openviking.storage.viking_vector_index_backend.create_collection_adapter", factory
    )
    store = VikingVectorIndexBackend(config)
    store.set_vector_config_resolver(SimpleNamespace(
        resolve=AsyncMock(return_value=SimpleNamespace(dedicated_vectordb=True, vectordb=remote))
    ))
    ctx = RequestContext(UserIdentifier("a", "user"), Role.ROOT)
    backend = await store.get_account_backend("a")
    row = {"id": "r", "vector": [1.0, 0.0]}
    args = row if operation == "upsert" else [row] if operation == "upsert_many" else ["r"]
    pending = asyncio.create_task(getattr(store, operation)(args, ctx=ctx))
    retiring = None
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        retiring = asyncio.create_task(store.close() if shutdown else store.release_account("a"))
        for _ in range(100):
            if store.is_closing if shutdown else "a" not in store._resolved_backends:
                break
            await asyncio.sleep(0)
        assert store.is_closing if shutdown else "a" not in store._resolved_backends
        dedicated.close.assert_not_called()
        finish.set()
        result = await pending
        assert result == ("r" if operation == "upsert" else ["r"] if operation == "upsert_many" else 1)
        await retiring
        dedicated.close.assert_called_once()
        assert backend.is_closing
    finally:
        finish.set()
        await asyncio.gather(pending, *([retiring] if retiring else []), return_exceptions=True)
        await store.close()
