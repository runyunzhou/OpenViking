from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from openviking.storage.queuefs.semantic_msg import SemanticMsg
from openviking.storage.queuefs.semantic_processor import SemanticProcessor
from openviking_cli.utils.config.parser_config import SemanticConfig


@pytest.mark.asyncio
async def test_bad_account_config_does_not_block_healthy_account(monkeypatch):
    calls = []
    vlm = SimpleNamespace(
        is_available=lambda: True, get_completion_async=AsyncMock(return_value="# Ready")
    )

    async def get_vlm(account):
        calls.append(account)
        if account == "a":
            raise OSError("account a configuration load failed")
        return vlm

    processor = SemanticProcessor(vlm_resolver=SimpleNamespace(get_vlm=get_vlm))
    # Queue/filesystem boundaries are controlled; dequeue and model routing are real.
    processor._reenqueue_semantic_msg = AsyncMock()
    fs = SimpleNamespace(
        exists=AsyncMock(return_value=True),
        ls=AsyncMock(return_value=[{"name": "memo.txt", "isDir": False}]),
        read_file=AsyncMock(return_value=b"memory"),
    )
    module = "openviking.storage.queuefs.semantic_processor"
    monkeypatch.setattr(module + ".get_viking_fs", lambda: fs)
    monkeypatch.setattr(
        module + ".get_openviking_config",
        lambda: SimpleNamespace(semantic=SemanticConfig(), output_language="en"),
    )
    monkeypatch.setattr(
        "openviking.storage.queuefs.semantic_work.SemanticLockScope.resolve",
        AsyncMock(return_value=SimpleNamespace(lock=None, close=AsyncMock())),
    )
    monkeypatch.setattr(
        processor, "_write_memory_directory_semantics",
        AsyncMock(return_value=SimpleNamespace(wrote=False)),
    )

    def message(account):
        return SemanticMsg(
            uri="viking://user/user/memories/test", context_type="memory",
            account_id=account, user_id="user", skip_vectorization=True,
        ).to_dict()

    assert (await processor.on_dequeue(message("b"))).outcome.value == "success"
    for _ in range(5):
        assert (await processor.on_dequeue(message("a"))).outcome.value == "requeued"
    before = vlm.get_completion_async.await_count
    calls.clear()
    assert (await processor.on_dequeue(message("b"))).outcome.value == "success"
    assert calls and set(calls) == {"b"}
    assert vlm.get_completion_async.await_count > before
    calls.clear()
    assert (await processor.on_dequeue(message("a"))).outcome.value == "requeued"
    assert not calls
