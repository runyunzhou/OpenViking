import asyncio

import pytest

from openviking.models.embedder.base import CompositeHybridEmbedder, EmbedderBase, EmbedResult


class ControlledChild(EmbedderBase):
    def __init__(self):
        super().__init__("test", {})
        self.started = asyncio.Event()
        self.finish = asyncio.Event()
        self.stopped = asyncio.Event()
        self.error = None

    def embed(self, content, is_query=False):
        raise AssertionError("async expected")

    async def embed_async(self, content, is_query=False):
        self.started.set()
        try:
            await self.finish.wait()
            if self.error:
                raise self.error
            return EmbedResult(dense_vector=[1.0], sparse_vector={"x": 1.0})
        finally:
            self.stopped.set()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_composite_waits_for_children_before_returning_error(cancel):
    dense, sparse = ControlledChild(), ControlledChild()
    composite = CompositeHybridEmbedder(dense, sparse)
    task = asyncio.create_task(composite.embed_async("text"))
    await dense.started.wait()
    await sparse.started.wait()
    if cancel:
        task.cancel()
    else:
        dense.error = ValueError("dense failed")
        dense.finish.set()
    for _ in range(10):
        await asyncio.sleep(0)
    try:
        # Returning means both children have stopped, including cancellation cleanup.
        assert not task.done() or sparse.stopped.is_set()
    finally:
        sparse.finish.set()
        dense.finish.set()
        with pytest.raises(asyncio.CancelledError if cancel else ValueError):
            await task
    assert dense.stopped.is_set() and sparse.stopped.is_set()
