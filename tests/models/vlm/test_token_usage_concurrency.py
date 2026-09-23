import threading
from concurrent.futures import ThreadPoolExecutor

from openviking.models.vlm.token_usage import TokenUsage, TokenUsageTracker


def test_merge_accumulates_counts_and_preserves_latest_timestamp():
    trackers = [TokenUsageTracker() for _ in range(3)]
    for tracker, calls in zip(trackers, (3, 2, 1), strict=True):
        for _ in range(calls):
            tracker.update("same", "openai", 10, 1)
    snapshots = [tracker.to_dict() for tracker in trackers]
    merged = TokenUsageTracker.merge(*trackers).to_dict()
    counts = merged["usage_by_model"]["same"]["usage_by_provider"]["openai"]
    assert counts["call_count"] == 6
    assert counts["total_tokens"] == 66
    assert counts["last_updated"] == max(
        snapshot["usage_by_model"]["same"]["usage_by_provider"]["openai"]["last_updated"]
        for snapshot in snapshots
    )
    assert merged["total_usage"]["call_count"] == 6
    assert merged["usage_by_model"]["same"]["total_usage"]["call_count"] == 6
    assert [tracker.to_dict() for tracker in trackers] == snapshots


def test_snapshot_serializes_model_update(monkeypatch):
    tracker = TokenUsageTracker()
    tracker.update("old", "openai", 1, 0)
    entered, finish, attempted = threading.Event(), threading.Event(), threading.Event()
    original = TokenUsage.to_dict

    def paused(usage):
        entered.set()
        assert finish.wait(5)
        return original(usage)

    def write():
        attempted.set()
        tracker.update("new", "openai", 2, 0)

    monkeypatch.setattr(TokenUsage, "to_dict", paused)
    with ThreadPoolExecutor(2) as pool:
        snapshot = pool.submit(tracker.to_dict)
        try:
            assert entered.wait(5)
            writer = pool.submit(write)
            assert attempted.wait(5)
            # The writer cannot mutate model membership in the middle of a snapshot.
            assert not writer.done()
        finally:
            finish.set()
        value = snapshot.result(5)
        writer.result(5)
    assert value["total_usage"]["total_tokens"] == 1
    assert set(value["usage_by_model"]) == {"old"}
    assert tracker.to_dict()["total_usage"]["total_tokens"] == 3
