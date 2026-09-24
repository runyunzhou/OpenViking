from __future__ import annotations

import httpx
import pytest

from .model_proxy import ACCOUNT_HEADER, create_app


@pytest.fixture
async def client():
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_app(dimension=8)),
        base_url="http://model-proxy",
    ) as value:
        yield value


async def test_embedding_is_deterministic_and_records_account_usage(client):
    request = {
        "model": "test-embedding-a",
        "input": ["tenant-a-secret-001", "shared marker"],
    }
    headers = {
        ACCOUNT_HEADER: "account_a",
        "X-OV-Test-Credential": "credential-a",
    }

    first = await client.post("/v1/embeddings", json=request, headers=headers)
    second = await client.post("/v1/embeddings", json=request, headers=headers)

    assert first.status_code == 200
    assert first.json()["data"] == second.json()["data"]
    assert len(first.json()["data"][0]["embedding"]) == 8
    assert first.json()["usage"]["total_tokens"] > 0

    ledger = (await client.get("/proxy/requests")).json()["requests"]
    assert [item["account_id"] for item in ledger] == ["account_a", "account_a"]
    assert [item["retry_attempt"] for item in ledger] == [1, 2]
    assert [item["credential_marker"] for item in ledger] == [
        "credential-a",
        "credential-a",
    ]


async def test_model_account_mismatch_is_rejected(client):
    response = await client.post(
        "/v1/chat/completions",
        json={"model": "test-vlm-a", "messages": [{"role": "user", "content": "hello"}]},
        headers={ACCOUNT_HEADER: "account_b"},
    )

    assert response.status_code == 409
    assert response.json()["error"]["type"] == "account_validation"


async def test_fault_is_scoped_to_operation_and_account(client):
    await client.post(
        "/proxy/faults",
        json={
            "operation": "embedding",
            "account_id": "account_a",
            "status_code": 503,
            "count": 1,
        },
    )

    failed = await client.post(
        "/v1/embeddings",
        json={"model": "test-embedding-a", "input": "hello"},
        headers={ACCOUNT_HEADER: "account_a"},
    )
    healthy = await client.post(
        "/v1/embeddings",
        json={"model": "test-embedding-b", "input": "hello"},
        headers={ACCOUNT_HEADER: "account_b"},
    )

    assert failed.status_code == 503
    assert healthy.status_code == 200


async def test_wrong_dimension_and_missing_usage_can_be_injected(client):
    await client.post(
        "/proxy/faults",
        json={
            "operation": "embedding",
            "account_id": "account_a",
            "wrong_dimension": 3,
            "omit_usage": True,
        },
    )

    response = await client.post(
        "/v1/embeddings",
        json={"model": "test-embedding-a", "input": "hello"},
        headers={ACCOUNT_HEADER: "account_a"},
    )

    assert response.status_code == 200
    assert len(response.json()["data"][0]["embedding"]) == 3
    assert "usage" not in response.json()


async def test_chat_completion_has_openai_shape_and_usage(client):
    response = await client.post(
        "/v1/chat/completions",
        json={"model": "test-vlm-b", "messages": [{"role": "user", "content": "hello"}]},
        headers={ACCOUNT_HEADER: "account_b"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["choices"][0]["message"]["content"] == "OV_PROXY_OK:account_b"
    assert payload["usage"]["total_tokens"] > 0
    usage = (await client.get("/proxy/usage")).json()["accounts"]["account_b"]
    assert usage["calls"] == 1


async def test_chat_completion_records_vision_modality(client):
    response = await client.post(
        "/v1/chat/completions",
        json={
            "model": "test-vlm-a",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "describe"},
                        {
                            "type": "image_url",
                            "image_url": {"url": "data:image/png;base64,aW1hZ2U="},
                        },
                    ],
                }
            ],
        },
        headers={ACCOUNT_HEADER: "account_a"},
    )

    assert response.status_code == 200
    ledger = (await client.get("/proxy/requests")).json()["requests"]
    assert ledger[0]["modalities"] == ["image", "text"]


async def test_memory_extraction_contract_returns_valid_empty_program(client):
    response = await client.post(
        "/v1/chat/completions",
        json={
            "model": "test-vlm-b",
            "messages": [
                {
                    "role": "system",
                    "content": "Output Format: restricted Python memory SDK",
                }
            ],
        },
        headers={ACCOUNT_HEADER: "account_b"},
    )

    assert response.status_code == 200
    assert response.json()["choices"][0]["message"]["content"] == "sdk.commit()"
