"""Deterministic OpenAI-compatible model proxy for account isolation E2E tests."""

from __future__ import annotations

import argparse
import hashlib
import math
import os
import re
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from threading import RLock
from typing import Any, Literal

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

ACCOUNT_HEADER = "x-ov-test-account"
DEFAULT_DIMENSION = 8
TOKEN_RE = re.compile(r"[\w-]+", re.UNICODE)
TRACE_RE = re.compile(r"E2E_TRACE_[A-Z0-9_]+")


class FaultRequest(BaseModel):
    operation: Literal["embedding", "vlm"]
    account_id: str | None = None
    count: int = Field(default=1, ge=1)
    status_code: int | None = Field(default=None, ge=400, le=599)
    delay_ms: int = Field(default=0, ge=0)
    wrong_dimension: int | None = Field(default=None, ge=1)
    malformed_response: bool = False
    omit_usage: bool = False


@dataclass
class FaultRule:
    operation: str
    account_id: str | None
    remaining: int
    status_code: int | None
    delay_ms: int
    wrong_dimension: int | None
    malformed_response: bool
    omit_usage: bool


class ProxyState:
    def __init__(self) -> None:
        self._lock = RLock()
        self._requests: list[dict[str, Any]] = []
        self._faults: list[FaultRule] = []
        self._attempts: dict[tuple[str, str, str], int] = defaultdict(int)

    def reset(self) -> None:
        with self._lock:
            self._requests.clear()
            self._faults.clear()
            self._attempts.clear()

    def add_fault(self, request: FaultRequest) -> None:
        with self._lock:
            self._faults.append(
                FaultRule(
                    operation=request.operation,
                    account_id=request.account_id,
                    remaining=request.count,
                    status_code=request.status_code,
                    delay_ms=request.delay_ms,
                    wrong_dimension=request.wrong_dimension,
                    malformed_response=request.malformed_response,
                    omit_usage=request.omit_usage,
                )
            )

    def take_fault(self, operation: str, account_id: str) -> FaultRule | None:
        with self._lock:
            for rule in self._faults:
                if (
                    rule.remaining > 0
                    and rule.operation == operation
                    and rule.account_id in (None, account_id)
                ):
                    rule.remaining -= 1
                    return rule
        return None

    def record(
        self,
        *,
        operation: str,
        account_id: str,
        model: str,
        request_id: str,
        prompt_tokens: int,
        completion_tokens: int,
        status_code: int,
        payload: Any = "",
        credential_marker: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            attempt_key = (operation, account_id, model)
            self._attempts[attempt_key] += 1
            record = {
                "sequence": len(self._requests) + 1,
                "request_id": request_id,
                "operation": operation,
                "account_id": account_id,
                "model": model,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": prompt_tokens + completion_tokens,
                "retry_attempt": self._attempts[attempt_key],
                "status_code": status_code,
                "credential_marker": credential_marker,
                "payload_sha256": hashlib.sha256(_text(payload).encode()).hexdigest()[:16],
                "trace_markers": sorted(set(TRACE_RE.findall(_text(payload)))),
                "modalities": _modalities(payload),
                "timestamp": time.time(),
            }
            self._requests.append(record)
            return record

    def requests(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(item) for item in self._requests]


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return " ".join(_text(item) for item in value)
    if isinstance(value, dict):
        if "text" in value:
            return _text(value["text"])
        if "content" in value:
            return _text(value["content"])
        return " ".join(_text(item) for item in value.values())
    return str(value)


def _token_count(value: Any) -> int:
    return max(1, len(TOKEN_RE.findall(_text(value))))


def _modalities(value: Any) -> list[str]:
    found = {"text"}

    def visit(item: Any) -> None:
        if isinstance(item, dict):
            item_type = item.get("type")
            if item_type in {"image_url", "video_url", "audio_url", "input_image"}:
                found.add(str(item_type).replace("_url", "").replace("input_", ""))
            for nested in item.values():
                visit(nested)
        elif isinstance(item, list):
            for nested in item:
                visit(nested)

    visit(value)
    return sorted(found)


def _embedding(value: Any, dimension: int) -> list[float]:
    vector = [0.0] * dimension
    tokens = TOKEN_RE.findall(_text(value).lower()) or [""]
    for token in tokens:
        digest = hashlib.sha256(token.encode()).digest()
        vector[int.from_bytes(digest[:4], "big") % dimension] += 1.0 if digest[4] % 2 == 0 else -1.0
    norm = math.sqrt(sum(item * item for item in vector))
    if norm == 0:
        vector[0] = 1.0
        return vector
    return [item / norm for item in vector]


def _expected_account(model: str) -> str | None:
    if model.endswith("-a"):
        return "account_a"
    if model.endswith("-b"):
        return "account_b"
    return None


def create_app(*, mode: str = "deterministic", dimension: int = DEFAULT_DIMENSION) -> FastAPI:
    app = FastAPI(title="OpenViking Account Isolation Model Proxy")
    state = ProxyState()
    app.state.proxy_state = state
    app.state.mode = mode
    app.state.dimension = dimension

    def validate_identity(request: Request, model: str) -> tuple[str | None, JSONResponse | None]:
        account_id = request.headers.get(ACCOUNT_HEADER)
        if not account_id:
            return None, JSONResponse(
                status_code=400,
                content={
                    "error": {"type": "account_validation", "message": "missing account header"}
                },
            )
        expected = _expected_account(model)
        if expected is not None and account_id != expected:
            return None, JSONResponse(
                status_code=409,
                content={
                    "error": {
                        "type": "account_validation",
                        "message": f"model {model} does not belong to {account_id}",
                    }
                },
            )
        return account_id, None

    async def apply_fault(
        operation: str, account_id: str
    ) -> tuple[FaultRule | None, JSONResponse | None]:
        fault = state.take_fault(operation, account_id)
        if fault is None:
            return None, None
        if fault.delay_ms:
            import asyncio

            await asyncio.sleep(fault.delay_ms / 1000)
        if fault.status_code is not None:
            return fault, JSONResponse(
                status_code=fault.status_code,
                content={"error": {"type": "injected", "message": "injected model failure"}},
            )
        if fault.malformed_response:
            return fault, JSONResponse(status_code=200, content={"malformed": True})
        return fault, None

    async def forward(
        request: Request, operation: str, body: dict[str, Any]
    ) -> tuple[JSONResponse, dict[str, Any]]:
        prefix = f"OV_E2E_FORWARD_{operation.upper()}"
        base_url = os.getenv(f"{prefix}_BASE_URL", "").rstrip("/")
        api_key = os.getenv(f"{prefix}_API_KEY", "")
        if not base_url:
            content = {
                "error": {
                    "type": "forwarding_config",
                    "message": f"{prefix}_BASE_URL is unset",
                }
            }
            return JSONResponse(status_code=503, content=content), content
        upstream_headers = {
            "content-type": "application/json",
            ACCOUNT_HEADER: request.headers[ACCOUNT_HEADER],
        }
        if api_key:
            upstream_headers["authorization"] = f"Bearer {api_key}"
        path = "/embeddings" if operation == "embedding" else "/chat/completions"
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                f"{base_url}{path}",
                headers=upstream_headers,
                json=body,
            )
        try:
            content = response.json()
        except ValueError:
            content = {"error": {"type": "upstream", "message": response.text[:500]}}
        return JSONResponse(status_code=response.status_code, content=content), content

    @app.get("/health")
    async def health():
        return {"status": "ok", "mode": app.state.mode, "dimension": app.state.dimension}

    @app.get("/proxy/requests")
    async def requests():
        return {"requests": state.requests()}

    @app.get("/proxy/usage")
    async def usage():
        records = state.requests()
        totals: dict[str, dict[str, int]] = {}
        for item in records:
            account = totals.setdefault(
                item["account_id"],
                {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0, "calls": 0},
            )
            for field in ("prompt_tokens", "completion_tokens", "total_tokens"):
                account[field] += item[field]
            account["calls"] += 1
        return {"accounts": totals}

    @app.post("/proxy/reset")
    async def reset():
        state.reset()
        return {"status": "ok"}

    @app.post("/proxy/faults")
    async def faults(body: FaultRequest):
        state.add_fault(body)
        return {"status": "ok"}

    @app.post("/v1/embeddings")
    async def embeddings(request: Request):
        body = await request.json()
        model = str(body.get("model") or "")
        credential_marker = request.headers.get("x-ov-test-credential")
        account_id, error = validate_identity(request, model)
        if error is not None:
            state.record(
                operation="embedding",
                account_id=request.headers.get(ACCOUNT_HEADER, "<missing>"),
                model=model,
                request_id=request.headers.get("x-request-id") or str(uuid.uuid4()),
                prompt_tokens=_token_count(body.get("input")),
                completion_tokens=0,
                status_code=error.status_code,
                payload=body.get("input"),
                credential_marker=credential_marker,
            )
            return error
        assert account_id is not None
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        fault, error = await apply_fault("embedding", account_id)
        if error is not None:
            state.record(
                operation="embedding",
                account_id=account_id,
                model=model,
                request_id=request_id,
                prompt_tokens=_token_count(body.get("input")),
                completion_tokens=0,
                status_code=error.status_code,
                payload=body.get("input"),
                credential_marker=credential_marker,
            )
            return error
        if mode == "forwarding":
            response, payload = await forward(request, "embedding", body)
            usage = payload.get("usage") if isinstance(payload, dict) else {}
            state.record(
                operation="embedding",
                account_id=account_id,
                model=model,
                request_id=request_id,
                prompt_tokens=int((usage or {}).get("prompt_tokens", 0)),
                completion_tokens=int((usage or {}).get("completion_tokens", 0)),
                status_code=response.status_code,
                payload=body.get("input"),
                credential_marker=credential_marker,
            )
            return response
        inputs = body.get("input", [])
        items = inputs if isinstance(inputs, list) else [inputs]
        result_dimension = fault.wrong_dimension if fault and fault.wrong_dimension else dimension
        prompt_tokens = sum(_token_count(item) for item in items)
        record = state.record(
            operation="embedding",
            account_id=account_id,
            model=model,
            request_id=request_id,
            prompt_tokens=prompt_tokens,
            completion_tokens=0,
            status_code=200,
            payload=body.get("input"),
            credential_marker=credential_marker,
        )
        response: dict[str, Any] = {
            "object": "list",
            "model": model,
            "data": [
                {
                    "object": "embedding",
                    "index": index,
                    "embedding": _embedding(item, result_dimension),
                }
                for index, item in enumerate(items)
            ],
        }
        if not (fault and fault.omit_usage):
            response["usage"] = {
                "prompt_tokens": record["prompt_tokens"],
                "total_tokens": record["total_tokens"],
            }
        return response

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request):
        body = await request.json()
        model = str(body.get("model") or "")
        credential_marker = request.headers.get("x-ov-test-credential")
        account_id, error = validate_identity(request, model)
        if error is not None:
            state.record(
                operation="vlm",
                account_id=request.headers.get(ACCOUNT_HEADER, "<missing>"),
                model=model,
                request_id=request.headers.get("x-request-id") or str(uuid.uuid4()),
                prompt_tokens=_token_count(body.get("messages")),
                completion_tokens=0,
                status_code=error.status_code,
                payload=body.get("messages"),
                credential_marker=credential_marker,
            )
            return error
        assert account_id is not None
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        fault, error = await apply_fault("vlm", account_id)
        if error is not None:
            state.record(
                operation="vlm",
                account_id=account_id,
                model=model,
                request_id=request_id,
                prompt_tokens=_token_count(body.get("messages")),
                completion_tokens=0,
                status_code=error.status_code,
                payload=body.get("messages"),
                credential_marker=credential_marker,
            )
            return error
        if mode == "forwarding":
            response, payload = await forward(request, "vlm", body)
            usage = payload.get("usage") if isinstance(payload, dict) else {}
            state.record(
                operation="vlm",
                account_id=account_id,
                model=model,
                request_id=request_id,
                prompt_tokens=int((usage or {}).get("prompt_tokens", 0)),
                completion_tokens=int((usage or {}).get("completion_tokens", 0)),
                status_code=response.status_code,
                payload=body.get("messages"),
                credential_marker=credential_marker,
            )
            return response
        request_text = _text(body.get("messages"))
        content = (
            "sdk.commit()"
            if "restricted Python memory SDK" in request_text
            else f"OV_PROXY_OK:{account_id}"
        )
        prompt_tokens = _token_count(body.get("messages"))
        completion_tokens = _token_count(content)
        record = state.record(
            operation="vlm",
            account_id=account_id,
            model=model,
            request_id=request_id,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            status_code=200,
            payload=body.get("messages"),
            credential_marker=credential_marker,
        )
        response = {
            "id": f"chatcmpl-{request_id}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": content},
                    "finish_reason": "stop",
                }
            ],
        }
        if not (fault and fault.omit_usage):
            response["usage"] = {
                "prompt_tokens": record["prompt_tokens"],
                "completion_tokens": record["completion_tokens"],
                "total_tokens": record["total_tokens"],
            }
        return response

    return app


app = create_app(
    mode=os.getenv("OV_E2E_MODEL_PROXY_MODE", "deterministic"),
    dimension=int(os.getenv("OV_E2E_MODEL_DIMENSION", str(DEFAULT_DIMENSION))),
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1940)
    parser.add_argument("--mode", choices=("deterministic", "forwarding"), default="deterministic")
    parser.add_argument("--dimension", type=int, default=DEFAULT_DIMENSION)
    args = parser.parse_args()
    uvicorn.run(
        create_app(mode=args.mode, dimension=args.dimension),
        host=args.host,
        port=args.port,
        log_level="warning",
    )


if __name__ == "__main__":
    main()
