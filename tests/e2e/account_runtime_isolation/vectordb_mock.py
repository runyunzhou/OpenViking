"""Instrumented HTTP VectorDB service for account isolation E2E tests."""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from collections import defaultdict
from threading import RLock
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field


class FaultRequest(BaseModel):
    operation: str
    collection: str | None = None
    count: int = Field(default=1, ge=1)
    status_code: int = Field(default=500, ge=400, le=599)
    delay_ms: int = Field(default=0, ge=0)


class VectorDBLedger:
    def __init__(self) -> None:
        self._lock = RLock()
        self._requests: list[dict[str, Any]] = []
        self._faults: list[dict[str, Any]] = []
        self._attempts: dict[tuple[str, str], int] = defaultdict(int)

    def reset(self) -> None:
        with self._lock:
            self._requests.clear()
            self._faults.clear()
            self._attempts.clear()

    def add_fault(self, fault: FaultRequest) -> None:
        with self._lock:
            self._faults.append(
                {
                    "operation": fault.operation,
                    "collection": fault.collection,
                    "remaining": fault.count,
                    "status_code": fault.status_code,
                    "delay_ms": fault.delay_ms,
                }
            )

    def take_fault(self, operation: str, collection: str) -> dict[str, Any] | None:
        with self._lock:
            for fault in self._faults:
                if (
                    fault["remaining"] > 0
                    and fault["operation"] == operation
                    and fault["collection"] in (None, collection)
                ):
                    fault["remaining"] -= 1
                    return dict(fault)
        return None

    def record(
        self,
        *,
        operation: str,
        project: str,
        collection: str,
        index: str,
        method: str,
        path: str,
        status_code: int,
    ) -> str:
        with self._lock:
            key = (operation, collection)
            self._attempts[key] += 1
            request_id = str(uuid.uuid4())
            self._requests.append(
                {
                    "sequence": len(self._requests) + 1,
                    "request_id": request_id,
                    "operation": operation,
                    "project": project,
                    "collection": collection,
                    "index": index,
                    "method": method,
                    "path": path,
                    "retry_attempt": self._attempts[key],
                    "status_code": status_code,
                    "timestamp": time.time(),
                }
            )
            return request_id

    def requests(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(item) for item in self._requests]


def _operation(path: str) -> str:
    leaf = path.rstrip("/").rsplit("/", 1)[-1]
    mapping = {
        "CreateVikingdbCollection": "create_collection",
        "ListVikingdbCollection": "list_collections",
        "GetVikingdbCollection": "get_collection",
        "DeleteVikingdbCollection": "delete_collection",
        "CreateVikingdbIndex": "create_index",
        "ListVikingdbIndex": "list_indexes",
        "GetVikingdbIndex": "get_index",
        "DeleteVikingdbIndex": "delete_index",
        "upsert": "upsert",
        "update": "update",
        "delete": "delete",
        "fetch_in_collection": "fetch",
        "vector": "search",
        "id": "search_by_id",
        "multi_modal": "search_multimodal",
        "keywords": "search_keywords",
        "scalar": "search_scalar",
        "random": "search_random",
    }
    return mapping.get(leaf, leaf)


def _decode_json_field(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except ValueError:
        return value


def _coordinates(request: Request, body: dict[str, Any]) -> tuple[str, str, str]:
    query = request.query_params
    project = str(
        body.get("project")
        or body.get("ProjectName")
        or query.get("project")
        or query.get("ProjectName")
        or "default"
    )
    collection = str(
        body.get("collection_name")
        or body.get("CollectionName")
        or query.get("collection_name")
        or query.get("CollectionName")
        or ""
    )
    index = str(
        body.get("index_name")
        or body.get("IndexName")
        or query.get("index_name")
        or query.get("IndexName")
        or ""
    )
    return project, collection, index


def create_app(
    persist_path: str,
    *,
    precreate: tuple[str, ...] = (),
    dimension: int = 8,
) -> FastAPI:
    os.environ["VIKINGDB_PERSIST_PATH"] = persist_path

    from openviking.storage.collection_schemas import CollectionSchemas
    from openviking.storage.vectordb.service import api_fastapi
    from openviking.storage.vectordb.service.server_fastapi import app

    for suffix in precreate:
        project = api_fastapi.get_project(f"project_{suffix}")
        collection_name = f"collection_{suffix}"
        if project.has_collection(collection_name):
            continue
        schema = CollectionSchemas.context_collection(collection_name, dimension)
        collection = project.create_collection(collection_name, schema)
        collection.create_index(
            "default",
            {
                "IndexName": "default",
                "VectorIndex": {
                    "IndexType": "flat",
                    "Distance": "cosine",
                    "Quant": "int8",
                },
                "ScalarIndex": schema["ScalarIndex"],
            },
        )

    ledger = VectorDBLedger()
    app.state.mock_ledger = ledger

    @app.post("/api/vikingdb/data/aggregate")
    async def aggregate():
        return {
            "code": 0,
            "message": "success",
            "data": {"agg": {"_total": 0}},
        }

    @app.middleware("http")
    async def record_requests(request: Request, call_next):
        if request.url.path.startswith("/mock/"):
            return await call_next(request)

        raw = await request.body()
        try:
            body = json.loads(raw) if raw else {}
        except ValueError:
            body = {}
        body = {key: _decode_json_field(value) for key, value in body.items()}
        project, collection, index = _coordinates(request, body)
        operation = _operation(request.url.path)
        fault = ledger.take_fault(operation, collection)
        if fault is not None and fault["delay_ms"]:
            import asyncio

            await asyncio.sleep(fault["delay_ms"] / 1000)
        if fault is not None:
            response = JSONResponse(
                status_code=fault["status_code"],
                content={"code": 100000, "message": "injected VectorDB failure", "data": {}},
            )
        else:

            async def receive():
                return {"type": "http.request", "body": raw, "more_body": False}

            request._receive = receive
            response = await call_next(request)

        request_id = ledger.record(
            operation=operation,
            project=project,
            collection=collection,
            index=index,
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
        )
        response.headers["X-OV-Mock-Request-ID"] = request_id
        return response

    @app.get("/mock/requests")
    async def requests():
        return {"requests": ledger.requests()}

    @app.get("/mock/state")
    async def state():
        records = ledger.requests()
        return {
            "requests": len(records),
            "collections": sorted(
                {(item["project"], item["collection"]) for item in records if item["collection"]}
            ),
        }

    @app.post("/mock/reset")
    async def reset():
        ledger.reset()
        return {"status": "ok"}

    @app.post("/mock/faults")
    async def faults(body: FaultRequest):
        ledger.add_fault(body)
        return {"status": "ok"}

    return app


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1941)
    parser.add_argument(
        "--persist-path",
        default=os.getenv("OV_E2E_VECTORDB_PATH", "/private/tmp/ov-e2e/vectordb"),
    )
    parser.add_argument(
        "--precreate",
        nargs="*",
        default=("a", "b"),
        help="Collection suffixes to provision as project_<suffix>/collection_<suffix>",
    )
    parser.add_argument("--dimension", type=int, default=8)
    args = parser.parse_args()
    uvicorn.run(
        create_app(
            args.persist_path,
            precreate=tuple(args.precreate),
            dimension=args.dimension,
        ),
        host=args.host,
        port=args.port,
        log_level="warning",
    )


if __name__ == "__main__":
    main()
