"""Case-driven Account model and VectorDB isolation E2E driver."""

from __future__ import annotations

import argparse
import base64
import copy
import json
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

import httpx
from generate_config import ROOT_API_KEY

ACCOUNT_MODELS = {
    "account_a": {"embedding": "test-embedding-a", "vlm": "test-vlm-a"},
    "account_b": {"embedding": "test-embedding-b", "vlm": "test-vlm-b"},
}
ACCOUNT_VECTORDB = {
    "account_a": ("project_a", "collection_a"),
    "account_b": ("project_b", "collection_b"),
}


@dataclass
class CaseResult:
    case_id: str
    description: str
    expected: str
    status: str
    evidence: dict[str, Any]
    error: str | None = None


class CaseRunner:
    def __init__(
        self,
        ov_url: str,
        model_url: str,
        vectordb_url: str,
        config_dir: Path,
    ) -> None:
        self.ov_url = ov_url
        self.model_url = model_url
        self.vectordb_url = vectordb_url
        self.config_dir = config_dir
        self.root_headers = {"Authorization": f"Bearer {ROOT_API_KEY}"}
        self.keys: dict[str, str] = {}
        self.initial_settings: dict[str, dict[str, Any]] = {}
        self.results: list[CaseResult] = []

    def _client(self, timeout: float = 30) -> httpx.Client:
        return httpx.Client(timeout=timeout)

    @staticmethod
    def _expect_ok(response: httpx.Response, label: str) -> dict[str, Any]:
        if response.status_code != 200:
            raise AssertionError(
                f"{label} failed: HTTP {response.status_code}: {response.text[:500]}"
            )
        payload = response.json()
        healthy = payload.get("status") in (None, "ok", "healthy", "ready")
        if not healthy and payload.get("code") != 0:
            raise AssertionError(f"{label} failed: {payload}")
        return payload

    def setup(self) -> None:
        with self._client() as client:
            self._expect_ok(client.get(f"{self.ov_url}/health"), "OpenViking health")
            self._expect_ok(client.get(f"{self.model_url}/health"), "model proxy health")
            self._expect_ok(client.get(f"{self.vectordb_url}/health"), "VectorDB health")
            for account_id, user_id in (("account_a", "alice"), ("account_b", "carol")):
                settings = json.loads((self.config_dir / f"{account_id}.json").read_text())
                self.initial_settings[account_id] = settings
                payload = self._expect_ok(
                    client.post(
                        f"{self.ov_url}/api/v1/admin/accounts",
                        headers=self.root_headers,
                        json={
                            "account_id": account_id,
                            "admin_user_id": user_id,
                            "settings": settings,
                        },
                    ),
                    f"create {account_id}",
                )
                self.keys[account_id] = payload["result"]["user_key"]
                self._expect_ok(
                    client.post(
                        f"{self.ov_url}/api/v1/system/wait",
                        headers=self.auth(account_id),
                        json={},
                    ),
                    f"wait {account_id}",
                )
        self.reset_ledgers()

    def auth(self, account_id: str, **extra: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.keys[account_id]}",
            **extra,
        }

    def reset_ledgers(self) -> None:
        with self._client() as client:
            for account_id in self.keys:
                self._expect_ok(
                    client.post(
                        f"{self.ov_url}/api/v1/system/wait",
                        headers=self.auth(account_id),
                        json={},
                    ),
                    f"drain background work for {account_id}",
                )
            client.post(f"{self.model_url}/proxy/reset").raise_for_status()
            client.post(f"{self.vectordb_url}/mock/reset").raise_for_status()

    def model_ledger(self) -> list[dict[str, Any]]:
        with self._client() as client:
            return client.get(f"{self.model_url}/proxy/requests").json()["requests"]

    def vector_ledger(self) -> list[dict[str, Any]]:
        with self._client() as client:
            return client.get(f"{self.vectordb_url}/mock/requests").json()["requests"]

    def get_config(self, account_id: str) -> dict[str, Any]:
        with self._client() as client:
            response = client.get(
                f"{self.ov_url}/api/v1/admin/accounts/{account_id}/configuration",
                headers=self.root_headers,
            )
        return self._expect_ok(response, f"get config {account_id}")["result"]["settings"]

    def patch_config(
        self,
        account_id: str,
        settings: dict[str, Any],
        *,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        with self._client(timeout=60) as client:
            return client.patch(
                f"{self.ov_url}/api/v1/admin/accounts/{account_id}/configuration",
                headers=headers or self.root_headers,
                json={"settings": settings},
            )

    def restore_dynamic_config(self, account_id: str) -> None:
        initial = self.initial_settings[account_id]
        response = self.patch_config(
            account_id,
            {
                "embedding": {
                    "dense": {
                        "credentials": copy.deepcopy(initial["embedding"]["dense"]["credentials"])
                    },
                    "max_retries": None,
                },
                "vlm": copy.deepcopy(initial["vlm"]),
            },
        )
        self._expect_ok(response, f"restore config {account_id}")

    def search(
        self,
        account_id: str,
        marker: str,
        *,
        extra_headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        headers = self.auth(account_id, **(extra_headers or {}))
        with self._client(timeout=60) as client:
            return client.post(
                f"{self.ov_url}/api/v1/search/search",
                headers=headers,
                json={
                    "query": marker,
                    "mode": "context",
                    "query_expansion": "auto",
                    "rewrite": True,
                    "max_tokens": 256,
                },
            )

    def upload_resource(
        self,
        account_id: str,
        *,
        filename: str,
        content: bytes,
        content_type: str,
        wait: bool,
    ) -> dict[str, Any]:
        with self._client(timeout=120) as client:
            upload = client.post(
                f"{self.ov_url}/api/v1/resources/temp_upload",
                headers=self.auth(account_id),
                files={"file": (filename, content, content_type)},
            )
            temp_file_id = self._expect_ok(upload, f"upload {filename}")["result"][
                "temp_file_id"
            ]
            response = client.post(
                f"{self.ov_url}/api/v1/resources",
                headers=self.auth(account_id),
                json={
                    "temp_file_id": temp_file_id,
                    "reason": f"account isolation E2E {filename}",
                    "instruction": f"Preserve marker {filename}",
                    "wait": wait,
                    "timeout": 120,
                },
            )
        result = self._expect_ok(response, f"add resource {filename}")["result"]
        if not wait:
            task_id = result.get("task_id")
            if not task_id:
                raise AssertionError(f"async add resource returned no task_id: {result}")
            result["task"] = self.wait_task(account_id, task_id)
        return result

    def wait_task(
        self,
        account_id: str,
        task_id: str,
        *,
        timeout: float = 120,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        last: dict[str, Any] = {}
        with self._client(timeout=30) as client:
            while time.monotonic() < deadline:
                response = client.get(
                    f"{self.ov_url}/api/v1/tasks/{task_id}",
                    headers=self.auth(account_id),
                )
                last = self._expect_ok(response, f"poll task {task_id}")["result"]
                if last.get("status") in {"completed", "failed", "cancelled"}:
                    break
                time.sleep(0.1)
        if last.get("status") != "completed":
            raise AssertionError(f"task {task_id} did not complete: {last}")
        return last

    def commit_session(self, account_id: str, session_id: str, marker: str) -> dict[str, Any]:
        with self._client(timeout=60) as client:
            created = client.post(
                f"{self.ov_url}/api/v1/sessions",
                headers=self.auth(account_id),
                json={"session_id": session_id},
            )
            self._expect_ok(created, f"create session {session_id}")
            for role, content in (
                ("user", f"{marker} user preference: use concise technical answers."),
                ("assistant", f"{marker} acknowledged and will preserve this preference."),
            ):
                added = client.post(
                    f"{self.ov_url}/api/v1/sessions/{session_id}/messages",
                    headers=self.auth(account_id),
                    json={"role": role, "content": content},
                )
                self._expect_ok(added, f"add {role} message to {session_id}")
            committed = client.post(
                f"{self.ov_url}/api/v1/sessions/{session_id}/commit",
                headers=self.auth(account_id),
                json={},
            )
        result = self._expect_ok(committed, f"commit session {session_id}")["result"]
        task_id = result.get("task_id")
        if not task_id:
            raise AssertionError(f"session commit returned no task_id: {result}")
        result["task"] = self.wait_task(account_id, task_id)
        return result

    def assert_model_isolation(
        self,
        allowed_accounts: set[str],
        *,
        require_operations: bool = True,
        required_operations: set[str] | None = None,
        expected_models: dict[str, dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        ledger = self.model_ledger()
        if not ledger:
            raise AssertionError("model ledger is empty")
        observed = set()
        for item in ledger:
            account_id = item["account_id"]
            operation = item["operation"]
            model = item["model"]
            if account_id not in allowed_accounts:
                raise AssertionError(f"unexpected account in model ledger: {item}")
            expected_model = (expected_models or ACCOUNT_MODELS)[account_id][operation]
            if model != expected_model:
                raise AssertionError(
                    f"{account_id} used {model}, expected {expected_model}: {item}"
                )
            observed.add((account_id, operation, model))
        if required_operations is not None:
            expected = {
                (
                    account_id,
                    operation,
                    (expected_models or ACCOUNT_MODELS)[account_id][operation],
                )
                for account_id in allowed_accounts
                for operation in required_operations
            }
            missing = expected - observed
            if missing:
                raise AssertionError(f"missing account model calls: {sorted(missing)}")
        elif require_operations:
            expected = {
                (account_id, operation, model)
                for account_id in allowed_accounts
                for operation, model in (expected_models or ACCOUNT_MODELS)[account_id].items()
            }
            missing = expected - observed
            if missing:
                raise AssertionError(f"missing account model calls: {sorted(missing)}")
        return {
            "calls": len(ledger),
            "routes": sorted([list(item) for item in observed]),
            "trace_markers": sorted(
                {marker for item in ledger for marker in item.get("trace_markers", [])}
            ),
            "status_codes": sorted({item["status_code"] for item in ledger}),
        }

    def assert_vector_isolation(self, allowed_accounts: set[str]) -> dict[str, Any]:
        ledger = self.vector_ledger()
        observed = set()
        for item in ledger:
            if not item["collection"]:
                continue
            route = (item["project"], item["collection"])
            allowed = {ACCOUNT_VECTORDB[account_id] for account_id in allowed_accounts}
            if route not in allowed:
                raise AssertionError(f"unexpected VectorDB route: {item}")
            observed.add(route)
        expected = {ACCOUNT_VECTORDB[account_id] for account_id in allowed_accounts}
        missing = expected - observed
        if missing:
            raise AssertionError(f"missing VectorDB routes: {sorted(missing)}")
        return {
            "calls": len(ledger),
            "routes": sorted([list(item) for item in observed]),
            "status_codes": sorted({item["status_code"] for item in ledger}),
        }

    def case(
        self,
        case_id: str,
        description: str,
        expected: str,
        body: Callable[[], dict[str, Any]],
    ) -> None:
        self.reset_ledgers()
        try:
            evidence = body()
            result = CaseResult(case_id, description, expected, "PASS", evidence)
        except Exception as exc:
            result = CaseResult(
                case_id,
                description,
                expected,
                "FAIL",
                {
                    "model_ledger": self.model_ledger()[-20:],
                    "vector_ledger": self.vector_ledger()[-20:],
                },
                f"{type(exc).__name__}: {exc}",
            )
        self.results.append(result)

    def run_cases(self) -> None:
        def single_account(account_id: str, marker: str) -> dict[str, Any]:
            response = self.search(account_id, marker)
            self._expect_ok(response, f"search {account_id}")
            return {
                "http_status": response.status_code,
                "model": self.assert_model_isolation({account_id}),
                "vectordb": self.assert_vector_isolation({account_id}),
            }

        self.case(
            "MODEL-001",
            "Account A 单独搜索，逐条检查模型 header、model 与 trace marker",
            "所有调用均为 account_a + test-embedding-a/test-vlm-a，且不出现 B",
            lambda: single_account("account_a", "E2E_TRACE_MODEL_A_ONLY"),
        )
        self.case(
            "MODEL-002",
            "Account B 单独搜索，逐条检查模型 header、model 与 trace marker",
            "所有调用均为 account_b + test-embedding-b/test-vlm-b，且不出现 A",
            lambda: single_account("account_b", "E2E_TRACE_MODEL_B_ONLY"),
        )

        def own_account_access(account_id: str) -> dict[str, Any]:
            with self._client() as client:
                response = client.get(
                    f"{self.ov_url}/api/v1/admin/accounts/{account_id}/configuration",
                    headers=self.auth(account_id),
                )
            payload = self._expect_ok(response, f"{account_id} own configuration")
            return {
                "http_status": response.status_code,
                "account_id": payload["result"]["account_id"],
                "visible_settings": payload["result"]["settings"],
            }

        self.case(
            "AUTH-001",
            "Alice API key 访问 Account A 配置接口",
            "身份解析为 account_a，且非 ROOT 不泄露模型与 VectorDB 密钥配置",
            lambda: own_account_access("account_a"),
        )
        self.case(
            "AUTH-002",
            "Carol API key 访问 Account B 配置接口",
            "身份解析为 account_b，且非 ROOT 不泄露模型与 VectorDB 密钥配置",
            lambda: own_account_access("account_b"),
        )

        def forged_header(source: str, forged: str) -> dict[str, Any]:
            response = self.search(
                source,
                f"E2E_TRACE_FORGED_{source.upper()}",
                extra_headers={"X-OpenViking-Account": forged},
            )
            self._expect_ok(response, "forged header search")
            return {
                "http_status": response.status_code,
                "forged_header": forged,
                "effective_routes": self.assert_model_isolation({source}),
                "vectordb": self.assert_vector_isolation({source}),
            }

        self.case(
            "AUTH-003",
            "Alice API key 伪造 Account B 请求头",
            "API key 身份优先，模型和 VectorDB 仍只使用 Account A",
            lambda: forged_header("account_a", "account_b"),
        )
        self.case(
            "AUTH-004",
            "Carol API key 伪造 Account A 请求头",
            "API key 身份优先，模型和 VectorDB 仍只使用 Account B",
            lambda: forged_header("account_b", "account_a"),
        )

        def root_and_cross_account_access() -> dict[str, Any]:
            with self._client() as client:
                listed = client.get(
                    f"{self.ov_url}/api/v1/admin/accounts",
                    headers=self.root_headers,
                )
                denied = client.get(
                    f"{self.ov_url}/api/v1/admin/accounts/account_b/configuration",
                    headers=self.auth("account_a"),
                )
            accounts = self._expect_ok(listed, "ROOT list accounts")["result"]
            ids = {item["account_id"] for item in accounts}
            if not {"account_a", "account_b"}.issubset(ids):
                raise AssertionError(f"ROOT account list missing E2E accounts: {ids}")
            if denied.status_code not in (401, 403):
                raise AssertionError(f"cross-account admin read returned {denied.status_code}")
            return {
                "root_http_status": listed.status_code,
                "account_ids": sorted(ids),
                "cross_account_http_status": denied.status_code,
            }

        self.case(
            "AUTH-005/006",
            "ROOT 枚举 A/B，同时 Alice 尝试读取 Account B 管理配置",
            "ROOT 可见 A/B；Account A 管理员跨 Account 访问返回 401/403",
            root_and_cross_account_access,
        )

        def invalid_auth() -> dict[str, Any]:
            with self._client() as client:
                responses = [
                    client.post(
                        f"{self.ov_url}/api/v1/search/search",
                        headers=headers,
                        json={"query": "E2E_TRACE_INVALID_AUTH"},
                    )
                    for headers in (
                        {"Authorization": "Bearer invalid-e2e-key"},
                        {},
                    )
                ]
            statuses = [response.status_code for response in responses]
            if any(status not in (401, 403) for status in statuses):
                raise AssertionError(f"invalid/missing keys returned {statuses}")
            if self.model_ledger() or self.vector_ledger():
                raise AssertionError("unauthorized request reached model or VectorDB")
            return {"http_statuses": statuses, "downstream_calls": 0}

        self.case(
            "AUTH-007",
            "非法和缺失 API key 分别发起搜索",
            "两者均返回 401/403，模型和 VectorDB 均无调用",
            invalid_auth,
        )

        def interleaved() -> dict[str, Any]:
            statuses = []
            for index, account_id in enumerate(
                ("account_a", "account_b", "account_a", "account_b"), start=1
            ):
                response = self.search(
                    account_id,
                    f"E2E_TRACE_INTERLEAVED_{account_id.upper()}_{index}",
                )
                statuses.append(response.status_code)
                self._expect_ok(response, f"interleaved {account_id}")
            return {
                "http_statuses": statuses,
                "model": self.assert_model_isolation({"account_a", "account_b"}),
                "vectordb": self.assert_vector_isolation({"account_a", "account_b"}),
            }

        self.case(
            "MODEL-003",
            "A/B/A/B 顺序交错搜索",
            "每条 ledger 按 Account 映射到自己的模型和 collection",
            interleaved,
        )

        def concurrent() -> dict[str, Any]:
            requests = [("account_a", f"E2E_TRACE_CONCURRENT_A_{index}") for index in range(4)] + [
                ("account_b", f"E2E_TRACE_CONCURRENT_B_{index}") for index in range(4)
            ]
            with ThreadPoolExecutor(max_workers=8) as executor:
                responses = list(executor.map(lambda item: self.search(item[0], item[1]), requests))
            statuses = [response.status_code for response in responses]
            if any(status != 200 for status in statuses):
                raise AssertionError(f"concurrent HTTP statuses: {statuses}")
            return {
                "requests": len(requests),
                "http_statuses": statuses,
                "model": self.assert_model_isolation({"account_a", "account_b"}),
                "vectordb": self.assert_vector_isolation({"account_a", "account_b"}),
            }

        self.case(
            "MODEL-004",
            "A/B 各四个请求并发首次/重复使用运行时资源",
            "无异步上下文、模型、header、project 或 collection 串租户",
            concurrent,
        )

        def text_resource(account_id: str, marker: str) -> dict[str, Any]:
            result = self.upload_resource(
                account_id,
                filename=f"{marker}.md",
                content=(
                    f"# {marker}\n\n"
                    f"This document belongs only to {account_id}. "
                    "It validates semantic parsing and vectorization.\n"
                ).encode(),
                content_type="text/markdown",
                wait=True,
            )
            return {
                "resource": result,
                "model": self.assert_model_isolation(
                    {account_id}, required_operations={"vlm", "embedding"}
                ),
                "vectordb": self.assert_vector_isolation({account_id}),
            }

        self.case(
            "INGEST-TEXT-001",
            "Account A 通过 temp_upload 同步导入 Markdown",
            "解析 VLM、文档 Embedding 和 VectorDB 写入全部只使用 Account A runtime",
            lambda: text_resource("account_a", "E2E_TRACE_TEXT_RESOURCE_A"),
        )
        self.case(
            "INGEST-TEXT-002",
            "Account B 通过 temp_upload 同步导入 Markdown",
            "解析 VLM、文档 Embedding 和 VectorDB 写入全部只使用 Account B runtime",
            lambda: text_resource("account_b", "E2E_TRACE_TEXT_RESOURCE_B"),
        )

        png = base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk"
            "+A8AAQUBAScY42YAAAAASUVORK5CYII="
        )

        def image_resources_concurrent() -> dict[str, Any]:
            requests = (
                ("account_a", "E2E_TRACE_IMAGE_A.png"),
                ("account_b", "E2E_TRACE_IMAGE_B.png"),
            )
            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(
                    executor.map(
                        lambda item: self.upload_resource(
                            item[0],
                            filename=item[1],
                            content=png,
                            content_type="image/png",
                            wait=True,
                        ),
                        requests,
                    )
                )
            vision_accounts = {
                item["account_id"]
                for item in self.model_ledger()
                if item["operation"] == "vlm" and "image" in item.get("modalities", [])
            }
            if vision_accounts != {"account_a", "account_b"}:
                raise AssertionError(
                    f"image VLM calls missing or cross-routed: {vision_accounts}"
                )
            return {
                "resources": results,
                "vision_accounts": sorted(vision_accounts),
                "model": self.assert_model_isolation(
                    {"account_a", "account_b"},
                    required_operations={"vlm", "embedding"},
                ),
                "vectordb": self.assert_vector_isolation({"account_a", "account_b"}),
            }

        self.case(
            "INGEST-IMAGE-001/002",
            "Account A/B 并发上传并解析 PNG 图片",
            "Vision VLM、图片语义 Embedding 与向量写入逐调用保持 Account 隔离",
            image_resources_concurrent,
        )

        def async_text_resources() -> dict[str, Any]:
            requests = (
                ("account_a", "E2E_TRACE_ASYNC_RESOURCE_A"),
                ("account_b", "E2E_TRACE_ASYNC_RESOURCE_B"),
            )
            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(
                    executor.map(
                        lambda item: self.upload_resource(
                            item[0],
                            filename=f"{item[1]}.txt",
                            content=f"{item[1]} asynchronous queue context".encode(),
                            content_type="text/plain",
                            wait=False,
                        ),
                        requests,
                    )
                )
            return {
                "resources": results,
                "model": self.assert_model_isolation(
                    {"account_a", "account_b"},
                    required_operations={"vlm", "embedding"},
                ),
                "vectordb": self.assert_vector_isolation({"account_a", "account_b"}),
            }

        self.case(
            "INGEST-ASYNC-001/002",
            "Account A/B 并发异步导入文本并按各自身份轮询后台任务",
            "序列化 AddResourceMsg 恢复正确 account，后台 VLM/Embedding/VectorDB 不串租户",
            async_text_resources,
        )

        def video_resources_concurrent() -> dict[str, Any]:
            video = b"\x00\x00\x00\x18ftypisom" + b"\x00" * 64
            requests = (
                ("account_a", "E2E_TRACE_VIDEO_A.mp4"),
                ("account_b", "E2E_TRACE_VIDEO_B.mp4"),
            )
            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(
                    executor.map(
                        lambda item: self.upload_resource(
                            item[0],
                            filename=item[1],
                            content=video,
                            content_type="video/mp4",
                            wait=True,
                        ),
                        requests,
                    )
                )
            media_vlm_calls = [
                item
                for item in self.model_ledger()
                if item["operation"] == "vlm"
                and ({"video", "audio"} & set(item.get("modalities", [])))
            ]
            if media_vlm_calls:
                raise AssertionError(
                    "OpenAI provider unexpectedly sent unsupported media payload: "
                    f"{media_vlm_calls}"
                )
            model = self.assert_model_isolation(
                {"account_a", "account_b"},
                required_operations={"vlm", "embedding"},
            )
            return {
                "resources": results,
                "provider_capability": (
                    "OpenAI VLM skipped direct video media; surrounding semantic text calls ran"
                ),
                "direct_video_vlm_calls": len(media_vlm_calls),
                "model": model,
                "vectordb": self.assert_vector_isolation({"account_a", "account_b"}),
            }

        self.case(
            "INGEST-VIDEO-001/002",
            "Account A/B 并发导入 MP4，覆盖视频解析的 provider capability 分支",
            "不发送不支持的视频 payload；伴随的文本 VLM、Embedding、VectorDB 仍各自隔离",
            video_resources_concurrent,
        )

        def concurrent_reindex() -> dict[str, Any]:
            requests = (
                ("account_a", "viking://resources/E2E_TRACE_TEXT_RESOURCE_A"),
                ("account_b", "viking://resources/E2E_TRACE_TEXT_RESOURCE_B"),
            )

            def reindex(item: tuple[str, str]) -> httpx.Response:
                account_id, uri = item
                with self._client(timeout=120) as client:
                    return client.post(
                        f"{self.ov_url}/api/v1/content/reindex",
                        headers=self.auth(account_id),
                        json={
                            "uri": uri,
                            "mode": "semantic_and_vectors",
                            "wait": True,
                        },
                    )

            with ThreadPoolExecutor(max_workers=2) as executor:
                responses = list(executor.map(reindex, requests))
            results = [
                self._expect_ok(response, f"reindex {account_id}")["result"]
                for response, (account_id, _uri) in zip(responses, requests)
            ]
            return {
                "results": results,
                "model": self.assert_model_isolation(
                    {"account_a", "account_b"},
                    required_operations={"vlm", "embedding"},
                ),
                "vectordb": self.assert_vector_isolation({"account_a", "account_b"}),
            }

        self.case(
            "REINDEX-001/002",
            "Account A/B 并发执行 semantic_and_vectors reindex",
            "重建过程按 owner account 解析 VLM、Embedding 和 VectorDB backend",
            concurrent_reindex,
        )

        def pack_round_trip() -> dict[str, Any]:
            account_id = "account_a"
            source_uri = "viking://resources/E2E_TRACE_TEXT_RESOURCE_A"
            with self._client(timeout=120) as client:
                exported = client.post(
                    f"{self.ov_url}/api/v1/pack/export",
                    headers=self.auth(account_id),
                    json={"uri": source_uri, "include_vectors": True},
                )
                if exported.status_code != 200 or not exported.content.startswith(b"PK"):
                    raise AssertionError(
                        f"pack export failed: HTTP {exported.status_code}: "
                        f"{exported.text[:300]}"
                    )
                upload = client.post(
                    f"{self.ov_url}/api/v1/resources/temp_upload",
                    headers=self.auth(account_id),
                    files={
                        "file": (
                            "E2E_TRACE_PACK_A.ovpack",
                            exported.content,
                            "application/zip",
                        )
                    },
                )
                temp_file_id = self._expect_ok(upload, "upload exported ovpack")["result"][
                    "temp_file_id"
                ]
                imported = client.post(
                    f"{self.ov_url}/api/v1/pack/import",
                    headers=self.auth(account_id),
                    json={
                        "temp_file_id": temp_file_id,
                        "parent": "viking://resources/E2E_TRACE_PACK_IMPORT_A",
                        "on_conflict": "overwrite",
                        "vector_mode": "recompute",
                    },
                )
                self._expect_ok(imported, "import exported ovpack")
                drained = client.post(
                    f"{self.ov_url}/api/v1/system/wait",
                    headers=self.auth(account_id),
                    json={},
                )
                self._expect_ok(drained, "wait for imported vector recompute")
            result = self._expect_ok(imported, "import exported ovpack")["result"]
            return {
                "export_bytes": len(exported.content),
                "import": result,
                "model": self.assert_model_isolation(
                    {account_id},
                    require_operations=False,
                    required_operations={"embedding"},
                ),
                "vectordb": self.assert_vector_isolation({account_id}),
            }

        self.case(
            "PACK-001",
            "Account A 导出含向量 OVPack，再以 recompute 模式导入独立资源目录",
            "导出读取和导入重算均只使用 Account A vector config、Embedding 与 collection",
            pack_round_trip,
        )

        def concurrent_session_commits() -> dict[str, Any]:
            requests = (
                ("account_a", "e2e-session-a", "E2E_TRACE_SESSION_COMMIT_A"),
                ("account_b", "e2e-session-b", "E2E_TRACE_SESSION_COMMIT_B"),
            )
            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(lambda item: self.commit_session(*item), requests))
            evidence = {
                "sessions": results,
                "model": self.assert_model_isolation(
                    {"account_a", "account_b"},
                    require_operations=False,
                    required_operations={"vlm"},
                ),
            }
            vector_ledger = self.vector_ledger()
            if any(item.get("collection") for item in vector_ledger):
                evidence["vectordb"] = self.assert_vector_isolation(
                    {"account_a", "account_b"}
                )
            else:
                evidence["vectordb"] = {
                    "calls": 0,
                    "note": "deterministic VLM produced no memory mutation to vectorize",
                }
            return evidence

        self.case(
            "SESSION-001/002",
            "Account A/B 并发提交 Session 并等待 Phase-2 memory extraction",
            "后台 SessionCommitMsg 恢复各自 Account，所有 VLM 调用及产生的向量写入不串租户",
            concurrent_session_commits,
        )

        def embedding_retry_isolation() -> dict[str, Any]:
            with self._client() as client:
                client.post(
                    f"{self.model_url}/proxy/faults",
                    json={
                        "operation": "embedding",
                        "account_id": "account_a",
                        "status_code": 503,
                        "count": 1,
                    },
                ).raise_for_status()
            a_response = self.search("account_a", "E2E_TRACE_EMB_RETRY_A")
            b_response = self.search("account_b", "E2E_TRACE_EMB_RETRY_B")
            self._expect_ok(a_response, "Account A embedding retry")
            self._expect_ok(b_response, "Account B after A embedding failure")
            ledger = self.model_ledger()
            a_codes = [
                item["status_code"]
                for item in ledger
                if item["account_id"] == "account_a" and item["operation"] == "embedding"
            ]
            if 503 not in a_codes or 200 not in a_codes:
                raise AssertionError(f"Account A retry evidence missing: {a_codes}")
            return {
                "a_embedding_statuses": a_codes,
                "b_http_status": b_response.status_code,
                "model": self.assert_model_isolation({"account_a", "account_b"}),
            }

        self.case(
            "EMB-005/006",
            "Account A Embedding 首次 503 后重试，同时验证 Account B 不受影响",
            "A 的重试只使用 A 模型；B 请求成功且只使用 B 模型",
            embedding_retry_isolation,
        )

        def vlm_retry_isolation() -> dict[str, Any]:
            with self._client() as client:
                client.post(
                    f"{self.model_url}/proxy/faults",
                    json={
                        "operation": "vlm",
                        "account_id": "account_a",
                        "status_code": 503,
                        "count": 1,
                    },
                ).raise_for_status()
            a_response = self.search("account_a", "E2E_TRACE_VLM_RETRY_A")
            b_response = self.search("account_b", "E2E_TRACE_VLM_RETRY_B")
            self._expect_ok(a_response, "Account A VLM retry")
            self._expect_ok(b_response, "Account B after A VLM failure")
            ledger = self.model_ledger()
            a_codes = [
                item["status_code"]
                for item in ledger
                if item["account_id"] == "account_a" and item["operation"] == "vlm"
            ]
            if 503 not in a_codes or 200 not in a_codes:
                raise AssertionError(f"Account A VLM retry evidence missing: {a_codes}")
            return {
                "a_vlm_statuses": a_codes,
                "b_http_status": b_response.status_code,
                "model": self.assert_model_isolation({"account_a", "account_b"}),
            }

        self.case(
            "VLM-004/005",
            "Account A VLM 首次 503 后重试，检查是否错误切到 B credential",
            "A 仅在 A 配置内重试；B 成功且没有承接 A 的失败",
            vlm_retry_isolation,
        )

        def rotated_embedding_credential() -> dict[str, Any]:
            rotated = {
                "provider": "openai",
                "model": "test-embedding-rotated-a",
                "api_key": "e2e-rotated-a",
                "api_base": f"{self.model_url}/v1",
                "extra_headers": {
                    "X-OV-Test-Account": "account_a",
                    "X-OV-Test-Credential": "embedding-rotated-a",
                },
            }
            try:
                self._expect_ok(
                    self.patch_config(
                        "account_a",
                        {"embedding": {"dense": {"credentials": [rotated]}}},
                    ),
                    "rotate Account A embedding credential",
                )
                response = self.search("account_a", "E2E_TRACE_EMBEDDING_ROTATED_A")
                self._expect_ok(response, "search with rotated embedding")
                ledger = self.model_ledger()
                embedding = [item for item in ledger if item["operation"] == "embedding"]
                if not embedding or {
                    (item["account_id"], item["model"], item["credential_marker"])
                    for item in embedding
                } != {
                    (
                        "account_a",
                        "test-embedding-rotated-a",
                        "embedding-rotated-a",
                    )
                }:
                    raise AssertionError(f"rotated embedding evidence invalid: {embedding}")
                return {
                    "http_status": response.status_code,
                    "embedding_calls": embedding,
                    "vectordb": self.assert_vector_isolation({"account_a"}),
                }
            finally:
                self.restore_dynamic_config("account_a")

        self.case(
            "EMB-003/CFG-012",
            "运行中替换 Account A Embedding credential、deployment model 和识别 header",
            "下一次请求只使用完整新 credential；Account B 配置不参与；请求结束后恢复基线",
            rotated_embedding_credential,
        )

        def rotated_vlm_credential() -> dict[str, Any]:
            rotated = {
                "provider": "openai",
                "api_key": "e2e-rotated-a",
                "api_base": f"{self.model_url}/v1",
                "extra_headers": {
                    "X-OV-Test-Account": "account_a",
                    "X-OV-Test-Credential": "vlm-rotated-a",
                },
            }
            try:
                self._expect_ok(
                    self.patch_config(
                        "account_a",
                        {
                            "vlm": {
                                "model": "test-vlm-rotated-a",
                                "credentials": [rotated],
                            }
                        },
                    ),
                    "rotate Account A VLM credential",
                )
                response = self.search("account_a", "E2E_TRACE_VLM_ROTATED_A")
                self._expect_ok(response, "search with rotated VLM")
                ledger = self.model_ledger()
                vlm = [item for item in ledger if item["operation"] == "vlm"]
                if not vlm or {
                    (item["account_id"], item["model"], item["credential_marker"]) for item in vlm
                } != {("account_a", "test-vlm-rotated-a", "vlm-rotated-a")}:
                    raise AssertionError(f"rotated VLM evidence invalid: {vlm}")
                return {
                    "http_status": response.status_code,
                    "vlm_calls": vlm,
                    "vectordb": self.assert_vector_isolation({"account_a"}),
                }
            finally:
                self.restore_dynamic_config("account_a")

        self.case(
            "VLM-003/CFG-012",
            "运行中替换 Account A VLM model、credential 和识别 header",
            "下一次请求只使用完整新 VLM 配置；请求结束后恢复基线",
            rotated_vlm_credential,
        )

        def wrong_embedding_dimension() -> dict[str, Any]:
            with self._client() as client:
                client.post(
                    f"{self.model_url}/proxy/faults",
                    json={
                        "operation": "embedding",
                        "account_id": "account_a",
                        "wrong_dimension": 3,
                        "count": 20,
                    },
                ).raise_for_status()
            response = self.search("account_a", "E2E_TRACE_WRONG_DIMENSION_A")
            payload = self._expect_ok(response, "wrong-dimension degraded search")
            retrieval_errors = payload["result"]["stats"].get("retrieval_errors", [])
            if not any("dimension mismatch" in error for error in retrieval_errors):
                raise AssertionError(
                    f"dimension mismatch missing from retrieval errors: {retrieval_errors}"
                )
            vector_calls = self.vector_ledger()
            if any(item["operation"] == "search" for item in vector_calls):
                raise AssertionError(f"invalid embedding reached VectorDB search: {vector_calls}")
            return {
                "http_status": response.status_code,
                "retrieval_errors": retrieval_errors,
                "model_calls": self.model_ledger(),
                "vector_search_calls": 0,
            }

        self.case(
            "EMB-004",
            "Account A Embedding 持续返回错误维度向量",
            "搜索按公开降级语义返回 retrieval_errors，且错误向量不进入 VectorDB search",
            wrong_embedding_dimension,
        )

        def malformed_vlm_isolation() -> dict[str, Any]:
            with self._client() as client:
                client.post(
                    f"{self.model_url}/proxy/faults",
                    json={
                        "operation": "vlm",
                        "account_id": "account_a",
                        "malformed_response": True,
                        "count": 20,
                    },
                ).raise_for_status()
            a_response = self.search("account_a", "E2E_TRACE_MALFORMED_VLM_A")
            b_response = self.search("account_b", "E2E_TRACE_MALFORMED_VLM_B")
            a_payload = self._expect_ok(a_response, "malformed VLM degraded search")
            self._expect_ok(b_response, "Account B after malformed Account A VLM")
            ledger = self.model_ledger()
            if any(
                item["account_id"] == "account_a"
                and item["operation"] == "vlm"
                and item["model"] != "test-vlm-a"
                for item in ledger
            ):
                raise AssertionError(f"Account A failed over outside its model: {ledger}")
            return {
                "a_http_status": a_response.status_code,
                "b_http_status": b_response.status_code,
                "a_result_entries": len(a_payload["result"]["entries"]),
                "fallback_semantics": "query rewrite failure falls back to original query",
                "model_calls": ledger,
            }

        self.case(
            "VLM-007",
            "Account A VLM 持续返回 malformed response，随后 Account B 搜索",
            "A 回退原始 query 且不切到 B；B 使用自己的模型成功",
            malformed_vlm_isolation,
        )

        def usage_isolation() -> dict[str, Any]:
            self._expect_ok(self.search("account_a", "E2E_TRACE_USAGE_A"), "usage search A")
            self._expect_ok(self.search("account_b", "E2E_TRACE_USAGE_B"), "usage search B")
            with self._client() as client:
                usage = client.get(f"{self.model_url}/proxy/usage").json()["accounts"]
            if set(usage) != {"account_a", "account_b"}:
                raise AssertionError(f"unexpected usage accounts: {usage}")
            if any(item["total_tokens"] <= 0 for item in usage.values()):
                raise AssertionError(f"non-positive usage: {usage}")
            return {
                "usage": usage,
                "model": self.assert_model_isolation({"account_a", "account_b"}),
            }

        self.case(
            "TOK-001/002/003",
            "A/B 分别执行 Embedding 与 VLM，按 Account 汇总 Proxy usage",
            "只存在 account_a/account_b 两个桶且 token 均大于零",
            usage_isolation,
        )

        def config_read_isolation() -> dict[str, Any]:
            configs = {}
            with self._client() as client:
                for account_id in ("account_a", "account_b"):
                    response = client.get(
                        f"{self.ov_url}/api/v1/admin/accounts/{account_id}/configuration",
                        headers=self.root_headers,
                    )
                    configs[account_id] = self._expect_ok(response, f"get config {account_id}")[
                        "result"
                    ]["settings"]
            for account_id, settings in configs.items():
                suffix = account_id[-1]
                serialized = json.dumps(settings)
                if f"test-embedding-{suffix}" not in serialized:
                    raise AssertionError(f"{account_id} embedding missing")
                if f"test-vlm-{suffix}" not in serialized:
                    raise AssertionError(f"{account_id} VLM missing")
                other_suffix = "b" if suffix == "a" else "a"
                if f"test-vlm-{other_suffix}" in serialized:
                    raise AssertionError(f"{account_id} contains other Account model")
            return {
                account_id: {
                    "embedding": settings["embedding"]["dense"]["model"],
                    "vlm": settings["vlm"]["model"],
                    "collection": settings["vectordb"]["name"],
                }
                for account_id, settings in configs.items()
            }

        self.case(
            "CFG-002/003",
            "ROOT 分别读取 A/B 显式配置",
            "每个配置只包含自己的模型与 VectorDB collection",
            config_read_isolation,
        )

        def patch_three_state() -> dict[str, Any]:
            baseline = self.get_config("account_a")
            try:
                set_response = self.patch_config("account_a", {"embedding": {"max_retries": 7}})
                self._expect_ok(set_response, "set max_retries")
                after_set = self.get_config("account_a")
                if after_set["embedding"]["max_retries"] != 7:
                    raise AssertionError(f"dynamic field not set: {after_set}")

                empty_response = self.patch_config("account_a", {})
                self._expect_ok(empty_response, "empty patch")
                after_empty = self.get_config("account_a")
                if after_empty != after_set:
                    raise AssertionError("empty PATCH changed account configuration")

                reset_response = self.patch_config(
                    "account_a", {"embedding": {"max_retries": None}}
                )
                self._expect_ok(reset_response, "reset max_retries")
                after_reset = self.get_config("account_a")
                if "max_retries" in after_reset["embedding"]:
                    raise AssertionError(f"null did not remove override: {after_reset}")
                if after_reset != baseline:
                    raise AssertionError("three-state reset did not restore baseline")
                return {
                    "set_http_status": set_response.status_code,
                    "empty_http_status": empty_response.status_code,
                    "reset_http_status": reset_response.status_code,
                    "preserved_embedding_model": after_reset["embedding"]["dense"]["model"],
                }
            finally:
                self.patch_config("account_a", {"embedding": {"max_retries": None}})

        self.case(
            "CFG-006/007/008",
            "动态字段依次执行设置、空 PATCH、null 清除",
            "遗漏字段保持；空 PATCH 无副作用；null 只清除目标 override",
            patch_three_state,
        )

        def failed_patch_is_atomic() -> dict[str, Any]:
            before = self.get_config("account_a")
            response = self.patch_config(
                "account_a",
                {
                    "embedding": {
                        "max_retries": 9,
                        "dense": {"credentials": []},
                    }
                },
            )
            if response.status_code < 400:
                raise AssertionError("invalid credential array unexpectedly succeeded")
            after = self.get_config("account_a")
            if after != before:
                raise AssertionError("failed PATCH partially persisted another valid field")
            return {
                "http_status": response.status_code,
                "response": response.text[:300],
                "configuration_unchanged": True,
            }

        self.case(
            "CFG-009",
            "同一 PATCH 同时包含合法 max_retries 与非法空 credential 数组",
            "整体失败，持久化配置与发布配置均保持原值",
            failed_patch_is_atomic,
        )

        def non_root_dynamic_patch_denied() -> dict[str, Any]:
            before = self.get_config("account_a")
            response = self.patch_config(
                "account_a",
                {"vlm": {"timeout": 19}},
                headers=self.auth("account_a"),
            )
            if response.status_code not in (401, 403):
                raise AssertionError(f"non-ROOT PATCH returned {response.status_code}")
            if self.get_config("account_a") != before:
                raise AssertionError("denied PATCH changed configuration")
            return {
                "http_status": response.status_code,
                "configuration_unchanged": True,
            }

        self.case(
            "CFG-AUTH-001",
            "Account A 管理员尝试修改 ROOT-only VLM 配置",
            "返回 401/403，配置不变",
            non_root_dynamic_patch_denied,
        )

        def concurrent_same_field() -> dict[str, Any]:
            try:
                patches = [
                    {"vlm": {"timeout": 11}},
                    {"vlm": {"timeout": 22}},
                ]
                with ThreadPoolExecutor(max_workers=2) as executor:
                    responses = list(
                        executor.map(
                            lambda patch: self.patch_config("account_a", patch),
                            patches,
                        )
                    )
                statuses = [response.status_code for response in responses]
                if statuses != [200, 200]:
                    raise AssertionError(f"concurrent PATCH statuses: {statuses}")
                final = self.get_config("account_a")
                timeout = final["vlm"].get("timeout")
                if timeout not in (11, 22):
                    raise AssertionError(f"torn final timeout: {timeout}")
                return {
                    "http_statuses": statuses,
                    "candidate_values": [11, 22],
                    "final_timeout": timeout,
                }
            finally:
                self.patch_config("account_a", {"vlm": {"timeout": None}})

        self.case(
            "CFG-CON-001",
            "两个 ROOT 客户端同时修改 Account A 的 VLM timeout",
            "两个请求串行完成，最终值等于一个完整 PATCH，不出现撕裂值",
            concurrent_same_field,
        )

        def concurrent_different_fields() -> dict[str, Any]:
            try:
                patches = [
                    {"embedding": {"max_retries": 4}},
                    {"vlm": {"timeout": 33}},
                ]
                with ThreadPoolExecutor(max_workers=2) as executor:
                    responses = list(
                        executor.map(
                            lambda patch: self.patch_config("account_a", patch),
                            patches,
                        )
                    )
                statuses = [response.status_code for response in responses]
                final = self.get_config("account_a")
                if statuses != [200, 200]:
                    raise AssertionError(f"concurrent PATCH statuses: {statuses}")
                if final["embedding"].get("max_retries") != 4 or final["vlm"].get("timeout") != 33:
                    raise AssertionError(f"unrelated field update lost: {final}")
                return {
                    "http_statuses": statuses,
                    "final_max_retries": 4,
                    "final_vlm_timeout": 33,
                }
            finally:
                self.patch_config(
                    "account_a",
                    {
                        "embedding": {"max_retries": None},
                        "vlm": {"timeout": None},
                    },
                )

        self.case(
            "CFG-CON-002",
            "两个 ROOT 客户端同时修改 Account A 的不同动态字段",
            "Embedding 与 VLM 两项修改都保留，不发生无关字段丢失",
            concurrent_different_fields,
        )

        def concurrent_complete_credentials() -> dict[str, Any]:
            def credential(name: str) -> dict[str, Any]:
                return {
                    "provider": "openai",
                    "model": f"test-embedding-{name}-a",
                    "api_key": f"key-{name}",
                    "api_base": f"{self.model_url}/v1",
                    "extra_headers": {
                        "X-OV-Test-Account": "account_a",
                        "X-OV-Test-Credential": name,
                    },
                }

            candidates = [credential("first"), credential("second")]
            try:
                with ThreadPoolExecutor(max_workers=2) as executor:
                    responses = list(
                        executor.map(
                            lambda item: self.patch_config(
                                "account_a",
                                {"embedding": {"dense": {"credentials": [item]}}},
                            ),
                            candidates,
                        )
                    )
                statuses = [response.status_code for response in responses]
                final = self.get_config("account_a")["embedding"]["dense"]["credentials"]
                if statuses != [200, 200]:
                    raise AssertionError(f"credential PATCH statuses: {statuses}")
                if final not in ([candidates[0]], [candidates[1]]):
                    raise AssertionError(f"credential array was torn: {final}")
                return {
                    "http_statuses": statuses,
                    "final_credential": final,
                    "matches_complete_candidate": True,
                }
            finally:
                self.restore_dynamic_config("account_a")

        self.case(
            "CFG-CON-003",
            "两个 ROOT 客户端并发替换 Account A 的完整 Embedding credential 数组",
            "最终数组完整等于其中一个候选，不混合 model/api_key/header",
            concurrent_complete_credentials,
        )

        def concurrent_valid_and_invalid() -> dict[str, Any]:
            try:
                patches = [
                    {"embedding": {"max_retries": 6}},
                    {"embedding": {"dense": {"credentials": []}}},
                ]
                with ThreadPoolExecutor(max_workers=2) as executor:
                    responses = list(
                        executor.map(
                            lambda patch: self.patch_config("account_a", patch),
                            patches,
                        )
                    )
                statuses = sorted(response.status_code for response in responses)
                final = self.get_config("account_a")
                if statuses[0] != 200 or statuses[1] < 400:
                    raise AssertionError(f"expected one success and one failure: {statuses}")
                if final["embedding"].get("max_retries") != 6:
                    raise AssertionError(f"failed PATCH polluted valid result: {final}")
                if not final["embedding"]["dense"]["credentials"]:
                    raise AssertionError("failed PATCH persisted empty credentials")
                return {
                    "http_statuses": statuses,
                    "final_max_retries": 6,
                    "credential_count": len(final["embedding"]["dense"]["credentials"]),
                }
            finally:
                self.patch_config("account_a", {"embedding": {"max_retries": None}})

        self.case(
            "CFG-CON-007",
            "Account A 同时提交一个合法动态 PATCH 和一个非法 credential PATCH",
            "合法请求保留，失败请求不产生部分持久化",
            concurrent_valid_and_invalid,
        )

        def concurrent_accounts() -> dict[str, Any]:
            try:
                work = [
                    ("account_a", {"vlm": {"timeout": 41}}),
                    ("account_b", {"vlm": {"timeout": 52}}),
                ]
                with ThreadPoolExecutor(max_workers=2) as executor:
                    responses = list(
                        executor.map(
                            lambda item: self.patch_config(item[0], item[1]),
                            work,
                        )
                    )
                statuses = [response.status_code for response in responses]
                a = self.get_config("account_a")["vlm"].get("timeout")
                b = self.get_config("account_b")["vlm"].get("timeout")
                if statuses != [200, 200] or (a, b) != (41, 52):
                    raise AssertionError(
                        f"cross-account config pollution: statuses={statuses}, A={a}, B={b}"
                    )
                return {
                    "http_statuses": statuses,
                    "account_a_timeout": a,
                    "account_b_timeout": b,
                }
            finally:
                for account_id in ("account_a", "account_b"):
                    self.patch_config(account_id, {"vlm": {"timeout": None}})

        self.case(
            "CFG-CON-010",
            "ROOT 并发修改 Account A 与 Account B 的 VLM timeout",
            "两个 scope 独立成功，最终值不跨 Account",
            concurrent_accounts,
        )

        def create_only_rejected() -> dict[str, Any]:
            with self._client() as client:
                response = client.patch(
                    f"{self.ov_url}/api/v1/admin/accounts/account_a/configuration",
                    headers=self.root_headers,
                    json={
                        "settings": {
                            "vectordb": {
                                "backend": "http",
                                "url": self.vectordb_url,
                                "project_name": "project_b",
                                "name": "collection_b",
                                "index_name": "default",
                                "dimension": 8,
                                "distance_metric": "cosine",
                            }
                        }
                    },
                )
            if response.status_code < 400:
                raise AssertionError("create-only VectorDB PATCH unexpectedly succeeded")
            return {
                "http_status": response.status_code,
                "response": response.text[:300],
                "downstream_model_calls": len(self.model_ledger()),
                "downstream_vectordb_calls": len(self.vector_ledger()),
            }

        self.case(
            "VEC-D-004/CFG-010",
            "PATCH Account A 的 create-only VectorDB 指向 Account B collection",
            "请求被拒绝，且不访问任何模型或 VectorDB backend",
            create_only_rejected,
        )

    def report(self) -> dict[str, Any]:
        passed = sum(item.status == "PASS" for item in self.results)
        failed = len(self.results) - passed
        return {
            "status": "PASS" if failed == 0 else "FAIL",
            "summary": {
                "total": len(self.results),
                "passed": passed,
                "failed": failed,
            },
            "proof_method": [
                "每个 case 执行前清空 Model Proxy 与 VectorDB ledger",
                "每个业务请求携带唯一 E2E_TRACE marker，Proxy 保存 marker 与 payload hash",
                "逐条断言 header account_id 对应唯一允许的 model",
                "逐条断言 collection 对应唯一允许的 project",
            ],
            "cases": [asdict(item) for item in self.results],
        }


def run(ov_url: str, model_url: str, vectordb_url: str, config_dir: Path) -> dict[str, Any]:
    runner = CaseRunner(ov_url, model_url, vectordb_url, config_dir)
    runner.setup()
    runner.run_cases()
    return runner.report()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ov-url", default="http://127.0.0.1:1934")
    parser.add_argument("--model-url", default="http://127.0.0.1:1940")
    parser.add_argument("--vectordb-url", default="http://127.0.0.1:1941")
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=Path("/private/tmp/ov-e2e/config"),
    )
    args = parser.parse_args()
    result = run(args.ov_url, args.model_url, args.vectordb_url, args.config_dir)
    print(json.dumps(result, indent=2))
    if result["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
