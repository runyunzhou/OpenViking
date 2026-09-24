"""Generate isolated deterministic configuration for account isolation E2E tests."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

DIMENSION = 8
ROOT_API_KEY = "ov-e2e-root-key"


def model_settings(account_id: str, model_proxy_url: str) -> dict[str, Any]:
    suffix = "a" if account_id == "account_a" else "b"
    headers = {"X-OV-Test-Account": account_id}
    return {
        "embedding": {
            "dense": {
                "model": f"test-embedding-{suffix}",
                "dimension": DIMENSION,
                "input": "text",
                "credentials": [
                    {
                        "provider": "openai",
                        "api_key": "e2e-only",
                        "api_base": f"{model_proxy_url}/v1",
                        "extra_headers": headers,
                    }
                ],
            }
        },
        "vlm": {
            "model": f"test-vlm-{suffix}",
            "credentials": [
                {
                    "provider": "openai",
                    "api_key": "e2e-only",
                    "api_base": f"{model_proxy_url}/v1",
                    "extra_headers": headers,
                }
            ],
        },
    }


def account_settings(
    account_id: str,
    model_proxy_url: str,
    vectordb_url: str,
) -> dict[str, Any]:
    suffix = "a" if account_id == "account_a" else "b"
    return {
        **model_settings(account_id, model_proxy_url),
        "vectordb": {
            "backend": "http",
            "url": vectordb_url,
            "project_name": f"project_{suffix}",
            "name": f"collection_{suffix}",
            "index_name": "default",
            "dimension": DIMENSION,
            "distance_metric": "cosine",
        },
    }


def server_config(run_dir: Path, model_proxy_url: str) -> dict[str, Any]:
    return {
        "server": {
            "host": "127.0.0.1",
            "port": 1934,
            "workers": 1,
            "root_api_key": ROOT_API_KEY,
        },
        "storage": {"workspace": str((run_dir / "workspace").resolve())},
        "embedding": {
            "dense": {
                "provider": "openai",
                "api_key": "e2e-only",
                "api_base": f"{model_proxy_url}/v1",
                "model": "test-embedding-cluster",
                "dimension": DIMENSION,
                "input": "text",
                "extra_headers": {"X-OV-Test-Account": "cluster"},
            }
        },
        "vlm": {
            "provider": "openai",
            "api_key": "e2e-only",
            "api_base": f"{model_proxy_url}/v1",
            "model": "test-vlm-cluster",
            "extra_headers": {"X-OV-Test-Account": "cluster"},
        },
    }


def write_configs(run_dir: Path) -> list[Path]:
    config_dir = run_dir / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    model_proxy_url = "http://127.0.0.1:1940"
    vectordb_url = "http://127.0.0.1:1941"
    outputs = {
        "ov.conf": server_config(run_dir, model_proxy_url),
        "account_a.json": account_settings("account_a", model_proxy_url, vectordb_url),
        "account_b.json": account_settings("account_b", model_proxy_url, vectordb_url),
    }
    paths = []
    for name, payload in outputs.items():
        path = config_dir / name
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        path.chmod(0o600)
        paths.append(path)
    return paths


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, default=Path("/private/tmp/ov-e2e"))
    args = parser.parse_args()
    for path in write_configs(args.run_dir.resolve()):
        print(path)


if __name__ == "__main__":
    main()
