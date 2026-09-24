"""Start the deterministic E2E stack, run smoke assertions, and stop it."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import time
from pathlib import Path

import httpx
from generate_config import write_configs
from smoke import run


def _wait_for(url: str, process: subprocess.Popen, timeout: float = 90) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"process exited before {url}: code={process.returncode}")
        try:
            if httpx.get(url, timeout=1).status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.25)
    raise TimeoutError(f"timed out waiting for {url}")


def _stop(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, default=Path("/private/tmp/ov-e2e"))
    parser.add_argument("--keep-data", action="store_true")
    args = parser.parse_args()

    run_dir = args.run_dir.resolve()
    if run_dir.exists() and not args.keep_data:
        shutil.rmtree(run_dir)
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    write_configs(run_dir)

    repo = Path(__file__).resolve().parents[3]
    python = repo / ".venv/bin/python"
    commands = [
        (
            "model-proxy",
            [
                str(python),
                str(Path(__file__).with_name("model_proxy.py")),
                "--port",
                "1940",
            ],
        ),
        (
            "vectordb-mock",
            [
                str(python),
                str(Path(__file__).with_name("vectordb_mock.py")),
                "--port",
                "1941",
                "--persist-path",
                str(run_dir / "vectordb"),
                "--precreate",
                "a",
                "b",
            ],
        ),
        (
            "openviking",
            [
                str(repo / ".venv/bin/openviking-server"),
                "--config",
                str(run_dir / "config/ov.conf"),
                "--port",
                "1934",
                "--workers",
                "1",
            ],
        ),
    ]
    processes: list[tuple[subprocess.Popen, object]] = []
    try:
        for name, command in commands:
            log = (run_dir / "logs" / f"{name}.log").open("w", encoding="utf-8")
            process = subprocess.Popen(
                command,
                cwd=run_dir,
                stdout=log,
                stderr=subprocess.STDOUT,
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
                start_new_session=True,
            )
            processes.append((process, log))
            health_url = {
                "model-proxy": "http://127.0.0.1:1940/health",
                "vectordb-mock": "http://127.0.0.1:1941/health",
                "openviking": "http://127.0.0.1:1934/health",
            }[name]
            _wait_for(health_url, process)

        result = run(
            "http://127.0.0.1:1934",
            "http://127.0.0.1:1940",
            "http://127.0.0.1:1941",
            run_dir / "config",
        )
        report_dir = run_dir / "report"
        report_dir.mkdir(parents=True, exist_ok=True)
        (report_dir / "results.json").write_text(
            json.dumps(result, indent=2) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(result, indent=2))
        if result["status"] != "PASS":
            raise RuntimeError(
                f"{result['summary']['failed']} E2E case(s) failed; "
                f"see {report_dir / 'results.json'}"
            )
    finally:
        for process, _log in reversed(processes):
            _stop(process)
        for _process, log in processes:
            log.close()


if __name__ == "__main__":
    main()
