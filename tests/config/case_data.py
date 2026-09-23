import json
from pathlib import Path

ACCOUNT_RUNTIME_CASES = json.loads(
    (Path(__file__).parent / "fixtures" / "account_runtime_cases.json").read_text(encoding="utf-8")
)
