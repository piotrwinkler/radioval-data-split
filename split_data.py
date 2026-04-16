"""Split method: run the 5152 split first, then the 53 split that excludes 5152 cases, then the ai_validation split that excludes both 5152 and 53 cases, and finally the train split with all remaining cases."""

from __future__ import annotations

import importlib.util
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_SPLIT_DIR = BASE_DIR / "data_split"

SPLIT_JOBS = [
    {
        "name": "clinical_validation_5152",
        "path": DATA_SPLIT_DIR / "split_data_5152.py",
    },
    {
        "name": "clinical_validation_53",
        "path": DATA_SPLIT_DIR / "split_data_53.py",
    },
    {
        "name": "ai_validation",
        "path": DATA_SPLIT_DIR / "split_data_ai_validation.py",
    },
    {
        "name": "train",
        "path": DATA_SPLIT_DIR / "split_data_train.py",
    },
]


def load_module(module_name: str, file_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module: {file_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_job(job: dict[str, object]) -> dict[str, object]:
    module = load_module(module_name=str(job["name"]), file_path=job["path"])
    if not hasattr(module, "run"):
        raise AttributeError(f"Script {job['path']} does not expose a run() function.")
    return module.run()


def main() -> None:
    print(f"Running {len(SPLIT_JOBS)} job(s)")

    for job in SPLIT_JOBS:
        result = run_job(job=job)
        print(f"[OK] {job['name']}: {result['row_count']} rows -> {result['output_path']}")


if __name__ == "__main__":
    main()
