"""Split method: build train.csv from all remaining HULAFE, KI, GUMED, and MUW cases in radioval_harmonized_new.csv after excluding cases already selected into clinical_validation_5152.csv, clinical_validation_53.csv, and ai_validation.csv, including all rows for each remaining patient ID."""

from __future__ import annotations

import sys
from pathlib import Path


CURRENT_DIR = Path(__file__).resolve().parent
BASE_DIR = CURRENT_DIR.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from split_utils import load_rows, write_rows


INPUT_CSV_PATH = BASE_DIR / "radioval_harmonized_new.csv"
EXCLUDED_5152_CSV_PATH = BASE_DIR / "output_files" / "clinical_validation_5152.csv"
EXCLUDED_53_CSV_PATH = BASE_DIR / "output_files" / "clinical_validation_53.csv"
EXCLUDED_AI_VALIDATION_CSV_PATH = BASE_DIR / "output_files" / "ai_validation.csv"
OUTPUT_CSV_PATH = BASE_DIR / "output_files" / "train.csv"
DATASET_KEY = "dataset"
PATIENT_ID_KEY = "patient_id"

TRAIN_DATASETS = ["HULAFE", "KI", "GUMED", "MUW"]


def load_excluded_cases(csv_path: Path) -> set[tuple[str, str]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing prerequisite file: {csv_path}")

    _, rows = load_rows(csv_path)
    return {
        (row[DATASET_KEY], row[PATIENT_ID_KEY])
        for row in rows
        if row[DATASET_KEY].strip() and row[PATIENT_ID_KEY].strip()
    }


def collect_all_excluded_cases() -> set[tuple[str, str]]:
    return (
        load_excluded_cases(EXCLUDED_5152_CSV_PATH)
        | load_excluded_cases(EXCLUDED_53_CSV_PATH)
        | load_excluded_cases(EXCLUDED_AI_VALIDATION_CSV_PATH)
    )


def build_train_rows(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], dict[str, dict[str, int]]]:
    excluded_cases = collect_all_excluded_cases()
    selected_rows: list[dict[str, str]] = []
    selection_summary: dict[str, dict[str, int]] = {}

    for dataset_name in TRAIN_DATASETS:
        dataset_rows = [
            row
            for row in rows
            if row[DATASET_KEY] == dataset_name
            and (row[DATASET_KEY], row[PATIENT_ID_KEY]) not in excluded_cases
        ]
        selected_rows.extend(dataset_rows)
        selection_summary[dataset_name] = {
            "rows": len(dataset_rows),
            "cases": len({row[PATIENT_ID_KEY] for row in dataset_rows}),
        }

    return selected_rows, selection_summary


def run() -> dict[str, object]:
    fieldnames, rows = load_rows(INPUT_CSV_PATH)
    selected_rows, selection_summary = build_train_rows(rows)
    write_rows(OUTPUT_CSV_PATH, fieldnames, selected_rows)

    return {
        "fieldnames": fieldnames,
        "selected_rows": selected_rows,
        "row_count": len(selected_rows),
        "selection_summary": selection_summary,
        "output_path": OUTPUT_CSV_PATH,
    }


def main() -> None:
    result = run()

    print(f"Created {result['output_path']} with {result['row_count']} rows.")
    for dataset_name, summary in result["selection_summary"].items():
        print(f"{dataset_name}: cases={summary['cases']}, rows={summary['rows']}")


if __name__ == "__main__":
    main()
