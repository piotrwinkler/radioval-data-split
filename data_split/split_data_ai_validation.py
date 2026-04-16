"""Split method: build ai_validation.csv from radioval_harmonized_new.csv by excluding cases already selected into clinical_validation_5152.csv and clinical_validation_53.csv, then selecting KI cases with split=5152/val and all remaining cases for UZSM, HUH, AFI, and ASU, including all rows for each selected patient ID."""

from __future__ import annotations

import sys
from pathlib import Path


CURRENT_DIR = Path(__file__).resolve().parent
BASE_DIR = CURRENT_DIR.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from split_utils import load_rows, select_all_rows_for_patient_ids, write_rows


INPUT_CSV_PATH = BASE_DIR / "radioval_harmonized_new.csv"
EXCLUDED_5152_CSV_PATH = BASE_DIR / "output_files" / "clinical_validation_5152.csv"
EXCLUDED_53_CSV_PATH = BASE_DIR / "output_files" / "clinical_validation_53.csv"
OUTPUT_CSV_PATH = BASE_DIR / "output_files" / "ai_validation.csv"
DATASET_KEY = "dataset"
PATIENT_ID_KEY = "patient_id"
SPLIT_KEY = "split"

DIRECT_DATASETS = ["UZSM", "HUH", "AFI", "ASU"]
KI_DATASET = "KI"
KI_REQUIRED_SPLIT = "5152/val"


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
    return load_excluded_cases(EXCLUDED_5152_CSV_PATH) | load_excluded_cases(EXCLUDED_53_CSV_PATH)


def filter_remaining_rows(
    rows: list[dict[str, str]],
    dataset_name: str,
    excluded_cases: set[tuple[str, str]],
) -> list[dict[str, str]]:
    return [
        row
        for row in rows
        if row[DATASET_KEY] == dataset_name and (row[DATASET_KEY], row[PATIENT_ID_KEY]) not in excluded_cases
    ]


def select_ki_rows(
    rows: list[dict[str, str]],
    excluded_cases: set[tuple[str, str]],
) -> list[dict[str, str]]:
    remaining_rows = filter_remaining_rows(rows, KI_DATASET, excluded_cases)
    selected_patient_ids = [
        row[PATIENT_ID_KEY]
        for row in remaining_rows
        if row[SPLIT_KEY].strip() == KI_REQUIRED_SPLIT
    ]
    return select_all_rows_for_patient_ids(
        rows=remaining_rows,
        patient_ids=list(dict.fromkeys(selected_patient_ids)),
    )


def select_direct_dataset_rows(
    rows: list[dict[str, str]],
    dataset_name: str,
    excluded_cases: set[tuple[str, str]],
) -> list[dict[str, str]]:
    remaining_rows = filter_remaining_rows(rows, dataset_name, excluded_cases)
    selected_patient_ids = list(dict.fromkeys(row[PATIENT_ID_KEY] for row in remaining_rows))
    return select_all_rows_for_patient_ids(
        rows=remaining_rows,
        patient_ids=selected_patient_ids,
    )


def build_ai_validation(rows: list[dict[str, str]]) -> tuple[list[dict[str, str]], dict[str, dict[str, int]]]:
    excluded_cases = collect_all_excluded_cases()
    selected_rows: list[dict[str, str]] = []
    selection_summary: dict[str, dict[str, int]] = {}

    ki_rows = select_ki_rows(rows, excluded_cases)
    selected_rows.extend(ki_rows)
    selection_summary[KI_DATASET] = {
        "rows": len(ki_rows),
        "cases": len({row[PATIENT_ID_KEY] for row in ki_rows}),
    }

    for dataset_name in DIRECT_DATASETS:
        dataset_rows = select_direct_dataset_rows(rows, dataset_name, excluded_cases)
        selected_rows.extend(dataset_rows)
        selection_summary[dataset_name] = {
            "rows": len(dataset_rows),
            "cases": len({row[PATIENT_ID_KEY] for row in dataset_rows}),
        }

    return selected_rows, selection_summary


def run() -> dict[str, object]:
    fieldnames, rows = load_rows(INPUT_CSV_PATH)
    selected_rows, selection_summary = build_ai_validation(rows)
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
