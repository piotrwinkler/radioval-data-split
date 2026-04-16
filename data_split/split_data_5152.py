"""Split method: select per-dataset candidate pools from radioval_harmonized_new.csv using split values that contain 5152 or are empty, then greedily maximize diversity across available stratification variables and include all rows for each selected patient ID."""

from __future__ import annotations

import sys
from pathlib import Path


CURRENT_DIR = Path(__file__).resolve().parent
BASE_DIR = CURRENT_DIR.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from split_utils import (
    COMMON_DIVERSITY_STRATIFIERS,
    load_rows,
    select_all_rows_for_patient_ids,
    select_diverse_patient_rows,
    write_rows,
)


INPUT_CSV_PATH = BASE_DIR / "radioval_harmonized_new.csv"
OUTPUT_CSV_PATH = BASE_DIR / "output_files" / "clinical_validation_5152.csv"
SPLIT_KEY = "split"
DATASET_KEY = "dataset"
PATIENT_ID_KEY = "patient_id"

TARGET_CASES_BY_DATASET = {
    "HULAFE": 6,
    "KI": 6,
    "GUMED": 6,
    "UZSM": 6,
    "MUW": 6,
    "HUH": 6,
    "AFI": 7,
    "ASU": 7,
}


def split_allows_5152(split_value: str) -> bool:
    cleaned_value = split_value.strip()
    if not cleaned_value:
        return True
    return "5152" in cleaned_value.split("/")


def filter_candidate_rows(rows: list[dict[str, str]], dataset_name: str) -> list[dict[str, str]]:
    return [
        row
        for row in rows
        if row[DATASET_KEY] == dataset_name and split_allows_5152(row[SPLIT_KEY])
    ]


def select_dataset_rows(
    rows: list[dict[str, str]],
    dataset_name: str,
    target_case_count: int,
) -> tuple[list[dict[str, str]], list[str], list[str]]:
    candidate_rows = filter_candidate_rows(rows, dataset_name)
    selected_patient_rows, active_stratifiers, skipped_stratifiers = select_diverse_patient_rows(
        rows=candidate_rows,
        target_count=target_case_count,
        stratifiers=COMMON_DIVERSITY_STRATIFIERS,
    )
    selected_patient_ids = [row[PATIENT_ID_KEY] for row in selected_patient_rows]
    selected_rows = select_all_rows_for_patient_ids(
        rows=candidate_rows,
        patient_ids=selected_patient_ids,
    )
    return selected_rows, active_stratifiers, skipped_stratifiers


def build_clinical_validation_5152(
    rows: list[dict[str, str]],
) -> tuple[list[dict[str, str]], dict[str, dict[str, object]]]:
    selected_rows: list[dict[str, str]] = []
    selection_summary: dict[str, dict[str, object]] = {}

    for dataset_name, target_case_count in TARGET_CASES_BY_DATASET.items():
        dataset_rows, active_stratifiers, skipped_stratifiers = select_dataset_rows(
            rows=rows,
            dataset_name=dataset_name,
            target_case_count=target_case_count,
        )
        selected_rows.extend(dataset_rows)
        selection_summary[dataset_name] = {
            "target_case_count": target_case_count,
            "selected_row_count": len(dataset_rows),
            "selected_case_count": len({row[PATIENT_ID_KEY] for row in dataset_rows}),
            "active_stratifiers": active_stratifiers,
            "skipped_stratifiers": skipped_stratifiers,
        }

    return selected_rows, selection_summary


def run() -> dict[str, object]:
    fieldnames, rows = load_rows(INPUT_CSV_PATH)
    selected_rows, selection_summary = build_clinical_validation_5152(rows)
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
        print(
            f"{dataset_name}: cases={summary['selected_case_count']}/{summary['target_case_count']}, "
            f"rows={summary['selected_row_count']}"
        )
        print(f"  active_stratifiers={summary['active_stratifiers']}")
        if summary["skipped_stratifiers"]:
            print(f"  skipped_stratifiers={summary['skipped_stratifiers']}")


if __name__ == "__main__":
    main()
