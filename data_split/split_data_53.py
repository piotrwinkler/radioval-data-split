"""Split method: build clinical_validation_53.csv from radioval_harmonized_new.csv by combining fixed split=53 cohorts for HULAFE and MUW, latest-case cohorts for GUMED/HUH/AFI/ASU, and excluding all cases already present in clinical_validation_5152.csv."""

from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path


CURRENT_DIR = Path(__file__).resolve().parent
BASE_DIR = CURRENT_DIR.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from split_utils import (
    load_rows,
    select_all_rows_for_patient_ids,
    select_latest_patient_ids,
    write_rows,
)


INPUT_CSV_PATH = BASE_DIR / "radioval_harmonized_new.csv"
EXCLUDED_CASES_CSV_PATH = BASE_DIR / "output_files" / "clinical_validation_5152.csv"
OUTPUT_CSV_PATH = BASE_DIR / "output_files" / "clinical_validation_53.csv"
DATASET_KEY = "dataset"
PATIENT_ID_KEY = "patient_id"
SPLIT_KEY = "split"

LATEST_CASES_BY_DATASET = {
    "GUMED": 60,
    "HUH": 44,
    "AFI": 34,
    "ASU": 40,
}

SYNTHETIC_GUMED_CASE_COUNT = 29
SYNTHETIC_GUMED_PATIENT_ID_TEMPLATE = "RV_03_{index:05d}_WP53"


def split_contains_tag(split_value: str, split_tag: str) -> bool:
    cleaned_value = split_value.strip()
    if not cleaned_value:
        return False
    return split_tag in cleaned_value.split("/")


def load_excluded_cases(csv_path: Path) -> set[tuple[str, str]]:
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Missing prerequisite file: {csv_path}. Run the 5152 split before generating the 53 split."
        )

    _, rows = load_rows(csv_path)
    return {
        (row[DATASET_KEY], row[PATIENT_ID_KEY])
        for row in rows
        if row[DATASET_KEY].strip() and row[PATIENT_ID_KEY].strip()
    }


def filter_rows(
    rows: list[dict[str, str]],
    dataset_name: str,
    excluded_cases: set[tuple[str, str]],
) -> list[dict[str, str]]:
    return [
        row
        for row in rows
        if row[DATASET_KEY] == dataset_name and (row[DATASET_KEY], row[PATIENT_ID_KEY]) not in excluded_cases
    ]


def select_fixed_split_rows(
    rows: list[dict[str, str]],
    dataset_name: str,
    split_tag: str,
    excluded_cases: set[tuple[str, str]],
) -> list[dict[str, str]]:
    return [
        row
        for row in rows
        if row[DATASET_KEY] == dataset_name
        and split_contains_tag(row[SPLIT_KEY], split_tag)
        and (row[DATASET_KEY], row[PATIENT_ID_KEY]) not in excluded_cases
    ]


def select_latest_dataset_rows(
    rows: list[dict[str, str]],
    dataset_name: str,
    target_case_count: int,
    excluded_cases: set[tuple[str, str]],
) -> list[dict[str, str]]:
    candidate_rows = filter_rows(rows, dataset_name, excluded_cases)
    selected_patient_ids = select_latest_patient_ids(
        rows=candidate_rows,
        count=target_case_count,
    )
    return select_all_rows_for_patient_ids(
        rows=candidate_rows,
        patient_ids=selected_patient_ids,
    )


def build_synthetic_gumed_rows(fieldnames: list[str]) -> list[dict[str, str]]:
    synthetic_rows: list[dict[str, str]] = []

    for index in range(1, SYNTHETIC_GUMED_CASE_COUNT + 1):
        row = {fieldname: "" for fieldname in fieldnames}
        row[PATIENT_ID_KEY] = SYNTHETIC_GUMED_PATIENT_ID_TEMPLATE.format(index=index)
        row[DATASET_KEY] = "GUMED"
        synthetic_rows.append(row)

    return synthetic_rows


def build_clinical_validation_53(
    fieldnames: list[str],
    rows: list[dict[str, str]],
    excluded_cases: set[tuple[str, str]],
) -> tuple[list[dict[str, str]], dict[str, dict[str, int]]]:
    selected_rows: list[dict[str, str]] = []
    selection_summary: dict[str, dict[str, int]] = {}

    hulafe_rows = select_fixed_split_rows(rows, "HULAFE", "53", excluded_cases)
    muw_rows = select_fixed_split_rows(rows, "MUW", "53", excluded_cases)
    selected_rows.extend(hulafe_rows)
    selected_rows.extend(muw_rows)

    selection_summary["HULAFE"] = {
        "rows": len(hulafe_rows),
        "cases": len({row[PATIENT_ID_KEY] for row in hulafe_rows}),
    }
    selection_summary["MUW"] = {
        "rows": len(muw_rows),
        "cases": len({row[PATIENT_ID_KEY] for row in muw_rows}),
    }

    for dataset_name, target_case_count in LATEST_CASES_BY_DATASET.items():
        dataset_rows = select_latest_dataset_rows(
            rows=rows,
            dataset_name=dataset_name,
            target_case_count=target_case_count,
            excluded_cases=excluded_cases,
        )
        selected_rows.extend(dataset_rows)
        selection_summary[dataset_name] = {
            "rows": len(dataset_rows),
            "cases": len({row[PATIENT_ID_KEY] for row in dataset_rows}),
        }

    synthetic_gumed_rows = build_synthetic_gumed_rows(fieldnames)
    selected_rows.extend(synthetic_gumed_rows)
    selection_summary["GUMED"]["rows"] += len(synthetic_gumed_rows)
    selection_summary["GUMED"]["cases"] += len(synthetic_gumed_rows)

    return selected_rows, selection_summary


def run() -> dict[str, object]:
    fieldnames, rows = load_rows(INPUT_CSV_PATH)
    excluded_cases = load_excluded_cases(EXCLUDED_CASES_CSV_PATH)
    selected_rows, selection_summary = build_clinical_validation_53(
        fieldnames=fieldnames,
        rows=rows,
        excluded_cases=excluded_cases,
    )
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
