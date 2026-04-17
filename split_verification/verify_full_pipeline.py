"""Verification method: report row and case counts for every split file, verify pairwise split disjointness on the (dataset, patient_id) level, and compare the combined splits against radioval_harmonized_new.csv."""

from __future__ import annotations

import csv
import os
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
PATIENT_ID_KEY = "patient_id"
DATASET_KEY = "dataset"
SOURCE_CSV_PATH = BASE_DIR / "radioval_harmonized_new.csv"

SPLIT_FILES = {
    "clinical_validation_5152": BASE_DIR / "output_files" / "clinical_validation_5152.csv",
    "clinical_validation_53": BASE_DIR / "output_files" / "clinical_validation_53.csv",
    "ai_validation": BASE_DIR / "output_files" / "ai_validation.csv",
    "train": BASE_DIR / "output_files" / "train.csv",
}

ANSI_RESET = "\033[0m"
ANSI_BOLD = "\033[1m"
ANSI_RED = "\033[31m"
ANSI_YELLOW = "\033[33m"
ANSI_GREEN = "\033[32m"


def load_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open(newline="", encoding="utf-8-sig") as csv_file:
        return list(csv.DictReader(csv_file))


def collect_case_keys(rows: list[dict[str, str]]) -> set[tuple[str, str]]:
    return {
        (row[DATASET_KEY].strip(), row[PATIENT_ID_KEY].strip())
        for row in rows
        if row[DATASET_KEY].strip() and row[PATIENT_ID_KEY].strip()
    }


def collect_per_dataset_stats(rows: list[dict[str, str]]) -> dict[str, tuple[int, int]]:
    rows_by_dataset: defaultdict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        dataset_name = row[DATASET_KEY].strip() or "UNASSIGNED"
        rows_by_dataset[dataset_name].append(row)

    stats_by_dataset: dict[str, tuple[int, int]] = {}
    for dataset_name in sorted(rows_by_dataset):
        dataset_rows = rows_by_dataset[dataset_name]
        dataset_case_count = len(
            {
                row[PATIENT_ID_KEY].strip()
                for row in dataset_rows
                if row[PATIENT_ID_KEY].strip()
            }
        )
        stats_by_dataset[dataset_name] = (len(dataset_rows), dataset_case_count)

    return stats_by_dataset


def collect_patient_ids_by_dataset(rows: list[dict[str, str]]) -> dict[str, set[str]]:
    patient_ids_by_dataset: defaultdict[str, set[str]] = defaultdict(set)
    for row in rows:
        dataset_name = row[DATASET_KEY].strip()
        patient_id = row[PATIENT_ID_KEY].strip()
        if dataset_name and patient_id:
            patient_ids_by_dataset[dataset_name].add(patient_id)
    return dict(patient_ids_by_dataset)


def build_coverage_status(missing_case_count: int, extra_case_count: int) -> str:
    if missing_case_count > 0:
        return "FAIL"
    if extra_case_count > 0:
        return "WARN"
    return "PASS"


def supports_color() -> bool:
    return os.getenv("TERM") not in {None, "dumb"} and os.isatty(1)


def colorize_status(status: str) -> str:
    if not supports_color():
        return status

    color_by_status = {
        "PASS": ANSI_GREEN,
        "WARN": ANSI_YELLOW,
        "FAIL": ANSI_RED,
    }
    color = color_by_status.get(status, "")
    if not color:
        return status
    return f"{ANSI_BOLD}{color}{status}{ANSI_RESET}"


def main() -> None:
    rows_by_split: dict[str, list[dict[str, str]]] = {}
    case_keys_by_split: dict[str, set[tuple[str, str]]] = {}

    for split_name, csv_path in SPLIT_FILES.items():
        rows = load_rows(csv_path)
        rows_by_split[split_name] = rows
        case_keys_by_split[split_name] = collect_case_keys(rows)

        print(f"File: {csv_path}")
        print(f"Split: {split_name}")
        print(f"Total rows: {len(rows)}")
        print(f"Total unique cases: {len(case_keys_by_split[split_name])}")
        print("Per-center stats:")
        for dataset_name, (row_count, case_count) in collect_per_dataset_stats(rows).items():
            print(f"{dataset_name}: rows={row_count}, unique_cases={case_count}")
        print()

    print("Pairwise overlap check on (dataset, patient_id):")
    overlaps_found = False
    for left_name, right_name in combinations(SPLIT_FILES, 2):
        overlap = case_keys_by_split[left_name] & case_keys_by_split[right_name]
        if overlap:
            overlaps_found = True
            overlap_by_dataset: Counter[str] = Counter(dataset for dataset, _ in overlap)
            print(
                f"{left_name} vs {right_name}: overlap_cases={len(overlap)} "
                f"by_dataset={dict(sorted(overlap_by_dataset.items()))}"
            )
        else:
            print(f"{left_name} vs {right_name}: overlap_cases=0")

    if not overlaps_found:
        print("All split files are disjoint on the (dataset, patient_id) level.")

    source_rows = load_rows(SOURCE_CSV_PATH)
    combined_split_rows = [row for rows in rows_by_split.values() for row in rows]

    print()
    print("Comparison against radioval_harmonized_new.csv:")
    print(f"Source file: {SOURCE_CSV_PATH}")
    print(f"Source total rows: {len(source_rows)}")
    print(f"Combined split total rows: {len(combined_split_rows)}")
    print(f"Row count delta (splits - source): {len(combined_split_rows) - len(source_rows)}")
    if len(combined_split_rows) == len(source_rows):
        print(f"Row count status: {colorize_status('PASS')}")
    else:
        print(f"Row count status: {colorize_status('WARN')}")

    source_patient_ids_by_dataset = collect_patient_ids_by_dataset(source_rows)
    split_patient_ids_by_dataset = collect_patient_ids_by_dataset(combined_split_rows)
    all_datasets = sorted(set(source_patient_ids_by_dataset) | set(split_patient_ids_by_dataset))

    print("Per-dataset patient_id coverage:")
    for dataset_name in all_datasets:
        source_patient_ids = source_patient_ids_by_dataset.get(dataset_name, set())
        split_patient_ids = split_patient_ids_by_dataset.get(dataset_name, set())
        missing_patient_ids = sorted(source_patient_ids - split_patient_ids)
        extra_patient_ids = sorted(split_patient_ids - source_patient_ids)
        status = build_coverage_status(
            missing_case_count=len(missing_patient_ids),
            extra_case_count=len(extra_patient_ids),
        )

        print(
            f"{dataset_name}: status={colorize_status(status)}, source_cases={len(source_patient_ids)}, "
            f"split_cases={len(split_patient_ids)}, "
            f"missing_cases={len(missing_patient_ids)}, "
            f"extra_cases={len(extra_patient_ids)}"
        )
        if missing_patient_ids:
            print(f"  missing_sample={missing_patient_ids[:10]}")
        if extra_patient_ids:
            print(f"  extra_sample={extra_patient_ids[:10]}")


if __name__ == "__main__":
    main()
