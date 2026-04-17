"""Normalize split assignments in radioval_harmonized.csv, append missing technical GUMED cases, and write the result to radioval_harmonized_new.csv."""

from __future__ import annotations

import csv
import re
from collections import Counter, defaultdict
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
INPUT_CSV_PATH = BASE_DIR / "radioval_harmonized.csv"
OUTPUT_CSV_PATH = BASE_DIR / "radioval_harmonized_new.csv"
SPLIT_KEY = "split"
DATASET_KEY = "dataset"
PATIENT_ID_KEY = "patient_id"

HULAFE_53_PATIENT_IDS = {
    "RV_01_00254",
    "RV_01_00256",
    "RV_01_00260",
    "RV_01_00261",
    "RV_01_00262",
    "RV_01_00263",
    "RV_01_00265",
    "RV_01_00267",
    "RV_01_00268",
    "RV_01_00271",
    "RV_01_00272",
    "RV_01_00274",
    "RV_01_00276",
    "RV_01_00277",
    "RV_01_00279",
    "RV_01_00280",
    "RV_01_00284",
    "RV_01_00285",
    "RV_01_00287",
    "RV_01_00288",
    "RV_01_00296",
    "RV_01_00297",
    "RV_01_00299",
    "RV_01_00300",
    "RV_01_00301",
    "RV_01_00302",
    "RV_01_00303",
    "RV_01_00304",
    "RV_01_00305",
    "RV_01_00306",
    "RV_01_00307",
    "RV_01_00308",
    "RV_01_00309",
    "RV_01_00310",
    "RV_01_00311",
    "RV_01_00312",
    "RV_01_00313",
    "RV_01_00314",
    "RV_01_00148",
    "RV_01_00298",
}

KI_SPLIT_MAPPING = {
    "test": "5152/val",
    "train/val": "train",
}

MUW_SPLIT_MAPPING = {
    "TR": "5152/train",
    "CV": "53",
}

GUMED_MISSING_PATIENT_IDS = [
    f"RV_03_{index:05d}_WP53"
    for index in range(1, 30)
]
PATIENT_ID_SUFFIX_PATTERN = re.compile(r"_(1|2)$")


def load_rows(csv_path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with csv_path.open(newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        return reader.fieldnames or [], list(reader)


def normalize_patient_id(patient_id: str) -> str:
    return PATIENT_ID_SUFFIX_PATTERN.sub("", patient_id.strip())


def normalize_split_value(row: dict[str, str]) -> str:
    dataset = row[DATASET_KEY]
    patient_id = normalize_patient_id(row[PATIENT_ID_KEY])
    current_split = row[SPLIT_KEY]

    if dataset == "HULAFE":
        if patient_id in HULAFE_53_PATIENT_IDS:
            return "53"
        return "5152/train"

    if dataset == "KI":
        return KI_SPLIT_MAPPING.get(current_split, current_split)

    if dataset == "MUW":
        return MUW_SPLIT_MAPPING.get(current_split, current_split)

    return current_split


def normalize_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    normalized_rows: list[dict[str, str]] = []

    for row in rows:
        normalized_row = dict(row)
        normalized_row[PATIENT_ID_KEY] = normalize_patient_id(row[PATIENT_ID_KEY])
        normalized_row[SPLIT_KEY] = normalize_split_value(row)
        normalized_rows.append(normalized_row)

    return normalized_rows


def append_missing_gumed_rows(
    fieldnames: list[str],
    rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    existing_case_keys = {
        (row.get(DATASET_KEY, "").strip(), row.get(PATIENT_ID_KEY, "").strip())
        for row in rows
    }
    completed_rows = list(rows)

    for patient_id in GUMED_MISSING_PATIENT_IDS:
        case_key = ("GUMED", patient_id)
        if case_key in existing_case_keys:
            continue

        missing_row = {fieldname: "" for fieldname in fieldnames}
        missing_row[DATASET_KEY] = "GUMED"
        missing_row[PATIENT_ID_KEY] = patient_id
        missing_row[SPLIT_KEY] = "53"
        completed_rows.append(missing_row)

    return completed_rows


def write_rows(csv_path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with csv_path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def collect_split_summary(rows: list[dict[str, str]]) -> dict[str, dict[str, int]]:
    split_counts_by_dataset: defaultdict[str, Counter[str]] = defaultdict(Counter)

    for row in rows:
        split_counts_by_dataset[row[DATASET_KEY]][row[SPLIT_KEY]] += 1

    return {
        dataset: dict(counter)
        for dataset, counter in sorted(split_counts_by_dataset.items())
        if dataset in {"HULAFE", "KI", "MUW", "GUMED"}
    }


def main() -> None:
    fieldnames, rows = load_rows(INPUT_CSV_PATH)
    normalized_rows = normalize_rows(rows)
    completed_rows = append_missing_gumed_rows(fieldnames, normalized_rows)
    write_rows(OUTPUT_CSV_PATH, fieldnames, completed_rows)

    print(f"Created enriched file: {OUTPUT_CSV_PATH}")
    print("Updated split summary for HULAFE, KI, MUW, and GUMED:")
    for dataset, split_counts in collect_split_summary(completed_rows).items():
        print(f"{dataset}: {split_counts}")


if __name__ == "__main__":
    main()
