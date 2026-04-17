"""Verification method: compare repeated output_files directories and confirm that matching split files have identical row counts and identical patient_id contents, including duplicate occurrences."""

from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
PATIENT_ID_KEY = "patient_id"
OUTPUT_DIRECTORIES = [
    BASE_DIR / "output_files",
    BASE_DIR / "output_files1",
    BASE_DIR / "output_files2",
    BASE_DIR / "output_files3",
    BASE_DIR / "output_files4",
    BASE_DIR / "output_files5",
]
OUTPUT_FILENAMES = [
    "clinical_validation_5152.csv",
    "clinical_validation_53.csv",
    "ai_validation.csv",
    "train.csv",
]


def load_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open(newline="", encoding="utf-8-sig") as csv_file:
        return list(csv.DictReader(csv_file))


def patient_id_counter(rows: list[dict[str, str]]) -> Counter[str]:
    return Counter((row.get(PATIENT_ID_KEY) or "").strip() for row in rows if (row.get(PATIENT_ID_KEY) or "").strip())


def compare_file_across_runs(filename: str) -> bool:
    print(f"File: {filename}")

    reference_path = OUTPUT_DIRECTORIES[0] / filename
    reference_rows = load_rows(reference_path)
    reference_row_count = len(reference_rows)
    reference_patient_ids = patient_id_counter(reference_rows)
    print(
        f"  reference={reference_path.parent.name}, rows={reference_row_count}, "
        f"unique_patient_ids={len(reference_patient_ids)}"
    )

    all_match = True
    for directory in OUTPUT_DIRECTORIES[1:]:
        csv_path = directory / filename
        rows = load_rows(csv_path)
        row_count = len(rows)
        patient_ids = patient_id_counter(rows)

        row_count_match = row_count == reference_row_count
        patient_ids_match = patient_ids == reference_patient_ids
        status = "PASS" if row_count_match and patient_ids_match else "FAIL"
        print(
            f"  compare={directory.name}, rows={row_count}, "
            f"unique_patient_ids={len(patient_ids)}, "
            f"row_count_match={row_count_match}, "
            f"patient_ids_match={patient_ids_match}, status={status}"
        )

        if not row_count_match or not patient_ids_match:
            all_match = False

    return all_match


def main() -> None:
    missing_directories = [directory for directory in OUTPUT_DIRECTORIES if not directory.exists()]
    if missing_directories:
        missing = ", ".join(str(directory) for directory in missing_directories)
        raise FileNotFoundError(f"Missing output directory/directories: {missing}")

    missing_files = [
        str(directory / filename)
        for directory in OUTPUT_DIRECTORIES
        for filename in OUTPUT_FILENAMES
        if not (directory / filename).exists()
    ]
    if missing_files:
        raise FileNotFoundError(
            "Missing output file(s):\n" + "\n".join(missing_files)
        )

    all_files_match = True
    for filename in OUTPUT_FILENAMES:
        file_matches = compare_file_across_runs(filename)
        all_files_match = all_files_match and file_matches
        print()

    if all_files_match:
        print("All repeated output directories contain identical row counts and patient_id contents.")
    else:
        print("At least one output file differs across output directories.")


if __name__ == "__main__":
    main()
