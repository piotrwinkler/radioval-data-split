"""Shared split helpers for dataset filtering, date parsing, row selection, and optional diversity-based sampling."""

from __future__ import annotations

import csv
import re
from collections import Counter, defaultdict
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable


Row = dict[str, str]
DATE_FORMATS = ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y")


@dataclass(frozen=True)
class Stratifier:
    name: str
    extractor: Callable[[Row], str]
    weight: float = 1.0


def clean_value(value: str | None) -> str:
    return (value or "").strip()


def load_rows(csv_path: Path) -> tuple[list[str], list[Row]]:
    with csv_path.open(newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        return reader.fieldnames or [], list(reader)


def write_rows(csv_path: Path, fieldnames: list[str], rows: list[Row]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_date(value: str | None) -> datetime:
    cleaned_value = clean_value(value)
    if not cleaned_value:
        return datetime.min

    for date_format in DATE_FORMATS:
        try:
            return datetime.strptime(cleaned_value, date_format)
        except ValueError:
            continue

    return datetime.min


def first_non_empty(row: Row, aliases: list[str]) -> str:
    for alias in aliases:
        value = clean_value(row.get(alias))
        if value:
            return value
    return ""


def direct_stratifier(name: str, aliases: list[str], weight: float = 1.0) -> Stratifier:
    return Stratifier(name=name, extractor=lambda row: first_non_empty(row, aliases), weight=weight)


def filter_rows_by_dataset(rows: list[Row], dataset_name: str) -> list[Row]:
    return [row for row in rows if clean_value(row.get("dataset")) == dataset_name]


def row_tiebreaker(row: Row) -> tuple[str, ...]:
    return tuple(clean_value(row.get(key)) for key in sorted(row))


def extract_age_range(row: Row) -> str:
    age_value = clean_value(row.get("age"))
    if age_value:
        normalized_age_value = age_value.replace(",", ".")
        try:
            age = float(normalized_age_value)
            if age < 20:
                return "<20"
            if age >= 80:
                return "80+"
            lower_bound = int(age // 10) * 10
            upper_bound = lower_bound + 10
            return f"{lower_bound}-{upper_bound}"
        except ValueError:
            pass

    return clean_value(row.get("age_group"))


def extract_pcr_status(row: Row) -> str:
    tumor_response = clean_value(row.get("tumor_response")).lower()
    if "pcr" in tumor_response or tumor_response == "grade 5 (pcr)":
        return "pCR"

    rcb = clean_value(row.get("rcb")).lower()
    if "complete response" in rcb:
        return "pCR"

    ypt = clean_value(row.get("ypt")).lower()
    ypn = clean_value(row.get("ypn")).lower()
    if ypt in {"ypt0", "yptis"} and ypn == "ypn0":
        return "pCR"

    if tumor_response or rcb or ypt or ypn:
        return "non-pCR"

    return ""


def extract_t_stage(row: Row) -> str:
    t_initial = clean_value(row.get("t_initial")).upper()
    if not t_initial:
        return ""

    match = re.match(r"^(TIS|TX|T[0-4])", t_initial)
    if match:
        return match.group(1)

    return t_initial


def extract_lesion_presentation(row: Row) -> str:
    mass_shape = clean_value(row.get("mass_shape"))
    if mass_shape:
        return "solid lesion"

    associated_features = clean_value(row.get("associated_features")).lower()
    if associated_features and associated_features != "unknown":
        return "nme / non-solid lesion"

    return ""


def extract_tumor_subtype(row: Row) -> str:
    raw_value = first_non_empty(row, ["tumor_subtype", "intrinsic_subtype", "s_biological_subtype"]).lower()
    if not raw_value:
        return ""

    if "luminal_a" in raw_value or raw_value == "luminal a":
        return "Luminal A"
    if "luminal_b" in raw_value or raw_value == "luminal b":
        return "Luminal B"
    if "triple_negative" in raw_value or "triple negative" in raw_value or "basal" in raw_value:
        return "Basal-like / triple negative"
    if "her2" in raw_value:
        return "HER2-positive"

    return raw_value


def extract_multifocality(row: Row) -> str:
    value = clean_value(row.get("presence_of_foci")).lower()
    if not value:
        return ""
    if "uni" in value:
        return "single lesion"
    if "multi" in value:
        return "multifocal lesion"
    return value


COMMON_DIVERSITY_STRATIFIERS = [
    direct_stratifier(
        "image_type",
        ["imaging_type"],
        weight=1.1,
    ),
    Stratifier(name="pcr", extractor=extract_pcr_status, weight=1.4),
    Stratifier(name="tumor_size_ct", extractor=extract_t_stage, weight=1.2),
    Stratifier(name="lesion_presentation", extractor=extract_lesion_presentation, weight=1.2),
    Stratifier(name="age_range", extractor=extract_age_range, weight=1.1),
    direct_stratifier("menopausal_status", ["menopausal_status"], weight=1.1),
    Stratifier(name="tumor_subtype", extractor=extract_tumor_subtype, weight=1.4),
    Stratifier(name="multifocality", extractor=extract_multifocality, weight=1.1),
    direct_stratifier(
        "race_ethnicity_proxy",
        ["country_of_origin"],
    ),
]


def choose_representative_rows(
    rows: list[Row],
    stratifiers: list[Stratifier] | None = None,
    patient_id_key: str = "patient_id",
    date_key: str = "mri_date",
) -> dict[str, Row]:
    stratifiers = stratifiers or []
    rows_by_patient_id: defaultdict[str, list[Row]] = defaultdict(list)
    for row in rows:
        patient_id = clean_value(row.get(patient_id_key))
        if patient_id:
            rows_by_patient_id[patient_id].append(row)

    representative_rows: dict[str, Row] = {}
    for patient_id, patient_rows in rows_by_patient_id.items():
        representative_rows[patient_id] = max(
            patient_rows,
            key=lambda row: (
                sum(1 for stratifier in stratifiers if clean_value(stratifier.extractor(row))),
                parse_date(row.get(date_key)),
                sum(1 for value in row.values() if clean_value(value)),
                row_tiebreaker(row),
            ),
        )

    return representative_rows


def select_rows_for_patient_ids(
    rows: list[Row],
    patient_ids: list[str],
    stratifiers: list[Stratifier] | None = None,
    patient_id_key: str = "patient_id",
    date_key: str = "mri_date",
) -> list[Row]:
    representative_rows = choose_representative_rows(
        rows=[row for row in rows if clean_value(row.get(patient_id_key)) in set(patient_ids)],
        stratifiers=stratifiers,
        patient_id_key=patient_id_key,
        date_key=date_key,
    )
    return [representative_rows[patient_id] for patient_id in patient_ids if patient_id in representative_rows]


def select_all_rows_for_patient_ids(
    rows: list[Row],
    patient_ids: list[str],
    patient_id_key: str = "patient_id",
) -> list[Row]:
    rows_by_patient_id: defaultdict[str, list[Row]] = defaultdict(list)
    requested_patient_ids = set(patient_ids)

    for row in rows:
        patient_id = clean_value(row.get(patient_id_key))
        if patient_id in requested_patient_ids:
            rows_by_patient_id[patient_id].append(deepcopy(row))

    selected_rows: list[Row] = []
    for patient_id in patient_ids:
        selected_rows.extend(rows_by_patient_id.get(patient_id, []))

    return selected_rows


def select_rows_for_patient_id_occurrences(
    rows: list[Row],
    patient_ids: list[str],
    patient_id_key: str = "patient_id",
    date_key: str = "mri_date",
) -> list[Row]:
    patient_id_counts = Counter(patient_ids)
    rows_by_patient_id: defaultdict[str, list[Row]] = defaultdict(list)

    for row in rows:
        patient_id = clean_value(row.get(patient_id_key))
        if patient_id in patient_id_counts:
            rows_by_patient_id[patient_id].append(row)

    selected_rows_by_patient_id: dict[str, list[Row]] = {}
    for patient_id, matching_rows in rows_by_patient_id.items():
        ranked_rows = sorted(
            matching_rows,
            key=lambda row: (
                parse_date(row.get(date_key)),
                sum(1 for value in row.values() if clean_value(value)),
                row_tiebreaker(row),
            ),
            reverse=True,
        )
        selected_rows_by_patient_id[patient_id] = ranked_rows[: patient_id_counts[patient_id]]

    selected_rows: list[Row] = []
    next_row_index_by_patient_id: Counter[str] = Counter()
    for patient_id in patient_ids:
        available_rows = selected_rows_by_patient_id.get(patient_id, [])
        next_index = next_row_index_by_patient_id[patient_id]
        if next_index < len(available_rows):
            selected_rows.append(deepcopy(available_rows[next_index]))
            next_row_index_by_patient_id[patient_id] += 1

    return selected_rows


def select_latest_patient_ids(
    rows: list[Row],
    count: int,
    stratifiers: list[Stratifier] | None = None,
    excluded_patient_ids: set[str] | None = None,
    patient_id_key: str = "patient_id",
    date_key: str = "mri_date",
) -> list[str]:
    excluded_patient_ids = excluded_patient_ids or set()
    representative_rows = choose_representative_rows(
        rows=rows,
        stratifiers=stratifiers,
        patient_id_key=patient_id_key,
        date_key=date_key,
    )
    ranked_rows = sorted(
        (
            row
            for row in representative_rows.values()
            if clean_value(row.get(patient_id_key)) not in excluded_patient_ids
        ),
        key=lambda row: (
            parse_date(row.get(date_key)),
            clean_value(row.get(patient_id_key)),
        ),
        reverse=True,
    )
    return [clean_value(row.get(patient_id_key)) for row in ranked_rows[:count]]


def get_active_stratifiers(rows: list[Row], stratifiers: list[Stratifier]) -> tuple[list[Stratifier], list[str]]:
    active_stratifiers: list[Stratifier] = []
    skipped_stratifiers: list[str] = []

    for stratifier in stratifiers:
        distinct_values = {
            clean_value(stratifier.extractor(row))
            for row in rows
            if clean_value(stratifier.extractor(row))
        }
        if len(distinct_values) > 1:
            active_stratifiers.append(stratifier)
        else:
            skipped_stratifiers.append(stratifier.name)

    return active_stratifiers, skipped_stratifiers


def score_candidate(
    row: Row,
    features: list[tuple[str, str, float]],
    global_counts: Counter[tuple[str, str]],
    selected_counts: Counter[tuple[str, str]],
    date_key: str,
) -> tuple[float, int, int]:
    score = 0.0

    for stratifier_name, value, weight in features:
        global_frequency = global_counts[(stratifier_name, value)]
        selected_frequency = selected_counts[(stratifier_name, value)]
        rarity_bonus = 1 / global_frequency

        if selected_frequency == 0:
            score += weight * (5.0 + 10.0 * rarity_bonus)
        else:
            score += weight * (rarity_bonus / (selected_frequency + 1))

    return (
        score,
        len(features),
        parse_date(row.get(date_key)).toordinal(),
        clean_value(row.get("patient_id")),
        row_tiebreaker(row),
    )


def select_diverse_patient_rows(
    rows: list[Row],
    target_count: int,
    stratifiers: list[Stratifier],
    patient_id_key: str = "patient_id",
    date_key: str = "mri_date",
) -> tuple[list[Row], list[str], list[str]]:
    representative_rows_by_patient = choose_representative_rows(
        rows=rows,
        stratifiers=stratifiers,
        patient_id_key=patient_id_key,
        date_key=date_key,
    )
    representative_rows = list(representative_rows_by_patient.values())
    active_stratifiers, skipped_stratifiers = get_active_stratifiers(representative_rows, stratifiers)

    features_by_patient_id: dict[str, list[tuple[str, str, float]]] = {}
    global_counts: Counter[tuple[str, str]] = Counter()

    for row in representative_rows:
        patient_id = clean_value(row.get(patient_id_key))
        patient_features: list[tuple[str, str, float]] = []

        for stratifier in active_stratifiers:
            value = clean_value(stratifier.extractor(row))
            if not value:
                continue
            feature = (stratifier.name, value, stratifier.weight)
            patient_features.append(feature)
            global_counts[(stratifier.name, value)] += 1

        features_by_patient_id[patient_id] = patient_features

    target_size = min(target_count, len(representative_rows))
    selected_rows: list[Row] = []
    selected_patient_ids: set[str] = set()
    selected_counts: Counter[tuple[str, str]] = Counter()

    while len(selected_rows) < target_size:
        best_row: Row | None = None
        best_score: tuple[float, int, int] | None = None

        for row in representative_rows:
            patient_id = clean_value(row.get(patient_id_key))
            if patient_id in selected_patient_ids:
                continue

            candidate_score = score_candidate(
                row=row,
                features=features_by_patient_id[patient_id],
                global_counts=global_counts,
                selected_counts=selected_counts,
                date_key=date_key,
            )
            if best_score is None or candidate_score > best_score:
                best_score = candidate_score
                best_row = row

        if best_row is None:
            break

        best_patient_id = clean_value(best_row.get(patient_id_key))
        selected_patient_ids.add(best_patient_id)
        selected_rows.append(best_row)

        for stratifier_name, value, _ in features_by_patient_id[best_patient_id]:
            selected_counts[(stratifier_name, value)] += 1

    selected_rows.sort(key=lambda row: clean_value(row.get(patient_id_key)))
    active_names = [stratifier.name for stratifier in active_stratifiers]
    return selected_rows, active_names, skipped_stratifiers
