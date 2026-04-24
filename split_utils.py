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
DIVERSITY_STRATIFIER_WEIGHTS = {
    "imaging_modality": 1.0,
    "pcr": 1.0,
    "tumor_size_ct": 1.0,
    "lesion_presentation": 1.0,
    "age_range": 1.0,
    "menopausal_status": 1.0,
    "tumor_subtype": 1.0,
    "multifocality": 1.0,
    "race_ethnicity_proxy": 1.0,
    "mri_metadata": 1.0,
    "scan_option": 0.5,
}
MRI_METADATA_STRATIFIER_COLUMNS = [
    "mri_machine_manufacturer",
    "mri_machine_model",
    "field_strength",
    "slice_thickness",
    "tr_repetition_time",
    "te_echo_time",
    "acquisition_method",
    "scan_option",
    "anatomical_plane",
    "medical_facility",
    "operator_variability",
    "contrast_agent_types",
]
MRI_METADATA_NUMERIC_BINS = {
    "field_strength": [
        (1.0, "<=1T"),
        (1.5, "1.5T"),
        (3.0, "3T"),
        (float("inf"), ">3T"),
    ],
    "slice_thickness": [
        (1.0, "<=1 mm"),
        (1.5, ">1-1.5 mm"),
        (2.0, ">1.5-2 mm"),
        (3.0, ">2-3 mm"),
        (float("inf"), ">3 mm"),
    ],
    "tr_repetition_time": [
        (4.0, "<=4 ms"),
        (5.0, ">4-5 ms"),
        (6.0, ">5-6 ms"),
        (8.0, ">6-8 ms"),
        (float("inf"), ">8 ms"),
    ],
    "te_echo_time": [
        (1.5, "<=1.5 ms"),
        (2.0, ">1.5-2 ms"),
        (2.5, ">2-2.5 ms"),
        (3.5, ">2.5-3.5 ms"),
        (float("inf"), ">3.5 ms"),
    ],
}


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


def parse_numeric_value(value: str | None) -> float | None:
    cleaned_value = clean_value(value).replace(",", ".")
    if not cleaned_value:
        return None
    try:
        return float(cleaned_value)
    except ValueError:
        return None


def numeric_bin_stratifier(name: str, aliases: list[str], weight: float = 1.0) -> Stratifier:
    def extract_numeric_bin(row: Row) -> str:
        bins = MRI_METADATA_NUMERIC_BINS[name]
        for alias in aliases:
            numeric_value = parse_numeric_value(row.get(alias))
            if numeric_value is None:
                continue
            for upper_bound, label in bins:
                if numeric_value <= upper_bound:
                    return label
        return ""

    return Stratifier(name=name, extractor=extract_numeric_bin, weight=weight)


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
    mass_shape = clean_value(row.get("mass_shape")).lower()
    if mass_shape:
        if "non mass" in mass_shape:
            return "nme / non-solid lesion"
        return "solid lesion"

    associated_features = clean_value(row.get("associated_features")).lower()
    if associated_features and associated_features not in {"0", "unknown"}:
        return "nme / non-solid lesion"

    return ""


def normalize_tumor_subtype_value(raw_value: str) -> str:
    normalized_value = raw_value.lower()
    if not normalized_value:
        return ""

    # Ignore legend-like values that encode the full category mapping rather than a case label.
    if (
        "1: luminal a" in normalized_value
        and "2: luminal b" in normalized_value
        and "3: triple negative" in normalized_value
    ):
        return ""

    if "luminal a" in normalized_value or "luminal_a" in normalized_value or normalized_value == "1":
        return "Luminal A"
    if (
        "luminal b" in normalized_value
        or "luminal_b" in normalized_value
        or normalized_value in {"2", "3"}
    ):
        return "Luminal B"
    if (
        "triple negative" in normalized_value
        or "triple_negative" in normalized_value
        or "tnbc" in normalized_value
        or "basal" in normalized_value
        or normalized_value == "5"
    ):
        return "Basal-like / triple negative"
    if (
        "her2+ enriched" in normalized_value
        or "her2 enriched" in normalized_value
        or "her2-enriched" in normalized_value
        or "her2 pure" in normalized_value
        or "her2+ pure" in normalized_value
        or "her2_pure" in normalized_value
        or normalized_value == "4"
    ):
        return "HER2-positive"

    return raw_value


def extract_tumor_subtype(row: Row) -> str:
    for column_name in ["tumor_subtype", "intrinsic_subtype", "s_biological_subtype"]:
        raw_value = clean_value(row.get(column_name))
        normalized_value = normalize_tumor_subtype_value(raw_value)
        if normalized_value:
            return normalized_value
    return ""


def extract_multifocality(row: Row) -> str:
    value = clean_value(row.get("presence_of_foci")).lower()
    if not value:
        return ""
    if value == "no":
        return "single lesion"
    if value == "yes":
        return "multifocal lesion"
    if value.isdigit():
        if int(value) <= 1:
            return "single lesion"
        return "multifocal lesion"
    if "uni" in value:
        return "single lesion"
    if "multi" in value:
        return "multifocal lesion"
    return value


COMMON_DIVERSITY_STRATIFIERS = [
    direct_stratifier(
        "imaging_modality",
        ["imaging_type"],
        weight=DIVERSITY_STRATIFIER_WEIGHTS["imaging_modality"],
    ),
    Stratifier(
        name="pcr",
        extractor=extract_pcr_status,
        weight=DIVERSITY_STRATIFIER_WEIGHTS["pcr"],
    ),
    Stratifier(
        name="tumor_size_ct",
        extractor=extract_t_stage,
        weight=DIVERSITY_STRATIFIER_WEIGHTS["tumor_size_ct"],
    ),
    Stratifier(
        name="lesion_presentation",
        extractor=extract_lesion_presentation,
        weight=DIVERSITY_STRATIFIER_WEIGHTS["lesion_presentation"],
    ),
    Stratifier(
        name="age_range",
        extractor=extract_age_range,
        weight=DIVERSITY_STRATIFIER_WEIGHTS["age_range"],
    ),
    direct_stratifier(
        "menopausal_status",
        ["menopausal_status"],
        weight=DIVERSITY_STRATIFIER_WEIGHTS["menopausal_status"],
    ),
    Stratifier(
        name="tumor_subtype",
        extractor=extract_tumor_subtype,
        weight=DIVERSITY_STRATIFIER_WEIGHTS["tumor_subtype"],
    ),
    Stratifier(
        name="multifocality",
        extractor=extract_multifocality,
        weight=DIVERSITY_STRATIFIER_WEIGHTS["multifocality"],
    ),
    direct_stratifier(
        "race_ethnicity_proxy",
        ["country_of_origin"],
        weight=DIVERSITY_STRATIFIER_WEIGHTS["race_ethnicity_proxy"],
    ),
]

MRI_METADATA_DIVERSITY_STRATIFIERS = [
    (numeric_bin_stratifier if column_name in MRI_METADATA_NUMERIC_BINS else direct_stratifier)(
        name=column_name,
        aliases=[column_name],
        weight=DIVERSITY_STRATIFIER_WEIGHTS.get(column_name, DIVERSITY_STRATIFIER_WEIGHTS["mri_metadata"]),
    )
    for column_name in MRI_METADATA_STRATIFIER_COLUMNS
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


def select_diverse_patient_rows_with_quotas(
    rows: list[Row],
    dataset_case_quota: dict[str, int],
    stratifiers: list[Stratifier],
    dataset_key: str = "dataset",
    patient_id_key: str = "patient_id",
    date_key: str = "mri_date",
) -> tuple[list[Row], list[str], list[str]]:
    representative_rows_by_patient = choose_representative_rows(
        rows=rows,
        stratifiers=stratifiers,
        patient_id_key=patient_id_key,
        date_key=date_key,
    )
    representative_rows = [
        row
        for row in representative_rows_by_patient.values()
        if clean_value(row.get(dataset_key)) in dataset_case_quota
    ]
    active_stratifiers, skipped_stratifiers = get_active_stratifiers(representative_rows, stratifiers)

    features_by_patient_id: dict[str, list[tuple[str, str, float]]] = {}
    global_counts: Counter[tuple[str, str]] = Counter()
    dataset_by_patient_id: dict[str, str] = {}

    for row in representative_rows:
        patient_id = clean_value(row.get(patient_id_key))
        dataset_name = clean_value(row.get(dataset_key))
        patient_features: list[tuple[str, str, float]] = []

        for stratifier in active_stratifiers:
            value = clean_value(stratifier.extractor(row))
            if not value:
                continue
            feature = (stratifier.name, value, stratifier.weight)
            patient_features.append(feature)
            global_counts[(stratifier.name, value)] += 1

        features_by_patient_id[patient_id] = patient_features
        dataset_by_patient_id[patient_id] = dataset_name

    selected_rows: list[Row] = []
    selected_patient_ids: set[str] = set()
    selected_counts: Counter[tuple[str, str]] = Counter()
    selected_case_count_by_dataset: Counter[str] = Counter()
    total_target_count = sum(dataset_case_quota.values())

    while len(selected_rows) < total_target_count:
        best_row: Row | None = None
        best_score: tuple[float, int, int] | None = None

        for row in representative_rows:
            patient_id = clean_value(row.get(patient_id_key))
            dataset_name = dataset_by_patient_id[patient_id]
            if patient_id in selected_patient_ids:
                continue
            if selected_case_count_by_dataset[dataset_name] >= dataset_case_quota[dataset_name]:
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
        best_dataset_name = dataset_by_patient_id[best_patient_id]
        selected_patient_ids.add(best_patient_id)
        selected_case_count_by_dataset[best_dataset_name] += 1
        selected_rows.append(best_row)

        for stratifier_name, value, _ in features_by_patient_id[best_patient_id]:
            selected_counts[(stratifier_name, value)] += 1

    dataset_order = {dataset_name: index for index, dataset_name in enumerate(dataset_case_quota)}
    selected_rows.sort(
        key=lambda row: (
            dataset_order[clean_value(row.get(dataset_key))],
            clean_value(row.get(patient_id_key)),
        )
    )
    active_names = [stratifier.name for stratifier in active_stratifiers]
    return selected_rows, active_names, skipped_stratifiers
