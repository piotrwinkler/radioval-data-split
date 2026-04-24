"""Enrich radioval_harmonized_new.csv with selected MRI metadata from XNAT."""

from __future__ import annotations

import csv
import os
import re
import warnings
from pathlib import Path
from typing import Any, Iterable

try:
    import xnat
except ImportError:  # pragma: no cover - depends on local environment
    xnat = None


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_CSV_PATH = REPO_ROOT / "radioval_harmonized_new.csv"
ERROR_LOG_PATH = SCRIPT_DIR / "xnat_enrichment_errors.log"
SKIP_LOG_PATH = SCRIPT_DIR / "xnat_enrichment_skips.log"
DEFAULT_SCAN_TYPE = "DCE"
DEFAULT_SAVE_EVERY_ROWS = 50
DICOM_HEADER_FALLBACK_DATASETS = {"AFI", "HULAFE"}
PATIENT_ID_SUFFIX_PATTERN = re.compile(r"_(1|2)$")
ENV_FILE_CANDIDATES = [
    Path.cwd() / ".env",
    REPO_ROOT / ".env",
    SCRIPT_DIR / ".env",
]

PROJECT_BY_DATASET = {
    "HULAFE": "radioval_hulafe",
    "KI": "radioval_ki",
    "GUMED": "radioval_gumed",
    "UZSM": "radioval_uzsm",
    "MUW": "radioval_muw",
    "HUH": "radioval_huh",
    "AFI": "radioval_afi",
    "ASU": "radioval_asu",
}

TARGET_COLUMNS = [
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

DICOM_FIELDS = [
    "Manufacturer",
    "ManufacturerModelName",
    "Modality",
    "StudyDescription",
    "BodyPartExamined",
    "MagneticFieldStrength",
    "SliceThickness",
    "RepetitionTime",
    "EchoTime",
    "ScanningSequence",
    "SequenceVariant",
    "SequenceName",
    "SeriesDescription",
    "ProtocolName",
    "ImageType",
    "ScanOptions",
    "MRAcquisitionType",
    "TemporalPositionIdentifier",
    "NumberOfTemporalPositions",
    "TemporalResolution",
    "ImageOrientationPatient",
    "InstitutionName",
    "OperatorsName",
    "PerformedProcedureStepDescription",
    "ContrastBolusAgent",
    "ContrastBolusIngredient",
    "ContrastBolusRoute",
    "ContrastBolusVolume",
]

DICOM_SCAN_DESCRIPTOR_FIELDS = (
    "SeriesDescription",
    "ProtocolName",
    "ImageType",
    "SequenceName",
    "ScanningSequence",
    "StudyDescription",
    "BodyPartExamined",
    "PerformedProcedureStepDescription",
)

SEQUENCE_CODE_LABELS = {
    "SE": "Spin Echo",
    "IR": "Inversion Recovery",
    "GR": "Gradient Recalled",
    "EP": "Echo Planar",
    "RM": "Research Mode",
}


def read_first_env(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def load_dotenv() -> None:
    loaded_paths: set[Path] = set()

    for dotenv_path in ENV_FILE_CANDIDATES:
        resolved_path = dotenv_path.resolve()
        if resolved_path in loaded_paths or not dotenv_path.exists():
            continue

        loaded_paths.add(resolved_path)

        for line_number, raw_line in enumerate(dotenv_path.read_text(encoding="utf-8").splitlines(), start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            if line.startswith("export "):
                line = line.removeprefix("export ").strip()

            if "=" not in line:
                raise SystemExit(f"Invalid .env line {line_number} in {dotenv_path}: expected KEY=VALUE.")

            key, value = line.split("=", maxsplit=1)
            key = key.strip()
            value = value.strip().strip("'\"")

            if not key:
                raise SystemExit(f"Invalid .env line {line_number} in {dotenv_path}: empty key.")

            os.environ.setdefault(key, value)


def read_bool_env(name: str, default: bool) -> bool:
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default

    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False

    raise SystemExit(
        f"Invalid boolean value for {name}: {raw_value!r}. Use one of 1/0, true/false, yes/no."
    )


def require_env(name: str, *aliases: str) -> str:
    value = read_first_env(name, *aliases)
    if value:
        return value

    all_names = ", ".join((name, *aliases))
    raise SystemExit(f"Missing required environment variable. Set one of: {all_names}")


def require_xnat_dependency() -> None:
    if xnat is None:
        raise SystemExit(
            "Missing dependency 'xnat'. Install it first, e.g. with: pip install -r requirements.txt"
        )


def normalize_patient_id(patient_id: str) -> str:
    return PATIENT_ID_SUFFIX_PATTERN.sub("", patient_id.strip())


def load_rows(csv_path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with csv_path.open(newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        return reader.fieldnames or [], list(reader)


def write_rows(csv_path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with csv_path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def list_children(collection: object) -> list[object]:
    if hasattr(collection, "values"):
        values = collection.values()
    else:
        values = collection
    return list(values)


def object_identifier(obj: object, *attrs: str) -> str:
    for attr in attrs:
        value = getattr(obj, attr, None)
        if value not in (None, ""):
            return str(value)
    return ""


def sorted_objects(objects: Iterable[object], *attrs: str) -> list[object]:
    return sorted(
        objects,
        key=lambda obj: tuple(object_identifier(obj, attr) for attr in attrs),
    )


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.strip().lower())


def normalize_value(value: Any) -> str:
    if value is None:
        return ""

    if hasattr(value, "value"):
        value = value.value

    if isinstance(value, (list, tuple)):
        normalized_items = [normalize_value(item) for item in value]
        normalized_items = [item for item in normalized_items if item]
        return " | ".join(normalized_items)

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace").strip()

    return str(value).strip()


def is_direct_dicom_value(value: Any) -> bool:
    if value is None:
        return False
    if hasattr(value, "value"):
        value = value.value
    if isinstance(value, dict):
        return False
    if isinstance(value, (list, tuple)):
        return all(not isinstance(item, (dict, list, tuple)) for item in value)
    return True


def build_flat_dicom_lookup(data: Any, lookup: dict[str, str] | None = None) -> dict[str, str]:
    lookup = lookup or {}

    if hasattr(data, "iterall"):
        for element in data.iterall():
            keyword = getattr(element, "keyword", "") or getattr(element, "name", "")
            value = normalize_value(getattr(element, "value", None))
            if keyword and value:
                lookup.setdefault(normalize_key(str(keyword)), value)
            tag = getattr(element, "tag", None)
            if tag is not None and value:
                lookup.setdefault(normalize_key(str(tag)), value)
        return lookup

    if isinstance(data, dict):
        entry_name = normalize_value(data.get("name") or data.get("keyword") or data.get("field"))
        entry_value = data.get("value", data.get("Value"))
        if entry_name and entry_value is not None:
            value = normalize_value(entry_value)
            if value:
                lookup.setdefault(normalize_key(entry_name), value)

        for key, value in data.items():
            if key not in {"name", "keyword", "field", "value", "Value", "vr", "VR", "tag", "Tag"} and is_direct_dicom_value(value):
                normalized = normalize_value(value)
                if normalized:
                    lookup.setdefault(normalize_key(str(key)), normalized)
            build_flat_dicom_lookup(value, lookup)
        return lookup

    if isinstance(data, list):
        for item in data:
            build_flat_dicom_lookup(item, lookup)

    return lookup


def get_dicom_value(dicom_data: Any, *candidate_keys: str) -> str:
    if hasattr(dicom_data, "get"):
        for key in candidate_keys:
            try:
                value = dicom_data.get(key)
            except Exception:  # pragma: no cover - defensive for non-standard objects
                value = None
            normalized = normalize_value(value) if is_direct_dicom_value(value) else ""
            if normalized:
                return normalized

    lookup = build_flat_dicom_lookup(dicom_data)
    for key in candidate_keys:
        normalized = lookup.get(normalize_key(key), "")
        if normalized:
            return normalized

    return ""


def resolve_project(connection: object, project_name: str) -> object:
    try:
        return connection.projects[project_name]
    except Exception:
        for project in sorted_objects(list_children(connection.projects), "id", "label"):
            candidates = (
                object_identifier(project, "id"),
                object_identifier(project, "label"),
                object_identifier(project, "name"),
            )
            if project_name in candidates:
                return project

    raise ValueError(f"Project '{project_name}' was not found on the connected XNAT.")


def build_subject_indexes(project: object) -> tuple[dict[str, object], dict[str, list[object]]]:
    exact_index: dict[str, object] = {}
    normalized_index: dict[str, list[object]] = {}

    for subject in sorted_objects(list_children(project.subjects), "label", "id"):
        keys = {
            object_identifier(subject, "id"),
            object_identifier(subject, "label"),
        }

        seen_normalized_keys: set[str] = set()
        for key in keys:
            if not key:
                continue
            exact_index.setdefault(key, subject)
            normalized_key = normalize_patient_id(key)
            if normalized_key in seen_normalized_keys:
                continue
            normalized_index.setdefault(normalized_key, []).append(subject)
            seen_normalized_keys.add(normalized_key)

    return exact_index, normalized_index


def normalize_type(value: str | None) -> str:
    if value is None:
        return ""
    return value.strip().casefold()


def normalize_scan_descriptor(value: str | None) -> str:
    if value is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", value.strip().casefold())


def scan_descriptor_matches_target(scan_descriptor: str, target_scan_type: str) -> bool:
    normalized_scan_type = normalize_type(scan_descriptor)
    normalized_target = normalize_type(target_scan_type)
    if normalized_scan_type == normalized_target:
        return True

    if normalize_scan_descriptor(target_scan_type) != "dce":
        return False

    condensed_scan_type = normalize_scan_descriptor(scan_descriptor)
    raw_scan_descriptor = scan_descriptor.casefold()
    if "dce" in condensed_scan_type:
        return True

    if "vibrant" in condensed_scan_type and ("multiphase" in condensed_scan_type or condensed_scan_type.startswith("ph")):
        return True

    if "vibrant" in condensed_scan_type and (
        "+c" in raw_scan_descriptor
        or " post contrast" in raw_scan_descriptor
        or " post-contrast" in raw_scan_descriptor
        or " postcontrast" in raw_scan_descriptor
    ):
        return True

    has_dynamic_marker = "dyn" in condensed_scan_type or "dynamic" in condensed_scan_type
    if not has_dynamic_marker:
        return False

    # Vendor-specific breast MRI dynamic contrast series often appear under
    # sequence family names instead of a literal DCE label.
    sequence_markers = ("vibrant", "ethrive", "thrivesense", "thrive", "t1")
    return any(marker in condensed_scan_type for marker in sequence_markers)


def parse_first_number(raw_value: str) -> float | None:
    match = re.search(r"-?\d+(?:\.\d+)?", raw_value)
    if match is None:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def get_numeric_dicom_value(dicom_data: Any, *candidate_keys: str) -> float | None:
    raw_value = get_dicom_value(dicom_data, *candidate_keys)
    if not raw_value:
        return None
    return parse_first_number(raw_value)


def dicom_temporal_markers_match_dce(dicom_data: Any) -> bool:
    number_of_temporal_positions = get_numeric_dicom_value(
        dicom_data,
        "NumberOfTemporalPositions",
        "Number of Temporal Positions",
        "(0020,0105)",
    )
    if number_of_temporal_positions is not None and number_of_temporal_positions >= 2:
        return True

    temporal_position_identifier = get_numeric_dicom_value(
        dicom_data,
        "TemporalPositionIdentifier",
        "Temporal Position Identifier",
        "(0020,0100)",
    )
    temporal_resolution = get_numeric_dicom_value(
        dicom_data,
        "TemporalResolution",
        "Temporal Resolution",
        "(0020,0110)",
    )
    return temporal_position_identifier is not None and temporal_resolution is not None


def scan_matches_target(scan: object, target_scan_type: str) -> bool:
    descriptors = [
        object_identifier(scan, "type", "label"),
        object_identifier(scan, "series_description"),
    ]
    return any(
        descriptor and scan_descriptor_matches_target(descriptor, target_scan_type)
        for descriptor in descriptors
    )


def iter_subject_experiments(subject: object) -> Iterable[object]:
    return sorted_objects(list_children(subject.experiments), "label", "id")


def subject_has_experiments(subject: object) -> bool:
    return any(True for _ in iter_subject_experiments(subject))


def find_first_scan_by_type(subject: object, target_scan_type: str) -> tuple[object | None, object | None]:
    for experiment in iter_subject_experiments(subject):
        scans = sorted_objects(list_children(experiment.scans), "id", "label", "type")
        for scan in scans:
            if scan_matches_target(scan, target_scan_type):
                return experiment, scan
    return None, None


def read_scan_dicom_data(scan: object) -> Any:
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=r"Invalid value for VR UI:.*",
                category=UserWarning,
            )
            return scan.read_dicom()
    except Exception:  # pragma: no cover - depends on local XNAT/pydicom setup
        return scan.dicom_dump(fields=DICOM_FIELDS)


def dicom_data_matches_target(dicom_data: Any, target_scan_type: str) -> bool:
    if normalize_scan_descriptor(target_scan_type) != "dce":
        return False

    descriptors = [
        get_dicom_value(dicom_data, field)
        for field in DICOM_SCAN_DESCRIPTOR_FIELDS
    ]
    if any(
        descriptor and scan_descriptor_matches_target(descriptor, target_scan_type)
        for descriptor in descriptors
    ):
        return True

    return dicom_temporal_markers_match_dce(dicom_data)


def find_first_scan_by_dicom_headers(subject: object, target_scan_type: str) -> tuple[object | None, object | None, Any | None]:
    for experiment in iter_subject_experiments(subject):
        scans = sorted_objects(list_children(experiment.scans), "id", "label", "type")
        for scan in scans:
            try:
                dicom_data = read_scan_dicom_data(scan)
            except Exception:  # pragma: no cover - depends on remote XNAT behavior
                continue
            if dicom_data_matches_target(dicom_data, target_scan_type):
                return experiment, scan, dicom_data
    return None, None, None


def humanize_sequence_codes(raw_value: str) -> str:
    parts = [part.strip() for part in re.split(r"[|\\\\,;/]+", raw_value) if part.strip()]
    labels = [SEQUENCE_CODE_LABELS.get(part.upper(), part) for part in parts]
    unique_labels = list(dict.fromkeys(labels))
    return ", ".join(unique_labels)


def infer_acquisition_method(dicom_data: Any) -> str:
    scanning_sequence = get_dicom_value(dicom_data, "ScanningSequence")
    if scanning_sequence:
        return humanize_sequence_codes(scanning_sequence)

    for key in ("SequenceName", "SequenceVariant", "MRAcquisitionType"):
        value = get_dicom_value(dicom_data, key)
        if value:
            return value

    return ""


def infer_scan_option(dicom_data: Any) -> str:
    options: list[str] = []

    contrast_values = [
        get_dicom_value(dicom_data, "ContrastBolusAgent"),
        get_dicom_value(dicom_data, "ContrastBolusIngredient"),
    ]
    if any(value for value in contrast_values):
        options.append("contrast-enhanced")

    scan_options = get_dicom_value(dicom_data, "ScanOptions")
    if scan_options:
        options.append(scan_options)

    unique_options = list(dict.fromkeys(option for option in options if option))
    return ", ".join(unique_options)


def parse_orientation_values(raw_value: str) -> list[float]:
    values: list[float] = []
    for item in re.split(r"[|\\\\, ]+", raw_value):
        item = item.strip()
        if not item:
            continue
        try:
            values.append(float(item))
        except ValueError:
            return []
    return values


def infer_anatomical_plane(dicom_data: Any) -> str:
    raw_value = get_dicom_value(dicom_data, "ImageOrientationPatient")
    orientation = parse_orientation_values(raw_value)
    if len(orientation) != 6:
        return ""

    row_x, row_y, row_z, col_x, col_y, col_z = orientation
    normal_x = row_y * col_z - row_z * col_y
    normal_y = row_z * col_x - row_x * col_z
    normal_z = row_x * col_y - row_y * col_x

    axis_strengths = {
        "Sagittal": abs(normal_x),
        "Coronal": abs(normal_y),
        "Axial": abs(normal_z),
    }
    return max(axis_strengths, key=axis_strengths.get)


def combine_non_empty(*values: str) -> str:
    unique_values = list(dict.fromkeys(value for value in values if value))
    return ", ".join(unique_values)


def extract_scan_metadata(scan: object, dicom_data: Any | None = None) -> dict[str, str]:
    if dicom_data is None:
        dicom_data = read_scan_dicom_data(scan)

    return {
        "mri_machine_manufacturer": get_dicom_value(dicom_data, "Manufacturer"),
        "mri_machine_model": get_dicom_value(dicom_data, "ManufacturerModelName"),
        "field_strength": get_dicom_value(dicom_data, "MagneticFieldStrength"),
        "slice_thickness": get_dicom_value(dicom_data, "SliceThickness"),
        "tr_repetition_time": get_dicom_value(dicom_data, "RepetitionTime"),
        "te_echo_time": get_dicom_value(dicom_data, "EchoTime"),
        "acquisition_method": infer_acquisition_method(dicom_data),
        "scan_option": infer_scan_option(dicom_data),
        "anatomical_plane": infer_anatomical_plane(dicom_data),
        "medical_facility": get_dicom_value(dicom_data, "InstitutionName"),
        "operator_variability": get_dicom_value(dicom_data, "OperatorsName"),
        "contrast_agent_types": combine_non_empty(
            get_dicom_value(dicom_data, "ContrastBolusAgent"),
            get_dicom_value(dicom_data, "ContrastBolusIngredient"),
        ),
    }


def ensure_target_columns(fieldnames: list[str]) -> list[str]:
    updated_fieldnames = list(fieldnames)
    for column in TARGET_COLUMNS:
        if column not in updated_fieldnames:
            updated_fieldnames.append(column)
    return updated_fieldnames


def collect_case_metadata_from_rows(case_rows: list[dict[str, str]]) -> dict[str, str]:
    metadata: dict[str, str] = {}

    for column in TARGET_COLUMNS:
        for row in case_rows:
            value = row.get(column, "").strip()
            if value:
                metadata[column] = value
                break

    return metadata


def case_has_existing_metadata(case_rows: list[dict[str, str]]) -> bool:
    existing_metadata = collect_case_metadata_from_rows(case_rows)
    return any(existing_metadata.get(column, "").strip() for column in TARGET_COLUMNS)


def apply_fetched_metadata_to_case_rows(case_rows: list[dict[str, str]], metadata: dict[str, str]) -> int:
    updated_rows = 0

    for row in case_rows:
        row_updated = False
        for column, value in metadata.items():
            if value and not row.get(column, "").strip():
                row[column] = value
                row_updated = True
        if row_updated:
            updated_rows += 1

    return updated_rows


def log_message(log_file: Any | None, message: str) -> None:
    print(message)
    if log_file is not None:
        log_file.write(message)
        log_file.write("\n")


def write_log_only(log_file: Any, message: str) -> None:
    log_file.write(message)
    log_file.write("\n")


def maybe_log_progress(
    log_file: Any,
    previous_processed_rows: int,
    processed_rows: int,
    total_rows: int,
    processed_cases: int,
    total_cases: int,
) -> None:
    for row_number in range(previous_processed_rows + 1, processed_rows + 1):
        log_message(
            log_file,
            f"[PROGRESS] rows={row_number}/{total_rows} cases={processed_cases}/{total_cases}",
        )


def maybe_save_checkpoint(
    csv_path: Path,
    fieldnames: list[str],
    rows: list[dict[str, str]],
    log_file: Any,
    processed_rows: int,
    total_rows: int,
    last_saved_rows: int,
    save_every_rows: int,
) -> int:
    should_save = processed_rows >= total_rows or processed_rows - last_saved_rows >= save_every_rows
    if not should_save:
        return last_saved_rows

    write_rows(csv_path, fieldnames, rows)
    log_message(log_file, f"[CHECKPOINT] saved_csv rows={processed_rows}/{total_rows}")
    return processed_rows


def group_rows_by_case(rows: list[dict[str, str]]) -> dict[tuple[str, str], list[dict[str, str]]]:
    grouped: dict[tuple[str, str], list[dict[str, str]]] = {}

    for row in rows:
        dataset = row.get("dataset", "").strip()
        patient_id = normalize_patient_id(row.get("patient_id", ""))
        case_key = (dataset, patient_id)
        grouped.setdefault(case_key, []).append(row)

    return grouped


def enrich_rows(
    connection: object,
    rows: list[dict[str, str]],
    fieldnames: list[str],
    csv_path: Path,
    error_log_file: Any,
    skip_log_file: Any,
    scan_type: str,
) -> dict[str, int]:
    project_cache: dict[str, object] = {}
    subject_lookup_cache: dict[str, tuple[dict[str, object], dict[str, list[object]]]] = {}
    metadata_cache: dict[tuple[str, str], dict[str, str] | None] = {}

    summary = {
        "total_rows": len(rows),
        "cases_seen": 0,
        "cases_enriched": 0,
        "rows_updated": 0,
        "patients_missing": 0,
        "scans_missing": 0,
        "errors": 0,
        "cases_skipped_complete": 0,
    }

    grouped_rows = group_rows_by_case(rows)
    total_cases = len(grouped_rows)
    processed_rows = 0
    processed_cases = 0
    last_saved_rows = 0
    save_every_rows = DEFAULT_SAVE_EVERY_ROWS

    for (dataset, patient_id), case_rows in grouped_rows.items():
        previous_processed_rows = processed_rows

        if not dataset or not patient_id:
            write_log_only(skip_log_file, f"[SKIP] Missing dataset or patient_id for case key ({dataset!r}, {patient_id!r}).")
            summary["errors"] += 1
        else:
            if case_has_existing_metadata(case_rows):
                summary["cases_skipped_complete"] += 1
            else:
                summary["cases_seen"] += 1
                cache_key = (dataset, patient_id)

                if cache_key not in metadata_cache:
                    project_name = PROJECT_BY_DATASET.get(dataset)
                    if project_name is None:
                        write_log_only(skip_log_file, f"[SKIP] No XNAT project mapping for dataset '{dataset}' (patient_id={patient_id}).")
                        summary["errors"] += 1
                        metadata_cache[cache_key] = None
                    else:
                        try:
                            project = project_cache.get(project_name)
                            if project is None:
                                project = resolve_project(connection, project_name)
                                project_cache[project_name] = project
                                subject_lookup_cache[project_name] = build_subject_indexes(project)

                            exact_index, normalized_index = subject_lookup_cache[project_name]
                            subject = exact_index.get(patient_id)
                            if subject is None:
                                matches = normalized_index.get(patient_id, [])
                                if len(matches) == 1:
                                    subject = matches[0]
                                elif len(matches) > 1:
                                    write_log_only(
                                        skip_log_file,
                                        f"[SKIP] Multiple XNAT subjects match normalized patient_id '{patient_id}' in project '{project_name}'.",
                                    )
                                    summary["patients_missing"] += 1
                                    metadata_cache[cache_key] = None

                            if subject is None and cache_key not in metadata_cache:
                                write_log_only(
                                    skip_log_file,
                                    f"[SKIP] Patient '{patient_id}' not found in XNAT project '{project_name}'.",
                                )
                                summary["patients_missing"] += 1
                                metadata_cache[cache_key] = None

                            if cache_key not in metadata_cache:
                                if not subject_has_experiments(subject):
                                    write_log_only(
                                        skip_log_file,
                                        f"[SKIP] Patient '{patient_id}' has no experiments in XNAT project '{project_name}' "
                                        f"and is treated as missing.",
                                    )
                                    summary["patients_missing"] += 1
                                    metadata_cache[cache_key] = None

                            if cache_key not in metadata_cache:
                                _, scan = find_first_scan_by_type(subject, scan_type)
                                dicom_data = None
                                if scan is None and dataset in DICOM_HEADER_FALLBACK_DATASETS:
                                    _, scan, dicom_data = find_first_scan_by_dicom_headers(subject, scan_type)
                                if scan is None:
                                    write_log_only(
                                        skip_log_file,
                                        f"[SKIP] No scan with type '{scan_type}' for patient '{patient_id}' in project '{project_name}'.",
                                    )
                                    summary["scans_missing"] += 1
                                    metadata_cache[cache_key] = None
                                else:
                                    metadata_cache[cache_key] = extract_scan_metadata(scan, dicom_data=dicom_data)
                                    summary["cases_enriched"] += 1
                        except Exception as error:  # pragma: no cover - depends on remote XNAT behavior
                            write_log_only(
                                error_log_file,
                                f"[ERROR] dataset={dataset} patient_id={patient_id} project={project_name}: "
                                f"{type(error).__name__}: {error}",
                            )
                            summary["errors"] += 1
                            metadata_cache[cache_key] = None

                metadata = metadata_cache.get(cache_key)
                if metadata:
                    summary["rows_updated"] += apply_fetched_metadata_to_case_rows(case_rows, metadata)

        processed_cases += 1
        processed_rows += len(case_rows)
        maybe_log_progress(
            log_file=None,
            previous_processed_rows=previous_processed_rows,
            processed_rows=processed_rows,
            total_rows=summary["total_rows"],
            processed_cases=processed_cases,
            total_cases=total_cases,
        )
        last_saved_rows = maybe_save_checkpoint(
            csv_path=csv_path,
            fieldnames=fieldnames,
            rows=rows,
            log_file=None,
            processed_rows=processed_rows,
            total_rows=summary["total_rows"],
            last_saved_rows=last_saved_rows,
            save_every_rows=save_every_rows,
        )

    return summary


def main() -> None:
    load_dotenv()
    require_xnat_dependency()

    server = require_env("XNAT_SERVER", "XNATPY_HOST", "XNAT_HOST")
    user = require_env("XNAT_USER", "XNATPY_USER", "XNAT_USERNAME")
    password = require_env("XNAT_PASSWORD", "XNATPY_PASS", "XNAT_PASS")
    verify_tls = read_bool_env("XNAT_VERIFY_TLS", default=True)
    scan_type = os.environ.get("XNAT_SCAN_TYPE", DEFAULT_SCAN_TYPE).strip() or DEFAULT_SCAN_TYPE

    fieldnames, rows = load_rows(INPUT_CSV_PATH)
    updated_fieldnames = ensure_target_columns(fieldnames)
    for row in rows:
        for column in TARGET_COLUMNS:
            row.setdefault(column, "")

    with xnat.connect(
        server=server,
        user=user,
        password=password,
        verify=verify_tls,
    ) as connection, ERROR_LOG_PATH.open("w", encoding="utf-8") as error_log_file, SKIP_LOG_PATH.open("w", encoding="utf-8") as skip_log_file:
        summary = enrich_rows(
            connection=connection,
            rows=rows,
            fieldnames=updated_fieldnames,
            csv_path=INPUT_CSV_PATH,
            error_log_file=error_log_file,
            skip_log_file=skip_log_file,
            scan_type=scan_type,
        )

    print(f"Updated CSV: {INPUT_CSV_PATH}")
    print(f"Error log: {ERROR_LOG_PATH}")
    print(f"Skip log: {SKIP_LOG_PATH}")
    print(f"Rows processed: {summary['total_rows']}/{summary['total_rows']}")
    print(f"Cases checked: {summary['cases_seen']}")
    print(f"Cases enriched: {summary['cases_enriched']}")
    print(f"Rows updated: {summary['rows_updated']}")
    print(f"Patients missing on XNAT: {summary['patients_missing']}")
    print(f"Patients without matching scan: {summary['scans_missing']}")
    print(f"Other errors: {summary['errors']}")
    print(f"Cases already complete: {summary['cases_skipped_complete']}")


if __name__ == "__main__":
    main()
