"""Normalize MRI metadata columns in radioval_harmonized_new.csv."""

from __future__ import annotations

import ast
import csv
import re
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_CSV_PATH = REPO_ROOT / "radioval_harmonized_new.csv"
MRI_METADATA_COLUMNS = [
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
ALLOW_EMPTY_NORMALIZED_COLUMNS = {
    "medical_facility",
    "operator_variability",
    "contrast_agent_types",
}
NUMERIC_LIKE_COLUMNS = {
    "field_strength",
    "slice_thickness",
    "tr_repetition_time",
    "te_echo_time",
}
ANATOMICAL_PLANE_MAPPING = {
    "ax": "Axial",
    "axi": "Axial",
    "axial": "Axial",
    "tra": "Axial",
    "transverse": "Axial",
    "cor": "Coronal",
    "coronal": "Coronal",
    "sag": "Sagittal",
    "sagittal": "Sagittal",
}
CONTRAST_AGENT_ALIASES = {
    "dotarem": "DOTAREM",
    "dotaren": "DOTAREM",
    "dortarem": "DOTAREM",
    "doatrem": "DOTAREM",
    "dt": "DOTAREM",
    "prohance": "PROHANCE",
    "prohans": "PROHANCE",
    "gadovist": "GADOVIST",
    "gadowist": "GADOVIST",
    "clariscan": "CLARISCAN",
    "multihance": "MULTIHANCE",
    "omniscan": "OMNISCAN",
    "cyclolux": "CYCLOLUX",
}
MANUFACTURER_ALIASES = {
    "siemens": "Siemens",
    "siemenshealthineers": "Siemens",
    "siemenshealthcare": "Siemens",
    "siemenshealthcaregmbh": "Siemens",
    "gehc": "GE",
    "ge": "GE",
    "gemedicalsystems": "GE",
    "gehealthcare": "GE",
    "generalelectric": "GE",
    "philips": "Philips",
    "philipshealthcare": "Philips",
    "philipsmedicalsystems": "Philips",
    "canon": "Canon",
    "canonmec": "Canon",
    "canonmedicalsystems": "Canon",
    "toshiba": "Canon",
    "toshibamedicalsystems": "Canon",
    "hitachi": "Hitachi",
    "fujifilm": "Fujifilm",
    "fujifilmhealthcare": "Fujifilm",
}
MODEL_ALIASES = {
    "aera": "MAGNETOM Aera",
    "magnetomaera": "MAGNETOM Aera",
    "avanto": "MAGNETOM Avanto",
    "magnetomavanto": "MAGNETOM Avanto",
    "avantofit": "MAGNETOM Avanto Fit",
    "magnetomavantofit": "MAGNETOM Avanto Fit",
    "essenza": "MAGNETOM Essenza",
    "magnetomessenza": "MAGNETOM Essenza",
    "espree": "MAGNETOM Espree",
    "magnetomespree": "MAGNETOM Espree",
    "lumina": "MAGNETOM Lumina",
    "magnetomlumina": "MAGNETOM Lumina",
    "prismafit": "MAGNETOM Prisma Fit",
    "magnetomprismafit": "MAGNETOM Prisma Fit",
    "skyra": "MAGNETOM Skyra",
    "skyrafit": "MAGNETOM Skyra Fit",
    "magnetomskyra": "MAGNETOM Skyra",
    "triotim": "MAGNETOM TrioTim",
    "magnetomtriotim": "MAGNETOM TrioTim",
    "symphony": "MAGNETOM Symphony",
    "symphonytim": "MAGNETOM SymphonyTim",
    "magnetomsymphonytim": "MAGNETOM SymphonyTim",
    "sonata": "MAGNETOM Sonata",
    "magnetomsonata": "MAGNETOM Sonata",
    "verio": "MAGNETOM Verio",
    "magnetomverio": "MAGNETOM Verio",
    "vida": "MAGNETOM Vida",
    "magnetomvida": "MAGNETOM Vida",
    "altea": "MAGNETOM Altea",
    "magnetomaltea": "MAGNETOM Altea",
    "signahdxt": "SIGNA HDxt",
    "signaexplorer": "SIGNA Explorer",
    "signapremier": "SIGNA Premier",
    "signavoyager": "SIGNA Voyager",
    "signaartist": "SIGNA Artist",
    "signaarchitect": "SIGNA Architect",
    "signapioneer": "SIGNA Pioneer",
    "optimamr450w": "Optima MR450w",
    "biographmmr": "Biograph mMR",
}
SEQUENCE_CODE_LABELS = {
    "SE": "Spin Echo",
    "IR": "Inversion Recovery",
    "GR": "Gradient Recalled",
    "EP": "Echo Planar",
    "RM": "Research Mode",
}


def load_rows(csv_path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with csv_path.open(newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        return reader.fieldnames or [], list(reader)


def write_rows(csv_path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with csv_path.open("w", newline="", encoding="utf-8-sig") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def collapse_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip())


def format_numeric_metadata_value(numeric_value: float) -> str:
    rounded_value = round(numeric_value, 3)
    if rounded_value.is_integer():
        return str(int(rounded_value))
    return f"{rounded_value:.3f}".rstrip("0").rstrip(".")


def normalize_numeric_like_value(value: str) -> str:
    cleaned_value = collapse_whitespace(value)
    cleaned_value = cleaned_value.replace(",", ".")
    cleaned_value = re.sub(r"\s*([()/^-])\s*", r"\1", cleaned_value)
    if re.fullmatch(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", cleaned_value):
        return format_numeric_metadata_value(float(cleaned_value))
    return cleaned_value


def normalize_anatomical_plane_value(value: str) -> str:
    cleaned_value = collapse_whitespace(value)
    return ANATOMICAL_PLANE_MAPPING.get(cleaned_value.casefold(), cleaned_value)


def normalize_manufacturer_value(value: str) -> str:
    cleaned_value = collapse_whitespace(value)
    alias_key = re.sub(r"[^a-z0-9]+", "", cleaned_value.casefold())
    return MANUFACTURER_ALIASES.get(alias_key, cleaned_value)


def normalize_model_value(value: str) -> str:
    cleaned_value = collapse_whitespace(value.replace("_", " "))
    alias_key = re.sub(r"[^a-z0-9]+", "", cleaned_value.casefold())
    return MODEL_ALIASES.get(alias_key, cleaned_value)


def parse_delimited_metadata_tokens(value: str) -> list[str]:
    cleaned_value = collapse_whitespace(value)

    try:
        parsed_value = ast.literal_eval(cleaned_value)
    except (SyntaxError, ValueError):
        parsed_value = None

    if isinstance(parsed_value, (list, tuple)):
        raw_tokens = [str(item) for item in parsed_value]
    else:
        raw_tokens = re.split(r"[|\\\\,;/]+", cleaned_value.replace("[", "").replace("]", ""))

    tokens: list[str] = []
    for token in raw_tokens:
        token = token.strip().strip("'\"")
        token = collapse_whitespace(token)
        if token:
            tokens.append(token)
    return tokens


def normalize_acquisition_method_value(value: str) -> str:
    tokens = parse_delimited_metadata_tokens(value)
    labels = [SEQUENCE_CODE_LABELS.get(token.upper(), token) for token in tokens]
    unique_labels = list(dict.fromkeys(label for label in labels if label))
    return ", ".join(unique_labels)


def normalize_scan_option_value(value: str) -> str:
    tokens = parse_delimited_metadata_tokens(value)
    normalized_tokens: list[str] = []

    for token in tokens:
        normalized_key = re.sub(r"[^a-z0-9]+", "", token.casefold())
        if normalized_key == "contrastenhanced":
            normalized_tokens.append("contrast-enhanced")
        else:
            normalized_tokens.append(token.upper() if re.fullmatch(r"[A-Za-z0-9_]+", token) else token)

    unique_tokens = list(dict.fromkeys(token for token in normalized_tokens if token))
    return ", ".join(unique_tokens)


def normalize_contrast_agent_types_value(value: str) -> str:
    cleaned_value = collapse_whitespace(value)
    lowered_value = cleaned_value.casefold()

    for alias, canonical_value in CONTRAST_AGENT_ALIASES.items():
        if alias in re.sub(r"[^a-z0-9]+", "", lowered_value):
            return canonical_value
    return ""


def normalize_free_text_value(value: str) -> str:
    cleaned_value = collapse_whitespace(value)
    cleaned_value = re.sub(r"\s*([/+|,;:()-])\s*", r"\1", cleaned_value)
    cleaned_value = cleaned_value.replace("|", " | ").replace(",", ", ")
    cleaned_value = re.sub(r"\s+", " ", cleaned_value)
    return cleaned_value.strip()


def normalize_mri_metadata_value(column_name: str, value: str | None) -> str:
    raw_value = (value or "").strip()
    if not raw_value:
        return ""

    if column_name in NUMERIC_LIKE_COLUMNS:
        return normalize_numeric_like_value(raw_value)
    if column_name == "anatomical_plane":
        return normalize_anatomical_plane_value(raw_value)
    if column_name == "mri_machine_manufacturer":
        return normalize_manufacturer_value(raw_value)
    if column_name == "mri_machine_model":
        return normalize_model_value(raw_value)
    if column_name == "medical_facility":
        return ""
    if column_name == "operator_variability":
        return ""
    if column_name == "contrast_agent_types":
        return normalize_contrast_agent_types_value(raw_value)
    if column_name == "acquisition_method":
        return normalize_acquisition_method_value(raw_value)
    if column_name == "scan_option":
        return normalize_scan_option_value(raw_value)
    return normalize_free_text_value(raw_value)


def canonicalize_mri_metadata_rows(rows: list[dict[str, str]]) -> int:
    canonical_value_by_column_and_key: dict[tuple[str, str], str] = {}
    updated_cell_count = 0

    for row in rows:
        for column_name in MRI_METADATA_COLUMNS:
            normalized_value = normalize_mri_metadata_value(column_name, row.get(column_name))
            if not normalized_value and column_name not in ALLOW_EMPTY_NORMALIZED_COLUMNS:
                continue

            if normalized_value:
                canonical_key = (column_name, normalized_value.casefold())
                canonical_value = canonical_value_by_column_and_key.setdefault(canonical_key, normalized_value)
            else:
                canonical_value = ""

            if row.get(column_name, "") != canonical_value:
                row[column_name] = canonical_value
                updated_cell_count += 1

    return updated_cell_count


def normalize_mri_metadata_csv(csv_path: Path = DEFAULT_CSV_PATH) -> dict[str, object]:
    fieldnames, rows = load_rows(csv_path)
    updated_cell_count = canonicalize_mri_metadata_rows(rows)
    write_rows(csv_path, fieldnames, rows)

    return {
        "csv_path": csv_path,
        "row_count": len(rows),
        "updated_cell_count": updated_cell_count,
    }


def main() -> None:
    result = normalize_mri_metadata_csv()
    print(
        f"Normalized MRI metadata in {result['csv_path']} "
        f"(rows={result['row_count']}, updated_cells={result['updated_cell_count']})."
    )


if __name__ == "__main__":
    main()
