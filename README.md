# radioval-data-split

Utilities for generating the Radioval dataset splits and validating the final split coverage.

## XNAT CSV Enrichment

To enrich `radioval_harmonized_new.csv` with MRI metadata from XNAT/DICOM headers, first install the XNAT dependency from `requirements.txt`:

```bash
pip install -r requirements.txt
```

Then create a `.env` file in the repository root:

```bash
export XNAT_SERVER=https://xnat.example.com
export XNAT_USER=your_username
export XNAT_PASSWORD=your_password
```

Optional:

```bash
export XNAT_SCAN_TYPE=DCE
export XNAT_VERIFY_TLS=true
```

Then run:

```bash
python3 dicom_enrichments/enrich_csv_from_xnat.py
```

The script reads `radioval_harmonized_new.csv`, maps each `dataset` to the matching XNAT project, finds the patient by `patient_id`, selects the first scan whose `type` matches `XNAT_SCAN_TYPE`, and fills missing MRI metadata columns in place.
For datasets with unreliable XNAT scan labels, the script can fall back to reading DICOM headers scan-by-scan and identifying DCE-like scans from header fields such as temporal position metadata.
Non-fatal lookup issues are written to `dicom_enrichments/xnat_enrichment_errors.log`, and skipped cases are written to `dicom_enrichments/xnat_enrichment_skips.log`.

After enrichment, normalize the DICOM-derived MRI metadata values:

```bash
python3 dicom_enrichments/normalize_mri_metadata_csv.py
```

The normalization step standardizes manufacturer/model names, numeric MRI parameters, acquisition/scan option formatting, anatomical plane labels, and contrast agent names.

## Overview

This repository builds four output datasets from a harmonized clinical source file:

- `clinical_validation_5152.csv`
- `clinical_validation_53.csv`
- `ai_validation.csv`
- `train.csv`

The split pipeline is orchestrated by `split_data.py`.  
Split-specific logic is implemented in:

- `data_split/split_data_5152.py`
- `data_split/split_data_53.py`
- `data_split/split_data_ai_validation.py`
- `data_split/split_data_train.py`

Shared helper functions live in `split_utils.py`.

## Required Input

The split pipeline expects the following input file in the project root:

- `radioval_harmonized_new.csv`

This is the file used by all split scripts and by the verification script.

If you need to regenerate it from `radioval_harmonized.csv`, make sure both files are located in the project root and run:

```bash
python add_missing_data.py
```

## How To Run The Split Pipeline

From the repository root:

```bash
python split_data.py
```

This runs the split jobs in the correct dependency order:

1. `clinical_validation_5152`
2. `clinical_validation_53`
3. `ai_validation`
4. `train`

If you enriched `radioval_harmonized_new.csv` from XNAT/DICOM headers, normalize MRI metadata columns before running the split pipeline:

```bash
python3 dicom_enrichments/normalize_mri_metadata_csv.py
```

The `clinical_validation_5152` step assumes `radioval_harmonized_new.csv` is already normalized and includes those MRI metadata columns in the diversity-based case selection.

## Output Files

All generated CSV files are written to `output_files/` in the project root.

The pipeline produces:

- `output_files/clinical_validation_5152.csv`
- `output_files/clinical_validation_53.csv`
- `output_files/ai_validation.csv`
- `output_files/train.csv`

## How To Run Validation

To validate the generated splits, run:

```bash
python split_verification/verify_full_pipeline.py
```

The validation script checks:

- row counts for each output file
- unique case counts on the `(dataset, patient_id)` level
- per-center statistics
- pairwise split disjointness
- coverage against `radioval_harmonized_new.csv` from the project root
- missing and extra patient IDs per dataset

## Important Notes

- Duplicate `patient_id` values are treated as one case at the `(dataset, patient_id)` level, even if multiple rows exist for the same patient.
- Current observed differences worth keeping in mind:
  - `UZSM`: `ai_validation` contains 95 cases instead of the previously declared 94.
  - `AFI`: `ai_validation` contains 71 cases instead of the previously declared 93.
  - `HULAFE`: `train` contains 219 cases instead of the previously declared 204.
  - `MUW`: `train` contains 397 cases instead of the previously declared 398.
- Some `AFI` cases expected from the harmonized data are currently not visible on XNAT.
- `ASU` is currently not present in either the harmonized source file or XNAT.

## Typical Workflows

### Case 1: `radioval_harmonized_new.csv` already exists

Use this path if the prepared source file is already present in the project root.

1. Make sure `radioval_harmonized_new.csv` is in the project root.
2. Run the split pipeline:

```bash
python split_data.py
```

3. Run the verification step:

```bash
python split_verification/verify_full_pipeline.py
```

4. Inspect the generated CSV files in `output_files/`.

### Case 2: only `radioval_harmonized.csv` is available

Use this path if you need to regenerate the prepared source file first.

1. Place `radioval_harmonized.csv` in the project root.
2. Generate `radioval_harmonized_new.csv`:

```bash
python add_missing_data.py
```

3. Run the split pipeline:

```bash
python split_data.py
```

4. Run the verification step:

```bash
python split_verification/verify_full_pipeline.py
```

5. Inspect the generated CSV files in `output_files/`.
