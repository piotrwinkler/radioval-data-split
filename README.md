# radioval-data-split

Utilities for generating the Radioval dataset splits and validating the final split coverage.

## Overview

This repository builds four output datasets from a harmonized clinical source file:

- `clinical_validation_5152.csv`
- `clinical_validation_53.csv`
- `ai_validation.csv`
- `train.csv`

The split pipeline is orchestrated by [split_data.py](/home/piotr/projects/radioval-data-split/split_data.py).  
Split-specific logic is implemented in:

- [data_split/split_data_5152.py](/home/piotr/projects/radioval-data-split/data_split/split_data_5152.py)
- [data_split/split_data_53.py](/home/piotr/projects/radioval-data-split/data_split/split_data_53.py)
- [data_split/split_data_ai_validation.py](/home/piotr/projects/radioval-data-split/data_split/split_data_ai_validation.py)
- [data_split/split_data_train.py](/home/piotr/projects/radioval-data-split/data_split/split_data_train.py)

Shared helper functions live in [split_utils.py](/home/piotr/projects/radioval-data-split/split_utils.py).

## Required Input

The split pipeline expects the following input file in the repository root:

- [radioval_harmonized_new.csv](/home/piotr/projects/radioval-data-split/radioval_harmonized_new.csv)

This is the file used by all split scripts and by the verification script.

If you need to regenerate it from [radioval_harmonized.csv](/home/piotr/projects/radioval-data-split/radioval_harmonized.csv), use:

```bash
python normalize_split_column.py
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

## Output Files

All generated CSV files are written to:

- [output_files](/home/piotr/projects/radioval-data-split/output_files)

The pipeline produces:

- [output_files/clinical_validation_5152.csv](/home/piotr/projects/radioval-data-split/output_files/clinical_validation_5152.csv)
- [output_files/clinical_validation_53.csv](/home/piotr/projects/radioval-data-split/output_files/clinical_validation_53.csv)
- [output_files/ai_validation.csv](/home/piotr/projects/radioval-data-split/output_files/ai_validation.csv)
- [output_files/train.csv](/home/piotr/projects/radioval-data-split/output_files/train.csv)

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
- coverage against `radioval_harmonized_new.csv`
- missing and extra patient IDs per dataset

## Typical Workflow

1. Place or update `radioval_harmonized_new.csv` in the repository root.
2. Run the split pipeline:

```bash
python split_data.py
```

3. Run the verification step:

```bash
python split_verification/verify_full_pipeline.py
```

4. Inspect the generated CSV files in `output_files/`.
