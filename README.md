# Thesis GoFundMe Data Pipeline

This repository rebuilds the GoFundMe thesis data pipeline from two selected raw CSV inputs:

- `temp_merged_sorted_cleaned.csv`: donation-level input
- `temp_funds_sorted_cleaned_lower.csv`: fundraiser-level input

The raw CSV files are treated as immutable and are not committed to git. The first pipeline phase creates faithful raw Parquet copies and curated normalized Parquet tables:

- `fact_donations`
- `dim_fundraisers`
- `fundraiser_text`
- `event_membership`

## Setup

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
cp config/config_template.yaml config/config_user.yaml
```

Edit `config/config_user.yaml` if your raw data paths differ.

## Commands

```bash
python analysis/run.py copy-events
python analysis/run.py manifest
python analysis/run.py raw-parquet
python analysis/run.py curated
python analysis/run.py validate
python analysis/run.py all
```

Generated data is written under `analysis/build/` and ignored by git.
