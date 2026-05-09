# Data Pipeline

The first pipeline phase starts from two immutable CSV inputs copied outside the repository:

- `temp_merged_sorted_cleaned.csv`
- `temp_funds_sorted_cleaned_lower.csv`

Event classification starts from local copies of the `all_*_fund_ids*` files. These copied raw inputs and all generated outputs are ignored by git.

## Raw Parquet

`analysis/build/raw_parquet/donations.parquet`

- Faithful conversion of `temp_merged_sorted_cleaned.csv`
- All columns are initially stored as strings
- Blank source index header is renamed to `__index_0`

`analysis/build/raw_parquet/fundraisers.parquet`

- Faithful conversion of `temp_funds_sorted_cleaned_lower.csv`
- All columns are initially stored as strings
- Blank source index header is renamed to `__index_0`

## Curated Tables

`fact_donations.parquet`

- Unit: one donation
- Key columns: `donation_id`, `fund_id`
- Main analysis columns: `amount`, `created_at`, `donation_date`, `currencycode`
- Includes `fundraiser_date_created` inherited from the source joined donation table

`dim_fundraisers.parquet`

- Unit: one fundraiser
- Key column: `fund_id`
- Main analysis columns: `date_created`, `tag`, `city`, `state`, `n_donors`, `progress`, `goal`

`fundraiser_text.parquet`

- Unit: one fundraiser
- Key column: `fund_id`
- Text/sensitive columns: `title`, `description`, `url`, `organizer_name`
- Descriptions are already lowercased in the chosen source CSV

`event_membership.parquet`

- Unit: one event-fundraiser link
- Key columns: `event_id`, `fund_id`
- Source tracking: `source_files`
- Stored in long format so fundraisers can belong to multiple events

## Current Deliberate Omissions

This phase does not create partitions, marts, keyword features, or regression panels.
