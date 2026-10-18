# Multi-file Data Contract and Historical Dataset Profiling

**Goal:** understand and validate the Kaggle Uber Movement download before adding Spark, Kafka, or the lakehouse runtime.

The Kaggle dataset is organized as multiple travel-time CSVs for different locations. Week 1 therefore treats the **dataset directory** as the input, recursively discovers CSVs, identifies the location for each file, normalizes schema aliases, validates travel-time bounds, and produces global + per-file/per-location profiles.

## What current codebase does

1. Recursively discovers `*.csv` files under `data/raw/uber_movement/`.
2. Ignores `Travel_Times.sample.csv` automatically when real downloaded CSVs are present.
3. Attempts to normalize only files matching the Uber Movement travel-time schema.
4. Records unrelated/auxiliary CSVs as `skipped_schema` instead of crashing the run.
5. Adds `location` and `source_file` to every normalized record.
6. Uses `(location, origin_id, destination_id)` when counting OD pairs, preventing Movement IDs from different cities from colliding.
7. Validates positive travel times and the constraint `lower_tt <= mean_tt <= upper_tt`.
8. Writes a compact normalized Parquet preview plus profiling reports.

The dataset is treated as **aggregated origin-destination travel-time statistics**, not as billions of individual trip rows.

## Expected input layouts

Both nested and flat Kaggle layouts are supported.

### Nested by location

```text
data/raw/uber_movement/
├── Boston/
│   ├── 2018/
│   │   └── Q1/
│   │       └── Travel_Times.csv
│   └── ...
├── London/
│   └── Travel_Times.csv
└── ...
```

### Flat filenames

```text
data/raw/uber_movement/
├── Boston_Travel_Times_2018_Q1.csv
├── London_Travel_Times_2018_Q1.csv
└── ...
```

Location inference prefers a non-generic parent directory. If the files are flat, it derives the location from the filename. If a single generically named CSV cannot reveal its location, use `--location` explicitly.

## Required travel-time columns

Column aliases are configured in `config/settings.yaml`. At minimum each processed travel-time file must provide equivalents of:

- `Origin Movement ID`
- `Destination Movement ID`
- `Mean Travel Time (Seconds)`
- `Range - Lower Bound Travel Time (Seconds)`
- `Range - Upper Bound Travel Time (Seconds)`

`Date Range` and a location/city column are optional. When no city column exists, location is inferred from the path.

## Setup

```bash
python -m venv .venv
```

Windows PowerShell:

```powershell
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
$env:PYTHONPATH="src"
```

Linux/macOS:

```bash
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
export PYTHONPATH=src
```

## Included dataset

This Week 1 snapshot already contains the uploaded Uber Movement dataset under:

```text
data/raw/uber_movement/
```

The included archive uses flat filenames such as:

```text
Travel_Times - Bogota.csv
Travel_Times - Boston.csv
Travel_Times - Johannesburg and Pretoria.csv
Travel_Times - Manila.csv
Travel_Times - Paris.csv
Travel_Times - Sydney.csv
Travel_Times - Washington DC.csv
```

The profiler supports this naming pattern directly; no renaming or manual merge is required. It also continues to support nested folders and `<location>_Travel_Times_...csv` naming.

Verified against the included files:

- 7 travel-time CSV files
- 5,547 aggregate OD travel-time rows
- 7 locations: Bogota, Boston, Johannesburg and Pretoria, Manila, Paris, Sydney, Washington DC
- 0 invalid travel-time rows
- 0 rows with unknown location

The accompanying `datapackage.json` declares the package license as **CC BY-NC**; the supplied metadata and methodology PDF are retained alongside the CSVs.

Run:

```bash
python scripts/profile_historical_data.py
```

You can also point at another extracted directory:

```bash
python scripts/profile_historical_data.py --input D:/datasets/uber-movement
```

For one standalone file with no location in its path/name:

```bash
python scripts/profile_historical_data.py \
  --input data/raw/uber_movement/Travel_Times.csv \
  --location Boston
```

## Outputs

```text
artifacts/
├── data_profile.json
├── file_profile.csv
└── normalized_preview.parquet
```

### `data_profile.json`

Global summary plus per-location statistics, for example:

```json
{
  "files_discovered": 42,
  "files_processed": 40,
  "files_skipped_schema": 2,
  "rows_raw": 1345721,
  "rows_invalid": 6513,
  "rows_after_global_dedup": 1339208,
  "unique_locations": 12,
  "locations": {
    "Boston": {
      "rows": 47987,
      "files": 4,
      "unique_od_pairs": 10432
    }
  }
}
```

### `file_profile.csv`

One row per discovered CSV with status and quality statistics. This is useful for identifying a malformed location file or auxiliary CSV before Week 2.

### `normalized_preview.parquet`

A small sample of canonical records from every location:

```text
location
source_file
origin_id
destination_id
mean_tt
lower_tt
upper_tt
date_range
```

It is a preview/contract-validation artifact, **not** the complete lakehouse dataset. Full distributed ingestion begins in Week 2.

## Tests

```bash
pytest -q
```

Tests cover recursive discovery, nested/flat location inference, auxiliary-file skipping, invalid travel-time rows, and protection against cross-city Movement-ID collisions.

## Suggested Git commit

```text
chore: bootstrap multi-location Uber Movement data profiling
```

## Next week

Move this canonical contract into Spark, ingest all location-specific CSVs into Bronze/Silver Delta tables, and build location-aware OD baselines.
