# Traffic Density Prediction

An urban-mobility data project that profiles historical Uber Movement travel times and builds a location-aware Delta Lake baseline for downstream traffic simulation and prediction.

## What this project does

The current implementation prepares historical origin-destination (OD) travel-time data. It:

1. Profiles and validates the supplied city CSV files.
2. Normalizes and enriches route records.
3. Builds Bronze, Silver, and Gold datasets with either a pandas reference runner or Spark and Delta Lake.
4. Validates that the Gold baseline has one unique record per route.

The project does **not** yet train or serve a traffic-density prediction model. The Gold OD baseline is intended as input to future simulation and prediction work.

## Data and key

The included Uber Movement dataset contains seven city exports and 5,547 valid route records. The canonical route key is:

```text
(location, origin_id, destination_id)
```

Movement IDs are city-local, so `origin_id` and `destination_id` must always be interpreted together with `location`.

Travel-time bounds are in seconds. The pipeline derives, among other fields:

```text
route_log_dispersion = 0.5 * ln(upper_tt / lower_tt)
```

This measures the spread of the supplied travel-time bounds; it is not a direct measurement of traffic density.

## Repository layout

```text
config/settings.yaml             Column aliases and profiling settings
data/raw/uber_movement/          Supplied city CSVs and dataset documentation
data/lakehouse/                  Default Spark/Delta output (generated)
notebooks/                       Data exploration notebook
scripts/                         Profiling, batch, inspection, and validation commands
src/urban_mobility/              Reusable Python package
tests/                            Unit and real-dataset tests
artifacts/                       Reports and generated outputs
```

## Setup

Python 3.11 or 3.12 is recommended for the Spark workflow. Use Java 17 for Spark 3.5 and Delta Lake 3.x.

Create and activate a virtual environment, then install the core dependencies:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

On macOS or Linux, use `python3.12 -m venv .venv` and `source .venv/bin/activate`.

To run the production Spark/Delta path, also install:

```powershell
python -m pip install -r requirements-spark.txt
```

On Windows, install the checksum-verified Hadoop helper binaries once:

```powershell
.\scripts\setup_hadoop.ps1
```

The setup script stores them in the ignored `.tools/hadoop/` directory. It does not change machine-wide environment variables. A valid existing `HADOOP_HOME` takes precedence.

## Run

Run commands from the repository root with the virtual environment active.

| Goal | Command |
|---|---|
| Profile the supplied dataset | `python scripts/profile_historical_data.py` |
| Run the exploration notebook | `python scripts/run_notebook.py` |
| Run the pandas reference pipeline | `python scripts/run_batch_reference.py` |
| Run the Spark/Delta pipeline | `python scripts/run_batch_lakehouse.py` |
| Inspect the generated Delta tables | `python scripts/inspect_lakehouse.py` |
| Run all validation checks | `python scripts/validate_week2.py` |
| Run tests only | `python -m pytest` |

The Spark command requires the Spark dependencies and, on Windows, the Hadoop helper setup. Validation runs the production Spark job automatically when Spark and Delta are installed.

Optional path overrides:

```powershell
python scripts/run_batch_lakehouse.py --input data/raw/uber_movement --lakehouse-root data/lakehouse
python scripts/run_batch_reference.py --input data/raw/uber_movement --output artifacts/week2_reference
python scripts/inspect_lakehouse.py --lakehouse-root data/lakehouse
```

## Pipeline outputs

### Profiling

`python scripts/profile_historical_data.py` writes the data profile and route summaries to `artifacts/`:

- `data_profile.json`
- `file_profile.csv`
- `location_summary.csv`
- `route_statistics.csv`
- `normalized_preview.csv`

### Reference pipeline

`python scripts/run_batch_reference.py` writes `bronze.csv`, `silver.csv`, `gold.csv`, and `metrics.json` under `artifacts/week2_reference/`. It mirrors the Spark transformations for local checks and does not replace the Delta output.

### Spark and Delta Lake

`python scripts/run_batch_lakehouse.py` writes three Delta tables under `data/lakehouse/`:

```text
bronze/uber_movement_raw
silver/uber_routes
gold/od_baseline
```

Each table is partitioned by `location`. Runs overwrite the current table snapshot at these paths. Delta may retain older files for time travel; removing historical files requires a separate `VACUUM` operation.

## Data layers

- **Bronze — `uber_movement_raw`:** source route fields, source filename, location, and ingestion timestamp.
- **Silver — `uber_routes`:** validated and deduplicated routes with parsed date ranges, ratios, geometry availability, and derived travel-time features.
- **Gold — `od_baseline`:** one baseline per `(location, origin_id, destination_id)`, with a deterministic SHA-256 `baseline_id`.

The Gold table is intended for downstream modeling and simulation. It is a historical baseline, not a live traffic feed.

## Configuration

- `config/settings.yaml` defines source-column aliases and profiling options.
- Optional `.env` values can override `UBER_MOVEMENT_INPUT`, `ARTIFACT_DIR`, `LAKEHOUSE_ROOT`, and `SPARK_APP_NAME`. See `.env.example`.
- Default paths are relative to the repository root.

## Validation

```powershell
python scripts/validate_week2.py
```

Validation compiles Python sources, runs the reference pipeline against the supplied dataset, checks the Gold contract, and runs the test suite. If Spark and Delta dependencies are installed, it also runs the production pipeline. The report is written to `artifacts/week2_validation.json`.

Expected dataset-level results:

```text
routes:                 5,547
locations:                  7
unique route keys:      5,547
ordered travel bounds:  true
```
