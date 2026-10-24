# Week 2 Validation Report

Validation date: 2026-10-18

## Result

**PASS for transformation logic, cumulative tests, source profiling, and Python compilation.**

The production Spark/Delta path is implemented but could not be executed in this sandbox because `pyspark` and `delta-spark` are not installed and the environment has no external package-network access. The repository therefore includes a deterministic pandas reference implementation that mirrors the same Bronze → Silver → Gold semantics and is executed during validation.

## Executed checks

### 1. Week 1 source profiling

Executed successfully on the included seven-city dataset:

- files processed: **7**
- raw rows: **5,547**
- valid rows: **5,547**
- invalid rows: **0**
- locations: **7**
- log-symmetric within 1%: **99.8738%**
- geometry coverage: **55.7779%**

### 2. Week 2 reference batch pipeline

Executed:

```bash
python scripts/run_batch_reference.py
```

Result:

```json
{
  "rows": 5547,
  "unique_route_keys": 5547,
  "unique_locations": 7,
  "unique_baseline_ids": 5547,
  "bounds_ordered": true,
  "finite_route_log_dispersion": true,
  "all_route_keys_unique": true
}
```

### 3. Automated tests

Executed:

```bash
pytest -q
```

Result:

```text
12 passed
```

This includes all Week 1 tests plus Week 2 route-contract, real-dataset baseline, log-dispersion, and deterministic-ID tests.

### 4. Python compilation

All Python files under `src/`, `scripts/`, and `tests/` compile successfully.

### 5. `validate_week2.py`

Executed successfully. The generated machine-readable report is:

```text
artifacts/week2_validation.json
```

Its Spark status records:

```json
{
  "spark_delta_dependencies_available": false,
  "production_spark_job_executed": false
}
```

## How to execute the actual Spark/Delta path

On a machine with network/package access, use a Python 3.11 environment and Java 17:

```bash
pip install -r requirements-spark.txt
```

On Windows, run the repo-local Hadoop helper setup once after installing the Spark dependencies:

```powershell
.\scripts\setup_hadoop.ps1
```

Then run the batch job and inspector:

```bash
python scripts/run_batch_lakehouse.py
python scripts/inspect_lakehouse.py
```

The expected Gold result is **5,547 rows across 7 locations**, with one unique baseline per `(location, origin_id, destination_id)`.
