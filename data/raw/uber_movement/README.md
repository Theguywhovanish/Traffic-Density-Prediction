# Uber Movement input directory

The actual uploaded Week 1 historical dataset is included in this directory. See `DATASET_README.md` for the included locations, provenance, license metadata, and canonical schema.

The profiling command can be run directly from the repository root:

```bash
python scripts/profile_historical_data.py
```

Do not merge the location CSVs manually. Week 1 discovers them recursively and uses `(location, origin_id, destination_id)` as the OD identity.
