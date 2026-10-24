from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

from urban_mobility.data.loader import enrich_routes, load_dataset
from urban_mobility.lakehouse.contracts import GOLD_COLUMNS, ROUTE_KEY, SILVER_COLUMNS


def _baseline_id(location: str, origin_id: str, destination_id: str) -> str:
    payload = f"{location}||{origin_id}||{destination_id}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_reference_layers(input_path: str | Path) -> dict[str, pd.DataFrame]:
    """Pandas mirror of the Week 2 Spark transformations.

    It is intentionally used for deterministic tests and CI environments that do not
    have a JVM/Spark installation. The production Week 2 job is the Spark/Delta job.
    """
    raw, file_profile = load_dataset(Path(input_path))

    bronze = raw[[
        "location", "source_file", "origin_id", "origin_name", "origin_geometry",
        "destination_id", "destination_name", "destination_geometry", "date_range",
        "mean_tt", "lower_tt", "upper_tt",
    ]].copy()
    bronze["ingested_at"] = pd.Timestamp.now(tz="UTC")

    silver = enrich_routes(raw)
    silver["has_geometry"] = silver["origin_geometry"].notna() & silver["destination_geometry"].notna()
    silver = silver[SILVER_COLUMNS].copy()
    silver = silver.drop_duplicates(subset=ROUTE_KEY, keep="first").reset_index(drop=True)

    gold = silver.copy()
    gold.insert(
        0,
        "baseline_id",
        [
            _baseline_id(str(loc), str(origin), str(dest))
            for loc, origin, dest in gold[ROUTE_KEY].itertuples(index=False, name=None)
        ],
    )
    gold = gold[GOLD_COLUMNS].copy()

    return {
        "bronze": bronze,
        "silver": silver,
        "gold": gold,
        "file_profile": file_profile,
    }


def validate_gold_baseline(gold: pd.DataFrame) -> dict[str, object]:
    unique_keys = gold[ROUTE_KEY].drop_duplicates().shape[0]
    finite_dispersion = np.isfinite(gold["route_log_dispersion"]).all()
    bounds_ordered = ((gold["lower_tt"] <= gold["mean_tt"]) & (gold["mean_tt"] <= gold["upper_tt"])).all()
    return {
        "rows": int(len(gold)),
        "unique_route_keys": int(unique_keys),
        "unique_locations": int(gold["location"].nunique()),
        "unique_baseline_ids": int(gold["baseline_id"].nunique()),
        "bounds_ordered": bool(bounds_ordered),
        "finite_route_log_dispersion": bool(finite_dispersion),
        "all_route_keys_unique": bool(unique_keys == len(gold)),
    }
