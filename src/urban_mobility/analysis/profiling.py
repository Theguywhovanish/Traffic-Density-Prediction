from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from urban_mobility.config import get_settings, get_yaml_config
from urban_mobility.data.loader import discover_csv_files, enrich_routes, load_dataset


def _spearman(a: pd.Series, b: pd.Series) -> float | None:
    frame = pd.concat([a, b], axis=1).dropna()
    if len(frame) < 3 or frame.iloc[:, 0].nunique() < 2 or frame.iloc[:, 1].nunique() < 2:
        return None
    return float(frame.iloc[:, 0].rank().corr(frame.iloc[:, 1].rank()))


def build_location_summary(routes: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for location, g in routes.groupby("location", sort=True):
        rows.append(
            {
                "location": location,
                "rows": int(len(g)),
                "unique_origins": int(g["origin_id"].nunique()),
                "unique_destinations": int(g["destination_id"].nunique()),
                "median_mean_tt_sec": float(g["mean_tt"].median()),
                "p10_mean_tt_sec": float(g["mean_tt"].quantile(0.10)),
                "p90_mean_tt_sec": float(g["mean_tt"].quantile(0.90)),
                "median_lower_ratio": float(g["lower_ratio"].median()),
                "median_upper_ratio": float(g["upper_ratio"].median()),
                "median_log_dispersion": float(g["route_log_dispersion"].median()),
                "mean_log_symmetry_error": float(g["log_symmetry_relative_error"].mean()),
                "geometry_coverage_pct": float(g["straight_line_distance_km"].notna().mean() * 100.0),
                "spearman_mean_tt_vs_dispersion": _spearman(g["mean_tt"], g["route_log_dispersion"]),
                "spearman_distance_vs_mean_tt": _spearman(g["straight_line_distance_km"], g["mean_tt"]),
            }
        )
    return pd.DataFrame(rows)


def build_profile(routes: pd.DataFrame, file_profile: pd.DataFrame, source: Path) -> dict:
    tol = float(get_yaml_config().get("profiling", {}).get("log_symmetry_relative_tolerance", 0.01))
    processed = file_profile[file_profile["status"] == "processed"]
    return {
        "source": str(source),
        "files_discovered": int(len(file_profile)),
        "files_processed": int(len(processed)),
        "rows_raw": int(processed["rows_raw"].sum()),
        "rows_valid": int(processed["rows_valid"].sum()),
        "rows_invalid": int(processed["rows_invalid"].sum()),
        "rows_after_dedup": int(len(routes)),
        "unique_locations": int(routes["location"].nunique()),
        "unique_od_keys": int(routes[["location", "origin_id", "destination_id"]].drop_duplicates().shape[0]),
        "all_bounds_ordered": bool(((routes["lower_tt"] <= routes["mean_tt"]) & (routes["mean_tt"] <= routes["upper_tt"])).all()),
        "log_symmetric_within_1pct_pct": float((routes["log_symmetry_relative_error"] <= tol).mean() * 100.0),
        "median_log_symmetry_relative_error": float(routes["log_symmetry_relative_error"].median()),
        "geometry_coverage_pct": float(routes["straight_line_distance_km"].notna().mean() * 100.0),
        "date_ranges": sorted(routes["date_range"].dropna().astype(str).unique().tolist()),
        "temporal_scopes": sorted(routes["temporal_scope"].dropna().astype(str).unique().tolist()),
    }


def write_artifacts(input_path: str | Path | None = None, artifact_dir: str | Path | None = None) -> dict:
    settings = get_settings()
    source = settings.resolve_repo_path(str(input_path or settings.uber_movement_input))
    out_dir = settings.resolve_repo_path(str(artifact_dir or settings.artifact_dir))
    out_dir.mkdir(parents=True, exist_ok=True)

    raw_routes, file_profile = load_dataset(source)
    routes = enrich_routes(raw_routes)
    location_summary = build_location_summary(routes)
    profile = build_profile(routes, file_profile, source)

    profile_path = out_dir / "data_profile.json"
    file_path = out_dir / "file_profile.csv"
    location_path = out_dir / "location_summary.csv"
    route_path = out_dir / "route_statistics.csv"
    preview_path = out_dir / "normalized_preview.csv"

    profile_path.write_text(json.dumps(profile, indent=2, default=str), encoding="utf-8")
    file_profile.to_csv(file_path, index=False)
    location_summary.to_csv(location_path, index=False)
    routes.to_csv(route_path, index=False)

    n = int(get_yaml_config().get("profiling", {}).get("preview_rows_per_location", 25))
    preview = (
        routes.sort_values(["location", "origin_id", "destination_id"], kind="stable")
        .groupby("location", group_keys=False)
        .head(n)
    )
    preview.to_csv(preview_path, index=False)

    return {
        "profile": profile,
        "file_profile": file_profile,
        "location_summary": location_summary,
        "routes": routes,
        "paths": {
            "data_profile": str(profile_path),
            "file_profile": str(file_path),
            "location_summary": str(location_path),
            "route_statistics": str(route_path),
            "normalized_preview": str(preview_path),
        },
    }
