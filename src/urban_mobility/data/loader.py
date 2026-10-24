from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from urban_mobility.config import get_yaml_config

CANONICAL_COLUMNS = [
    "location",
    "source_file",
    "origin_id",
    "origin_name",
    "origin_geometry",
    "destination_id",
    "destination_name",
    "destination_geometry",
    "date_range",
    "start_date",
    "end_date",
    "temporal_scope",
    "mean_tt",
    "lower_tt",
    "upper_tt",
]

REQUIRED_KEYS = ["origin_id", "destination_id", "mean_tt", "lower_tt", "upper_tt"]


def discover_csv_files(input_path: Path) -> list[Path]:
    if input_path.is_file():
        if input_path.suffix.lower() != ".csv":
            raise ValueError(f"Expected a CSV or directory, got {input_path}")
        return [input_path]
    if not input_path.is_dir():
        raise FileNotFoundError(input_path)
    return sorted(p for p in input_path.rglob("*.csv") if p.is_file())


def infer_location(path: Path, root: Path) -> str:
    """Infer locations for both `Travel_Times - Boston.csv` and nested layouts."""
    root = root if root.is_dir() else root.parent
    try:
        parent_parts = path.parent.relative_to(root).parts
    except ValueError:
        parent_parts = ()

    generic = {"data", "raw", "uber_movement", "travel_times", "travel_time", "csv"}
    for part in reversed(parent_parts):
        token = re.sub(r"[^a-z0-9]+", "_", part.lower()).strip("_")
        if token and token not in generic and not re.fullmatch(r"(?:19|20)\d{2}|q[1-4]", token):
            return re.sub(r"[_-]+", " ", part).strip()

    stem = path.stem.strip()
    match = re.match(r"(?i)^travel[_ ]*times?\s*-\s*(.+)$", stem)
    if match:
        return match.group(1).strip()
    match = re.match(r"(?i)^(.+?)[_ -]+travel[_ ]*times?", stem)
    if match:
        return re.sub(r"[_-]+", " ", match.group(1)).strip()
    return "unknown"


def _pick_column(columns: Iterable[str], aliases: list[str], required: bool) -> str | None:
    lookup = {str(c).strip().lower(): str(c) for c in columns}
    for alias in aliases:
        if alias.strip().lower() in lookup:
            return lookup[alias.strip().lower()]
    if required:
        raise ValueError(f"Missing required column; expected one of {aliases}")
    return None


def _normalise_id(series: pd.Series) -> pd.Series:
    out = series.astype("string").str.strip()
    return out.str.replace(r"^(\d+)\.0$", r"\1", regex=True)


def parse_date_range(series: pd.Series) -> pd.DataFrame:
    text = series.astype("string")
    dates = text.str.extract(r"(?P<start>\d{2}/\d{2}/\d{4})\s*-\s*(?P<end>\d{2}/\d{2}/\d{4})")
    temporal_scope = text.str.split(",", n=1).str[1].str.strip()
    return pd.DataFrame(
        {
            "start_date": pd.to_datetime(dates["start"], format="%m/%d/%Y", errors="coerce"),
            "end_date": pd.to_datetime(dates["end"], format="%m/%d/%Y", errors="coerce"),
            "temporal_scope": temporal_scope,
        }
    )


def canonicalize(df: pd.DataFrame, *, location: str, source_file: str) -> pd.DataFrame:
    cfg = get_yaml_config()["historical_columns"]
    mapping: dict[str, str | None] = {}
    for key, aliases in cfg.items():
        mapping[key] = _pick_column(df.columns, aliases, required=key in REQUIRED_KEYS)

    out = pd.DataFrame(index=df.index)
    out["location"] = location
    out["source_file"] = source_file
    out["origin_id"] = _normalise_id(df[mapping["origin_id"]])
    out["destination_id"] = _normalise_id(df[mapping["destination_id"]])

    for key in ["origin_name", "origin_geometry", "destination_name", "destination_geometry", "date_range"]:
        col = mapping.get(key)
        out[key] = df[col].astype("string") if col else pd.Series(pd.NA, index=df.index, dtype="string")

    parsed = parse_date_range(out["date_range"])
    out["start_date"] = parsed["start_date"]
    out["end_date"] = parsed["end_date"]
    out["temporal_scope"] = parsed["temporal_scope"]

    for key in ["mean_tt", "lower_tt", "upper_tt"]:
        out[key] = pd.to_numeric(df[mapping[key]], errors="coerce")

    return out[CANONICAL_COLUMNS]


def valid_mask(df: pd.DataFrame) -> pd.Series:
    return (
        df["location"].notna()
        & df["origin_id"].notna()
        & df["destination_id"].notna()
        & (df["origin_id"].str.len() > 0)
        & (df["destination_id"].str.len() > 0)
        & (df["lower_tt"] > 0)
        & (df["mean_tt"] > 0)
        & (df["upper_tt"] > 0)
        & (df["lower_tt"] <= df["mean_tt"])
        & (df["mean_tt"] <= df["upper_tt"])
    )


def load_dataset(input_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return canonical valid rows plus one profiling row per source CSV."""
    files = discover_csv_files(input_path)
    if not files:
        raise FileNotFoundError(f"No CSV files found under {input_path}")

    frames: list[pd.DataFrame] = []
    file_reports: list[dict] = []
    for path in files:
        location = infer_location(path, input_path)
        source_file = path.relative_to(input_path).as_posix() if input_path.is_dir() else path.name
        try:
            raw = pd.read_csv(path)
        except Exception as exc:
            file_reports.append(
                {
                    "source_file": source_file,
                    "location": location,
                    "status": "read_error",
                    "rows_raw": 0,
                    "rows_valid": 0,
                    "rows_invalid": 0,
                    "error": str(exc),
                }
            )
            continue

        try:
            canonical = canonicalize(raw, location=location, source_file=source_file)
        except ValueError as exc:
            file_reports.append(
                {
                    "source_file": source_file,
                    "location": location,
                    "status": "skipped_schema",
                    "rows_raw": int(len(raw)),
                    "rows_valid": 0,
                    "rows_invalid": 0,
                    "error": str(exc),
                }
            )
            continue

        mask = valid_mask(canonical)
        clean = canonical.loc[mask].copy()
        frames.append(clean)
        file_reports.append(
            {
                "source_file": source_file,
                "location": location,
                "status": "processed",
                "rows_raw": int(len(raw)),
                "rows_valid": int(mask.sum()),
                "rows_invalid": int((~mask).sum()),
                "unique_origins": int(clean["origin_id"].nunique()),
                "unique_destinations": int(clean["destination_id"].nunique()),
                "error": None,
            }
        )

    if not frames:
        raise ValueError("No CSV matched the configured Uber Movement travel-time schema")

    data = pd.concat(frames, ignore_index=True)
    data = data.drop_duplicates(
        subset=["location", "origin_id", "destination_id", "date_range", "mean_tt", "lower_tt", "upper_tt"]
    ).reset_index(drop=True)
    return data, pd.DataFrame(file_reports)


def _polygon_centroid(value: object) -> tuple[float, float] | None:
    if value is None or pd.isna(value):
        return None
    try:
        pts = json.loads(str(value))
    except Exception:
        return None
    if not isinstance(pts, list) or len(pts) < 3:
        return None
    try:
        coords = [(float(p[0]), float(p[1])) for p in pts]
    except Exception:
        return None

    # Remove duplicated closing point for numerical stability.
    if coords[0] == coords[-1]:
        coords = coords[:-1]
    if len(coords) < 3:
        return None

    area2 = 0.0
    cx_num = 0.0
    cy_num = 0.0
    for i, (x0, y0) in enumerate(coords):
        x1, y1 = coords[(i + 1) % len(coords)]
        cross = x0 * y1 - x1 * y0
        area2 += cross
        cx_num += (x0 + x1) * cross
        cy_num += (y0 + y1) * cross
    if abs(area2) < 1e-12:
        return (float(np.mean([p[0] for p in coords])), float(np.mean([p[1] for p in coords])))
    return (cx_num / (3.0 * area2), cy_num / (3.0 * area2))


def _haversine_km(a: tuple[float, float] | None, b: tuple[float, float] | None) -> float:
    if a is None or b is None:
        return float("nan")
    lon1, lat1 = map(math.radians, a)
    lon2, lat2 = map(math.radians, b)
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 6371.0088 * 2 * math.asin(min(1.0, math.sqrt(h)))


def enrich_routes(data: pd.DataFrame) -> pd.DataFrame:
    out = data.copy()
    out["lower_ratio"] = out["lower_tt"] / out["mean_tt"]
    out["upper_ratio"] = out["upper_tt"] / out["mean_tt"]
    out["route_log_dispersion"] = 0.5 * np.log(out["upper_tt"] / out["lower_tt"])
    out["geometric_midpoint_tt"] = np.sqrt(out["lower_tt"] * out["upper_tt"])
    out["log_symmetry_relative_error"] = (
        (out["geometric_midpoint_tt"] - out["mean_tt"]).abs() / out["mean_tt"]
    )

    origin_centroids = out["origin_geometry"].map(_polygon_centroid)
    destination_centroids = out["destination_geometry"].map(_polygon_centroid)
    out["straight_line_distance_km"] = [
        _haversine_km(a, b) for a, b in zip(origin_centroids, destination_centroids)
    ]
    out["straight_line_speed_proxy_kmh"] = (
        out["straight_line_distance_km"] / (out["mean_tt"] / 3600.0)
    )
    return out
