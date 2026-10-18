from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable

import pandas as pd

from urban_mobility.config import get_settings, get_yaml_config

CANONICAL = (
    "location",
    "source_file",
    "origin_id",
    "destination_id",
    "mean_tt",
    "lower_tt",
    "upper_tt",
    "date_range",
)

DEDUP_COLUMNS = (
    "location",
    "origin_id",
    "destination_id",
    "mean_tt",
    "lower_tt",
    "upper_tt",
    "date_range",
)

_GENERIC_PATH_PARTS = {
    "data",
    "raw",
    "csv",
    "files",
    "uber",
    "uber_movement",
    "movement",
    "travel_time",
    "travel_times",
    "traveltime",
    "traveltimes",
    "sample",
    "samples",
}


def pick_column(columns: list[str], candidates: list[str], required: bool = True) -> str | None:
    lookup = {str(c).strip().lower(): str(c) for c in columns}
    for candidate in candidates:
        key = candidate.strip().lower()
        if key in lookup:
            return lookup[key]
    if required:
        raise ValueError(f"Missing required column. Expected one of {candidates}; got {list(columns)}")
    return None


def _normalise_id(series: pd.Series) -> pd.Series:
    """Movement IDs are integer-like in the source; avoid strings such as '101.0'."""
    out = series.astype("string").str.strip()
    return out.str.replace(r"^(\d+)\.0$", r"\1", regex=True)


def canonicalize_pandas(
    df: pd.DataFrame,
    *,
    location: str | None = None,
    source_file: str | None = None,
) -> pd.DataFrame:
    cfg = get_yaml_config()["historical_columns"]
    mapping = {
        "origin_id": pick_column(list(df.columns), cfg["origin_id"]),
        "destination_id": pick_column(list(df.columns), cfg["destination_id"]),
        "mean_tt": pick_column(list(df.columns), cfg["mean_tt"]),
        "lower_tt": pick_column(list(df.columns), cfg["lower_tt"]),
        "upper_tt": pick_column(list(df.columns), cfg["upper_tt"]),
        "date_range": pick_column(list(df.columns), cfg["date_range"], required=False),
        "location": pick_column(list(df.columns), cfg.get("location", []), required=False),
    }

    if mapping["location"]:
        locations = df[mapping["location"]].astype("string").str.strip()
        if location:
            locations = locations.fillna(location).replace("", location)
    else:
        locations = pd.Series(location or "unknown", index=df.index, dtype="string")

    out = pd.DataFrame(
        {
            "location": locations,
            "source_file": pd.Series(source_file or "unknown", index=df.index, dtype="string"),
            "origin_id": _normalise_id(df[mapping["origin_id"]]),
            "destination_id": _normalise_id(df[mapping["destination_id"]]),
            "mean_tt": pd.to_numeric(df[mapping["mean_tt"]], errors="coerce"),
            "lower_tt": pd.to_numeric(df[mapping["lower_tt"]], errors="coerce"),
            "upper_tt": pd.to_numeric(df[mapping["upper_tt"]], errors="coerce"),
        }
    )
    out["date_range"] = (
        df[mapping["date_range"]].astype("string") if mapping["date_range"] else pd.NA
    )
    return out.loc[:, list(CANONICAL)]


def valid_mask(df: pd.DataFrame) -> pd.Series:
    return (
        df["location"].notna()
        & df["origin_id"].notna()
        & df["destination_id"].notna()
        & (df["origin_id"].str.len() > 0)
        & (df["destination_id"].str.len() > 0)
        & (df["mean_tt"] > 0)
        & (df["lower_tt"] > 0)
        & (df["upper_tt"] > 0)
        & (df["lower_tt"] <= df["mean_tt"])
        & (df["mean_tt"] <= df["upper_tt"])
    )


def _is_sample_file(path: Path) -> bool:
    name = path.name.lower()
    return ".sample." in name or name.startswith("sample_") or name.endswith("_sample.csv")


def discover_csv_files(input_path: Path) -> list[Path]:
    """Discover travel-time candidates recursively.

    If a directory contains real CSVs plus the repository's smoke-test sample,
    the sample is excluded. If the sample is the only CSV, it is kept so Week 1
    still runs immediately after cloning.
    """
    if input_path.is_file():
        if input_path.suffix.lower() != ".csv":
            raise ValueError(f"Expected a CSV file or directory, got: {input_path}")
        return [input_path]
    if not input_path.is_dir():
        raise FileNotFoundError(f"Input path does not exist: {input_path}")

    files = sorted(p for p in input_path.rglob("*.csv") if p.is_file())
    real_files = [p for p in files if not _is_sample_file(p)]
    return real_files or files


def _looks_generic(part: str) -> bool:
    token = re.sub(r"[^a-z0-9]+", "_", part.lower()).strip("_")
    if token in _GENERIC_PATH_PARTS:
        return True
    if re.fullmatch(r"(?:19|20)\d{2}", token):
        return True
    if re.fullmatch(r"q[1-4]", token):
        return True
    if re.fullmatch(r"(?:quarter|month|week)_?\d+", token):
        return True
    if re.fullmatch(r"(?:19|20)\d{2}_?q[1-4]", token):
        return True
    return False


def _clean_location_label(value: str) -> str:
    value = re.sub(r"[_\-]+", " ", value).strip()
    value = re.sub(r"\s+", " ", value)
    return value or "unknown"


def infer_location(file_path: Path, input_root: Path) -> str:
    """Infer city/location from directories first, then from a flat filename."""
    root = input_root if input_root.is_dir() else input_root.parent

    try:
        relative_parent = file_path.parent.relative_to(root)
        parts = list(relative_parent.parts)
    except ValueError:
        parts = []

    # In layouts such as root/Boston/2018/Q1/file.csv, walk upward until
    # finding the nearest non-generic, non-date directory.
    for part in reversed(parts):
        if not _looks_generic(part):
            return _clean_location_label(part)

    # Flat layout fallback supports both common forms:
    #   Boston_Travel_Times_2018_Q1.csv -> Boston
    #   Travel_Times - Boston.csv       -> Boston
    stem = file_path.stem
    marker = re.search(r"(?i)(?:travel[_\- ]*times?|movement)", stem)
    if marker:
        before = stem[: marker.start()].strip(" _-")
        after = stem[marker.end() :].strip(" _-")
        # Prefer the non-generic side. Kaggle's actual archive uses
        # `Travel_Times - <location>.csv`, while other exports commonly use
        # `<location>_Travel_Times_....csv`.
        candidates = [before, after]
        cleaned = next(
            (candidate for candidate in candidates if candidate and not _looks_generic(candidate)),
            "",
        )
    else:
        cleaned = stem

    cleaned = re.sub(r"(?i)[_\- ]*(?:19|20)\d{2}(?:[_\- ]*q[1-4])?.*$", "", cleaned)
    cleaned = re.sub(r"(?i)[_\- ]*q[1-4].*$", "", cleaned)

    if cleaned and not _looks_generic(cleaned):
        return _clean_location_label(cleaned)
    return "unknown"


def _relative_source_name(file_path: Path, input_root: Path) -> str:
    root = input_root if input_root.is_dir() else input_root.parent
    try:
        return file_path.relative_to(root).as_posix()
    except ValueError:
        return file_path.name


def _file_stats(clean: pd.DataFrame) -> dict:
    return {
        "unique_origins": int(clean["origin_id"].nunique()),
        "unique_destinations": int(clean["destination_id"].nunique()),
        "unique_od_pairs": int(
            clean[["location", "origin_id", "destination_id"]].drop_duplicates().shape[0]
        ),
        "mean_travel_time_seconds": float(clean["mean_tt"].mean()) if len(clean) else None,
    }


def _location_summary(clean: pd.DataFrame) -> dict[str, dict]:
    result: dict[str, dict] = {}
    if clean.empty:
        return result
    for location, group in clean.groupby("location", dropna=False, sort=True):
        label = str(location)
        result[label] = {
            "rows": int(len(group)),
            "files": int(group["source_file"].nunique()),
            **_file_stats(group),
        }
    return result


def profile_dataset(
    input_path: Path,
    *,
    location_override: str | None = None,
) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    files = discover_csv_files(input_path)
    if not files:
        raise FileNotFoundError(f"No CSV files found under: {input_path}")
    if location_override and len(files) != 1:
        raise ValueError("--location may only be used when exactly one CSV file is processed")

    file_reports: list[dict] = []
    clean_frames: list[pd.DataFrame] = []
    discovered_count = len(files)

    for file_path in files:
        source_name = _relative_source_name(file_path, input_path)
        location = location_override or infer_location(file_path, input_path)
        base_report = {
            "source_file": source_name,
            "location": location,
            "status": "processed",
        }
        try:
            raw = pd.read_csv(file_path)
        except Exception as exc:  # keep scanning the remaining Kaggle files
            file_reports.append({**base_report, "status": "read_error", "error": str(exc)})
            continue

        try:
            canonical = canonicalize_pandas(
                raw,
                location=location,
                source_file=source_name,
            )
        except ValueError as exc:
            # The Kaggle download can contain auxiliary CSVs. They are recorded,
            # not allowed to abort profiling of the actual travel-time files.
            file_reports.append(
                {
                    **base_report,
                    "status": "skipped_schema",
                    "rows_raw": int(len(raw)),
                    "columns": " | ".join(map(str, raw.columns)),
                    "error": str(exc),
                }
            )
            continue

        mask = valid_mask(canonical)
        clean = canonical.loc[mask].drop_duplicates(subset=list(DEDUP_COLUMNS)).copy()
        clean_frames.append(clean)
        file_reports.append(
            {
                **base_report,
                "rows_raw": int(len(raw)),
                "rows_valid": int(mask.sum()),
                "rows_invalid": int((~mask).sum()),
                "rows_after_dedup": int(len(clean)),
                **_file_stats(clean),
                "columns": " | ".join(map(str, raw.columns)),
                "error": None,
            }
        )

    if not clean_frames:
        raise ValueError(
            "CSV files were discovered, but none matched the configured Uber Movement travel-time schema. "
            "Check the source files or update config/settings.yaml column aliases."
        )

    clean_all = pd.concat(clean_frames, ignore_index=True)
    clean_all = clean_all.drop_duplicates(subset=list(DEDUP_COLUMNS)).reset_index(drop=True)
    file_profile = pd.DataFrame(file_reports)

    processed = file_profile[file_profile["status"] == "processed"]
    rows_raw = int(processed["rows_raw"].fillna(0).sum()) if not processed.empty else 0
    rows_valid = int(processed["rows_valid"].fillna(0).sum()) if not processed.empty else 0
    rows_invalid = int(processed["rows_invalid"].fillna(0).sum()) if not processed.empty else 0

    report = {
        "source": str(input_path),
        "files_discovered": discovered_count,
        "files_processed": int((file_profile["status"] == "processed").sum()),
        "files_skipped_schema": int((file_profile["status"] == "skipped_schema").sum()),
        "files_read_error": int((file_profile["status"] == "read_error").sum()),
        "rows_raw": rows_raw,
        "rows_valid_before_global_dedup": rows_valid,
        "rows_invalid": rows_invalid,
        "rows_after_global_dedup": int(len(clean_all)),
        "unique_locations": int(clean_all["location"].nunique()),
        "unknown_location_rows": int((clean_all["location"] == "unknown").sum()),
        **_file_stats(clean_all),
        "locations": _location_summary(clean_all),
        "canonical_columns": list(CANONICAL),
    }
    return report, file_profile, clean_all


def _make_preview(clean: pd.DataFrame, rows_per_location: int) -> pd.DataFrame:
    if clean.empty:
        return clean.copy()
    return (
        clean.sort_values(["location", "origin_id", "destination_id"], kind="stable")
        .groupby("location", dropna=False, group_keys=False)
        .head(rows_per_location)
        .reset_index(drop=True)
    )


def run(input_path: str | None = None, *, location_override: str | None = None) -> dict:
    settings = get_settings()
    source = settings.resolve_repo_path(input_path or settings.uber_movement_input)
    if not source.exists():
        raise FileNotFoundError(
            f"Missing {source}. Extract the Kaggle dataset under that directory, "
            "or pass --input to a CSV/directory."
        )

    report, file_profile, clean = profile_dataset(source, location_override=location_override)
    profiling_cfg = get_yaml_config().get("profiling", {})
    preview_rows = int(profiling_cfg.get("preview_rows_per_location", 200))
    preview = _make_preview(clean, preview_rows)

    report_path = settings.resolve_repo_path(settings.profile_report)
    file_profile_path = settings.resolve_repo_path(settings.file_profile_report)
    preview_path = settings.resolve_repo_path(settings.normalized_preview)
    for path in (report_path, file_profile_path, preview_path):
        path.parent.mkdir(parents=True, exist_ok=True)

    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    file_profile.to_csv(file_profile_path, index=False)
    preview.to_parquet(preview_path, index=False)

    print(json.dumps(report, indent=2))
    print(f"[profile] report={report_path}")
    print(f"[profile] file_profile={file_profile_path}")
    print(f"[profile] preview={preview_path}")
    return report
