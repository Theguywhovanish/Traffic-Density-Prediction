"""Chronological density-forecast training with honest persistence baselines."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from .data import TrafficData, load_pems04
from .features import FEATURE_NAMES, HORIZONS, LOOKBACK, feature_matrix, supervised_sample


def chronological_boundaries(num_steps: int) -> tuple[int, int]:
    if num_steps < 120:
        raise ValueError("At least 120 time steps required for train/validation/test")
    return int(num_steps * 0.70), int(num_steps * 0.85)


def choose_times(start: int, end: int, count: int, seed: int) -> np.ndarray:
    """Choose origins wholly within [start,end), with targets in same split."""
    valid = np.arange(max(start, LOOKBACK), end - max(HORIZONS), dtype=np.int32)
    if len(valid) == 0:
        raise ValueError(f"Empty split: start={start}, end={end}")
    if count <= 0 or len(valid) <= count:
        return valid
    return np.sort(np.random.default_rng(seed).choice(valid, size=count, replace=False))


def metric_report(truth: np.ndarray, predictions: np.ndarray, persistence: np.ndarray) -> dict:
    forecasts = np.maximum(predictions, 0.0)
    result = {}
    for ix, h in enumerate(HORIZONS):
        errors = truth[:, ix] - forecasts[:, ix]
        naive_errors = truth[:, ix] - persistence
        mae = float(np.mean(np.abs(errors)))
        naive_mae = float(np.mean(np.abs(naive_errors)))
        result[f"{h * 5}min"] = {
            "mae_veh_per_mile": round(mae, 4),
            "rmse_veh_per_mile": round(float(np.sqrt(np.mean(errors ** 2))), 4),
            "persistence_mae": round(naive_mae, 4),
            "persistence_rmse": round(float(np.sqrt(np.mean(naive_errors ** 2))), 4),
            "mae_improvement_pct": round(100 * (naive_mae - mae) / naive_mae, 2) if naive_mae else None,
        }
    return result


def train_pipeline(
    data_path: str | Path,
    graph_path: str | Path,
    artifacts_dir: str | Path,
    train_ticks: int = 500,
    eval_ticks: int = 200,
    seed: int = 42,
) -> dict:
    data = load_pems04(data_path, graph_path)
    train_end, val_end = chronological_boundaries(data.steps)
    train_origins = choose_times(LOOKBACK, train_end, train_ticks, seed)
    X_train, y_train, _ = supervised_sample(data, train_origins)
    estimator = make_pipeline(StandardScaler(), Ridge(alpha=20.0, solver="lsqr"))
    estimator.fit(X_train, y_train)

    results = {}
    for split_name, start, end in (
        ("validation", train_end, val_end),
        ("test", val_end, data.steps),
    ):
        origins = choose_times(start, end, eval_ticks, seed + (1 if split_name == "validation" else 2))
        X, y, last = supervised_sample(data, origins)
        results[split_name] = {"origin_ticks": len(origins), "sensor_examples": len(X),
                               "metrics": metric_report(y, estimator.predict(X), last)}

    # Threshold calculated only on historical training observations.
    high_density_threshold = float(np.quantile(data.density[:train_end], 0.90))
    metadata = {
        "dataset": "PEMS04", "dataset_shape": list(data.raw.shape),
        "feature_names": list(FEATURE_NAMES), "lookback_steps": LOOKBACK,
        "horizons_steps": list(HORIZONS),
        "split": {"train": [0, train_end], "validation": [train_end, val_end],
                  "test": [val_end, data.steps]},
        "train_sample_timestamps": len(train_origins),
        "train_sensor_examples": len(X_train), "seed": seed,
        "target": "estimated density (veh/mile) = 12 * flow_count_5min / speed_mph",
        "high_density_threshold_veh_per_mile": high_density_threshold,
        "model": "StandardScaler + Ridge(alpha=20, solver=lsqr), multi-output",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
    }
    artifacts = Path(artifacts_dir)
    artifacts.mkdir(parents=True, exist_ok=True)
    joblib.dump({"estimator": estimator, "metadata": metadata}, artifacts / "model.joblib")
    report = {"metadata": metadata, **results}
    (artifacts / "metrics.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def load_model(model_path: str | Path, data: TrafficData):
    model_path = Path(model_path)
    if not model_path.exists():
        raise FileNotFoundError(f"No model at {model_path}. Run `python -m traffic_density train` first.")
    content = joblib.load(model_path)  # trust only project-generated model files
    metadata = content["metadata"]
    if (metadata["feature_names"] != list(FEATURE_NAMES)
            or metadata["dataset_shape"][1] != data.sensors
            or metadata["horizons_steps"] != list(HORIZONS)):
        raise ValueError("Model metadata and data are incompatible")
    probe = feature_matrix(data, LOOKBACK)
    if probe.shape[1] != len(metadata["feature_names"]):
        raise ValueError("Feature dimension mismatch")
    return content["estimator"], metadata
