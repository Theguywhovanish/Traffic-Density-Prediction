"""Live-looking historical replay, causal inference, and forecast verification."""

from __future__ import annotations

import csv
import json
import logging
import sys
import time
from collections import defaultdict
from pathlib import Path

import numpy as np

from .data import TrafficData, estimate_density, load_pems04
from .features import HORIZONS, LOOKBACK, feature_matrix
from .pipeline import load_model


def make_logger(log_path: str | Path) -> logging.Logger:
    logger = logging.getLogger("pems04.demo")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_path, mode="w", encoding="utf-8")):
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def apply_synthetic_incident(data: TrafficData, step: int, sensor_ids: list[int]):
    """Override only the newly arrived observation, never future time steps."""
    if not sensor_ids:
        return
    data.raw[step, sensor_ids, 0] *= 0.75  # reduced throughput
    data.raw[step, sensor_ids, 1] = np.minimum(1, data.raw[step, sensor_ids, 1] + 0.15)
    data.raw[step, sensor_ids, 2] *= 0.45  # speed reduction
    data.density[step, sensor_ids] = estimate_density(data.raw[step, sensor_ids, 0],
                                                      data.raw[step, sensor_ids, 2])


def run_demo(
    data_path: str | Path,
    graph_path: str | Path,
    model_path: str | Path,
    ticks: int = 24,
    delay: float = 0.05,
    start_step: int | None = None,
    inspect_sensors: list[int] | None = None,
    log_path: str | Path = "logs/live.log",
    jsonl_path: str | Path = "logs/events.jsonl",
    predictions_csv: str | Path = "logs/predictions.csv",
    inject_incident_at: int = -1,
    incident_duration: int = 6,
    incident_sensors: list[int] | None = None,
) -> dict:
    data = load_pems04(data_path, graph_path)
    estimator, metadata = load_model(model_path, data)
    logger = make_logger(log_path)
    if start_step is None:
        start_step = metadata["split"]["test"][0] + LOOKBACK
    if ticks <= 0 or delay < 0 or start_step < LOOKBACK or start_step + ticks > data.steps:
        raise ValueError("Invalid ticks, delay, or start_step; replay must fit within dataset")
    ids = [0, data.sensors // 2, data.sensors - 1] if inspect_sensors is None else inspect_sensors
    affected = ids if incident_sensors is None else incident_sensors
    for sensor in ids + affected:
        if not 0 <= sensor < data.sensors:
            raise ValueError(f"Invalid sensor ID {sensor}")

    Path(jsonl_path).parent.mkdir(parents=True, exist_ok=True)
    Path(predictions_csv).parent.mkdir(parents=True, exist_ok=True)
    pending: dict[int, list[tuple[int, np.ndarray, int]]] = defaultdict(list)
    errors: dict[int, list[float]] = defaultdict(list)
    output_count = 0
    logger.info("BEGIN | source=PEMS04 | sensors=%d | first=%s | ticks=%d | replay_delay=%.2fs",
                data.sensors, data.timestamp(start_step), ticks, delay)
    logger.info("TARGET | estimated density (veh/mile), NOT directly measured density")
    if inject_incident_at >= 0:
        logger.info("SCENARIO | synthetic incident at replay tick %d for %d ticks, sensors=%s",
                    inject_incident_at, incident_duration, affected)
    with Path(jsonl_path).open("w", encoding="utf-8") as journal, Path(predictions_csv).open(
        "w", newline="", encoding="utf-8"
    ) as fp:
        writer = csv.writer(fp)
        writer.writerow(["emitted_timestamp", "target_timestamp", "sensor_id", "horizon_min",
                         "observed_density", "forecast_density", "scenario"])
        for tick in range(ticks):
            t = start_step + tick
            scenario = inject_incident_at >= 0 and inject_incident_at <= tick < inject_incident_at + incident_duration
            if scenario:
                apply_synthetic_incident(data, t, affected)
            matured = pending.pop(t, [])
            for origin, past_pred, horizon in matured:
                absolute_errors = np.abs(data.density[t] - past_pred)
                errors[horizon].extend(absolute_errors.tolist())
                logger.info("VERIFY | origin=%s | target=%s | horizon=+%dmin | MAE=%.2f veh/mi",
                            data.timestamp(origin), data.timestamp(t), horizon * 5, absolute_errors.mean())
                journal.write(json.dumps({"event": "verification", "origin": data.timestamp(origin),
                                          "target": data.timestamp(t), "horizon_min": horizon * 5,
                                          "mae": round(float(absolute_errors.mean()), 3),
                                          "synthetic_incident_target": scenario}) + "\n")

            predictions = np.maximum(estimator.predict(feature_matrix(data, t)), 0.0)
            current = data.density[t]
            threshold = metadata["high_density_threshold_veh_per_mile"]
            logger.info("TICK %02d | %s | observed mean=%.1f veh/mi | high sensors=%d | %s",
                        tick + 1, data.timestamp(t), float(current.mean()), int((current > threshold).sum()),
                        "SYNTHETIC INCIDENT" if scenario else "historical observation")
            for h_index, horizon in enumerate(HORIZONS):
                horizon_minutes = horizon * 5
                predicted = predictions[:, h_index].copy()
                pending[t + horizon].append((t, predicted, horizon))
                logger.info("PREDICT | +%02d min | mean=%.1f veh/mi | high=%d/%d | %s",
                            horizon_minutes, predicted.mean(), int((predicted > threshold).sum()), data.sensors,
                            " ".join(f"S{i}={predicted[i]:.1f}" for i in ids))
                for sensor in range(data.sensors):
                    writer.writerow([data.timestamp(t), data.timestamp(t + horizon), sensor, horizon_minutes,
                                     f"{current[sensor]:.4f}", f"{predicted[sensor]:.4f}", int(scenario)])
                    output_count += 1
                journal.write(json.dumps({"event": "prediction_summary", "time": data.timestamp(t),
                                          "target": data.timestamp(t + horizon), "horizon_min": horizon_minutes,
                                          "mean_density": round(float(predicted.mean()), 3),
                                          "high_sensors": int((predicted > threshold).sum()),
                                          "sample_sensors": {str(i): round(float(predicted[i]), 3) for i in ids},
                                          "synthetic_incident": scenario}) + "\n")
            journal.flush()
            fp.flush()
            if delay:
                time.sleep(delay)
    metrics = {f"{h * 5}min": {"verified_sensor_predictions": len(values),
                               "online_mae": round(float(np.mean(values)), 4)}
               for h, values in sorted(errors.items()) if values}
    result = {"source": "PEMS04", "start_step": start_step, "ticks": ticks,
              "prediction_rows": output_count, "online_metrics": metrics,
              "scenario_injected": inject_incident_at >= 0}
    logger.info("END | prediction rows=%d | online scores=%s", output_count, json.dumps(metrics))
    return result
