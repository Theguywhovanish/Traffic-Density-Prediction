"""Strict sensor-tick event format shared by producer, bridge and predictor."""

from __future__ import annotations

from typing import Any

import numpy as np

from traffic_density.data import TrafficData, estimate_density


def build_tick(raw: np.ndarray, step: int, scenario: str | None = None) -> dict[str, Any]:
    if raw.ndim != 2 or raw.shape[1] != 3:
        raise ValueError("Expected one tick with shape [sensor,3]")
    if not np.isfinite(raw).all() or (raw[:, 0] < 0).any() or (raw[:, 2] <= 0).any():
        raise ValueError("Invalid measurements")
    return {
        "version": 1,
        "source": "PEMS04",
        "step": int(step),
        "timestamp": TrafficData.timestamp(int(step)),
        "scenario": scenario,
        "observations": [
            {"sensor_id": int(i), "flow": float(v[0]), "occupancy": float(v[1]),
             "speed": float(v[2])}
            for i, v in enumerate(raw)
        ],
    }


def tick_to_array(event: dict, expected_sensors: int = 307) -> np.ndarray:
    if event.get("source") != "PEMS04" or event.get("version") != 1:
        raise ValueError("Unsupported event source/schema version")
    if not isinstance(event.get("step"), int) or event["step"] < 0:
        raise ValueError("Invalid timestamp step")
    if event.get("timestamp") != TrafficData.timestamp(event["step"]):
        raise ValueError("Event timestamp does not match step")
    observations = event.get("observations")
    if not isinstance(observations, list) or len(observations) != expected_sensors:
        raise ValueError(f"Expected exactly {expected_sensors} sensor readings")
    data = np.empty((expected_sensors, 3), dtype=np.float32)
    seen = set()
    for record in observations:
        sid = record["sensor_id"]
        if not isinstance(sid, int) or sid < 0 or sid >= expected_sensors or sid in seen:
            raise ValueError("Invalid or duplicated sensor ID")
        seen.add(sid)
        data[sid] = [float(record["flow"]), float(record["occupancy"]), float(record["speed"])]
    if (not np.isfinite(data).all() or (data[:, 0] < 0).any()
            or (data[:, 2] <= 0).any() or (data[:, 1] < 0).any()
            or (data[:, 1] > 1).any()):
        raise ValueError("Sensor measurement out of range")
    return data


def rows_for_spark(event: dict, data: np.ndarray) -> list[dict]:
    density = estimate_density(data[:, 0], data[:, 2])
    return [
        {"step": event["step"], "timestamp": event["timestamp"], "sensor_id": int(i),
         "flow": float(row[0]), "occupancy": float(row[1]), "speed": float(row[2]),
         "density": float(density[i]), "synthetic": bool(event.get("scenario"))}
        for i, row in enumerate(data)
    ]
