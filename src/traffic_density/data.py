"""Validated PEMS04 loading and sensor graph construction."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np


START_DATE = datetime(2018, 1, 1)
INTERVAL_MINUTES = 5
CHANNELS = ("flow", "occupancy", "speed")


@dataclass
class TrafficData:
    raw: np.ndarray  # time, sensor, channel: flow, occupancy, speed
    density: np.ndarray  # estimated vehicles / mile, not observed density
    neighbors: np.ndarray  # sensor, neighboring sensors (including fallback self)

    @property
    def steps(self) -> int:
        return int(self.raw.shape[0])

    @property
    def sensors(self) -> int:
        return int(self.raw.shape[1])

    @staticmethod
    def timestamp(step: int) -> str:
        return (START_DATE + timedelta(minutes=INTERVAL_MINUTES * step)).isoformat(timespec="minutes")


def estimate_density(flow: np.ndarray, speed: np.ndarray) -> np.ndarray:
    """Compute k = q/v, where flow is a five-minute count and speed is mph.

    A detector speed of zero/negative or missing is NOT silently replaced with an
    artificial floor: the density estimate is undefined and marked NaN.
    """
    result = np.full(np.broadcast_shapes(flow.shape, speed.shape), np.nan, dtype=np.float32)
    good = np.isfinite(flow) & np.isfinite(speed) & (flow >= 0) & (speed > 0)
    np.divide(12.0 * flow, speed, out=result, where=good)
    return result


def build_neighbor_index(distance_path: Path | None, num_sensors: int, k: int = 4) -> np.ndarray:
    """Return nearby graph sensors; use self only for isolated nodes.

    The benchmark distance.csv has directed from,to,cost edges. For compact
    demo features, we consider both incoming and outgoing edges as neighbors.
    This is an explicit undirected approximation, not a routing graph.
    """
    buckets: list[dict[int, float]] = [dict() for _ in range(num_sensors)]
    if distance_path is not None:
        if not distance_path.exists():
            raise FileNotFoundError(f"Distance graph not found: {distance_path}")
        with distance_path.open(newline="", encoding="utf-8") as fp:
            rows = csv.DictReader(fp)
            if not {"from", "to", "cost"}.issubset(rows.fieldnames or []):
                raise ValueError("distance.csv needs from,to,cost columns")
            for row in rows:
                a, b, cost = int(row["from"]), int(row["to"]), float(row["cost"])
                if not (0 <= a < num_sensors and 0 <= b < num_sensors and np.isfinite(cost)):
                    continue
                if a != b:
                    buckets[a][b] = min(cost, buckets[a].get(b, float("inf")))
                    buckets[b][a] = min(cost, buckets[b].get(a, float("inf")))
    neighbors = np.empty((num_sensors, k), dtype=np.int32)
    for sensor, costs in enumerate(buckets):
        nearby = [node for node, _ in sorted(costs.items(), key=lambda kv: kv[1])[:k]]
        if not nearby:
            nearby = [sensor]
        neighbors[sensor] = (nearby + [nearby[0]] * k)[:k]
    return neighbors


def load_pems04(data_path: str | Path, distance_path: str | Path | None = None) -> TrafficData:
    data_path = Path(data_path)
    if not data_path.exists():
        raise FileNotFoundError(
            f"Missing {data_path}. Download pems04.zip from "
            "TorchSpatiotemporal/ProcessedDatasets or use the bundled archive."
        )
    with np.load(data_path, allow_pickle=False) as archive:
        if "data" not in archive.files:
            raise ValueError(f"Expected NPZ key 'data', got {archive.files}")
        raw = np.asarray(archive["data"], dtype=np.float32)
    if raw.ndim != 3 or raw.shape[2] != 3:
        raise ValueError(f"Expected [time,sensor,3], got {raw.shape}")
    if raw.shape[0] < 50 or raw.shape[1] < 1:
        raise ValueError("Insufficient timesteps or sensors for demo")
    if not np.all(np.isfinite(raw)):
        raise ValueError("Input has NaN/Inf; clean the file before running this demo")
    density = estimate_density(raw[:, :, 0], raw[:, :, 2])
    if not np.all(np.isfinite(density)):
        raise ValueError("Invalid density labels due to zero/negative speed or flow")
    graph_path = Path(distance_path) if distance_path is not None else None
    neighbors = build_neighbor_index(graph_path, raw.shape[1])
    return TrafficData(raw=raw, density=density, neighbors=neighbors)
