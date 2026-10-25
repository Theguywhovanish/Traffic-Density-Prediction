"""Causal feature extraction for every sensor and selected timestamp."""

from __future__ import annotations

import numpy as np

from .data import TrafficData


LOOKBACK = 12  # 60 min; step t includes the just-arrived measurement
HORIZONS = (3, 6, 12)  # 15, 30, 60 minutes

FEATURE_NAMES = (
    "density_t", "density_t-1", "density_t-2", "density_t-3", "density_t-6", "density_t-12",
    "flow_t", "flow_t-3", "flow_t-12",
    "occupancy_t", "occupancy_t-3", "occupancy_t-12",
    "speed_t", "speed_t-3", "speed_t-12",
    "neighbors_density_t", "neighbors_density_t-3", "neighbors_density_t-12",
    "density_last_hour_mean", "density_last_hour_std",
    "occupancy_last_hour_mean", "speed_last_hour_mean",
    "time_of_day_sin", "time_of_day_cos", "day_of_week_sin", "day_of_week_cos",
)


def feature_matrix(data: TrafficData, step: int, sensor_ids: np.ndarray | None = None,
                   time_step_override: int | None = None) -> np.ndarray:
    """Features use observations at or before step, never future observations."""
    if step < LOOKBACK or step >= data.steps:
        raise IndexError(f"step={step}, require {LOOKBACK} <= step < {data.steps}")
    ids = np.arange(data.sensors) if sensor_ids is None else np.asarray(sensor_ids, dtype=np.int32)
    if len(ids) == 0 or np.any(ids < 0) or np.any(ids >= data.sensors):
        raise ValueError("sensor_ids must be non-empty and inside graph")
    raw, k = data.raw, data.density
    features: list[np.ndarray] = []
    for lag in (0, 1, 2, 3, 6, 12):
        features.append(k[step - lag, ids])
    for channel in (0, 1, 2):
        for lag in (0, 3, 12):
            features.append(raw[step - lag, ids, channel])
    for lag in (0, 3, 12):
        features.append(k[step - lag, data.neighbors[ids]].mean(axis=1))
    past_density = k[step - 11: step + 1, :][:, ids]
    features.extend((past_density.mean(axis=0), past_density.std(axis=0)))
    features.append(raw[step - 11:step + 1, :, 1][:, ids].mean(axis=0))
    features.append(raw[step - 11:step + 1, :, 2][:, ids].mean(axis=0))
    calendar_step = step if time_step_override is None else time_step_override
    slot = calendar_step % 288
    day = (calendar_step // 288) % 7  # 2018-01-01 was a Monday
    for val in (np.sin(2 * np.pi * slot / 288), np.cos(2 * np.pi * slot / 288),
                np.sin(2 * np.pi * day / 7), np.cos(2 * np.pi * day / 7)):
        features.append(np.full(len(ids), val, dtype=np.float32))
    matrix = np.column_stack(features).astype(np.float32)
    if matrix.shape[1] != len(FEATURE_NAMES) or not np.isfinite(matrix).all():
        raise ValueError("Bad feature dimensions or invalid values")
    return matrix


def supervised_sample(data: TrafficData, steps: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return X, multi-horizon y and persistence baseline for the same indices."""
    if len(steps) == 0:
        raise ValueError("No sample steps")
    x_chunks, y_chunks, baseline_chunks = [], [], []
    for t in steps:
        t = int(t)
        if t < LOOKBACK or t + max(HORIZONS) >= data.steps:
            raise ValueError(f"Sample step {t} out of bounds")
        x_chunks.append(feature_matrix(data, t))
        y_chunks.append(np.column_stack([data.density[t + h] for h in HORIZONS]))
        baseline_chunks.append(data.density[t])
    return np.vstack(x_chunks), np.vstack(y_chunks), np.concatenate(baseline_chunks)
