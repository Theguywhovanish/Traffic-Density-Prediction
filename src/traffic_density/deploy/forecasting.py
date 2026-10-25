"""Forecast from actual received ticks, without loading unreplayed future data."""

from __future__ import annotations

from collections import deque
from pathlib import Path

import joblib
import numpy as np

from traffic_density.data import TrafficData, build_neighbor_index, estimate_density
from traffic_density.features import FEATURE_NAMES, HORIZONS, LOOKBACK, feature_matrix


class StatefulForecaster:
    def __init__(self, model_path: str | Path, distance_path: str | Path, sensors: int = 307):
        loaded = joblib.load(model_path)  # only load the trusted, bundled model
        self.model = loaded["estimator"]
        self.metadata = loaded["metadata"]
        if (self.metadata["feature_names"] != list(FEATURE_NAMES)
                or self.metadata["dataset_shape"][1] != sensors
                or self.metadata["horizons_steps"] != list(HORIZONS)):
            raise ValueError("Pretrained model incompatible with this event stream")
        self.neighbors = build_neighbor_index(Path(distance_path), sensors)
        self.sensors = sensors
        self.history: deque[tuple[int, np.ndarray]] = deque(maxlen=LOOKBACK + 1)

    def seed(self, rows: list[tuple[int, np.ndarray]]) -> None:
        """Use prior persisted observations only when they are contiguous."""
        self.history.clear()
        for step, readings in rows:
            self.accept(step, readings)

    def accept(self, step: int, readings: np.ndarray) -> np.ndarray | None:
        if readings.shape != (self.sensors, 3):
            raise ValueError("Sensor-tick shape mismatch")
        if self.history:
            previous = self.history[-1][0]
            if step <= previous:
                return None  # duplicate/replayed tick
            if step != previous + 1:
                self.history.clear()
        self.history.append((int(step), readings.copy()))
        if len(self.history) != LOOKBACK + 1:
            return None
        window = np.stack([v for _, v in self.history], axis=0)
        density = estimate_density(window[:, :, 0], window[:, :, 2])
        sample = TrafficData(raw=window, density=density, neighbors=self.neighbors)
        X = feature_matrix(sample, LOOKBACK, time_step_override=step)
        return np.maximum(self.model.predict(X), 0.0).astype(np.float32)
