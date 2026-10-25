"""Export train-only PEMS04 history as an honest input to the Spark batch job."""

from __future__ import annotations

import csv
import os
from pathlib import Path

from traffic_density.data import load_pems04
from traffic_density.pipeline import chronological_boundaries


def export_batch_csv(data_path: str | Path, distance_path: str | Path,
                     output_path: str | Path, max_steps: int | None = None) -> int:
    traffic = load_pems04(data_path, distance_path)
    train_end, _ = chronological_boundaries(traffic.steps)
    end = min(train_end, max_steps) if max_steps is not None else train_end
    if end <= 0:
        raise ValueError("No training records to export")
    dest = Path(output_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    temporary = dest.with_suffix(dest.suffix + ".part")
    with temporary.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.writer(fp)
        writer.writerow(["step", "timestamp", "sensor_id", "flow", "occupancy", "speed",
                         "density", "day_of_week", "hour"])
        for step in range(end):
            hour = (step % 288) // 12
            weekday = (step // 288) % 7
            stamp = traffic.timestamp(step)
            for sensor in range(traffic.sensors):
                flow, occ, speed = traffic.raw[step, sensor]
                writer.writerow((step, stamp, sensor, round(float(flow), 5),
                                 round(float(occ), 6), round(float(speed), 5),
                                 round(float(traffic.density[step, sensor]), 5), weekday, hour))
    os.replace(temporary, dest)
    return end * traffic.sensors
