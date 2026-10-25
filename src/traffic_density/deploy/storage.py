"""PostgreSQL serving writes. Each full tick is transactional and idempotent."""

from __future__ import annotations

import os
from typing import Any

import numpy as np

from traffic_density.data import estimate_density
from traffic_density.features import HORIZONS


def db_connection():
    import psycopg

    # Read queries do not leave an implicit outer transaction open; commit_tick
    # owns one explicit atomic transaction for observations and predictions.
    return psycopg.connect(os.getenv("DATABASE_URL", "postgresql://traffic:traffic_demo@localhost:5432/traffic"),
                           autocommit=True)


def previous_complete_ticks(conn, before_step: int, sensors: int, count: int = 12) -> list[tuple[int, np.ndarray]]:
    first = max(0, before_step - count)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT step, sensor_id, flow, occupancy, speed
            FROM observations WHERE step >= %s AND step < %s
            ORDER BY step, sensor_id
        """, (first, before_step))
        result = cur.fetchall()
    grouped: dict[int, list] = {}
    for step, sid, flow, occ, speed in result:
        grouped.setdefault(step, []).append((sid, flow, occ, speed))
    expected = list(range(before_step - count, before_step))
    if before_step < count or list(grouped) != expected:
        return []
    values = []
    for step in expected:
        if len(grouped[step]) != sensors or [row[0] for row in grouped[step]] != list(range(sensors)):
            return []
        values.append((step, np.array([record[1:] for record in grouped[step]], dtype=np.float32)))
    return values


def commit_tick(conn, event: dict[str, Any], data: np.ndarray,
                predictions: np.ndarray | None) -> None:
    """Upsert raw events and forecasts, then verify forecasts maturing now."""
    step, stamp = event["step"], event["timestamp"]
    density = estimate_density(data[:, 0], data[:, 2])
    rows = [
        (step, int(i), stamp, float(row[0]), float(row[1]), float(row[2]),
         float(density[i]), bool(event.get("scenario")))
        for i, row in enumerate(data)
    ]
    with conn.transaction():
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO observations(step,sensor_id,event_time,flow,occupancy,speed,density,synthetic)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (step,sensor_id) DO UPDATE SET
                    flow=EXCLUDED.flow, occupancy=EXCLUDED.occupancy,
                    speed=EXCLUDED.speed, density=EXCLUDED.density,
                    synthetic=EXCLUDED.synthetic
            """, rows)
            if predictions is not None:
                forecast_rows = [
                    (step, step + horizon, sensor, horizon * 5,
                     float(predictions[sensor, h_idx]), float(density[sensor]))
                    for h_idx, horizon in enumerate(HORIZONS)
                    for sensor in range(data.shape[0])
                ]
                cur.executemany("""
                    INSERT INTO forecasts(origin_step,target_step,sensor_id,horizon_min,predicted_density,origin_density)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (origin_step,sensor_id,horizon_min) DO UPDATE SET
                        predicted_density=EXCLUDED.predicted_density,
                        origin_density=EXCLUDED.origin_density
                """, forecast_rows)
            cur.execute("""
                UPDATE forecasts AS p SET
                    absolute_error = ABS(p.predicted_density - o.density)
                FROM observations AS o
                WHERE p.target_step = o.step AND p.sensor_id = o.sensor_id AND o.step = %s
            """, (step,))


def rows_as_dict(cur) -> list[dict]:
    columns = [desc.name for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]
