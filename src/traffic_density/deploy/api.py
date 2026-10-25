"""FastAPI serving layer over persisted forecasts and Spark materialized views."""

from __future__ import annotations

import json
import os
import csv
from functools import lru_cache
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from traffic_density.deploy.storage import db_connection, rows_as_dict

app = FastAPI(title="PEMS04 Lambda Traffic Density", version="1.0.0")
SHARED = Path(os.getenv("SHARED_DIR", "/shared"))
DASHBOARD = Path(os.getenv("DASHBOARD_PATH", "/app/stack/dashboard.html"))
DISTANCE = Path(os.getenv("DISTANCE_PATH", "/app/data/raw/distance.csv"))
REPLAY_STATUS = SHARED / "replay_status.json"
REPLAY_CONTROL = SHARED / "replay_control.json"


class ReplayControlUpdate(BaseModel):
    delay_seconds: float = Field(ge=0.5, le=10.0)


def read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def latest_speed_snapshot() -> dict | None:
    files = sorted((SHARED / "speed_output").glob("batch_*.json"))
    if not files:
        return None
    content = json.loads(files[-1].read_text(encoding="utf-8"))
    rows = content.get("rows", [])
    return max(rows, key=lambda item: item["step"]) if rows else None


@lru_cache(maxsize=1)
def sensor_edges() -> list[dict]:
    """Load the benchmark's graph edges; these are not geographic coordinates."""
    if not DISTANCE.exists():
        raise HTTPException(status_code=503, detail="Sensor graph file is unavailable")
    with DISTANCE.open(newline="", encoding="utf-8") as fp:
        return [
            {"from": int(row["from"]), "to": int(row["to"]), "cost": float(row["cost"])}
            for row in csv.DictReader(fp)
            if 0 <= int(row["from"]) < 307 and 0 <= int(row["to"]) < 307
        ]


def load_batch_baseline(sensor: int, weekday: int, hour: int) -> dict | None:
    directory = SHARED / "batch_output/seasonal"
    for path in directory.glob("part-*.json"):
        with path.open(encoding="utf-8") as fp:
            for line in fp:
                row = json.loads(line)
                if row["sensor_id"] == sensor and row["day_of_week"] == weekday and row["hour"] == hour:
                    return row
    return None


@app.get("/")
def home():
    return FileResponse(DASHBOARD)


@app.get("/health")
def health():
    try:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"status": "ok", "postgres": True}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Postgres unavailable: {type(exc).__name__}")


@app.get("/api/status")
def status():
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), MAX(step), MAX(event_time), MAX(received_at) FROM observations")
            obs_count, latest_step, latest_event_time, last_received_at = cur.fetchone()
            cur.execute("SELECT COUNT(*), COUNT(absolute_error) FROM forecasts")
            total, verified = cur.fetchone()
    return {
        "dataset": "PEMS04", "target": "estimated_density_veh_per_mile",
        "sensor_observations": obs_count, "last_step": latest_step,
        "last_event_time": latest_event_time, "last_received_at": last_received_at,
        "forecasts": total, "verified_forecasts": verified,
        "batch_ready": (SHARED / "batch_output/seasonal/_SUCCESS").exists(),
        "spark_speed_latest": latest_speed_snapshot(),
        "replay_status": read_json(REPLAY_STATUS),
    }


@app.get("/api/replay/control")
def get_replay_control():
    state = read_json(REPLAY_STATUS)
    control = read_json(REPLAY_CONTROL)
    delay = control.get("delay_seconds", state.get("delay_seconds", os.getenv("REPLAY_DELAY", "2")))
    return {"delay_seconds": float(delay), "loop_enabled": state.get("loop_enabled", True),
            "min_delay_seconds": 0.5,
            "max_delay_seconds": 10.0}


@app.put("/api/replay/control")
def set_replay_control(update: ReplayControlUpdate):
    SHARED.mkdir(parents=True, exist_ok=True)
    temporary = REPLAY_CONTROL.with_suffix(f".{os.getpid()}.tmp")
    try:
        temporary.write_text(json.dumps({"delay_seconds": update.delay_seconds}), encoding="utf-8")
        os.replace(temporary, REPLAY_CONTROL)
    except OSError as exc:
        temporary.unlink(missing_ok=True)
        raise HTTPException(status_code=503, detail="Replay control storage is unavailable") from exc
    return {"delay_seconds": update.delay_seconds, "applied": True}


@app.get("/api/network")
def network():
    """Latest detector readings plus the available sensor graph topology."""
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT step, event_time FROM observations ORDER BY step DESC LIMIT 1")
            latest = cur.fetchone()
            if latest is None:
                return {"step": None, "event_time": None, "nodes": [], "edges": sensor_edges()}
            step, event_time = latest
            cur.execute("""
                SELECT sensor_id, flow, occupancy, speed, density, synthetic
                FROM observations WHERE step=%s ORDER BY sensor_id
            """, (step,))
            nodes = rows_as_dict(cur)
    return {"step": step, "event_time": event_time, "nodes": nodes,
            "edges": sensor_edges()}


@app.get("/api/metrics")
def metrics():
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT horizon_min, COUNT(*) AS n,
                       ROUND(AVG(absolute_error)::numeric, 4) AS mae
                FROM forecasts WHERE absolute_error IS NOT NULL
                GROUP BY horizon_min ORDER BY horizon_min
            """)
            return {"online_metrics": rows_as_dict(cur)}


@app.get("/api/forecasts/{sensor_id}")
def forecasts(sensor_id: int, limit: int = Query(12, ge=1, le=300)):
    if not 0 <= sensor_id < 307:
        raise HTTPException(400, "Sensor ID must be 0..306")
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT origin_step,target_step,sensor_id,horizon_min,
                       predicted_density,origin_density,absolute_error
                FROM forecasts WHERE sensor_id=%s
                ORDER BY origin_step DESC,horizon_min LIMIT %s
            """, (sensor_id, limit))
            return {"sensor_id": sensor_id, "forecasts": rows_as_dict(cur)}


@app.get("/api/observations/{sensor_id}")
def observations(sensor_id: int, limit: int = Query(48, ge=1, le=500)):
    if not 0 <= sensor_id < 307:
        raise HTTPException(400, "Sensor ID must be 0..306")
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT step,event_time,flow,occupancy,speed,density,synthetic
                FROM observations WHERE sensor_id=%s ORDER BY step DESC LIMIT %s
            """, (sensor_id, limit))
            return {"sensor_id": sensor_id, "observations": rows_as_dict(cur)}


@app.get("/api/reconciliation/{sensor_id}")
def reconciliation(sensor_id: int):
    if not 0 <= sensor_id < 307:
        raise HTTPException(400, "Sensor ID must be 0..306")
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT step,event_time,density FROM observations
                WHERE sensor_id=%s ORDER BY step DESC LIMIT 1
            """, (sensor_id,))
            result = cur.fetchone()
    if result is None:
        return {"sensor_id": sensor_id, "status": "waiting_for_stream"}
    step, timestamp, observed = result
    baseline = load_batch_baseline(sensor_id, (step // 288) % 7, (step % 288) // 12)
    if baseline is None:
        return {"sensor_id": sensor_id, "step": step, "event_time": timestamp,
                "observed_density": observed, "status": "waiting_for_batch"}
    historical = float(baseline["historical_mean_density"])
    return {"sensor_id": sensor_id, "step": step, "event_time": timestamp,
            "observed_density": observed, "historical_mean_density": historical,
            "density_delta": round(observed - historical, 4),
            "history_count": baseline["sample_count"], "status": "ready"}
