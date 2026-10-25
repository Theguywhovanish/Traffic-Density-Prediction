"""Replay real PEMS04 sensor-tick envelopes to Kafka with optional labeled scenarios."""

from __future__ import annotations

import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from traffic_density.data import load_pems04
from traffic_density.deploy.contracts import build_tick
from traffic_density.features import LOOKBACK
from traffic_density.pipeline import chronological_boundaries

LOG = logging.getLogger("stack.producer")


def replay_messages(raw, start_step: int, ticks: int | None, scenario_tick: int = -1,
                    scenario_duration: int = 6, scenario_sensors: tuple[int, ...] = (0, 100, 200),
                    repeat_dataset: bool = False, include_warmup: bool = True):
    """Generator also used by tests; the first 12 ticks are history warm-up."""
    if (start_step < LOOKBACK or (ticks is not None and ticks <= 0)
            or (ticks is not None and not repeat_dataset and start_step + ticks > raw.shape[0])):
        raise ValueError("Invalid replay boundaries")
    step = start_step - LOOKBACK if include_warmup else start_step
    stop_step = None if ticks is None else start_step + ticks
    while stop_step is None or step < stop_step:
        source_step = step % raw.shape[0] if repeat_dataset else step
        reading = raw[source_step].copy()
        scenario = None
        if scenario_tick >= 0 and start_step + scenario_tick <= step < start_step + scenario_tick + scenario_duration:
            for sensor in scenario_sensors:
                if not 0 <= sensor < raw.shape[1]:
                    raise ValueError(f"Invalid scenario sensor ID {sensor}")
                reading[sensor, 0] *= 0.75
                reading[sensor, 1] = min(1., reading[sensor, 1] + .15)
                reading[sensor, 2] *= 0.45
            scenario = "SYNTHETIC_INCIDENT"
        yield build_tick(reading, step, scenario=scenario)
        step += 1


def latest_persisted_step() -> int | None:
    """Resume from committed data so container restarts don't rewind playback."""
    from traffic_density.deploy.storage import db_connection

    for attempt in range(40):
        try:
            with db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT MAX(step) FROM observations")
                    result = cur.fetchone()[0]
            return int(result) if result is not None else None
        except Exception as exc:
            LOG.warning("PostgreSQL not ready for replay resume (%d): %s", attempt + 1, exc)
            time.sleep(3)
    raise RuntimeError("PostgreSQL unavailable; cannot resume replay safely")


def read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def write_json_atomic(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value), encoding="utf-8")
    os.replace(temporary, path)


def current_delay(control_path: Path, default: float) -> float:
    saved = read_json(control_path).get("delay_seconds", default)
    try:
        delay = float(saved)
    except (TypeError, ValueError):
        return default
    return max(0.0, delay) if math.isfinite(delay) else default


def run() -> None:
    from kafka import KafkaProducer

    root = os.getenv("DATA_PATH", "/app/data/raw/pems04.npz")
    distance = os.getenv("DISTANCE_PATH", "/app/data/raw/distance.csv")
    traffic = load_pems04(root, distance)
    _, test_start = chronological_boundaries(traffic.steps)
    latest_step = latest_persisted_step()
    configured_start = os.getenv("REPLAY_START_STEP", "auto").strip().lower()
    if configured_start == "auto":
        start = latest_step + 1 if latest_step is not None else test_start + LOOKBACK
    else:
        start = int(configured_start)
    include_warmup = latest_step is None
    ticks = int(os.getenv("REPLAY_TICKS", "60"))
    repeat_dataset = os.getenv("REPLAY_LOOP", "true").strip().lower() in {"1", "true", "yes", "on"}
    default_delay = float(os.getenv("REPLAY_DELAY", "2"))
    shared = Path(os.getenv("SHARED_DIR", "/shared"))
    control_path = Path(os.getenv("REPLAY_CONTROL_PATH", str(shared / "replay_control.json")))
    status_path = Path(os.getenv("REPLAY_STATUS_PATH", str(shared / "replay_status.json")))
    scenario_tick = int(os.getenv("SCENARIO_TICK", "-1"))
    scenario_duration = int(os.getenv("SCENARIO_DURATION", "6"))
    scenario_ids = tuple(int(s) for s in os.getenv("SCENARIO_SENSORS", "0,100,200").split(","))
    old_status = read_json(status_path)
    loop_count = int(old_status.get("loop_count", 0))
    loop_started_at = old_status.get("loop_started_at")
    loop_started_step = old_status.get("loop_started_step")
    if loop_started_step is not None:
        loop_started_step = int(loop_started_step)
    previous_source_step = latest_step % traffic.steps if latest_step is not None else None

    if not repeat_dataset and start >= traffic.steps:
        old_status["running"] = False
        old_status["updated_at"] = datetime.now(timezone.utc).isoformat()
        write_json_atomic(status_path, old_status)
        LOG.info("Replay already reached the end of PEMS04 at step %s", latest_step)
        return
    if not repeat_dataset:
        ticks = min(ticks, traffic.steps - start)

    state = {
        "running": True, "loop_enabled": repeat_dataset, "loop_count": loop_count,
        "loop_started_at": loop_started_at, "loop_started_step": loop_started_step,
        "current_step": latest_step,
        "source_step": previous_source_step,
        "delay_seconds": current_delay(control_path, default_delay),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    write_json_atomic(status_path, state)
    LOG.info("REPLAY RESUME last_persisted_step=%s start_step=%d warmup=%s loop=%s delay=%.2fs",
             latest_step, start, include_warmup, repeat_dataset, state["delay_seconds"])
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")
    topic = os.getenv("KAFKA_TOPIC", "pems04.sensor_ticks")
    producer = None
    for attempt in range(40):
        try:
            producer = KafkaProducer(bootstrap_servers=brokers, acks="all", retries=5,
                                     value_serializer=lambda value: json.dumps(value, allow_nan=False).encode(),
                                     request_timeout_ms=30000)
            break
        except Exception as exc:
            LOG.warning("Kafka not ready (attempt %d): %s", attempt + 1, exc)
            time.sleep(3)
    if producer is None:
        raise RuntimeError("Kafka still unavailable after startup retries")

    sent = 0
    for event in replay_messages(traffic.raw, start, None if repeat_dataset else ticks,
                                 scenario_tick, scenario_duration, scenario_ids,
                                 repeat_dataset=repeat_dataset,
                                 include_warmup=include_warmup):
        source_step = event["step"] % traffic.steps
        if (repeat_dataset and previous_source_step is not None and source_step < previous_source_step
                and event["step"] != loop_started_step):
            loop_count += 1
            loop_started_step = event["step"]
            loop_started_at = datetime.now(timezone.utc).isoformat()
            LOG.info("REPLAY LOOP START loop=%d step=%d source_step=%d",
                     loop_count, event["step"], source_step)
        producer.send(topic, key=str(event["step"]).encode(), value=event).get(timeout=30)
        sent += 1
        LOG.info("EMIT step=%s time=%s sensors=%d scenario=%s", event["step"],
                 event["timestamp"], len(event["observations"]), event.get("scenario"))
        delay = current_delay(control_path, default_delay)
        state = {
            "running": True, "loop_enabled": repeat_dataset, "loop_count": loop_count,
            "loop_started_at": loop_started_at, "loop_started_step": loop_started_step,
            "current_step": event["step"],
            "source_step": source_step, "delay_seconds": delay,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        write_json_atomic(status_path, state)
        previous_source_step = source_step
        if delay:
            time.sleep(delay)
    producer.flush(timeout=30)
    producer.close()
    state["running"] = False
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    write_json_atomic(status_path, state)
    if repeat_dataset:
        LOG.info("Continuous historical replay stopped after %d Kafka envelopes", sent)
    else:
        LOG.info("Replay complete: %d Kafka envelopes, including %d warm-up ticks", sent, LOOKBACK)
