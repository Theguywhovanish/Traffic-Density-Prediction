"""Replay real PEMS04 sensor-tick envelopes to Kafka with optional labeled scenarios."""

from __future__ import annotations

import json
import logging
import os
import time

from traffic_density.data import load_pems04
from traffic_density.deploy.contracts import build_tick
from traffic_density.features import LOOKBACK
from traffic_density.pipeline import chronological_boundaries

LOG = logging.getLogger("stack.producer")


def replay_messages(raw, start_step: int, ticks: int | None, scenario_tick: int = -1,
                    scenario_duration: int = 6, scenario_sensors: tuple[int, ...] = (0, 100, 200),
                    repeat_dataset: bool = False):
    """Generator also used by tests; the first 12 ticks are history warm-up."""
    if (start_step < LOOKBACK or (ticks is not None and ticks <= 0)
            or (ticks is not None and not repeat_dataset and start_step + ticks > raw.shape[0])):
        raise ValueError("Invalid replay boundaries")
    step = start_step - LOOKBACK
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


def run() -> None:
    from kafka import KafkaProducer

    root = os.getenv("DATA_PATH", "/app/data/raw/pems04.npz")
    distance = os.getenv("DISTANCE_PATH", "/app/data/raw/distance.csv")
    traffic = load_pems04(root, distance)
    _, test_start = chronological_boundaries(traffic.steps)
    start = int(os.getenv("REPLAY_START_STEP", str(test_start + LOOKBACK)))
    ticks = int(os.getenv("REPLAY_TICKS", "60"))
    repeat_dataset = os.getenv("REPLAY_LOOP", "false").strip().lower() in {"1", "true", "yes", "on"}
    delay = float(os.getenv("REPLAY_DELAY", "0.5"))
    scenario_tick = int(os.getenv("SCENARIO_TICK", "-1"))
    scenario_duration = int(os.getenv("SCENARIO_DURATION", "6"))
    scenario_ids = tuple(int(s) for s in os.getenv("SCENARIO_SENSORS", "0,100,200").split(","))
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
                                 repeat_dataset=repeat_dataset):
        producer.send(topic, key=str(event["step"]).encode(), value=event).get(timeout=30)
        sent += 1
        LOG.info("EMIT step=%s time=%s sensors=%d scenario=%s", event["step"],
                 event["timestamp"], len(event["observations"]), event.get("scenario"))
        if delay:
            time.sleep(delay)
    producer.flush(timeout=30)
    producer.close()
    if repeat_dataset:
        LOG.info("Continuous historical replay stopped after %d Kafka envelopes", sent)
    else:
        LOG.info("Replay complete: %d Kafka envelopes, including %d warm-up ticks", sent, LOOKBACK)
