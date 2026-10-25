"""Durable Kafka -> atomic JSON tick files for Spark's file streaming source.

This avoids a network-time Maven Kafka connector dependency in the Spark image.
It is a real Kafka consumer feeding real Spark Structured Streaming via files.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

from traffic_density.deploy.contracts import rows_for_spark, tick_to_array

LOG = logging.getLogger("stack.bridge")


def publish_atomic(event: dict, output_dir: str | Path, expected_sensors: int = 307) -> Path:
    values = tick_to_array(event, expected_sensors)
    folder = Path(output_dir)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"tick_{event['step']:06d}.json"
    if path.exists():
        return path  # idempotent on Kafka re-delivery
    staging = folder / f".tick_{event['step']:06d}.{os.getpid()}.tmp"
    try:
        with staging.open("w", encoding="utf-8") as fp:
            for row in rows_for_spark(event, values):
                fp.write(json.dumps(row, allow_nan=False) + "\n")
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(staging, path)
    finally:
        if staging.exists():
            staging.unlink()
    return path


def run() -> None:
    from kafka import KafkaConsumer

    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")
    topic = os.getenv("KAFKA_TOPIC", "pems04.sensor_ticks")
    folder = os.getenv("SPARK_LANDING_DIR", "/shared/landing")
    consumer = None
    for attempt in range(40):
        try:
            consumer = KafkaConsumer(topic, bootstrap_servers=servers,
                                     group_id="pems04-spark-bridge-v1", enable_auto_commit=False,
                                     auto_offset_reset="earliest", value_deserializer=json.loads)
            break
        except Exception as exc:
            LOG.warning("Kafka bridge waiting (%d): %s", attempt + 1, exc)
            time.sleep(3)
    if consumer is None:
        raise RuntimeError("Kafka not available")
    for message in consumer:
        path = publish_atomic(message.value, folder)
        consumer.commit()
        LOG.info("KAFKA -> SPARK step=%s file=%s", message.value["step"], path.name)
