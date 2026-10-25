"""Kafka consumer for stateful, causally valid multi-horizon predictions."""

from __future__ import annotations

import json
import logging
import os
import time

from traffic_density.deploy.contracts import tick_to_array
from traffic_density.deploy.forecasting import StatefulForecaster
from traffic_density.deploy.storage import commit_tick, db_connection, previous_complete_ticks

LOG = logging.getLogger("stack.predictor")


def run() -> None:
    from kafka import KafkaConsumer

    forecaster = StatefulForecaster(
        os.getenv("MODEL_PATH", "/app/artifacts/model.joblib"),
        os.getenv("DISTANCE_PATH", "/app/data/raw/distance.csv"),
    )
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")
    topic = os.getenv("KAFKA_TOPIC", "pems04.sensor_ticks")
    conn = None
    for attempt in range(40):
        try:
            conn = db_connection()
            break
        except Exception as exc:
            LOG.warning("PostgreSQL not ready (%d): %s", attempt + 1, exc)
            time.sleep(3)
    if conn is None:
        raise RuntimeError("PostgreSQL unavailable")

    consumer = None
    for attempt in range(40):
        try:
            consumer = KafkaConsumer(topic, bootstrap_servers=servers,
                                     group_id="pems04-predictor-v1", enable_auto_commit=False,
                                     auto_offset_reset="earliest", value_deserializer=json.loads)
            break
        except Exception as exc:
            LOG.warning("Kafka predictor waiting (%d): %s", attempt + 1, exc)
            time.sleep(3)
    if consumer is None:
        raise RuntimeError("Kafka unavailable")
    for message in consumer:
        event = message.value
        observations = tick_to_array(event, forecaster.sensors)
        step = event["step"]
        if not forecaster.history or step != forecaster.history[-1][0] + 1:
            forecaster.seed(previous_complete_ticks(conn, step, forecaster.sensors))
        forecast = forecaster.accept(step, observations)
        try:
            commit_tick(conn, event, observations, forecast)
        except Exception:
            LOG.exception("DB write failed for step=%d; offset not committed", step)
            raise
        consumer.commit()
        if forecast is None:
            LOG.info("WARMUP step=%d history=%d/13", step, len(forecaster.history))
        else:
            LOG.info("PREDICT step=%d +15=%.2f +30=%.2f +60=%.2f veh/mi (307 sensors)",
                     step, *forecast.mean(axis=0))
