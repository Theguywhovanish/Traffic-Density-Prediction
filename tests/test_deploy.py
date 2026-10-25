"""Actual deployed-service building blocks tested without Kafka/Postgres/Docker."""

import csv
import json
import tempfile
import unittest
from pathlib import Path

import numpy as np

from traffic_density.deploy.bridge import publish_atomic
from traffic_density.deploy.contracts import build_tick, tick_to_array
from traffic_density.deploy.forecasting import StatefulForecaster
from traffic_density.deploy.prepare import export_batch_csv
from traffic_density.deploy.producer import replay_messages
from traffic_density.data import load_pems04
from traffic_density.features import feature_matrix
from traffic_density.pipeline import train_pipeline
import joblib


class StackTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.raw = np.empty((260, 6, 3), dtype=np.float32)
        for step in range(260):
            for sensor in range(6):
                self.raw[step, sensor] = [140 + 25 * np.sin(step / 20) + sensor * 4,
                                          .05 + .01 * sensor,
                                          55 + 6 * np.cos(step / 20)]
        self.data = self.root / "fixture.npz"
        np.savez_compressed(self.data, data=self.raw)
        self.graph = self.root / "distance.csv"
        self.graph.write_text("from,to,cost\n0,1,1\n1,2,2\n2,3,3\n3,4,4\n4,5,5\n")

    def tearDown(self):
        self.temporary.cleanup()

    def test_contract_and_duplicate_detection(self):
        event = build_tick(self.raw[20], 20)
        np.testing.assert_allclose(tick_to_array(event, 6), self.raw[20])
        event["observations"][1]["sensor_id"] = 0
        with self.assertRaises(ValueError):
            tick_to_array(event, 6)

    def test_kafka_bridge_atomic_idempotence(self):
        event = build_tick(self.raw[30], 30)
        folder = self.root / "landing"
        path = publish_atomic(event, folder, expected_sensors=6)
        self.assertEqual(path, publish_atomic(event, folder, expected_sensors=6))
        self.assertEqual(len(list(folder.glob("tick*.json"))), 1)
        with path.open() as fp:
            rows = [json.loads(line) for line in fp]
        self.assertEqual(len(rows), 6)
        self.assertAlmostEqual(rows[0]["density"], 12 * rows[0]["flow"] / rows[0]["speed"], places=3)

    def test_train_only_batch_export(self):
        filename = self.root / "batch.csv"
        n = export_batch_csv(self.data, self.graph, filename, max_steps=5)
        self.assertEqual(n, 30)
        with filename.open(newline="") as fp:
            rows = list(csv.DictReader(fp))
        self.assertEqual(len(rows), 30)
        self.assertEqual({int(row["step"]) for row in rows}, set(range(5)))

    def test_producer_to_forecaster_no_future_access(self):
        model_dir = self.root / "model"
        train_pipeline(self.data, self.graph, model_dir, train_ticks=25, eval_ticks=6)
        forecaster = StatefulForecaster(model_dir / "model.joblib", self.graph, sensors=6)
        events = list(replay_messages(self.raw, 230, ticks=16))
        self.assertEqual(events[0]["step"], 218)
        forecasts = []
        for event in events:
            forecast = forecaster.accept(event["step"], tick_to_array(event, 6))
            if event["step"] < 230:
                self.assertIsNone(forecast)
            else:
                self.assertEqual(forecast.shape, (6, 3))
                forecasts.append(forecast)
        self.assertEqual(len(forecasts), 16)
        np.testing.assert_array_equal(tick_to_array(events[-1], 6), self.raw[245])
        # Independent batch/incremental implementations must predict identically
        # at the same absolute timestamp, proving the rolling-window alignment.
        offline = load_pems04(self.data, self.graph)
        saved = joblib.load(model_dir / "model.joblib")["estimator"]
        full_prediction = np.maximum(saved.predict(feature_matrix(offline, 230)), 0)
        np.testing.assert_allclose(forecasts[0], full_prediction, atol=1e-4)


if __name__ == "__main__":
    unittest.main()
