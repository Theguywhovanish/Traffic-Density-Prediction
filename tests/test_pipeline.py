"""Unit and end-to-end tests using explicitly synthetic fixtures only."""

import tempfile
import unittest
from pathlib import Path

import numpy as np

from traffic_density.data import estimate_density, load_pems04
from traffic_density.features import HORIZONS, LOOKBACK, feature_matrix
from traffic_density.pipeline import chronological_boundaries, choose_times, train_pipeline
from traffic_density.demo import run_demo


class DensityDemoTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        rng = np.random.default_rng(17)
        self.raw = np.empty((260, 6, 3), dtype=np.float32)
        step = np.arange(260, dtype=np.float32)[:, None]
        sensor = np.arange(6, dtype=np.float32)[None, :]
        self.raw[:, :, 0] = 120 + sensor * 12 + 30 * np.sin(step / 11) + rng.normal(0, 5, (260, 6))
        self.raw[:, :, 1] = np.clip(.06 + .02 * np.sin(step / 20) + sensor * .003, 0, 1)
        self.raw[:, :, 2] = 55 + 3 * np.cos(step / 17) + sensor
        self.npz = self.root / "fixture.npz"
        np.savez_compressed(self.npz, data=self.raw)
        self.distance = self.root / "distance.csv"
        self.distance.write_text("from,to,cost\n0,1,1\n1,2,2\n2,3,3\n3,4,4\n4,5,5\n")

    def tearDown(self):
        self.temp.cleanup()

    def test_density_units(self):
        self.assertAlmostEqual(float(estimate_density(np.array([120]), np.array([60]))[0]), 24.)
        self.assertTrue(np.isnan(estimate_density(np.array([10]), np.array([0]))[0]))

    def test_no_future_feature_leakage(self):
        dataset = load_pems04(self.npz, self.distance)
        old = feature_matrix(dataset, 100).copy()
        dataset.raw[101:, :, :] = 1e6
        dataset.density[101:, :] = 1e6
        np.testing.assert_array_equal(old, feature_matrix(dataset, 100))

    def test_chronological_splits_horizon_guard(self):
        a, b = chronological_boundaries(260)
        for start, end in ((12, a), (a, b), (b, 260)):
            for t in choose_times(start, end, 99999, 42):
                self.assertGreaterEqual(t, start)
                self.assertLess(t + max(HORIZONS), end)

    def test_train_then_live_replay(self):
        artifact = self.root / "artifacts"
        report = train_pipeline(self.npz, self.distance, artifact, train_ticks=35, eval_ticks=12)
        self.assertIn("15min", report["test"]["metrics"])
        outcome = run_demo(self.npz, self.distance, artifact / "model.joblib", ticks=16,
                           delay=0, start_step=230, inspect_sensors=[0, 3],
                           log_path=self.root / "log/live.log",
                           jsonl_path=self.root / "log/events.jsonl",
                           predictions_csv=self.root / "log/predictions.csv",
                           inject_incident_at=5, incident_duration=3,
                           incident_sensors=[0])
        self.assertEqual(outcome["prediction_rows"], 16 * 6 * len(HORIZONS))
        self.assertIn("15min", outcome["online_metrics"])
        self.assertIn("60min", outcome["online_metrics"])
        self.assertTrue((self.root / "log/events.jsonl").exists())


if __name__ == "__main__":
    unittest.main()
