"""CLI entrypoint: inspect, train, demo, run-all."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from .data import load_pems04
from .demo import run_demo
from .pipeline import train_pipeline

ROOT = Path(__file__).resolve().parents[2]


def _int_list(value: str) -> list[int]:
    try:
        result = [int(x.strip()) for x in value.split(",") if x.strip()]
    except ValueError as err:
        raise argparse.ArgumentTypeError("Use comma-separated sensor IDs (e.g. 0,100,200)") from err
    if not result:
        raise argparse.ArgumentTypeError("Specify at least one sensor")
    return result


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="PEMS04 Week 4 traffic-density live forecasting demo")
    commands = p.add_subparsers(dest="command", required=True)
    for name in ("inspect", "train", "demo", "all"):
        cmd = commands.add_parser(name)
        cmd.add_argument("--data", type=Path, default=ROOT / "data/raw/pems04.npz")
        cmd.add_argument("--distance", type=Path, default=ROOT / "data/raw/distance.csv")
        if name in ("train", "all"):
            cmd.add_argument("--artifacts", type=Path, default=ROOT / "artifacts")
            cmd.add_argument("--train-ticks", type=int, default=500,
                             help="Number of sampled train timestamps; every sensor at each timestamp")
            cmd.add_argument("--eval-ticks", type=int, default=200)
            cmd.add_argument("--seed", type=int, default=42)
        if name in ("demo", "all"):
            cmd.add_argument("--model", type=Path, default=ROOT / "artifacts/model.joblib")
            cmd.add_argument("--ticks", type=int, default=24)
            cmd.add_argument("--delay", type=float, default=0.05, help="Wall-clock seconds per replay tick (0 is fast)")
            cmd.add_argument("--start-step", type=int, default=None)
            cmd.add_argument("--inspect-sensors", type=_int_list, default=[0, 100, 200])
            cmd.add_argument("--inject-incident-at", type=int, default=-1,
                             help="Replay tick offset to start a clearly labeled SYNTHETIC disturbance")
            cmd.add_argument("--incident-duration", type=int, default=6)
            cmd.add_argument("--incident-sensors", type=_int_list, default=[0, 100, 200])
            cmd.add_argument("--log-dir", type=Path, default=ROOT / "logs")
    return p


def main(argv: list[str] | None = None) -> None:
    args = parser().parse_args(argv)
    if args.command == "inspect":
        traffic = load_pems04(args.data, args.distance)
        print(f"source=PEMS04 shape={traffic.raw.shape}, interval=5min")
        print("channels=[flow_count_5min, occupancy_fraction, speed_mph]")
        print(f"timestamps={traffic.timestamp(0)} to {traffic.timestamp(traffic.steps - 1)}")
        for ix, name in enumerate(("flow", "occupancy", "speed")):
            print(f"{name} p05/p50/p95={np.quantile(traffic.raw[:, :, ix], [.05, .5, .95])}")
        print(f"estimated_density p05/p50/p95={np.quantile(traffic.density, [.05, .5, .95])}")
        return
    if args.command in ("train", "all"):
        print("TRAIN | loading real PEMS04 and fitting chronological multi-horizon baseline...", flush=True)
        result = train_pipeline(args.data, args.distance, args.artifacts,
                                train_ticks=args.train_ticks, eval_ticks=args.eval_ticks, seed=args.seed)
        print("TRAIN | completed, metrics (vehicles/mile):", flush=True)
        print(json.dumps({key: result[key] for key in ("validation", "test")}, indent=2), flush=True)
        print(f"TRAIN | model saved: {args.artifacts / 'model.joblib'}", flush=True)
        if args.command == "train":
            return
    if args.command in ("demo", "all"):
        directory = args.log_dir
        model_path = args.artifacts / "model.joblib" if args.command == "all" else args.model
        result = run_demo(args.data, args.distance, model_path, ticks=args.ticks, delay=args.delay,
                          start_step=args.start_step, inspect_sensors=args.inspect_sensors,
                          log_path=directory / "live.log", jsonl_path=directory / "events.jsonl",
                          predictions_csv=directory / "predictions.csv",
                          inject_incident_at=args.inject_incident_at,
                          incident_duration=args.incident_duration,
                          incident_sensors=args.incident_sensors)
        print(f"\nDONE | {json.dumps(result, indent=2)}", flush=True)
        print(f"Files: {directory / 'live.log'}, {directory / 'events.jsonl'}, "
              f"{directory / 'predictions.csv'}", flush=True)


if __name__ == "__main__":
    main()
