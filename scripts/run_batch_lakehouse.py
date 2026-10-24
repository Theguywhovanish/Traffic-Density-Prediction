from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from urban_mobility.config import get_settings
from urban_mobility.lakehouse.batch_pipeline import run_batch_pipeline
from urban_mobility.lakehouse.spark_session import build_spark_session


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Week 2 Spark + Delta batch lakehouse pipeline")
    parser.add_argument("--input", default=None, help="Uber Movement CSV directory")
    parser.add_argument("--lakehouse-root", default=None, help="Lakehouse output root")
    parser.add_argument("--master", default="local[*]", help="Spark master")
    args = parser.parse_args()

    settings = get_settings()
    spark = build_spark_session(app_name=settings.spark_app_name, master=args.master)
    try:
        metrics = run_batch_pipeline(spark, args.input, args.lakehouse_root)
        print(json.dumps(metrics, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
