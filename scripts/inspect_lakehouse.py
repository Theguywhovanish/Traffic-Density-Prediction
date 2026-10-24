from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from urban_mobility.config import get_settings
from urban_mobility.lakehouse.spark_session import build_spark_session


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lakehouse-root", default=None)
    args = parser.parse_args()
    settings = get_settings()
    root = settings.resolve_repo_path(args.lakehouse_root or settings.lakehouse_root)

    spark = build_spark_session(app_name="week2-inspect")
    try:
        for layer, table in [
            ("bronze", "uber_movement_raw"),
            ("silver", "uber_routes"),
            ("gold", "od_baseline"),
        ]:
            path = str(root / layer / table)
            df = spark.read.format("delta").load(path)
            print(f"\n[{layer.upper()}] {path}")
            print(f"rows={df.count()} columns={len(df.columns)}")
            df.groupBy("location").count().orderBy("location").show(50, truncate=False)
        gold_path = str(root / "gold" / "od_baseline")
        spark.read.format("delta").load(gold_path).orderBy("location", "origin_id", "destination_id").show(20, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
