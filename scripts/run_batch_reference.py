from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from urban_mobility.config import get_settings
from urban_mobility.lakehouse.reference import build_reference_layers, validate_gold_baseline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the pandas reference mirror of Week 2 transformations")
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", default="artifacts/week2_reference")
    args = parser.parse_args()

    settings = get_settings()
    source = settings.resolve_repo_path(args.input or settings.uber_movement_input)
    output = settings.resolve_repo_path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    layers = build_reference_layers(source)
    for name in ["bronze", "silver", "gold"]:
        layers[name].to_csv(output / f"{name}.csv", index=False)

    metrics = validate_gold_baseline(layers["gold"])
    (output / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
