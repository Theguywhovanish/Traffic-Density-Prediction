"""Standalone Week 4 training launcher: python scripts/train_model.py."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from traffic_density.cli import main


if __name__ == "__main__":
    main(["train", *sys.argv[1:]])
