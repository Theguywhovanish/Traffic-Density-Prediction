"""Standalone live prediction launcher: python scripts/run_demo.py."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from traffic_density.cli import main


if __name__ == "__main__":
    main(["demo", *sys.argv[1:]])
