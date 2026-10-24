from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from urban_mobility.analysis.profiling import write_artifacts


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile and enrich the Uber Movement Week 1 dataset.")
    parser.add_argument("--input", default=None, help="CSV or directory; defaults to data/raw/uber_movement")
    parser.add_argument("--artifact-dir", default=None, help="Output directory; defaults to artifacts")
    args = parser.parse_args()

    result = write_artifacts(args.input, args.artifact_dir)
    print(json.dumps(result["profile"], indent=2))
    print("\nArtifacts:")
    for name, path in result["paths"].items():
        print(f"  {name}: {path}")


if __name__ == "__main__":
    main()
