from __future__ import annotations

import compileall
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(cmd: list[str]) -> None:
    print("\n$", " ".join(cmd))
    subprocess.run(cmd, cwd=ROOT, check=True)


def main() -> None:
    print("Validating Week 1 repository:", ROOT)
    if not compileall.compile_dir(ROOT / "src", quiet=1):
        raise SystemExit("Python compilation failed")
    if not compileall.compile_dir(ROOT / "scripts", quiet=1):
        raise SystemExit("Script compilation failed")
    print("Python compilation: PASS")

    run([sys.executable, "scripts/profile_historical_data.py"])
    run([sys.executable, "-m", "pytest"])
    run([sys.executable, "scripts/run_notebook.py"])
    print("\nWeek 1 validation: PASS")


if __name__ == "__main__":
    main()
