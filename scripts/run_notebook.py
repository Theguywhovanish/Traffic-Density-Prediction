from __future__ import annotations

import argparse
import numpy as np
from pathlib import Path

import nbformat
from nbclient import NotebookClient


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute the Week 1 exploration notebook in place.")
    parser.add_argument(
        "--notebook",
        default="notebooks/01_uber_movement_data_exploration.ipynb",
    )
    args = parser.parse_args()

    repo = Path(__file__).resolve().parents[1]
    path = repo / args.notebook
    nb = nbformat.read(path, as_version=4)
    client = NotebookClient(nb, timeout=180, kernel_name="python3", resources={"metadata": {"path": str(repo)}})
    client.execute()
    nbformat.write(nb, path)
    print(f"Executed notebook: {path}")


if __name__ == "__main__":
    main()
