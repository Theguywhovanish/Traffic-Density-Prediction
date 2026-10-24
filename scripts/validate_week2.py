from __future__ import annotations

import importlib.util
import json
import py_compile
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from urban_mobility.config import get_settings
from urban_mobility.lakehouse.reference import build_reference_layers, validate_gold_baseline


def compile_python() -> None:
    for path in list((ROOT / "src").rglob("*.py")) + list((ROOT / "scripts").glob("*.py")):
        py_compile.compile(str(path), doraise=True)


def run_tests() -> None:
    subprocess.run([sys.executable, "-m", "pytest", "-q"], cwd=ROOT, check=True)


def main() -> None:
    compile_python()
    settings = get_settings()
    layers = build_reference_layers(settings.resolve_repo_path(settings.uber_movement_input))
    metrics = validate_gold_baseline(layers["gold"])
    assert metrics["rows"] == 5547
    assert metrics["unique_locations"] == 7
    assert metrics["all_route_keys_unique"] is True
    run_tests()

    spark_available = importlib.util.find_spec("pyspark") is not None and importlib.util.find_spec("delta") is not None
    report = {
        "python_compilation": "PASS",
        "reference_batch_logic": "PASS",
        "reference_metrics": metrics,
        "tests": "PASS",
        "spark_delta_dependencies_available": spark_available,
        "production_spark_job_executed": False,
    }
    if spark_available:
        from urban_mobility.lakehouse.batch_pipeline import run_batch_pipeline
        from urban_mobility.lakehouse.spark_session import build_spark_session
        spark = build_spark_session(app_name="week2-validation", master="local[2]")
        try:
            spark_metrics = run_batch_pipeline(
                spark,
                lakehouse_root="artifacts/week2_spark_validation_lakehouse",
            )
            assert spark_metrics["gold_rows"] == 5547
            assert spark_metrics["locations"] == 7
            report["production_spark_job_executed"] = True
            report["spark_metrics"] = spark_metrics
        finally:
            spark.stop()

    path = ROOT / "artifacts" / "week2_validation.json"
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
