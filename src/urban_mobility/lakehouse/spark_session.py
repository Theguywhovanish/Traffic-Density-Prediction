from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def _configure_windows_hadoop() -> None:
    """Use the repo-local Windows Hadoop helpers when HADOOP_HOME is unavailable."""
    if os.name != "nt":
        return

    repo_root = Path(__file__).resolve().parents[3]
    local_home = repo_root / ".tools" / "hadoop"
    configured_home = os.environ.get("HADOOP_HOME")
    configured_path = Path(configured_home).expanduser() if configured_home else None

    if configured_path and (configured_path / "bin" / "winutils.exe").is_file():
        hadoop_home = configured_path
    elif (local_home / "bin" / "winutils.exe").is_file():
        hadoop_home = local_home
    else:
        configured_note = (
            f" HADOOP_HOME currently points to {configured_path}, which has no bin/winutils.exe."
            if configured_path
            else ""
        )
        raise RuntimeError(
            "Spark on Windows requires Hadoop's winutils.exe."
            f"{configured_note} Run `powershell -ExecutionPolicy Bypass -File .\\scripts\\setup_hadoop.ps1` "
            "from the repository root after installing requirements-spark.txt."
        )

    hadoop_home = hadoop_home.resolve()
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    bin_dir = str(hadoop_home / "bin")
    path_entries = os.environ.get("PATH", "").split(os.pathsep)
    normalized_entries = {
        os.path.normcase(os.path.abspath(entry.strip('"')))
        for entry in path_entries
        if entry.strip()
    }
    if os.path.normcase(os.path.abspath(bin_dir)) not in normalized_entries:
        os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")


def build_spark_session(app_name: str = "urban-mobility-week2", master: str = "local[*]") -> Any:
    """Create a SparkSession configured for Delta Lake.

    Imports are lazy so Week 1 utilities/tests still work without Spark installed.
    Install requirements-spark.txt before running the production batch job.
    """
    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise RuntimeError(
            "Spark/Delta dependencies are missing. Install them with "
            "`pip install -r requirements-spark.txt`."
        ) from exc

    _configure_windows_hadoop()

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
