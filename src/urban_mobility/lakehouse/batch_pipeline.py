from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from urban_mobility.config import get_settings, get_yaml_config
from urban_mobility.data.loader import discover_csv_files, infer_location
from urban_mobility.lakehouse.contracts import GOLD_COLUMNS, ROUTE_KEY, SILVER_COLUMNS


def _snake(name: str) -> str:
    value = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
    return re.sub(r"_+", "_", value)


def _read_raw_files(spark: Any, input_path: Path) -> Any:
    """Read heterogeneous Uber Movement CSVs and union them by name."""
    from pyspark.sql import functions as F

    files = discover_csv_files(input_path)
    if not files:
        raise FileNotFoundError(f"No CSV files found under {input_path}")

    frames = []
    for file_path in files:
        location = infer_location(file_path, input_path)
        source_file = file_path.relative_to(input_path).as_posix() if input_path.is_dir() else file_path.name
        frame = spark.read.option("header", True).option("multiLine", False).csv(str(file_path))
        for old in frame.columns:
            frame = frame.withColumnRenamed(old, _snake(old))
        frame = frame.withColumn("location", F.lit(location)).withColumn("source_file", F.lit(source_file))
        frames.append(frame)

    unioned = frames[0]
    for frame in frames[1:]:
        unioned = unioned.unionByName(frame, allowMissingColumns=True)
    return unioned


def _get_column(df: Any, canonical_key: str) -> str | None:
    aliases = get_yaml_config()["historical_columns"][canonical_key]
    candidates = {_snake(c): c for c in aliases}
    for name in df.columns:
        if name in candidates:
            return name
    return None


def build_bronze(spark: Any, input_path: Path) -> Any:
    from pyspark.sql import functions as F

    raw = _read_raw_files(spark, input_path)
    aliases = {
        key: _get_column(raw, key)
        for key in [
            "origin_id", "origin_name", "origin_geometry", "destination_id",
            "destination_name", "destination_geometry", "date_range",
            "mean_tt", "lower_tt", "upper_tt",
        ]
    }
    required = ["origin_id", "destination_id", "mean_tt", "lower_tt", "upper_tt"]
    missing = [key for key in required if aliases[key] is None]
    if missing:
        raise ValueError(f"Missing required Uber Movement columns after union: {missing}")

    def col_or_null(key: str, dtype: str = "string") -> Any:
        return F.col(aliases[key]).cast(dtype) if aliases[key] else F.lit(None).cast(dtype)

    bronze = raw.select(
        F.col("location").cast("string").alias("location"),
        F.col("source_file").cast("string").alias("source_file"),
        col_or_null("origin_id").alias("origin_id"),
        col_or_null("origin_name").alias("origin_name"),
        col_or_null("origin_geometry").alias("origin_geometry"),
        col_or_null("destination_id").alias("destination_id"),
        col_or_null("destination_name").alias("destination_name"),
        col_or_null("destination_geometry").alias("destination_geometry"),
        col_or_null("date_range").alias("date_range"),
        col_or_null("mean_tt", "double").alias("mean_tt"),
        col_or_null("lower_tt", "double").alias("lower_tt"),
        col_or_null("upper_tt", "double").alias("upper_tt"),
        F.current_timestamp().alias("ingested_at"),
    )
    return bronze


def build_silver(bronze: Any) -> Any:
    from pyspark.sql import functions as F

    start_text = F.regexp_extract(F.col("date_range"), r"(\d{2}/\d{2}/\d{4})", 1)
    end_text = F.regexp_extract(F.col("date_range"), r"\d{2}/\d{2}/\d{4}\s*-\s*(\d{2}/\d{2}/\d{4})", 1)
    temporal_scope = F.trim(F.regexp_extract(F.col("date_range"), r",\s*(.*)$", 1))

    valid = (
        F.col("location").isNotNull()
        & F.col("origin_id").isNotNull()
        & F.col("destination_id").isNotNull()
        & (F.length(F.trim(F.col("origin_id"))) > 0)
        & (F.length(F.trim(F.col("destination_id"))) > 0)
        & (F.col("lower_tt") > 0)
        & (F.col("mean_tt") > 0)
        & (F.col("upper_tt") > 0)
        & (F.col("lower_tt") <= F.col("mean_tt"))
        & (F.col("mean_tt") <= F.col("upper_tt"))
    )

    silver = (
        bronze.filter(valid)
        .withColumn("origin_id", F.regexp_replace(F.trim(F.col("origin_id")), r"^(\d+)\.0$", "$1"))
        .withColumn("destination_id", F.regexp_replace(F.trim(F.col("destination_id")), r"^(\d+)\.0$", "$1"))
        .withColumn("start_date", F.to_date(start_text, "MM/dd/yyyy"))
        .withColumn("end_date", F.to_date(end_text, "MM/dd/yyyy"))
        .withColumn("temporal_scope", temporal_scope)
        .withColumn("lower_ratio", F.col("lower_tt") / F.col("mean_tt"))
        .withColumn("upper_ratio", F.col("upper_tt") / F.col("mean_tt"))
        .withColumn("route_log_dispersion", F.lit(0.5) * F.log(F.col("upper_tt") / F.col("lower_tt")))
        .withColumn("geometric_midpoint_tt", F.sqrt(F.col("lower_tt") * F.col("upper_tt")))
        .withColumn(
            "log_symmetry_relative_error",
            F.abs(F.col("geometric_midpoint_tt") - F.col("mean_tt")) / F.col("mean_tt"),
        )
        .withColumn(
            "has_geometry",
            F.col("origin_geometry").isNotNull() & F.col("destination_geometry").isNotNull(),
        )
        .dropDuplicates(ROUTE_KEY)
        .select(*SILVER_COLUMNS)
    )
    return silver


def build_gold(silver: Any) -> Any:
    from pyspark.sql import functions as F

    gold = (
        silver.withColumn(
            "baseline_id",
            F.sha2(F.concat_ws("||", *[F.col(c) for c in ROUTE_KEY]), 256),
        )
        .select(*GOLD_COLUMNS)
    )
    return gold


def _write_delta(df: Any, path: str, partition_by: list[str] | None = None) -> None:
    """Replace the current Delta table snapshot at ``path``."""
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def run_batch_pipeline(
    spark: Any,
    input_path: str | Path | None = None,
    lakehouse_root: str | Path | None = None,
) -> dict[str, object]:
    settings = get_settings()
    input_resolved = settings.resolve_repo_path(str(input_path or settings.uber_movement_input))
    root = settings.resolve_repo_path(str(lakehouse_root or settings.lakehouse_root))
    root.mkdir(parents=True, exist_ok=True)

    bronze = build_bronze(spark, input_resolved)
    silver = build_silver(bronze)
    gold = build_gold(silver)

    paths = {
        "bronze": str(root / "bronze" / "uber_movement_raw"),
        "silver": str(root / "silver" / "uber_routes"),
        "gold": str(root / "gold" / "od_baseline"),
    }

    _write_delta(bronze, paths["bronze"], ["location"])
    _write_delta(silver, paths["silver"], ["location"])
    _write_delta(gold, paths["gold"], ["location"])

    metrics = {
        "bronze_rows": int(bronze.count()),
        "silver_rows": int(silver.count()),
        "gold_rows": int(gold.count()),
        "locations": int(gold.select("location").distinct().count()),
        "unique_route_keys": int(gold.select(*ROUTE_KEY).distinct().count()),
        "paths": paths,
    }
    return metrics
