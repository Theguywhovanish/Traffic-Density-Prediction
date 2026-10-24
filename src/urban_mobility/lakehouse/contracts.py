from __future__ import annotations

BRONZE_TABLE = "uber_movement_raw"
SILVER_TABLE = "uber_routes"
GOLD_TABLE = "od_baseline"

ROUTE_KEY = ["location", "origin_id", "destination_id"]

BRONZE_COLUMNS = [
    "location",
    "source_file",
    "origin_id",
    "origin_name",
    "origin_geometry",
    "destination_id",
    "destination_name",
    "destination_geometry",
    "date_range",
    "mean_tt",
    "lower_tt",
    "upper_tt",
    "ingested_at",
]

SILVER_COLUMNS = [
    "location",
    "source_file",
    "origin_id",
    "origin_name",
    "origin_geometry",
    "destination_id",
    "destination_name",
    "destination_geometry",
    "date_range",
    "start_date",
    "end_date",
    "temporal_scope",
    "mean_tt",
    "lower_tt",
    "upper_tt",
    "lower_ratio",
    "upper_ratio",
    "route_log_dispersion",
    "geometric_midpoint_tt",
    "log_symmetry_relative_error",
    "has_geometry",
]

GOLD_COLUMNS = [
    "baseline_id",
    "location",
    "origin_id",
    "origin_name",
    "destination_id",
    "destination_name",
    "date_range",
    "start_date",
    "end_date",
    "temporal_scope",
    "mean_tt",
    "lower_tt",
    "upper_tt",
    "lower_ratio",
    "upper_ratio",
    "route_log_dispersion",
    "geometric_midpoint_tt",
    "log_symmetry_relative_error",
    "has_geometry",
    "source_file",
]
