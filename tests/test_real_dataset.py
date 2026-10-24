from pathlib import Path

from urban_mobility.analysis.profiling import build_location_summary, build_profile
from urban_mobility.data.loader import enrich_routes, load_dataset

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "raw" / "uber_movement"


def test_real_dataset_inventory_and_quality():
    data, file_profile = load_dataset(DATA)
    routes = enrich_routes(data)
    profile = build_profile(routes, file_profile, DATA)

    assert profile["files_processed"] == 7
    assert profile["rows_raw"] == 5547
    assert profile["rows_valid"] == 5547
    assert profile["rows_invalid"] == 0
    assert profile["rows_after_dedup"] == 5547
    assert profile["unique_locations"] == 7
    assert profile["all_bounds_ordered"] is True
    assert profile["log_symmetric_within_1pct_pct"] > 99.0


def test_one_origin_per_location_in_supplied_dataset():
    data, _ = load_dataset(DATA)
    assert (data.groupby("location")["origin_id"].nunique() == 1).all()


def test_location_summary_contains_expected_locations():
    data, _ = load_dataset(DATA)
    summary = build_location_summary(enrich_routes(data))
    expected = {
        "Bogota", "Boston", "Johannesburg and Pretoria", "Manila", "Paris", "Sydney", "Washington DC"
    }
    assert set(summary["location"]) == expected
