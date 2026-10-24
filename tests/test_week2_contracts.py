from urban_mobility.lakehouse.contracts import GOLD_COLUMNS, ROUTE_KEY, SILVER_COLUMNS


def test_route_key_is_location_aware() -> None:
    assert ROUTE_KEY == ["location", "origin_id", "destination_id"]


def test_gold_contains_streaming_baseline_fields() -> None:
    required = {
        "baseline_id", "location", "origin_id", "destination_id",
        "mean_tt", "lower_tt", "upper_tt", "route_log_dispersion",
    }
    assert required.issubset(GOLD_COLUMNS)
    assert "route_log_dispersion" in SILVER_COLUMNS
