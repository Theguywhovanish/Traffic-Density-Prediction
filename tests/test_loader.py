from pathlib import Path

import pandas as pd

from urban_mobility.data.loader import canonicalize, enrich_routes, infer_location, parse_date_range, valid_mask


def test_kaggle_location_filename():
    assert infer_location(Path("Travel_Times - Washington DC.csv"), Path(".")) == "Washington DC"


def test_date_range_parsing():
    parsed = parse_date_range(pd.Series(["07/31/2016 - 08/28/2016, Every day, Daily Average"]))
    assert str(parsed.loc[0, "start_date"].date()) == "2016-07-31"
    assert str(parsed.loc[0, "end_date"].date()) == "2016-08-28"
    assert parsed.loc[0, "temporal_scope"] == "Every day, Daily Average"


def test_canonical_validation_and_enrichment():
    raw = pd.DataFrame([
        {
            "Origin Movement ID": 1,
            "Origin Display Name": "A",
            "Destination Movement ID": 2,
            "Destination Display Name": "B",
            "Date Range": "07/31/2016 - 08/28/2016, Every day, Daily Average",
            "Mean Travel Time (Seconds)": 600,
            "Range - Lower Bound Travel Time (Seconds)": 400,
            "Range - Upper Bound Travel Time (Seconds)": 900,
        }
    ])
    canonical = canonicalize(raw, location="Boston", source_file="x.csv")
    assert bool(valid_mask(canonical).iloc[0])
    enriched = enrich_routes(canonical)
    assert abs(enriched.loc[0, "route_log_dispersion"] - 0.4054651081) < 1e-8
    assert abs(enriched.loc[0, "geometric_midpoint_tt"] - 600.0) < 1e-8
    assert enriched.loc[0, "log_symmetry_relative_error"] < 1e-12
