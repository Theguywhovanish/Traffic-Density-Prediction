from __future__ import annotations

import math
from pathlib import Path

from urban_mobility.lakehouse.contracts import ROUTE_KEY
from urban_mobility.lakehouse.reference import build_reference_layers, validate_gold_baseline


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "raw" / "uber_movement"


def test_reference_layers_real_dataset() -> None:
    layers = build_reference_layers(DATA)
    assert len(layers["bronze"]) == 5547
    assert len(layers["silver"]) == 5547
    assert len(layers["gold"]) == 5547
    assert layers["gold"]["location"].nunique() == 7
    assert layers["gold"][ROUTE_KEY].drop_duplicates().shape[0] == 5547


def test_gold_baseline_contract() -> None:
    gold = build_reference_layers(DATA)["gold"]
    metrics = validate_gold_baseline(gold)
    assert metrics == {
        "rows": 5547,
        "unique_route_keys": 5547,
        "unique_locations": 7,
        "unique_baseline_ids": 5547,
        "bounds_ordered": True,
        "finite_route_log_dispersion": True,
        "all_route_keys_unique": True,
    }


def test_log_dispersion_matches_formula() -> None:
    gold = build_reference_layers(DATA)["gold"]
    row = gold.iloc[0]
    expected = 0.5 * math.log(float(row.upper_tt) / float(row.lower_tt))
    assert math.isclose(float(row.route_log_dispersion), expected, rel_tol=1e-12)


def test_baseline_ids_are_deterministic() -> None:
    a = build_reference_layers(DATA)["gold"].sort_values(ROUTE_KEY).reset_index(drop=True)
    b = build_reference_layers(DATA)["gold"].sort_values(ROUTE_KEY).reset_index(drop=True)
    assert a["baseline_id"].tolist() == b["baseline_id"].tolist()
