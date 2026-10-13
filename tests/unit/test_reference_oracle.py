import json
from pathlib import Path

from commerce.fixture.reference import compute_from_dir

GOLDEN = Path(__file__).resolve().parents[1] / "fixtures" / "golden_v1"


def test_reference_matches_hand_calculated_oracle():
    expected = json.loads((GOLDEN / "expected.json").read_text())["baseline"]
    reference = compute_from_dir(GOLDEN)
    for section, value in reference.items():
        assert value == expected[section], section


def test_headline_golden_numbers():
    e = compute_from_dir(GOLDEN)["eligible"]
    assert (e["eligible_orders"], e["merchandise_value_brl"], e["freight_value_brl"],
            e["recorded_payment_brl"], e["average_order_value_brl"]) == (
        2, "130.00", "13.00", "143.00", "65.00")
