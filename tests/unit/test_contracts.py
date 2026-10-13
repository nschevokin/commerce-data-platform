import csv
from decimal import Decimal
from pathlib import Path

import pytest

from commerce.contracts.money import MoneyFormatError, fmt_money, parse_money
from commerce.contracts.orders import ORDER_STATUSES_V1, UnknownStatusError, is_eligible

REPO = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("text,expected", [
    ("60", Decimal("60.00")), ("60.0", Decimal("60.00")), ("0.10", Decimal("0.10")),
    (" 33.00 ", Decimal("33.00")), ("-5.25", Decimal("-5.25")),
])
def test_money_parses_exactly(text, expected):
    assert parse_money(text) == expected


@pytest.mark.parametrize("bad", ["10.005", "1e3", "", "abc", "12,50", "NaN", "Infinity", "1.2.3"])
def test_money_rejects_instead_of_rounding(bad):
    with pytest.raises(MoneyFormatError):
        parse_money(bad)


def test_money_sum_has_no_float_drift():
    assert fmt_money(parse_money("0.10") + parse_money("0.20")) == "0.30"


def test_unknown_status_blocks():
    with pytest.raises(UnknownStatusError):
        is_eligible("returned")


def test_python_and_sql_status_contracts_agree():
    with (REPO / "dbt" / "seeds" / "order_status_contract.csv").open() as fh:
        seed = {r["order_status"]: r["is_eligible"] == "true" for r in csv.DictReader(fh)}
    assert seed == ORDER_STATUSES_V1
