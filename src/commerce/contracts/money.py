from __future__ import annotations

import re
from decimal import ROUND_HALF_UP, Decimal

_MONEY = re.compile(r"^-?\d{1,16}(\.\d{1,2})?$")
CENT = Decimal("0.01")


class MoneyFormatError(ValueError):
    pass


def parse_money(text: str) -> Decimal:
    s = text.strip()
    if not _MONEY.match(s):
        raise MoneyFormatError(f"malformed money value: {text!r}")
    return Decimal(s).quantize(CENT)


def fmt_money(value: Decimal) -> str:
    return str(value.quantize(CENT, rounding=ROUND_HALF_UP))
