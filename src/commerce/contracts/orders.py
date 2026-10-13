ORDER_STATUS_CONTRACT_VERSION = 1

ORDER_STATUSES_V1: dict[str, bool] = {
    "created": True,
    "approved": True,
    "invoiced": True,
    "processing": True,
    "shipped": True,
    "delivered": True,
    "canceled": False,
    "unavailable": False,
}


class UnknownStatusError(ValueError):
    pass


def is_eligible(status: str) -> bool:
    try:
        return ORDER_STATUSES_V1[status]
    except KeyError as exc:
        raise UnknownStatusError(f"order_status {status!r} not in contract v1") from exc
