from datetime import UTC, datetime, timedelta

from commerce.release.retention import plan

NOW = datetime(2026, 10, 12, tzinfo=UTC)


def row(i, status="VALIDATED", age_days=1.0):
    return {"release_id": f"r{i}", "schema_name": f"cand_{i:032x}", "status": status,
            "created_at": NOW - timedelta(days=age_days)}


def decide(rows, current="r9", published=("r9", "r8", "r7", "r6", "r5"), holds=frozenset()):
    out = plan(rows, current=current, published_order=list(published), keep_published=2,
               failed_cutoff=NOW - timedelta(days=7), holds=set(holds))
    return {d.release_id: d.keep for d in out}


def test_keeps_current_and_the_previous_two_published_only():
    k = decide([row(i) for i in range(5, 10)])
    assert k == {"r5": False, "r6": False, "r7": True, "r8": True, "r9": True}


def test_in_progress_builds_holds_and_recent_failures_survive_but_old_ones_do_not():
    rows = [row(1, "BUILDING", 30), row(2, "BUILT", 30), row(3, "REJECTED", 2), row(4, "REJECTED", 30),
            row(5, "VALIDATED", 30), row(9)]
    k = decide(rows, published=("r9",))
    assert k["r1"] and k["r2"]
    assert k["r3"] and not k["r4"]
    assert not k["r5"]
    assert decide(rows, published=("r9",), holds={"r4"})["r4"]


def test_already_dropped_releases_are_ignored():
    assert decide([row(1, "DROPPED", 90), row(9)], published=("r9",)) == {"r9": True}
