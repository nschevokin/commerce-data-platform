from __future__ import annotations

import os
from decimal import Decimal

import streamlit as st

from commerce.config import role_dsn
from commerce.dashboard import data, metrics

st.set_page_config(page_title="Commerce data platform", layout="wide")


@st.cache_resource
def refresher() -> data.Refresher:
    return data.Refresher(os.environ.get("COMMERCE_DASHBOARD_DSN") or role_dsn("commerce_dashboard"))


def floats(rows, *keys):
    return [{k: (float(v) if isinstance(v, Decimal) else v) for k, v in r.items() if k in keys} for r in rows]


bundle, status = refresher().refresh_if_due()

st.title("E-commerce data platform")
st.caption("Historical Olist replay for a portfolio project. Not live orders, not recognised revenue, "
           "not profit. Currency BRL. Dates are purchase dates unless stated.")

if status.state == data.UNAVAILABLE:
    st.error(f"Data unavailable: {status.error}. No release has been read in this session.")
    st.stop()
if status.state == data.EMPTY:
    st.warning("No release has been published yet. Run the pipeline, then reload.")
    st.stop()
if status.state == data.STALE:
    st.warning(f"STALE: the database is unreachable ({status.error}). Showing release {bundle.release_id[:8]} "
               f"as last read at {status.last_success_at:%Y-%m-%d %H:%M:%S} UTC.")

rel = bundle.release
st.markdown(f"**Release** `{bundle.release_id[:8]}` · status **{rel['status']}** · replay "
            f"`{rel['replay_version']}` · accepted data before **{rel['frontier_end']:%Y-%m-%d} UTC** · "
            f"generation {rel['generation']}")

sales = bundle.tables["sales_daily"]
bounds = metrics.date_bounds(sales)
tab_over, tab_cat, tab_del, tab_pipe = st.tabs(["Overview", "Categories", "Delivery", "Pipeline evidence"])

with tab_over:
    if bounds is None:
        st.info("This release has no sales days.")
    else:
        rng = st.date_input("Purchase date range", value=bounds, min_value=bounds[0], max_value=bounds[1],
                            key="range")
        start, end = (rng if isinstance(rng, tuple) and len(rng) == 2 else bounds)
        o = metrics.overview(sales, start, end)
        c = st.columns(4)
        c[0].metric("Eligible orders", f"{o.eligible_orders:,}")
        c[1].metric("Merchandise value", metrics.brl(o.merchandise))
        c[2].metric("Shipping charges", metrics.brl(o.freight))
        c[3].metric("Average order value", metrics.brl(o.aov))
        st.caption(f"Recorded payment value {metrics.brl(o.recorded_payment)} (a separate measure from "
                   f"merchandise value). AOV = merchandise / {o.with_items:,} eligible orders with items.")
        st.markdown(f"**Coverage:** {o.missing_items:,} eligible orders have no items, {o.missing_payments:,} "
                    f"have no recorded payment; {o.excluded_orders:,} orders are excluded by status "
                    f"(canceled or unavailable). Totals above cover only orders that have the data.")
        series = metrics.daily_series(sales, start, end)
        st.line_chart(floats(series, "purchase_date", "merchandise_value_brl"), x="purchase_date",
                      y="merchandise_value_brl", x_label="Purchase date", y_label="Merchandise value (BRL)")
        st.download_button("Download daily table (CSV)", metrics.to_csv(series), "sales_daily_selection.csv",
                           "text/csv")
        st.caption("The customer-state filter applies on the Delivery tab only: the sales table is not "
                   "stored by state (D-054).")

with tab_cat:
    if bounds is None:
        st.info("No data.")
    else:
        top = st.slider("Categories shown", 5, 30, 10)
        ranking = metrics.category_ranking(bundle.tables["category_daily"], *bounds)
        st.bar_chart(floats(ranking[:top], "category_label", "merchandise_value_brl"), x="category_label",
                     y="merchandise_value_brl", horizontal=True, sort="-merchandise_value_brl",
                     x_label="Category", y_label="Merchandise value (BRL)")
        st.dataframe([{**r, "merchandise_value_brl": str(r["merchandise_value_brl"]),
                       "freight_value_brl": str(r["freight_value_brl"])} for r in ranking[:top]],
                     use_container_width=True)
        st.caption("Ranked by merchandise value at item grain. Distinct-order counts are exact per category "
                   "but must not be added across categories: an order can contain several. "
                   "Category label 'unmapped' means the source has no translation for it.")
        st.download_button("Download category table (CSV)", metrics.to_csv(ranking), "category_ranking.csv",
                           "text/csv")

with tab_del:
    cohorts = bundle.tables["delivery_cohorts"]
    states = st.multiselect("Customer state", metrics.delivery_states(cohorts))
    d, rows = metrics.delivery(cohorts, states or None)
    c = st.columns(3)
    c[0].metric("On-time rate", d.rate)
    c[1].metric("Delivered orders", f"{d.delivered:,}")
    c[2].metric("Missing delivery date", f"{d.missing_date:,}")
    st.caption(f"Rate = on-time / {d.with_both_dates:,} delivered orders that have both an actual and an "
               f"estimated date. {d.negative_lead_time:,} orders have a negative lead time and are flagged. "
               "Lead-time percentiles are per cohort and are not averaged across cohorts.")
    st.dataframe([{k: (str(v) if isinstance(v, Decimal) else v) for k, v in r.items()} for r in rows],
                 use_container_width=True)
    st.download_button("Download cohort table (CSV)", metrics.to_csv(rows), "delivery_cohorts_selection.csv",
                       "text/csv")

with tab_pipe:
    st.subheader("Published release")
    st.json({k: str(v) for k, v in rel.items()})
    st.caption(f"Read in one transaction in {bundle.read_seconds * 1000:.0f} ms at "
               f"{bundle.fetched_at:%Y-%m-%d %H:%M:%S} UTC.")
    st.subheader("Quality and source rows in this release")
    st.dataframe([{**r, "frontier_end": str(r["frontier_end"]), "release_id": str(r["release_id"])[:8]}
                  for r in metrics.health_summary(bundle.tables["pipeline_health"])], use_container_width=True)
    st.caption("Blocking failures must be 0 for a release to be published. Warnings are disclosed data gaps. "
               "Run history, receipts and logs live in Airflow and the control schema, which this "
               "read-only role cannot see by design.")
