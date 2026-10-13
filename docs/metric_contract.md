# Metric contract v1

All money is BRL, `NUMERIC(18,2)` in SQL and `Decimal` in Python. No floating point anywhere in the money path.
Dates are **source-local purchase dates** unless labelled otherwise. This is a historical public dataset
replay: none of these figures are live orders, recognised revenue, profit or settlement.

| Metric | Definition | Population / denominator | Coverage shown beside it |
|--------|------------|--------------------------|--------------------------|
| Eligible orders | count of orders with `is_eligible` | status ∉ {canceled, unavailable} (contract v1) | excluded orders count |
| Merchandise value | Σ item `price` | eligible orders, item grain summed to order first | eligible orders missing items |
| Shipping charges | Σ item `freight_value` | same as merchandise | same |
| Average order value | merchandise / eligible orders **with ≥1 item**; half-up to cents; NULL if 0 | period AOV = Σ numerator / Σ denominator (D-011) | orders with items |
| Recorded payment value | Σ `payment_value` | eligible orders | eligible orders missing payments |
| Payment difference | payments − (items + freight), per order | only when both sides exist, else NULL | `has_items`, `has_payments` |
| On-time delivery rate | delivered with actual date ≤ estimated date (calendar dates) / delivered with both dates | status = delivered **and** eligible (D-007) | missing-date orders |
| Delivery lead time | actual delivery − purchase (seconds) | delivered and eligible with an actual date; negatives flagged, excluded from percentiles | negative lead-time count |

**Eligibility is defined once per side.** In SQL it is `fct_orders.is_eligible`, from the seed
`order_status_contract.csv`. In Python it is `commerce.contracts.orders`. A unit test asserts the two
copies are identical. An unknown status fails the build.

**Missing is not zero (D-010).** Order-grain amounts are NULL when a side is absent. Only the daily serving
marts zero-fill sums over an empty set, and they publish coverage counts next to them.

**Not additive:** `mart_category_daily.distinct_orders` (an order can span categories). Sum `item_rows` or
money across categories, never distinct orders.

**Reconciliation is evidence, not editing.** Real gaps between payments and items+freight are reported (warn).
In the exact synthetic fixture any gap blocks.
