# Golden fixture `golden_v1` — hand calculation

Synthetic Olist-shaped CSVs in `tests/fixtures/golden_v1/`. Every expected value in `expected.json` was
calculated by hand from the table below, *before* any SQL existed. Code never regenerates it.

| Order | Status | Purchase | Items (price + freight) | Payments | Delivery (actual / estimate) |
|-------|--------|----------|--------------------------|----------|------------------------------|
| A | delivered | 2017-03-10 14:20 | 60+5 (prod_1, seller_1), 40+5 (prod_2, seller_2) | 70 (card, 3x) + 40 (voucher) | 03-17 / 03-24 → on time |
| B | delivered | 2017-03-11 09:05 | 30+3 (prod_3, seller_1) | 33 (boleto) | 03-30 / 03-28 → late |
| C | canceled | 2017-03-11 18:30 | 20+2 (prod_1, seller_2) | 22 (card) | — / 03-30 |

**Eligible (A, B):** orders 2 · merchandise 100+30 = **130.00** · freight 10+3 = **13.00** · payments
110+33 = **143.00** · AOV 130/2 = **65.00** · on-time 1 of 2 = **50 %**.
**All statuses:** merchandise 150.00 · freight 15.00 · payments 165.00. C is retained in the facts.
**Why A matters:** 2 items × 2 payments. A raw item–payment join gives 4 rows and doubles A to 200 / 220.
`test_item_by_payment_fan_out_is_caught` proves the reconciliation tests fail on that mutant.
**Per day:** 03-10 → 1 eligible, 100.00, AOV 100.00 · 03-11 → 1 eligible + 1 excluded, 30.00, AOV 30.00.
**Lead times:** A 6d 19h 40m = 589 200 s · B 19d 6h 55m = 1 666 500 s.

Edge cases also exercised: customers A and B share `customer_unique_id` (not merged), an accented city,
a leading-zero postal prefix (`01310`), a 4-digit seller prefix (`4567` → `04567`), a lower-case state
(`sp` → `SP`), an untranslated category (`pc_gamer`), and a blank category (`(unknown)`).

**Later scenarios (oracle fixed now, exercised from T04/T08):** A redelivered → no change. B two replay days
late → 03-11 shows 0 eligible until B is available. B revision 2 (price 35, freight 3, payment 38) →
merchandise 135.00, freight 13.00, payments 148.00, AOV 67.50.

**Broken variants** (each must block): malformed money `60.005`, unknown status `returned`, item with a
missing parent order, NULL payment `order_id`, malformed timestamp `10/03/2017 14:20`. The conflicting-hash
variant belongs to ingestion (T04).
