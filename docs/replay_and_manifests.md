# Replay and manifest contract (T03)

Honest boundary: the Olist data is a final historical snapshot. Replay re-times it; it does not recreate what a
merchant knew on the original purchase date. Every replay is labelled a simulation.

## Layout (immutable, create-exclusive)

```
archive/dataset=<sha>/original/<zip>, inventory.json, profile.json
raw/dataset=<sha>/replay=<v>/day=<date>/source=<name>/sha256=<h>.jsonl.gz
manifests/dataset=<sha>/replay=<v>/day=<date>.json
replays/dataset=<sha>/replay=<v>/replay.json      # written last: the replay exists only once this exists
```

## Rules
- Interval 0 is a reference bootstrap (products, sellers, translations); then N one-day UTC intervals.
- Each order, its customer (first referencing order), items and payments share one `available_at`.
- Every manifest lists all 7 sources, with empty files for empty sources. A missing manifest means
  "interval incomplete", never "empty" (`IntervalIncomplete`).
- Files are written to a temp name, fsynced, and linked into place only if the key is new. Same bytes: no-op.
  Different bytes under an existing key: error. Order: files, manifests, descriptor.
- Scenarios (duplicate, late, correction, five fault variants) are separate replay versions derived from the
  stored baseline; the baseline is never modified.

## Commands
```
make import-data ARCHIVE=... RETRIEVED_ON=YYYY-MM-DD LICENCE="..."
make replay                      # baseline + verify
commerce replay scenario --dataset-sha256 <sha> --kind late --count 3 --seed 42
commerce replay verify --dataset-sha256 <sha>
```

## Comparing machines
Cross-machine equality is judged on the uncompressed content (`content_sha256`, and the pinned digest in
`tests/unit/test_replay_baseline.py`: `40d5918f...0877f16`), NOT on gzip bytes or the descriptor hash (D-021).

Measured on the golden fixture, sandbox (Linux) vs owner's Mac: the pinned digest passes on both; the
uncompressed SHA-256 of the `customers` and `order_items` day-01 files is identical
(`ab870aed...fbe691`, `3162b563...f81ad0`); the gzip file hashes and therefore the descriptor hash differ
(`1ebb4917...` vs `834dd298...`). Cause: different zlib builds. Within one machine, reruns are byte-identical.

## Ingestion (T04)
```
commerce ingest --store data/store --dataset-sha256 <sha> --version baseline-v1     # writer role
commerce guard status                                                                  # who holds the writer guard
commerce guard takeover --operator <name> --reason "<old run is terminal>"             # admin, operator only
```
Per file: fence check, receipt lookup, validation (keys, types, money, timestamps, status, window), staging
table, conflict check against bronze, insert, receipt, all in one transaction. See D-005, D-006, D-027 to D-029.

## Candidates from bronze (T05)
```
commerce build-candidate --store data/store --dataset-sha256 <sha> --version baseline-v1 [--frontier-index N]
commerce crosscheck --source <archive.zip> --candidate-schema cand_<hex>      # independent reference, 0 differences
```
Frontier rules and source selection: D-030.
