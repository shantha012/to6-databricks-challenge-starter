# My dataset submission

Complete this file in Databricks. Keep README.md as the course navigation page.

## Inputs and outputs

* **Inputs (read-only):**
  * `training.to6_challenge3_inputs.returns_initial`: rejected.
  * `training.to6_challenge3_inputs.returns_corrected`: accepted and used for the final run.
* **Lineage:** according to `training.to6_challenge3_inputs.source_lineage`, both delivery tables were built by SQL from `training.to6_challenge3_inputs.parsed_source`.
* **Row meaning:** one row is one tax return. `return_id` identifies the return and must be unique. `taxpayer_id` identifies the filer, who may have returns in several years.
* **Outputs (schema `training.to6_c3_c399a52409d8`):**
  * `initial_rejected_records`: affected initial records with their rule and reason.
  * `return_features`: all accepted returns.
  * `features_train`: 2022 returns.
  * `features_validation`: 2023 returns.
  * `features_test`: 2024 returns.
* **Feature columns:** exactly `return_id`, `tax_year` and `deduction_to_income`. `taxpayer_id`, `income` and `deduction` are excluded under the need-to-know rule.
* **Ratio:** `deduction_to_income = deduction / income`, stored as a double. If `income = 0`, the ratio is NULL and the return is kept.

## Checks and decisions

* **Source references (notebook section 1):** the Catalog table comments, 5 sample rows from each delivery and the lineage view rows.
* **Full-batch profile:**
  * Initial batch: 13 rows covering 12 distinct IDs, 1 NULL income (`R02`), `R01` duplicated, and zero income for `R03` and `R09`.
  * Corrected batch: 12 rows, with 4 per year.
* **Quality gate (section 2):** `require_valid()` runs three rules and raises `BatchRejected` if any fails. The same rules run on both batches.
  * completeness: `return_id`, `tax_year`, `income` and `deduction` must not be NULL
  * uniqueness: each `return_id` occurs exactly once
  * amounts must not be negative
* **Initial batch rejected:** before any feature output was written.
  * Completeness fails on `R02` (F1, 2022): income is NULL, deduction 33.
  * Uniqueness fails on `R01` (F0, 2022, 100, 20), which appears 2 times.
  * All 3 affected rows are kept in `outputs/initial_rejected_records.csv` and in `initial_rejected_records`. The decision is in `outputs/batch_decision.json`.
  * No zero was filled in for the missing income, no rows were dropped and no rule was relaxed.
* **Replacement batch accepted:** `returns_corrected` passes all three rules on its 12 rows, with 0 violations.
* **Nonzero expected value:** `R04` has source deduction 59 and income 400, so the expected ratio is 59 / 400 = 0.1475. The saved value is 0.1475. I calculated it in Python from the source, separately from the SQL that builds the table.
* **Read-back of the saved `return_features` table:** 10 of 10 checks pass, listed in `outputs/return_features_checks.csv`.
  * The table has exactly the three columns, 12 rows and 12 distinct IDs, the same IDs and years as the accepted batch.
  * All nonzero ratios match the source.
  * Zero-income `R03` and `R09` are present with a NULL ratio, and they are the only NULL ratios.
* **Separate SQL cross-check:** a full outer join of the saved features to the source found 0 unmatched rows, 0 wrong ratios and 0 zero-income rows with a non-NULL ratio.

## Two successful runs

Both were complete interactive runs of every code cell, so there is no Jobs run ID. Notebook URL for both: https://dbc-48a16403-f66f.cloud.databricks.com/editor/notebooks/127024866559576

| Run | Started (UTC) | Completed (UTC) | Snapshot |
| --- | --- | --- | --- |
| 1 | 2026-11-20T16:50:29Z | 2026-11-20T16:51:28Z | `outputs/run_snapshots/run_20261120T165029Z.csv` and `.json` |
| 2 | 2026-11-20T16:51:59Z | 2026-11-20T16:52:54Z | `outputs/run_snapshots/run_20261120T165159Z.csv` and `.json` |

* **Snapshot contents:** each snapshot holds the actual `table`, `return_id`, `tax_year` and `deduction_to_income` of every row in `return_features` and in the three split tables.
* **Run comparison:** `outputs/run_comparison.json` compares the two snapshots on (`table`, `return_id`). It covers 24 rows and finds 0 differences, so the IDs and values are identical.
* **Year assignment:** after reading back each split table, the notebook checks that it contains only its own year: `features_train` 2022 (4 rows), `features_validation` 2023 (4) and `features_test` 2024 (4).
* **One copy of every return:** across the three splits, 12 IDs each appear exactly once. The splits match `return_features` exactly by ID and value, with 0 differing rows. The checks are in `outputs/split_checks.csv`. A separate SQL check also found 0 wrong years, 0 unmatched IDs and 0 value differences.
* **No appended copies:** every table is written in overwrite mode. After run 2, each table's row count equals its distinct ID count: 12/12 for `return_features` and 4/4 for each split.

## Final files and limitations

**Final exports in `outputs/`:**
* `initial_rejected_records.csv`
* `batch_decision.json`
* `return_features.csv`
* `return_features_checks.csv`
* `features_train.csv`
* `features_validation.csv`
* `features_test.csv`
* `split_checks.csv`
* `run_comparison.json`
* `run_snapshots/run_20261120T165029Z.csv` and `.json`
* `run_snapshots/run_20261120T165159Z.csv` and `.json`

**Practice file only, not part of the task:** `connection-practice.json`.

**Not verified:**
* Lineage was taken from the supplied `source_lineage` view. The view says it does not claim a Catalog UI lineage check, and I did not check lineage in the Catalog UI or inspect `parsed_source` directly.
* Snapshot ratios are written with 17 significant digits, for example 0.14749999999999999 for 0.1475, so that the run comparison is exact.
* The commit and push of these files to GitHub `main`, and the notebook's source format in Git (`notebook.py`), are not yet done or checked.
* These checks do not rule out model leakage. Labels and an assessment of what is available at prediction time belong to later modelling work.
