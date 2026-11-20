# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Prepare the next team's dataset
# MAGIC Follow CHALLENGE.md. These headings are a starting point; implement and verify each stage yourself.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 1 AS connection_check

# COMMAND ----------

from pathlib import Path
import json

# Copy this Git folder's absolute path from its Workspace menu; include /Workspace/.
repo_root = Path("/Workspace/Users/shantha.mahabhashyam.irs@catalyte.ai/to6-databricks-challenge-starter")
assert (repo_root / "README.md").is_file(), "Check the Git folder path"
practice_result = spark.sql("SELECT 1 AS connection_check").first().asDict()
assert practice_result == {"connection_check": 1}
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs/connection-practice.json").write_text(
    json.dumps(practice_result) + "\n", encoding="utf-8"
)

# COMMAND ----------

import hashlib

INPUT_SCHEMA = "training.to6_challenge3_inputs"
INITIAL_TABLE = f"{INPUT_SCHEMA}.returns_initial"
CORRECTED_TABLE = f"{INPUT_SCHEMA}.returns_corrected"
LINEAGE_VIEW = f"{INPUT_SCHEMA}.source_lineage"
workspace_user = spark.sql("SELECT current_user() AS username").first()["username"]
OUTPUT_SCHEMA = "training.to6_c3_" + hashlib.sha256(workspace_user.encode("utf-8")).hexdigest()[:12]

# Run reference: start time of this complete run and this notebook's URL (kept in the run snapshot)
from datetime import datetime, timezone
RUN_STARTED_AT = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
NOTEBOOK_ID = "127024866559576"
try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    NOTEBOOK_URL = f"https://{_ctx.browserHostName().get()}/editor/notebooks/{NOTEBOOK_ID}"
except Exception:
    NOTEBOOK_URL = f"https://{spark.conf.get('spark.databricks.workspaceUrl', '<workspace-host>')}/editor/notebooks/{NOTEBOOK_ID}"
print({"input_schema": INPUT_SCHEMA, "output_schema": OUTPUT_SCHEMA,
       "run_started_at": RUN_STARTED_AT, "notebook_url": NOTEBOOK_URL})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understand the input
# MAGIC Record Catalog descriptions, sample observations and the supplied lineage source.

# COMMAND ----------

# MAGIC %md
# MAGIC return_initial , return_corrected:
# MAGIC
# MAGIC Return-Initial : table witht he intial batch data
# MAGIC return_corrected : table with corrected infromation after eliminating duplicates and incorrect data from return_initial. 
# MAGIC
# MAGIC return_id identifies the return; 
# MAGIC taxpayer_id identifies its filer . There can be multiple return per fielr over the years.
# MAGIC Required fields are return_id, tax_year, income and deduction. 
# MAGIC Each return must occur exactly once. Amounts are nonnegative; zero income is valid. You do not need to build an XML parser.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Catalog descriptions, sample rows and lineage
# 1a. Catalog descriptions (table comments) for the two deliveries and the lineage view
display(spark.sql(f"""
    SELECT table_catalog || '.' || table_schema || '.' || table_name AS full_name,
           table_type, comment
    FROM training.information_schema.tables
    WHERE table_schema = 'to6_challenge3_inputs'
      AND table_name IN ('returns_initial', 'returns_corrected', 'source_lineage')
    ORDER BY table_name
"""))

# Column names and types (same schema expected in both deliveries)
for t in (INITIAL_TABLE, CORRECTED_TABLE):
    print(t, spark.table(t).dtypes)

# 1b. At least three sample rows from each delivery (ordered so the sample is reproducible)
for t in (INITIAL_TABLE, CORRECTED_TABLE):
    print(f"Sample rows: {t}")
    display(spark.table(t).orderBy("tax_year", "return_id").limit(5))

# 1c. Supplied lineage view: upstream source of each delivered table
display(spark.table(LINEAGE_VIEW))

# COMMAND ----------

# DBTITLE 1,Input understanding: source reference and row meaning
# MAGIC %md
# MAGIC ### Input understanding (source reference)
# MAGIC
# MAGIC **Sources:** Catalog table comments on `training.to6_challenge3_inputs.returns_initial`, `training.to6_challenge3_inputs.returns_corrected` and `training.to6_challenge3_inputs.source_lineage`, the sample rows above, and the lineage view rows.
# MAGIC
# MAGIC | Item | Finding | Evidence |
# MAGIC | --- | --- | --- |
# MAGIC | Catalog description, initial | "Initial synthetic delivery from parsed_source; known delivery defects. Validate before feature output." | Table comment |
# MAGIC | Catalog description, corrected | "Corrected synthetic delivery from parsed_source; one row per return. Validate required values and return_id uniqueness." | Table comment |
# MAGIC | Upstream source | Both tables were created by SQL from `training.to6_challenge3_inputs.parsed_source`. Initial: "Synthetic initial delivery transformation; validation required". Corrected: "Synthetic corrected delivery; same validation rules". | `source_lineage` view (its comment says it does not claim a Catalog UI lineage check) |
# MAGIC | Row meaning | One row = one tax return for one filer in one tax year (e.g. `R01`, filer `F0`, 2022, income 100, deduction 20). | Sample rows, corrected table comment, CHALLENGE.md |
# MAGIC | Uniqueness identifier | `return_id`. It must occur exactly once. `taxpayer_id` is **not** unique, because one filer (e.g. `F0`) files in 2022, 2023 and 2024. | Sample rows, corrected table comment |
# MAGIC | Relevant fields | Required: `return_id` (string), `tax_year` (int), `income` (int), `deduction` (int). `taxpayer_id` is the filer identifier and is excluded from features (need-to-know). | Schema printout, CHALLENGE.md |
# MAGIC | Early sample signals (initial) | `R01` appears twice, and `R02` has NULL `income`. `R03` has income 0, which is valid but needs a null ratio later. | Sample rows above |
# MAGIC
# MAGIC The signals from the samples are not a quality check. The full-batch summary and the formal checks follow.

# COMMAND ----------

# DBTITLE 1,Full-batch profile: years, missing values, duplicates, zero income
# Full-batch profile of BOTH deliveries (all rows, not samples). Observations only; formal checks follow in section 2.
batches = {"initial": INITIAL_TABLE, "corrected": CORRECTED_TABLE}
union_sql = " UNION ALL ".join(
    f"SELECT '{name}' AS batch, * FROM {table}" for name, table in batches.items()
)
spark.sql(union_sql).createOrReplaceTempView("all_batches")

# 1. Returns by year: rows vs distinct return_ids
print("Returns by year")
display(spark.sql("""
    SELECT batch, tax_year, COUNT(*) AS row_count, COUNT(DISTINCT return_id) AS distinct_return_ids
    FROM all_batches GROUP BY batch, tax_year ORDER BY batch, tax_year
"""))

# 2. Missing required values per field (return_id, tax_year, income, deduction)
print("Missing required values (counts of NULL per field)")
display(spark.sql("""
    SELECT batch, COUNT(*) AS total_rows,
           COUNT_IF(return_id IS NULL) AS null_return_id,
           COUNT_IF(tax_year  IS NULL) AS null_tax_year,
           COUNT_IF(income    IS NULL) AS null_income,
           COUNT_IF(deduction IS NULL) AS null_deduction
    FROM all_batches GROUP BY batch ORDER BY batch
"""))
print("Rows with any missing required value")
display(spark.sql("""
    SELECT * FROM all_batches
    WHERE return_id IS NULL OR tax_year IS NULL OR income IS NULL OR deduction IS NULL
    ORDER BY batch, return_id
"""))

# 3. Duplicate return IDs (and whether duplicates are exact copies)
print("Duplicate return_ids")
display(spark.sql("""
    SELECT batch, return_id, COUNT(*) AS occurrences,
           COUNT(DISTINCT taxpayer_id, tax_year, income, deduction) AS distinct_versions
    FROM all_batches GROUP BY batch, return_id HAVING COUNT(*) > 1 ORDER BY batch, return_id
"""))

# 4. Zero-income records (valid, but the ratio must be NULL later) and negative-amount scan
print("Zero-income records")
display(spark.sql("""
    SELECT * FROM all_batches WHERE income = 0 ORDER BY batch, return_id
"""))
print("Negative amounts (should be none)")
display(spark.sql("""
    SELECT batch, COUNT_IF(income < 0) AS negative_income, COUNT_IF(deduction < 0) AS negative_deduction
    FROM all_batches GROUP BY batch ORDER BY batch
"""))

# 5. Do the two batches differ beyond the defects? Rows present in one but not the other
print("Row-level differences between batches")
display(spark.sql(f"""
    (SELECT 'only_in_initial' AS side, * FROM {INITIAL_TABLE} EXCEPT ALL SELECT 'only_in_initial', * FROM {CORRECTED_TABLE})
    UNION ALL
    (SELECT 'only_in_corrected' AS side, * FROM {CORRECTED_TABLE} EXCEPT ALL SELECT 'only_in_corrected', * FROM {INITIAL_TABLE})
    ORDER BY side, return_id
"""))

# COMMAND ----------

# DBTITLE 1,Full-batch findings and their impact on next steps
# MAGIC %md
# MAGIC ### Full-batch findings (all rows of both deliveries)
# MAGIC
# MAGIC | Observation | `returns_initial` (13 rows) | `returns_corrected` (12 rows) |
# MAGIC | --- | --- | --- |
# MAGIC | Returns by year (rows / distinct `return_id`) | 2022: 5 / 4, 2023: 4 / 4, 2024: 4 / 4 | 2022: 4 / 4, 2023: 4 / 4, 2024: 4 / 4 |
# MAGIC | Missing required values | 1: `R02` (F1, 2022) has NULL `income` | 0 |
# MAGIC | Duplicate `return_id` | `R01` appears twice; both rows are identical (1 distinct version) | None |
# MAGIC | Zero-income records | `R03` (2022), `R09` (2024) | `R03` (2022), `R09` (2024) |
# MAGIC | Negative amounts | 0 | 0 |
# MAGIC | Row differences between batches | The extra `R01` copy, and `R02` with NULL income | `R02` with income 200. All other rows are identical. |
# MAGIC
# MAGIC **How these findings affect the next steps**
# MAGIC * **Completeness check (section 2):** the initial batch must fail on `R02`. I will keep that record and its reason. I will not set the unknown income to 0, because 0 is a valid value with a different meaning.
# MAGIC * **Uniqueness check (section 2):** the initial batch must fail on `R01`, which occurs 2 times. I will not drop the copy silently, even though both copies are identical. I will keep both rows as rejection evidence.
# MAGIC * **Batch decision:** the corrected batch has no missing values or duplicates in this profile. The formal checks must still pass on it before features are written. I expect 12 accepted returns, 4 per year.
# MAGIC * **Ratio transformation (section 3):** `R03` and `R09` have zero income, so they must stay in the table with a NULL `deduction_to_income` and no division error. One of them is the readback test case. Use a nonzero case such as `R04` (59 / 400 = 0.1475) for the expected-value check.
# MAGIC * **Splits (section 4):** years 2022–2024 only, 4 returns each, so the train, validation and test tables should each hold 4 rows.
# MAGIC
# MAGIC These observations do not replace the quality checks in section 2.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Decide which batch is usable
# MAGIC Keep the initial rejection evidence and apply the same checks to the replacement.

# COMMAND ----------

# DBTITLE 1,Quality gate: reject initial batch, assess corrected batch
from datetime import datetime, timezone

REQUIRED_FIELDS = ["return_id", "tax_year", "income", "deduction"]
RULES = {
    "completeness": "Required fields return_id, tax_year, income and deduction must be non-null",
    "uniqueness": "Each return_id must occur exactly once in the batch",
    "nonnegative_amounts": "income and deduction must be >= 0 (zero income is valid)",
}

class BatchRejected(Exception):
    """Raised when a batch fails a quality rule; feature output must not be written."""

def find_violations(batch_name, table):
    """Return every affected record (all columns kept) with the rule it breaks and why."""
    missing_expr = "concat_ws(', ', " + ", ".join(
        f"CASE WHEN {c} IS NULL THEN '{c}' END" for c in REQUIRED_FIELDS) + ")"
    return spark.sql(f"""
        WITH src AS (
            SELECT *, COUNT(*) OVER (PARTITION BY return_id) AS id_occurrences FROM {table}
        )
        SELECT '{batch_name}' AS batch, 'completeness' AS rule,
               'missing required value(s): ' || {missing_expr} AS reason,
               return_id, taxpayer_id, tax_year, income, deduction
        FROM src WHERE {' OR '.join(f'{c} IS NULL' for c in REQUIRED_FIELDS)}
        UNION ALL
        SELECT '{batch_name}', 'uniqueness',
               'return_id occurs ' || id_occurrences || ' times (must be exactly once)',
               return_id, taxpayer_id, tax_year, income, deduction
        FROM src WHERE return_id IS NOT NULL AND id_occurrences > 1
        UNION ALL
        SELECT '{batch_name}', 'nonnegative_amounts', 'negative income or deduction',
               return_id, taxpayer_id, tax_year, income, deduction
        FROM src WHERE income < 0 OR deduction < 0
    """)

def assess_batch(batch_name, table):
    """Apply the same rules to any batch; return (violations_df, per-rule summary list)."""
    violations = find_violations(batch_name, table)
    counts = {r["rule"]: r["n"] for r in violations.groupBy("rule").agg(F.count("*").alias("n")).collect()}
    total_rows = spark.table(table).count()
    summary = [
        {"batch": batch_name, "table": table, "rule": rule, "definition": text,
         "total_rows": total_rows, "violating_rows": counts.get(rule, 0),
         "result": "FAIL" if counts.get(rule, 0) else "PASS"}
        for rule, text in RULES.items()
    ]
    return violations, summary

def require_valid(batch_name, table):
    """Gate called BEFORE any feature output is written. Raises if any rule fails."""
    violations, summary = assess_batch(batch_name, table)
    failed = [s for s in summary if s["result"] == "FAIL"]
    if failed:
        raise BatchRejected(
            f"{table} rejected: " + "; ".join(f"{s['rule']} failed on {s['violating_rows']} row(s)" for s in failed))
    return table, summary

from pyspark.sql import functions as F
checked_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

# --- 1. Initial batch: must be rejected before feature output ---
initial_violations, initial_summary = assess_batch("initial", INITIAL_TABLE)
try:
    require_valid("initial", INITIAL_TABLE)
    initial_rejection = None
except BatchRejected as e:
    initial_rejection = str(e)
print("Initial batch gate:", initial_rejection or "PASSED")
assert initial_rejection is not None, "Expected the initial batch to be rejected"

# Preserve affected records and reasons: Delta table (overwrite = rerun-safe) + repo exports
REJECTED_TABLE = f"{OUTPUT_SCHEMA}.initial_rejected_records"
(initial_violations.withColumn("checked_at_utc", F.lit(checked_at))
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(REJECTED_TABLE))
rejected_pdf = (spark.table(REJECTED_TABLE).orderBy("rule", "return_id").toPandas()
                .astype({"tax_year": "Int64", "income": "Int64", "deduction": "Int64"}))  # keep NULL as empty, not 100.0
rejected_pdf.to_csv(repo_root / "outputs/initial_rejected_records.csv", index=False)

# --- 2. Corrected batch: same rules; becomes the accepted input only if the gate passes ---
corrected_violations, corrected_summary = assess_batch("corrected", CORRECTED_TABLE)
ACCEPTED_TABLE, _ = require_valid("corrected", CORRECTED_TABLE)  # raises -> run stops, no features written
print("Corrected batch gate: PASSED ->", ACCEPTED_TABLE)

# Decision record for the evidence files
decision = {
    "checked_at_utc": checked_at,
    "rules": RULES,
    "initial": {"table": INITIAL_TABLE, "decision": "REJECTED", "reason": initial_rejection,
                "affected_records_table": REJECTED_TABLE, "rule_results": initial_summary},
    "corrected": {"table": CORRECTED_TABLE, "decision": "ACCEPTED", "rule_results": corrected_summary},
    "accepted_input": ACCEPTED_TABLE,
}
(repo_root / "outputs/batch_decision.json").write_text(json.dumps(decision, indent=2) + "\n", encoding="utf-8")

print("Rule results, both batches")
display(spark.createDataFrame(initial_summary + corrected_summary))
print("Preserved affected records (read back from", REJECTED_TABLE, ")")
display(spark.table(REJECTED_TABLE).orderBy("rule", "return_id"))
print("Corrected batch violations (expect none):", corrected_violations.count())

# COMMAND ----------

# DBTITLE 1,Batch decision: failure explanation and next action
# MAGIC %md
# MAGIC ### Batch decision
# MAGIC
# MAGIC The gate `require_valid()` runs every rule and raises `BatchRejected` if any rule fails. Section 3 writes features only from `ACCEPTED_TABLE`, which is set only after the gate passes. Both batches go through the same `RULES` and `find_violations()` code.
# MAGIC
# MAGIC **`returns_initial`: REJECTED.** No feature output was written from it.
# MAGIC
# MAGIC | Rule | Affected records | Why it fails |
# MAGIC | --- | --- | --- |
# MAGIC | completeness: `return_id`, `tax_year`, `income` and `deduction` must not be null | `R02` (F1, 2022, income NULL, deduction 33) | The income is unknown, so the deduction-to-income ratio cannot be computed. |
# MAGIC | uniqueness: each `return_id` must occur exactly once | `R01` (F0, 2022, 100, 20), 2 identical rows | The return would appear twice in the features and splits. |
# MAGIC | nonnegative amounts | none | PASS |
# MAGIC
# MAGIC All 3 affected rows are kept with their rule and reason in `training.to6_c3_c399a52409d8.initial_rejected_records` and `outputs/initial_rejected_records.csv`. The decision and rule results are in `outputs/batch_decision.json`.
# MAGIC
# MAGIC **Corrections I did not make:** I did not set `R02`'s income to 0. Zero income is a valid value that produces a null ratio, so filling it in would invent data. I did not drop the second `R01` row. I did not relax or remove a rule.
# MAGIC
# MAGIC **Next action:** `returns_corrected` was assessed with the same rules. Completeness, uniqueness and nonnegative amounts all PASS on its 12 rows, with 0 violations. `R02` now has income 200, and `R01` occurs once. It is therefore the accepted input for the final run, `ACCEPTED_TABLE = training.to6_challenge3_inputs.returns_corrected`. If a future corrected delivery failed, the gate would stop the run before section 3.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

# DBTITLE 1,Build, save and verify return_features
# Only the batch that passed the section 2 gate may feed feature output.
assert ACCEPTED_TABLE == CORRECTED_TABLE, "Run section 2 first; no accepted batch"
FEATURES_TABLE = f"{OUTPUT_SCHEMA}.return_features"
FEATURE_COLUMNS = ["return_id", "tax_year", "deduction_to_income"]

# deduction_to_income = deduction / income; NULL when income = 0 (return is kept, not dropped).
# taxpayer_id, income and deduction are excluded under the need-to-know rule.
features = spark.sql(f"""
    SELECT return_id,
           tax_year,
           CASE WHEN income = 0 THEN CAST(NULL AS DOUBLE)
                ELSE CAST(deduction AS DOUBLE) / income END AS deduction_to_income
    FROM {ACCEPTED_TABLE}
""")
# Overwrite (not append) so reruns leave exactly one copy of each return.
features.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(FEATURES_TABLE)

# ---------- Verification: read the SAVED table back, compare with the source ----------
saved = spark.table(FEATURES_TABLE)
saved_rows = {r["return_id"]: r.asDict() for r in saved.collect()}
source_rows = {r["return_id"]: r.asDict() for r in spark.table(ACCEPTED_TABLE).collect()}

checks = []
def check(name, passed, detail):
    checks.append({"check": name, "result": "PASS" if passed else "FAIL", "detail": str(detail)})

check("exact columns", saved.columns == FEATURE_COLUMNS, saved.columns)
check("no filer id or raw amounts",
      not {"taxpayer_id", "income", "deduction"} & set(saved.columns), saved.columns)
check("row count = accepted returns", saved.count() == len(source_rows), f"{saved.count()} vs {len(source_rows)}")
check("each return_id once", saved.select("return_id").distinct().count() == saved.count(),
      f"{saved.select('return_id').distinct().count()} distinct of {saved.count()}")
check("same return_ids as accepted batch", set(saved_rows) == set(source_rows),
      sorted(set(saved_rows) ^ set(source_rows)) or "identical")
check("tax_year unchanged", all(saved_rows[i]["tax_year"] == s["tax_year"] for i, s in source_rows.items()), "all rows")

# Nonzero expected value derived from the source in Python (independent of the SQL above)
src = source_rows["R04"]
expected_r04 = src["deduction"] / src["income"]   # 59 / 400 = 0.1475
actual_r04 = saved_rows["R04"]["deduction_to_income"]
check("R04 ratio = deduction/income from source",
      actual_r04 is not None and abs(actual_r04 - expected_r04) < 1e-12,
      f"source {src['deduction']}/{src['income']} = {expected_r04}; saved {actual_r04}")

# Every nonzero-income return matches deduction/income from the source
mismatches = [i for i, s in source_rows.items() if s["income"] != 0
              and abs(saved_rows[i]["deduction_to_income"] - s["deduction"] / s["income"]) > 1e-12]
check("all nonzero ratios match source", not mismatches, mismatches or "all match")

# Zero-income returns are kept with a NULL ratio
zero_ids = sorted(i for i, s in source_rows.items() if s["income"] == 0)
check("zero-income returns kept with NULL ratio",
      bool(zero_ids) and all(i in saved_rows and saved_rows[i]["deduction_to_income"] is None for i in zero_ids),
      {i: saved_rows.get(i) for i in zero_ids})
null_ids = sorted(i for i, r in saved_rows.items() if r["deduction_to_income"] is None)
check("NULL ratio only for zero income", null_ids == zero_ids, null_ids)

checks_df = spark.createDataFrame(checks)
display(checks_df)
assert all(c["result"] == "PASS" for c in checks), "Feature table verification failed"

print("Read-back of", FEATURES_TABLE)
display(saved.orderBy("tax_year", "return_id"))

# Small synthetic exports for the submission
saved.orderBy("return_id").toPandas().to_csv(repo_root / "outputs/return_features.csv", index=False)
checks_df.toPandas().to_csv(repo_root / "outputs/return_features_checks.csv", index=False)

# COMMAND ----------

# DBTITLE 1,Feature table contract and verification results
# MAGIC %md
# MAGIC ### Feature table: `training.to6_c3_c399a52409d8.return_features`
# MAGIC
# MAGIC * **Input:** `ACCEPTED_TABLE` = `training.to6_challenge3_inputs.returns_corrected`. Section 2's gate must pass before this table is built; the rejected initial batch is never used.
# MAGIC * **Columns:** `return_id` (string), `tax_year` (int), `deduction_to_income` (double).
# MAGIC * **Ratio:** `deduction_to_income = deduction / income`. If `income = 0`, the ratio is NULL and the return stays in the table.
# MAGIC * **Need-to-know:** `taxpayer_id`, `income` and `deduction` are not written.
# MAGIC * **Rerun safety:** the table is written in overwrite mode, so rerunning the notebook leaves one copy of each return.
# MAGIC
# MAGIC **Read-back verification:** all 10 checks PASS on the saved table.
# MAGIC * **Contents:** exactly the three columns above, 12 rows and 12 distinct `return_id` values. These are the same IDs and tax years as the accepted batch.
# MAGIC * **Nonzero expected value:** for `R04`, the source has deduction 59 and income 400, so the expected ratio is 59 / 400 = **0.1475**. The saved value is 0.1475. All other nonzero-income ratios also match the source.
# MAGIC * **Zero income:** `R03` (2022) and `R09` (2024) have source income 0. Both are in the table with a NULL ratio, and they are the only rows with a NULL ratio.
# MAGIC
# MAGIC The check results are exported to `outputs/return_features_checks.csv`, and the table itself to `outputs/return_features.csv`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# DBTITLE 1,Save year splits, verify, snapshot this run
# Splits are built FROM THE SAVED feature table, one tax year each, overwritten on every run.
SPLITS = {2022: f"{OUTPUT_SCHEMA}.features_train",
          2023: f"{OUTPUT_SCHEMA}.features_validation",
          2024: f"{OUTPUT_SCHEMA}.features_test"}
saved_features = spark.table(FEATURES_TABLE)
for year, table in SPLITS.items():
    (saved_features.where(F.col("tax_year") == year)
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table))

# ---------- Verify the saved splits by reading them back ----------
split_checks = []
def scheck(name, passed, detail):
    split_checks.append({"check": name, "result": "PASS" if passed else "FAIL", "detail": str(detail)})

union_splits = None
for year, table in SPLITS.items():
    t = spark.table(table)
    years = sorted(r[0] for r in t.select("tax_year").distinct().collect())
    scheck(f"{table.split('.')[-1]} only {year}", years == [year], f"years={years}, rows={t.count()}")
    scheck(f"{table.split('.')[-1]} columns", t.columns == FEATURE_COLUMNS, t.columns)
    tagged = t.withColumn("split_table", F.lit(table))
    union_splits = tagged if union_splits is None else union_splits.unionByName(tagged)

id_counts = union_splits.groupBy("return_id").count()
scheck("every accepted return in exactly one split",
       id_counts.where("count <> 1").count() == 0 and id_counts.count() == saved_features.count(),
       f"{id_counts.count()} ids in splits, {saved_features.count()} in return_features")
missing_or_extra = (union_splits.drop("split_table").exceptAll(saved_features).count()
                    + saved_features.exceptAll(union_splits.drop("split_table")).count())
scheck("splits == return_features (ids and values)", missing_or_extra == 0, f"{missing_or_extra} differing rows")

split_checks_df = spark.createDataFrame(split_checks)
display(split_checks_df)
assert all(c["result"] == "PASS" for c in split_checks), "Split verification failed"
display(union_splits.groupBy("split_table", "tax_year").count().orderBy("tax_year"))

# ---------- Exports and run snapshot (actual IDs and values) ----------
for year, table in SPLITS.items():
    spark.table(table).orderBy("return_id").toPandas().to_csv(
        repo_root / f"outputs/{table.split('.')[-1]}.csv", index=False)
split_checks_df.toPandas().to_csv(repo_root / "outputs/split_checks.csv", index=False)

snapshot = (saved_features.withColumn("table", F.lit(FEATURES_TABLE))
            .unionByName(union_splits.withColumnRenamed("split_table", "table"))
            .select("table", "return_id", "tax_year", "deduction_to_income")
            .orderBy("table", "return_id").toPandas())
snap_dir = repo_root / "outputs/run_snapshots"
snap_dir.mkdir(parents=True, exist_ok=True)
snap_name = f"run_{RUN_STARTED_AT.replace(':', '').replace('-', '')}"
snapshot.to_csv(snap_dir / f"{snap_name}.csv", index=False, float_format="%.17g")
(snap_dir / f"{snap_name}.json").write_text(json.dumps({
    "run_started_at_utc": RUN_STARTED_AT,
    "run_completed_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "notebook_url": NOTEBOOK_URL, "input": ACCEPTED_TABLE,
    "tables": {t: int((snapshot["table"] == t).sum()) for t in snapshot["table"].unique()},
}, indent=2) + "\n", encoding="utf-8")
print("Snapshot written:", snap_name, "rows:", len(snapshot))

# COMMAND ----------

# DBTITLE 1,Compare the two latest complete runs
import pandas as pd

# Snapshots are only written at the end of a COMPLETE run (after every assert passed).
snaps = sorted(snap_dir.glob("run_*.csv"))
print("Complete-run snapshots:", [p.stem for p in snaps])
if len(snaps) < 2:
    print("Only one complete run so far: run the whole notebook again to produce the comparison.")
else:
    prev_path, curr_path = snaps[-2], snaps[-1]
    prev = pd.read_csv(prev_path, dtype=str, keep_default_na=False)
    curr = pd.read_csv(curr_path, dtype=str, keep_default_na=False)
    key = ["table", "return_id"]
    merged = prev.merge(curr, on=key, how="outer", suffixes=("_run1", "_run2"), indicator=True)
    diffs = merged[(merged["_merge"] != "both")
                   | (merged["tax_year_run1"] != merged["tax_year_run2"])
                   | (merged["deduction_to_income_run1"] != merged["deduction_to_income_run2"])]
    per_table = curr.groupby("table").agg(rows=("return_id", "size"), distinct_ids=("return_id", "nunique"))
    comparison = {
        "run1": json.loads(prev_path.with_suffix(".json").read_text()),
        "run2": json.loads(curr_path.with_suffix(".json").read_text()),
        "run1_snapshot": f"outputs/run_snapshots/{prev_path.name}",
        "run2_snapshot": f"outputs/run_snapshots/{curr_path.name}",
        "rows_compared": int(len(merged)),
        "differing_rows": int(len(diffs)),
        "identical_ids_and_values": bool(len(diffs) == 0 and len(prev) == len(curr)),
        "run2_rows_per_table": {t: {"rows": int(r.rows), "distinct_ids": int(r.distinct_ids)}
                                for t, r in per_table.iterrows()},
        "no_appended_copies": bool((per_table["rows"] == per_table["distinct_ids"]).all()),
    }
    (repo_root / "outputs/run_comparison.json").write_text(json.dumps(comparison, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(comparison, indent=2))
    assert comparison["identical_ids_and_values"], diffs
    assert comparison["no_appended_copies"]

# COMMAND ----------

# DBTITLE 1,Handoff
# MAGIC %md
# MAGIC ### Handoff
# MAGIC
# MAGIC * **Inputs:**
# MAGIC   * `training.to6_challenge3_inputs.returns_corrected`: accepted.
# MAGIC   * `training.to6_challenge3_inputs.returns_initial`: rejected on completeness (`R02` has NULL income) and uniqueness (`R01` appears 2 times).
# MAGIC   * Both come from `training.to6_challenge3_inputs.parsed_source`, according to `source_lineage`.
# MAGIC * **Outputs (`training.to6_c3_c399a52409d8`):**
# MAGIC   * `return_features`: 12 rows.
# MAGIC   * `features_train`: 2022, 4 rows.
# MAGIC   * `features_validation`: 2023, 4 rows.
# MAGIC   * `features_test`: 2024, 4 rows.
# MAGIC   * `initial_rejected_records`: 3 rows.
# MAGIC * **Ratio:** `deduction_to_income = deduction / income`, NULL when `income = 0` (`R03`, `R09`). Only `return_id`, `tax_year` and the ratio are saved.
# MAGIC * **Checks performed:**
# MAGIC   * The quality gate runs on both batches.
# MAGIC   * 10 read-back checks pass on `return_features`, including `R04` = 59 / 400 = 0.1475 and a NULL ratio for zero income.
# MAGIC   * 8 split checks pass: each split holds only its year, and each accepted return appears in exactly one split.
# MAGIC   * Runs 1 (2026-11-20T16:50:29Z) and 2 (16:51:59Z) are identical across 24 rows, with no appended copies (`outputs/run_comparison.json`).
# MAGIC * **Not verified:**
# MAGIC   * Catalog UI lineage.
# MAGIC   * The `parsed_source` contents.
# MAGIC   * The GitHub commit and push.
# MAGIC   * Model leakage, which is out of scope here.
# MAGIC
# MAGIC The full details are in `SUBMISSION.md`.