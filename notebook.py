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
print({"input_schema": INPUT_SCHEMA, "output_schema": OUTPUT_SCHEMA})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understand the input
# MAGIC Record Catalog descriptions, sample observations and the supplied lineage source.

# COMMAND ----------

# MAGIC %md
# MAGIC return_initial , return_corrected:
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

# Your checks and decision here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

# Your implementation and verification here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# Your implementation, rerun comparison and exports here.