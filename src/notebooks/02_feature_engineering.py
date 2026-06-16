# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # 02 — Feature Engineering
# MAGIC
# MAGIC ## Purpose
# MAGIC Compute features from source tables and write them to the feature table.
# MAGIC This is the ONLY notebook (besides config) that you MUST customize.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - `01_eda.py` has been run (data profiled, no leakage)
# MAGIC - Source tables identified from EDA Step 5
# MAGIC
# MAGIC ## Outputs
# MAGIC - Feature table (Feature Store): `{config.feature_table_name}`
# MAGIC - Registered with primary key + timestamp for FE Client lookups
# MAGIC
# MAGIC ## Next Step
# MAGIC → `03_train_tune.py` (train mode) or `03_wrap_and_register.py` (migrate mode)

# COMMAND ----------

# Setup & Config
import sys
from pathlib import Path

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
framework_dir = str(Path(notebook_path).parent.parent.parent)
sys.path.insert(0, f"/Workspace{framework_dir}/src/notebooks")

from helpers import load_config
config = load_config()

entity_key = config["entity_key"]
timestamp_key = config["timestamp_key"]
feature_table_name = config["feature_table_name"]

print(f"Entity key:     {entity_key}")
print(f"Timestamp key:  {timestamp_key}")
print(f"Output table:   {feature_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 YOUR FEATURE LOGIC — Edit Below
# MAGIC
# MAGIC Define your feature computation using Spark SQL or PySpark.
# MAGIC **Requirements:**
# MAGIC - Output MUST contain the `entity_key` column (`customer_id`)
# MAGIC - Output MUST contain the `timestamp_key` column (`event_timestamp`)
# MAGIC - All other columns become model features
# MAGIC
# MAGIC See samples below — uncomment and adapt for your use case.

# COMMAND ----------

# DBTITLE 1,YOUR FEATURE LOGIC — Edit Below
# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  🔧 USER ACTION REQUIRED — Define your features here                       ║
# ╠══════════════════════════════════════════════════════════════════════════╣
# ║  Uncomment ONE of the samples below, or write your own.                    ║
# ║  Output must be a Spark DataFrame with entity_key + timestamp_key + features║
# ╚══════════════════════════════════════════════════════════════════════════╝

# ─── SAMPLE 1: Aggregation features ──────────────────────────────────────────
# features_df = spark.sql("""
#     SELECT customer_id, event_timestamp,
#            SUM(amount) as total_spend_30d,
#            COUNT(*) as txn_count_30d,
#            AVG(amount) as avg_txn_amount,
#            MAX(amount) as max_txn_amount,
#            DATEDIFF(current_date(), MAX(txn_date)) as days_since_last_txn
#     FROM catalog.schema.transactions
#     WHERE txn_date >= date_sub(current_date(), 30)
#     GROUP BY customer_id, event_timestamp
# """)

# ─── SAMPLE 2: Window function features ──────────────────────────────────────
# features_df = spark.sql("""
#     SELECT customer_id, event_timestamp,
#            LAG(amount, 1) OVER (PARTITION BY customer_id ORDER BY txn_date) as prev_amount,
#            amount - AVG(amount) OVER (PARTITION BY customer_id) as amount_vs_avg
#     FROM catalog.schema.transactions
# """)

# ─── SAMPLE 3: Lookup / passthrough features ─────────────────────────────────
# features_df = spark.sql("""
#     SELECT customer_id, event_timestamp,
#            DATEDIFF(current_date(), signup_date) as tenure_days,
#            account_type, region, credit_score
#     FROM catalog.schema.customer_profile
# """)

# ─── SAMPLE 4: Join multiple sources ─────────────────────────────────────────
# txn_features = spark.sql("...")
# profile_features = spark.sql("...")
# features_df = txn_features.join(profile_features, on="customer_id", how="left")

# ─── YOUR CODE HERE ──────────────────────────────────────────────────────────
features_df = spark.sql(f"""
    SELECT
        customer_id,
        event_date,
        monthly_sessions,
        monthly_transactions,
        avg_order_value,
        days_since_last_session,
        total_spend_90d,
        support_tickets,
        email_opens_30d,
        subscription_months
    FROM {config['catalog']}.{config['schema']}.customer_activity
""")

# COMMAND ----------

# DBTITLE 1,Install feature engineering library
# MAGIC %pip install databricks-feature-engineering --quiet

# COMMAND ----------

# DBTITLE 1,Restart Python kernel
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Write to Feature Store
# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  ✅ END OF USER-EDITABLE SECTION                                           ║
# ║  Everything below is managed by the framework — do not modify              ║
# ╚══════════════════════════════════════════════════════════════════════════╝

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# Write features to Feature Store with primary key registration.
# This enables: create_training_set() joins, score_batch() auto-lookups, lineage tracking.
try:
    # First run: create the feature table with primary key + timestamp
    fe.create_table(
        name=feature_table_name,
        primary_keys=[entity_key] + ([timestamp_key] if timestamp_key else []),
        timeseries_columns=timestamp_key if timestamp_key else None,
        df=features_df,
        description=f"Features for {config['model_name']} model",
    )
    print(f"✅ Feature table CREATED: {feature_table_name}")
except Exception as e:
    if "already exists" in str(e).lower():
        # Subsequent runs: overwrite existing feature table
        fe.write_table(
            name=feature_table_name,
            df=features_df,
            mode="overwrite",
        )
        print(f"✅ Feature table UPDATED: {feature_table_name}")
    else:
        raise

print(f"   Primary key:      {entity_key}")
print(f"   Timestamp key:    {timestamp_key}")
print(f"   Features:         {len(features_df.columns) - (2 if timestamp_key else 1)} columns")

# COMMAND ----------

# Validate
result_df = spark.table(feature_table_name)
row_count = result_df.count()
null_in_key = result_df.filter(f"{entity_key} IS NULL").count()

print("\n" + "═" * 50)
if row_count > 0 and null_in_key == 0:
    print("  ✅ VALIDATION PASSED")
else:
    print("  ❌ VALIDATION FAILED")
print("═" * 50)
print(f"  Table:       {feature_table_name}")
print(f"  Row count:   {row_count:,}")
print(f"  Null in key: {null_in_key}")
print(f"  Columns:     {len(result_df.columns)}")
print("═" * 50)

assert row_count > 0, "Feature table is empty!"
assert null_in_key == 0, f"{null_in_key} nulls found in entity_key!"

print("→ Next: run 03_train_tune.py")
