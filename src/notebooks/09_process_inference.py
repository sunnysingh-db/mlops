# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Process Inference Table
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC The serving endpoint logs raw requests/responses as JSON to an **inference table**.
# MAGIC This notebook flattens that JSON into a clean, queryable Delta table for:
# MAGIC - Monitoring and dashboards
# MAGIC - Ad-hoc analysis of model behavior
# MAGIC - Joining predictions back to ground truth
# MAGIC
# MAGIC ## How It Works
# MAGIC 1. Reads the raw inference table (auto-created by serving endpoint)
# MAGIC 2. Extracts/flattens the JSON request/response payloads
# MAGIC 3. Converts timestamps from unix to human-readable
# MAGIC 4. Writes to a processed table
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Serving endpoint running with inference logging enabled (from `07_serve.py`)
# MAGIC - Some traffic must have hit the endpoint
# MAGIC
# MAGIC ## Output
# MAGIC - Processed table: `{catalog}.{schema}.{model_name}_inference_processed`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup

# COMMAND ----------

import sys
from pathlib import Path

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
framework_dir = str(Path(notebook_path).parent.parent.parent)
sys.path.insert(0, f"/Workspace{framework_dir}/src/notebooks")

from helpers import load_config
from pyspark.sql.functions import from_unixtime, col

config = load_config()

# Self-skip gate
if config["inference_mode"] not in ["serving", "both"]:
    print("Serving not enabled. No inference table to process.")
    dbutils.notebook.exit("skipped - serving not enabled")

inference_table = f"{config['catalog']}.{config['schema']}.{config['model_name']}_payload"
output_table = f"{config['catalog']}.{config['schema']}.{config['model_name']}_inference_processed"
print(f"Raw table:       {inference_table}")
print(f"Processed table: {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Flatten Inference Logs
# MAGIC Extract JSON fields and convert unix timestamps to readable format.

# COMMAND ----------

raw_df = spark.table(inference_table)

# Flatten: extract key columns and convert timestamps
processed_df = (
    raw_df
    .withColumn("request_time", from_unixtime(col("timestamp_ms") / 1000))
    .withColumn("scoring_duration_ms", col("execution_time_ms"))
    .select(
        "request_time",
        "scoring_duration_ms",
        "request",
        "response",
        "status_code",
        col("timestamp_ms").alias("timestamp_epoch_ms"),
    )
)

processed_df.write.mode("append").saveAsTable(output_table)
count = processed_df.count()
print(f"Processed {count:,} inference records to {output_table}")

