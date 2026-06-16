# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Batch Inference
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC Scores a table of entities (e.g., customers) using the **Champion model** registered
# MAGIC in Unity Catalog. It uses the **Feature Engineering Client** to automatically look up
# MAGIC features from the Feature Store — you only need to provide entity IDs.
# MAGIC
# MAGIC ## How It Works
# MAGIC 1. Loads the scoring table (configured in `config.yaml` → `batch.scoring_table`)
# MAGIC 2. Calls `fe.score_batch()` which:
# MAGIC    - Reads the model's packaged feature lookups (saved during training)
# MAGIC    - Auto-fetches features from the Feature Store for each entity
# MAGIC    - Runs predictions
# MAGIC 3. Appends predictions + metadata to the output table
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - A model registered in UC with `@Champion` alias (from `05_register.py`)
# MAGIC - Feature table populated (from `02_feature_engineering.py`)
# MAGIC - Scoring table exists with at least the `entity_key` column
# MAGIC
# MAGIC ## Configuration (from config.yaml)
# MAGIC | Key | Description |
# MAGIC |-----|-------------|
# MAGIC | `batch.scoring_table` | Table containing entities to score (must have entity_key) |
# MAGIC | `batch.output_table` | Where predictions are appended |
# MAGIC | `batch.schedule` | Cron schedule for automated runs (used by DAB job) |
# MAGIC
# MAGIC ## Output
# MAGIC - Predictions appended to `batch.output_table` with `scored_at` timestamp and `model_version`
# MAGIC
# MAGIC ## Next Steps
# MAGIC → `08_monitor.py` (data drift monitoring)
# MAGIC → `07_serve.py` (if real-time serving needed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup & Load Config
# MAGIC Load configuration from `config.yaml` and initialize the Feature Engineering Client.

# COMMAND ----------

import sys
from pathlib import Path

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
framework_dir = str(Path(notebook_path).parent.parent.parent)
sys.path.insert(0, f"/Workspace{framework_dir}/src/notebooks")

from helpers import load_config, append_deployment_log
from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql.functions import lit, current_timestamp

config = load_config()
fe = FeatureEngineeringClient()

# Self-skip gate: only run if batch inference is enabled
if config["inference_mode"] not in ["batch", "both"]:
    print("⏭️ Batch inference not enabled in config.yaml. Skipping.")
    dbutils.notebook.exit("skipped - batch not enabled")

print(f"✅ Config loaded")
print(f"   Model:         {config['catalog']}.{config['schema']}.{config['model_name']}")
print(f"   Scoring table: {config['batch']['scoring_table']}")
print(f"   Output table:  {config['batch']['output_table']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Score with Champion Model
# MAGIC
# MAGIC `fe.score_batch()` does the heavy lifting:
# MAGIC - It knows which features the model needs (stored during `fe.log_model()` in training)
# MAGIC - It automatically fetches those features from the Feature Store
# MAGIC - You only provide a DataFrame with entity IDs — no manual joins needed

# COMMAND ----------

# Build model URI pointing to Champion alias
model_uc_name = f"{config['catalog']}.{config['schema']}.{config['model_name']}"
model_uri = f"models:/{model_uc_name}@Champion"

# Load entities to score
scoring_df = spark.table(config["batch"]["scoring_table"])
print(f"Entities to score: {scoring_df.count():,}")

# Score using Feature Engineering Client (auto-fetches features)
predictions_df = fe.score_batch(model_uri=model_uri, df=scoring_df)

# Add metadata columns
predictions_df = (
    predictions_df
    .withColumn("scored_at", current_timestamp())
    .withColumn("model_version", lit(model_uri))
)

print(f"✅ Scoring complete: {predictions_df.count():,} predictions generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write Predictions
# MAGIC Append predictions to the output table. Using `append` mode so historical
# MAGIC predictions are preserved (useful for monitoring drift over time).

# COMMAND ----------

predictions_df.write.mode("append").saveAsTable(config["batch"]["output_table"])
print(f"✅ Predictions written to: {config['batch']['output_table']}")

# Log to deployment log
append_deployment_log(event="batch_scored", resource=config["batch"]["output_table"], version=model_uri)

