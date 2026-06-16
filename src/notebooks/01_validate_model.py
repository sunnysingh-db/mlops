# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Validate Existing Model (Migrate Mode)
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC When migrating an existing pickle/model file into Databricks MLOps, this notebook
# MAGIC validates that the model loads correctly and produces expected predictions.
# MAGIC
# MAGIC ## How It Works
# MAGIC 1. Loads the model from the specified Volume path
# MAGIC 2. Runs a smoke test on sample data
# MAGIC 3. Compares predictions against a reference table (>= 99% match required)
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Model file uploaded to a UC Volume (configured in `config.yaml` → `migrate.model_path`)
# MAGIC - Reference table with known-good predictions (for validation)
# MAGIC
# MAGIC ## Configuration (from config.yaml)
# MAGIC | Key | Description |
# MAGIC |-----|-------------|
# MAGIC | `migrate.model_path` | Path to pickle file in UC Volume |
# MAGIC | `migrate.model_type` | "sklearn", "xgboost", "lightgbm", or "pytorch" |
# MAGIC | `migrate.reference_table` | Table with expected predictions for validation |
# MAGIC
# MAGIC ## Next Steps
# MAGIC → `02_feature_engineering.py` (define features for production inference)
# MAGIC → `03_wrap_and_register.py` (wrap as pyfunc and register)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Model from Volume

# COMMAND ----------

import sys, pickle
from pathlib import Path
import pandas as pd
import numpy as np

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
framework_dir = str(Path(notebook_path).parent.parent.parent)
sys.path.insert(0, f"/Workspace{framework_dir}/src/notebooks")

from helpers import load_config
config = load_config()

# Self-skip gate
if config["mode"] != "migrate":
    print("Mode is not migrate. Skipping.")
    dbutils.notebook.exit("skipped - not migrate mode")

model_path = config["migrate"]["model_path"]
print(f"Loading model from: {model_path}")

with open(model_path, "rb") as f:
    model = pickle.load(f)

print(f"Model type: {type(model).__name__}")
print(f"Model loaded successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Smoke Test
# MAGIC Verify the model can produce predictions on sample data.

# COMMAND ----------

ref_table = config["migrate"]["reference_table"]
ref_df = spark.table(ref_table).limit(100).toPandas()

# Separate features from label
label_col = config["label_column"]
X_sample = ref_df.drop(columns=[label_col, config["entity_key"]], errors="ignore")

preds = model.predict(X_sample)
print(f"Smoke test passed: {len(preds)} predictions generated")
print(f"Sample predictions: {preds[:5]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Reference Validation
# MAGIC Compare predictions against known-good reference to ensure >= 99%% match.

# COMMAND ----------

# If reference has a prediction column, compare
if "prediction" in ref_df.columns:
    expected = ref_df["prediction"].values
    match_rate = np.mean(np.isclose(preds, expected, rtol=0.01))
    print(f"Match rate: {match_rate:.2%}")
    assert match_rate >= 0.99, f"Match rate {match_rate:.2%} < 99%%. Model may have changed."
    print("Reference validation PASSED")
else:
    print("No prediction column in reference table. Skipping reference validation.")
    print("(Add a prediction column to reference table for full validation)")

print("\nModel is valid. Proceed to 03_wrap_and_register.py")

