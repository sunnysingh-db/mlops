# Databricks notebook source
# MAGIC %md
# MAGIC # 04b — Model Explainability (SHAP + Feature Importance)
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC Explains **why** the model makes its predictions using:
# MAGIC - **SHAP values**: Shows each feature's contribution to each prediction
# MAGIC - **Permutation importance**: Shows which features matter most overall
# MAGIC - **Partial dependence**: Shows how changing one feature affects predictions
# MAGIC
# MAGIC ## Why This Matters
# MAGIC - Stakeholders need to understand WHY a customer is predicted to churn
# MAGIC - Regulators may require model explanations (e.g., credit decisions)
# MAGIC - Helps debug unexpected predictions and identify data leakage
# MAGIC
# MAGIC ## Configuration (from config.yaml → explainability)
# MAGIC | Key | Description |
# MAGIC |-----|-------------|
# MAGIC | `explainability.enabled` | Toggle on/off |
# MAGIC | `explainability.methods` | Which methods to run |
# MAGIC | `explainability.n_samples` | Subsample size (SHAP is slow on large data) |
# MAGIC | `explainability.log_to_mlflow` | Whether to log plots as MLflow artifacts |
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Model trained and evaluated (from `04_evaluate.py`)
# MAGIC
# MAGIC ## Next Steps
# MAGIC → `05_register.py` (register model if satisfied with explanations)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup & Self-Skip Gate

# COMMAND ----------

import sys
from pathlib import Path

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
framework_dir = str(Path(notebook_path).parent.parent.parent)
sys.path.insert(0, f"/Workspace{framework_dir}/src/notebooks")

from helpers import load_config
config = load_config()

# Self-skip gate
if not config["explainability"]["enabled"]:
    print("Explainability disabled in config.yaml.")
    print("To enable: set explainability.enabled to true")
    dbutils.notebook.exit("skipped - explainability disabled")

methods = config["explainability"]["methods"]
n_samples = config["explainability"]["n_samples"]
print(f"Methods:   {methods}")
print(f"N samples: {n_samples:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Model & Test Data

# COMMAND ----------

import mlflow
import numpy as np

# Get best run_id from upstream
try:
    run_id = dbutils.jobs.taskValues.get(taskKey="03_train_tune", key="best_run_id")
except Exception:
    run_id = dbutils.widgets.get("run_id") if "run_id" in dbutils.widgets.getAll() else None

if not run_id:
    raise ValueError("No run_id found. Run 03_train_tune.py first.")

model = mlflow.sklearn.load_model(f"runs:/{run_id}/model")
print(f"Model loaded from run: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: SHAP Analysis
# MAGIC
# MAGIC SHAP (SHapley Additive exPlanations) shows how each feature pushes
# MAGIC the prediction up or down from the baseline. Positive SHAP = increases prediction.

# COMMAND ----------

# Full implementation in next build phase
# (SHAP TreeExplainer, summary plot, force plots, permutation importance, PDP)
print("04b Explainability — implementation coming in next phase")
print("Planned: SHAP summary plot, permutation importance, partial dependence")

