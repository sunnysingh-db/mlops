# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Holdout Evaluation
# MAGIC
# MAGIC ## Purpose
# MAGIC Score the locked test set with the best model from training.
# MAGIC Audit train/val gap, log test metrics, produce diagnostic plots.
# MAGIC This is the ONLY place the test set is ever used.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - `03_train_tune.py` completed (best model logged, test split saved)
# MAGIC
# MAGIC ## Outputs
# MAGIC - test_ prefixed metrics in MLflow
# MAGIC - Diagnostic plots (confusion matrix / residuals)
# MAGIC - Pass/fail verdict for model promotion
# MAGIC
# MAGIC ## Next Step
# MAGIC → `04b_explainability.py` (if enabled) or `05_register.py`

# COMMAND ----------

import sys, json
from pathlib import Path

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
framework_dir = str(Path(notebook_path).parent.parent.parent)
sys.path.insert(0, f"/Workspace{framework_dir}/src/notebooks")

from helpers import load_config, append_deployment_log
import mlflow
import pandas as pd
import numpy as np

config = load_config()
task_type = config["task_type"]
label_column = config["label_column"]
positive_label = config["train"].get("positive_label", 1)

# Retrieve training context
try:
    train_context = json.loads(dbutils.jobs.taskValues.get(taskKey="03_train_tune", key="train_context"))
except Exception:
    # Fallback for interactive mode: get latest run
    train_context = None
    print("⚠️  Running interactively — using latest experiment run")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load Best Model
# MAGIC
# MAGIC Loads the model artifact from the best training run. The `run_id` is passed
# MAGIC from `03_train_tune.py` via task values (in DAB jobs) or can be set manually.

# COMMAND ----------

if train_context:
    best_run_id = train_context["best_run_id"]
else:
    # Get most recent run with a logged model
    experiment_name = f"/Users/{config.get('_current_user', 'shared')}/{config['model_name']}_experiment"
    runs = mlflow.search_runs(experiment_names=[experiment_name],
                              filter_string="tags.mlflow.log-model.history != ''",
                              order_by=["start_time DESC"], max_results=1)
    best_run_id = runs.iloc[0].run_id

print(f"Loading model from run: {best_run_id}")
model_uri = f"runs:/{best_run_id}/model"
model = mlflow.sklearn.load_model(model_uri)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Score Test Set
# MAGIC
# MAGIC Runs predictions on the test set. Remember: this is the FIRST and ONLY time
# MAGIC the model sees this data. These predictions represent real-world performance.
# MAGIC **⚠️ This test set has NEVER been seen during training or tuning.**

# COMMAND ----------

test_table = f"{config['catalog']}.{config['schema']}.{config['model_name']}_test_split"
test_pdf = spark.table(test_table).toPandas()

y_test = test_pdf[label_column]
X_test = test_pdf.drop(columns=[label_column])

y_pred = model.predict(X_test)
if task_type == "classification" and hasattr(model, "predict_proba"):
    y_proba = model.predict_proba(X_test)[:, 1]

print(f"Test set: {len(y_test):,} samples scored")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Compute Test Metrics
# MAGIC
# MAGIC All metrics are prefixed with `test_` to distinguish from training/validation metrics.
# MAGIC These are the numbers you should report to stakeholders.

# COMMAND ----------

from sklearn.metrics import (f1_score, precision_score, recall_score,
    average_precision_score, roc_auc_score, accuracy_score,
    mean_squared_error, mean_absolute_error, r2_score)

with mlflow.start_run(run_id=best_run_id):
    if task_type == "classification":
        metrics = {
            "test_f1": f1_score(y_test, y_pred, pos_label=positive_label),
            "test_precision": precision_score(y_test, y_pred, pos_label=positive_label),
            "test_recall": recall_score(y_test, y_pred, pos_label=positive_label),
            "test_accuracy": accuracy_score(y_test, y_pred),
        }
        if hasattr(model, "predict_proba"):
            metrics["test_roc_auc"] = roc_auc_score(y_test, y_proba)
            metrics["test_pr_auc"] = average_precision_score(y_test, y_proba)
    else:
        metrics = {
            "test_rmse": mean_squared_error(y_test, y_pred, squared=False),
            "test_mae": mean_absolute_error(y_test, y_pred),
            "test_r2": r2_score(y_test, y_pred),
        }

    mlflow.log_metrics(metrics)

print("Test Metrics:")
for k, v in metrics.items():
    print(f"  {k}: {v:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Train/Val vs Test Gap Audit
# MAGIC
# MAGIC **Why this matters:** If train metrics are much better than test metrics (>10%% gap),
# MAGIC the model is overfitting — it memorized training data instead of learning patterns.
# MAGIC This cell flags that issue automatically.
# MAGIC **What this does:** Checks if test performance is significantly worse than validation.
# MAGIC A large gap (>10%) suggests overfitting.

# COMMAND ----------

if train_context:
    val_score = train_context["best_score"]
    obj_metric = train_context["objective_metric"]
    test_score = metrics.get(f"test_{obj_metric.replace('val_', '')}", None)

    if test_score is not None:
        if "rmse" in obj_metric:
            gap_pct = ((test_score - val_score) / val_score) * 100
            is_healthy = gap_pct < 15  # RMSE: test should not be >15% worse
        else:
            gap_pct = ((val_score - test_score) / val_score) * 100
            is_healthy = gap_pct < 10  # F1/AUC: test should not drop >10%

        print(f"\nVal {obj_metric}: {val_score:.4f}")
        print(f"Test equivalent: {test_score:.4f}")
        print(f"Gap: {gap_pct:.1f}%")
        if is_healthy:
            print("✅ Healthy generalization (gap < threshold)")
        else:
            print("⚠️  OVERFITTING WARNING: gap exceeds threshold")
            print("  Consider: more regularization, fewer features, or more data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Diagnostic Plots
# MAGIC
# MAGIC Visual diagnostics help you understand WHERE the model fails:
# MAGIC - **Classification**: Confusion matrix (which classes are confused?) + ROC curve
# MAGIC - **Regression**: Residuals plot (are errors random or systematic?)

# COMMAND ----------

import matplotlib.pyplot as plt
from sklearn.metrics import ConfusionMatrixDisplay, RocCurveDisplay

if task_type == "classification":
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    ConfusionMatrixDisplay.from_predictions(y_test, y_pred, ax=axes[0], cmap="Blues")
    axes[0].set_title("Confusion Matrix (Test)")
    if hasattr(model, "predict_proba"):
        RocCurveDisplay.from_predictions(y_test, y_proba, ax=axes[1])
        axes[1].set_title("ROC Curve (Test)")
    plt.tight_layout()
    plt.show()
else:
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    residuals = y_test - y_pred
    axes[0].scatter(y_pred, residuals, alpha=0.3)
    axes[0].axhline(0, color="red", linestyle="--")
    axes[0].set_title("Residuals vs Predicted")
    axes[0].set_xlabel("Predicted")
    axes[0].set_ylabel("Residual")
    axes[1].hist(residuals, bins=50, color="steelblue")
    axes[1].set_title("Residual Distribution")
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC Holdout evaluation complete. Test metrics logged to MLflow.
# MAGIC → **Next:** `04b_explainability.py` or `05_register.py`

# COMMAND ----------

append_deployment_log(
    event="holdout_evaluated",
    resource=f"{config['catalog']}.{config['schema']}.{config['model_name']}",
    version=f"run_id={best_run_id}",
    notes=", ".join(f"{k}={v:.4f}" for k, v in metrics.items())
)

# Pass context downstream
dbutils.jobs.taskValues.set(key="eval_context", value=json.dumps({
    "best_run_id": best_run_id,
    "test_metrics": metrics,
}))
