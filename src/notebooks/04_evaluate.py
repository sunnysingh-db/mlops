# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ml_v5"
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # 04 — Holdout Evaluation
# MAGIC
# MAGIC ## Purpose
# MAGIC Score the locked test set with the best model from training using `mlflow.evaluate()`.
# MAGIC This single API call computes all metrics, generates diagnostic plots, and logs everything
# MAGIC to MLflow automatically. This is the ONLY place the test set is ever used.
# MAGIC
# MAGIC ## What `mlflow.evaluate()` gives you (automatically):
# MAGIC - **Classification**: accuracy, F1, precision, recall, ROC-AUC, PR-AUC, log loss, confusion matrix, ROC curve, precision-recall curve, SHAP plots
# MAGIC - **Regression**: RMSE, MAE, R², residual plots
# MAGIC - All metrics logged under the same run as the trained model
# MAGIC - All plots saved as MLflow artifacts (viewable in the experiment UI)
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - `03_train_tune.py` completed (best model logged, test split saved)
# MAGIC
# MAGIC ## Next Step
# MAGIC → `04b_explainability.py` (if enabled) or `05_register.py`

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install pyyaml lightgbm mlflow scikit-learn databricks-feature-engineering --quiet

# COMMAND ----------

# DBTITLE 1,Restart Python
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Setup
import sys, json, warnings
from pathlib import Path
warnings.filterwarnings("ignore")

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
framework_dir = str(Path(notebook_path).parent.parent.parent)
sys.path.insert(0, f"/Workspace{framework_dir}/src/notebooks")

from helpers import load_config, append_deployment_log
import mlflow

config = load_config()
task_type = config["task_type"]
label_column = config["label_column"]
positive_label = config["train"].get("positive_label", 1)
mlflow.set_registry_uri("databricks-uc")

# Retrieve training context
try:
    train_context = json.loads(dbutils.jobs.taskValues.get(taskKey="03_train_tune", key="train_context"))
except Exception:
    train_context = None
    print("⚠️  Running interactively — using latest experiment run")

# COMMAND ----------

# DBTITLE 1,Cell 5
# MAGIC %md
# MAGIC ## Step 1: Resolve Best Run
# MAGIC
# MAGIC Finds the best model run from training. No need to manually load the model —
# MAGIC `mlflow.evaluate()` loads it directly from the run URI.

# COMMAND ----------

# DBTITLE 1,Resolve Best Run
if train_context:
    best_run_id = train_context["best_run_id"]
    print(f"Source: task_values")
else:
    # Interactive mode: find the latest 'best_model_*' run (the final retrained model)
    experiment_name = f"/Users/{config.get('_current_user', 'shared')}/{config['model_name']}_experiment"
    runs = mlflow.search_runs(
        experiment_names=[experiment_name],
        filter_string="attributes.run_name LIKE 'best_model_%'",
        order_by=["start_time DESC"],
        max_results=1,
    )
    if len(runs) == 0:
        raise ValueError(f"No 'best_model_*' runs found. Run 03_train_tune first.")
    best_run_id = runs.iloc[0].run_id
    print(f"Source: mlflow (latest best_model run)")

print(f"Run ID:    {best_run_id}")

# COMMAND ----------

# DBTITLE 1,Cell 7
# MAGIC %md
# MAGIC ## Step 2: Evaluate with `mlflow.evaluate()`
# MAGIC
# MAGIC **One call does everything:**
# MAGIC 1. Loads the model from MLflow
# MAGIC 2. Scores the test set
# MAGIC 3. Computes all relevant metrics (F1, precision, recall, ROC-AUC, PR-AUC, log loss, etc.)
# MAGIC 4. Generates diagnostic plots (confusion matrix, ROC curve, PR curve, lift curve)
# MAGIC 5. Logs everything as metrics + artifacts to the MLflow run
# MAGIC
# MAGIC **⚠️ This test set has NEVER been seen during training or tuning.**

# COMMAND ----------

# DBTITLE 1,Cell 6
import pandas as pd
import pickle, os, logging

# Suppress MLflow info/warning logs and tqdm progress bars (SHAP, artifact download)
logging.getLogger("mlflow").setLevel(logging.ERROR)
os.environ["TQDM_DISABLE"] = "1"

# Load test set
test_table = f"{config['catalog']}.{config['schema']}.{config['model_name']}_test_split"
test_pdf = spark.table(test_table).toPandas()
print(f"Test set: {len(test_pdf):,} samples")

# Why we load the raw sklearn pipeline instead of using model_uri directly:
# The model was logged with fe.log_model() which wraps it as a PyFunc that expects
# entity keys (customer_id, event_date) as input — it tries to look up features at
# inference time. Our test split already has features pre-joined (the correct snapshot
# from training time). Passing model_uri to mlflow.evaluate() would fail with:
#   "Model is missing inputs ['customer_id', 'event_date']"
# So we extract the raw sklearn pipeline and pass it as a function instead.
artifact_path = mlflow.artifacts.download_artifacts(run_id=best_run_id, artifact_path="model")
for root, dirs, files in os.walk(artifact_path):
    for f in files:
        if f == "model.pkl":
            with open(os.path.join(root, f), "rb") as fh:
                sklearn_model = pickle.load(fh)
            break

# mlflow.evaluate() with the sklearn model wrapped as a callable function
model_type = "classifier" if task_type == "classification" else "regressor"

def predict_fn(df):
    return sklearn_model.predict(df)

with mlflow.start_run(run_id=best_run_id):
    eval_result = mlflow.evaluate(
        model=predict_fn,
        data=test_pdf,
        targets=label_column,
        model_type=model_type,
        evaluators="default",
    )

# Print results
metrics = eval_result.metrics
print(f"\n{'=' * 60}")
print(f"  TEST SET EVALUATION (via mlflow.evaluate)")
print(f"{'=' * 60}")
for k, v in sorted(metrics.items()):
    if isinstance(v, float):
        print(f"  {k:<30s} {v:.4f}")
print(f"{'=' * 60}")

# Print experiment URL for viewing plots (confusion matrix, ROC, PR curve, SHAP)
workspace_host = spark.conf.get("spark.databricks.workspaceUrl")
run_url = f"https://{workspace_host}/ml/experiments/{mlflow.get_run(best_run_id).info.experiment_id}/runs/{best_run_id}"
print(f"\n✅ All metrics + diagnostic plots logged to MLflow.")
print(f"   View results: {run_url}")

# COMMAND ----------

# DBTITLE 1,Cell 9
# MAGIC %md
# MAGIC ## Summary & Log
# MAGIC Holdout evaluation complete. All metrics + plots logged to MLflow.
# MAGIC → **Next:** `04b_explainability.py` (if enabled) or `05_register.py`

# COMMAND ----------

# DBTITLE 1,Log & Pass Context
# Log key metrics to deployment log
key_metrics = {k: v for k, v in metrics.items() if isinstance(v, float)}
append_deployment_log(
    event="holdout_evaluated",
    resource=f"{config['catalog']}.{config['schema']}.{config['model_name']}",
    version=f"run_id={best_run_id}",
    notes=", ".join(f"{k}={v:.4f}" for k, v in list(key_metrics.items())[:6])
)

# Pass context downstream
try:
    dbutils.jobs.taskValues.set(key="eval_context", value=json.dumps({
        "best_run_id": best_run_id,
        "test_metrics": key_metrics,
    }))
except Exception:
    pass  # Not in a job context
