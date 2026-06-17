# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ml_v5"
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 05 — Register Model to Unity Catalog
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC Promotes the best model from training to the **Unity Catalog Model Registry**,
# MAGIC assigns it the `Champion` alias, and demotes the previous champion to `Challenger`.
# MAGIC
# MAGIC ## Why This Matters
# MAGIC - **Aliases** (`@Champion`, `@Challenger`) are how serving endpoints and batch jobs
# MAGIC   reference models without hardcoding version numbers
# MAGIC - When you deploy a new model, downstream consumers automatically pick it up
# MAGIC - The Challenger alias keeps the previous version accessible for A/B testing
# MAGIC
# MAGIC ## How It Works
# MAGIC 1. Gets the best `run_id` from training (passed via task values or from latest run)
# MAGIC 2. Registers the model artifact to Unity Catalog
# MAGIC 3. Sets `Champion` alias on the new version
# MAGIC 4. Moves previous Champion to `Challenger` alias
# MAGIC 5. Logs the event to the deployment log
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - `04_evaluate.py` passed (test metrics are acceptable)
# MAGIC - Model artifact logged via `fe.log_model()` during training
# MAGIC
# MAGIC ## Output
# MAGIC - Model registered at: `{catalog}.{schema}.{model_name}`
# MAGIC - `@Champion` alias set on new version
# MAGIC
# MAGIC ## Next Steps
# MAGIC → `06_batch_inference.py` (score with Champion model)
# MAGIC → `07_serve.py` (deploy real-time endpoint)

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install pyyaml "mlflow[databricks]" --quiet

# COMMAND ----------

# DBTITLE 1,Restart Python
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup & Get Best Run

# COMMAND ----------

# DBTITLE 1,Setup & Get Best Run
import sys
from pathlib import Path
import mlflow

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
framework_dir = str(Path(notebook_path).parent.parent.parent)
sys.path.insert(0, f"/Workspace{framework_dir}/src/notebooks")

from helpers import load_config, append_deployment_log

config = load_config()
mlflow.set_registry_uri("databricks-uc")

model_uc_name = f"{config['catalog']}.{config['schema']}.{config['model_name']}"

# Get best run_id — priority order:
# 1. Job task values (DAB orchestration)
# 2. MLflow experiment: latest 'best_model_*' run (filters out HPO trials & eval runs)
run_id = None
source = None

try:
    run_id = dbutils.jobs.taskValues.get(taskKey="03_train_tune", key="best_run_id")
    source = "task_values"
except Exception:
    pass

if not run_id:
    current_user = config.get('_current_user', 'shared')
    experiment_name = f"/Users/{current_user}/{config['model_name']}_experiment"
    runs = mlflow.search_runs(
        experiment_names=[experiment_name],
        filter_string="attributes.run_name LIKE 'best_model_%'",
        order_by=["start_time DESC"],
        max_results=1,
    )
    if len(runs) > 0:
        run_id = runs.iloc[0].run_id
        source = "mlflow (latest best_model run)"

if not run_id:
    raise ValueError("No run_id found. Run 03_train_tune.py first.")

print(f"Best run_id:  {run_id}")
print(f"Source:       {source}")
print(f"Register to:  {model_uc_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Register Model & Set Aliases
# MAGIC
# MAGIC **Champion** = the model currently serving production traffic.
# MAGIC **Challenger** = the previous champion, kept for rollback or A/B testing.

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()

# Register the model from the best run
model_uri = f"runs:/{run_id}/model"
mv = mlflow.register_model(model_uri=model_uri, name=model_uc_name)
new_version = mv.version
print(f"Registered: {model_uc_name} version {new_version}")

# Demote current Champion to Challenger
try:
    current_champion = client.get_model_version_by_alias(model_uc_name, "Champion")
    client.set_registered_model_alias(model_uc_name, "Challenger", current_champion.version)
    print(f"Previous Champion (v{current_champion.version}) moved to Challenger")
except Exception:
    print("No previous Champion found (first registration)")

# Set new Champion
client.set_registered_model_alias(model_uc_name, "Champion", new_version)
print(f"Champion alias set to version {new_version}")

# Log to deployment log
append_deployment_log(event="model_registered", resource=model_uc_name, version=f"v{new_version}")

# Pass version downstream
try:
    dbutils.jobs.taskValues.set(key="model_version", value=str(new_version))
except Exception:
    pass  # Not running in a job

print(f"\nDone. Model ready at: models:/{model_uc_name}@Champion")

# COMMAND ----------

# DBTITLE 1,Attach Test Metrics to UC Model Version
import json, warnings, logging, os

warnings.filterwarnings("ignore")
logging.getLogger("mlflow").setLevel(logging.ERROR)
os.environ["TQDM_DISABLE"] = "1"

# Attach ALL metrics (train + val + test) and params to the LoggedModel entity.
# This makes them visible on the model in the MLflow UI.

# Get all metrics from the training run
run_data = client.get_run(run_id).data
all_metrics = {k: v for k, v in run_data.metrics.items() if isinstance(v, (int, float))}
all_params = run_data.params

# Also get test metrics from 04_evaluate (may be on a different run)
try:
    eval_context_raw = dbutils.jobs.taskValues.get(taskKey="04_evaluate", key="eval_context")
    eval_context = json.loads(eval_context_raw)
    test_metrics = eval_context.get("test_metrics", {})
    for k, v in test_metrics.items():
        if isinstance(v, (int, float)):
            all_metrics[f"test_{k}"] = v
except Exception:
    pass

if all_metrics:
    model_version_info = client.get_model_version(model_uc_name, new_version)
    model_id = model_version_info.source.replace("models:/", "")

    # Log metrics to LoggedModel
    mlflow.log_metrics(metrics=all_metrics, model_id=model_id)

    print(f"✅ Attached to {model_uc_name} v{new_version}:")
    print(f"   {len(all_metrics)} metrics (train + val + test)")
    print(f"   {len(all_params)} parameters")
else:
    print("⚠️  No metrics found. Run 04_evaluate before 05_register.")

# COMMAND ----------

# DBTITLE 1,Register Serving Model (no Feature Lookups)
import os, pickle

# End any active run left over from cell 8
mlflow.end_run()

# Register a serving-optimized version (raw sklearn pipeline, no FeatureLookups).
serving_model_name = f"{model_uc_name}_serving"

if config["inference_mode"] in ["serving", "both"]:
    # Extract raw sklearn pipeline from the FE model we just registered
    model_download_uri = f"models:/{model_uc_name}/{new_version}"
    local_dir = mlflow.artifacts.download_artifacts(artifact_uri=model_download_uri)
    pkl_path = os.path.join(local_dir, "data", "feature_store", "raw_model", "model.pkl")
    with open(pkl_path, "rb") as f:
        sklearn_pipeline = pickle.load(f)

    # Infer signature from feature table
    feature_table = config["feature_table_name"]
    feature_cols = [c for c in spark.table(feature_table).columns
                    if c not in [config["entity_key"], config["timestamp_key"], config.get("label_column", "")]]
    import pandas as pd
    from mlflow.models.signature import infer_signature
    sample_input = spark.table(feature_table).select(feature_cols).limit(5).toPandas()
    sample_output = pd.DataFrame(sklearn_pipeline.predict(sample_input), columns=["prediction"])
    signature = infer_signature(sample_input, sample_output)

    # Log ALL params and metrics from the training run (same model, same performance)
    with mlflow.start_run(run_name="serving_model") as serving_run:
        mlflow.log_params(all_params)
        mlflow.log_metrics(all_metrics)

        model_info = mlflow.sklearn.log_model(
            sklearn_pipeline,
            artifact_path="model",
            signature=signature,
            registered_model_name=serving_model_name,
        )

    serving_versions = client.search_model_versions(f"name='{serving_model_name}'")
    serving_version = max(serving_versions, key=lambda v: int(v.version)).version

    # Demote current Champion to Challenger
    try:
        current_serving_champion = client.get_model_version_by_alias(serving_model_name, "Champion")
        if current_serving_champion.version != serving_version:
            client.set_registered_model_alias(serving_model_name, "Challenger", current_serving_champion.version)
            print(f"   Previous Champion (v{current_serving_champion.version}) → @Challenger")
    except Exception:
        pass

    client.set_registered_model_alias(serving_model_name, "Champion", serving_version)

    # Attach metrics to LoggedModel entity
    if hasattr(model_info, "model_id") and model_info.model_id:
        mlflow.log_metrics(metrics=all_metrics, model_id=model_info.model_id)

    print(f"✅ Serving model registered: {serving_model_name} v{serving_version} (@Champion)")
    print(f"   {len(all_metrics)} metrics + {len(all_params)} params attached")
    append_deployment_log(event="serving_model_registered", resource=serving_model_name, version=f"v{serving_version}")
else:
    print(f"⏭️  Serving model skipped (inference_mode={config['inference_mode']})")
