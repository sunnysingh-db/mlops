# Databricks notebook source
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

# MAGIC %md
# MAGIC ## Step 1: Setup & Get Best Run

# COMMAND ----------

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

# Get best run_id — try job taskValues first, then widget, then MLflow search
run_id = None
try:
    run_id = dbutils.jobs.taskValues.get(taskKey="03_train_tune", key="best_run_id")
except Exception:
    pass

if not run_id:
    try:
        run_id = dbutils.widgets.get("run_id") if "run_id" in dbutils.widgets.getAll() else None
    except Exception:
        pass

if not run_id:
    # Fallback for interactive/dbutils.notebook.run(): find latest logged model in MLflow
    current_user = config.get('_current_user', 'shared')
    experiment_name = f"/Users/{current_user}/{config['model_name']}_experiment"
    try:
        runs = mlflow.search_runs(
            experiment_names=[experiment_name],
            filter_string="tags.mlflow.log-model.history != ''",
            order_by=["start_time DESC"],
            max_results=1,
        )
        if len(runs) > 0:
            run_id = runs.iloc[0].run_id
            print(f"Found run via MLflow search: {run_id}")
    except Exception as e:
        print(f"⚠️  MLflow search failed: {e}")

if not run_id:
    raise ValueError("No run_id found. Run 03_train_tune.py first or pass run_id widget.")

print(f"Best run_id:  {run_id}")
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
