# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Wrap & Register Model (Migrate Mode)
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC Wraps your existing model (pickle) in an **MLflow PythonModel** wrapper, logs it
# MAGIC with the Feature Engineering Client for lineage tracking, and registers it to
# MAGIC Unity Catalog with the `Champion` alias.
# MAGIC
# MAGIC ## Why Wrapping Is Needed
# MAGIC - Your pickle needs to be packaged as an MLflow model for UC registration
# MAGIC - `fe.log_model()` attaches Feature Store metadata so `score_batch()` works
# MAGIC - Custom preprocessing (scaling, encoding) can be embedded in the wrapper
# MAGIC
# MAGIC ## How It Works
# MAGIC 1. Defines a `MigratedModel(mlflow.pyfunc.PythonModel)` wrapper class
# MAGIC 2. Logs the wrapped model with `fe.log_model()` (includes feature lookups)
# MAGIC 3. Registers to Unity Catalog and sets Champion alias
# MAGIC
# MAGIC ## USER ACTION REQUIRED
# MAGIC If your model needs preprocessing (e.g., scaling, encoding), edit the
# MAGIC `predict()` method in the `MigratedModel` class below.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - `01_validate_model.py` passed
# MAGIC - `02_feature_engineering.py` feature table written
# MAGIC
# MAGIC ## Next Steps
# MAGIC → `06_batch_inference.py` or `07_serve.py`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup

# COMMAND ----------

import sys, pickle
from pathlib import Path
import mlflow
from mlflow.pyfunc import PythonModel

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
framework_dir = str(Path(notebook_path).parent.parent.parent)
sys.path.insert(0, f"/Workspace{framework_dir}/src/notebooks")

from helpers import load_config, get_feature_lookups, append_deployment_log
from databricks.feature_engineering import FeatureEngineeringClient

config = load_config()
fe = FeatureEngineeringClient()
mlflow.set_registry_uri("databricks-uc")

# Self-skip gate
if config["mode"] != "migrate":
    print("Mode is not migrate. Skipping.")
    dbutils.notebook.exit("skipped - not migrate mode")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Define Model Wrapper
# MAGIC
# MAGIC **USER ACTION**: If your model needs preprocessing (scaling, encoding, etc.),
# MAGIC edit the `predict()` method below to add that logic.

# COMMAND ----------

class MigratedModel(PythonModel):
    """Wrapper for existing pickle model.

    Edit predict() if your model needs custom preprocessing.
    """

    def load_context(self, context):
        import pickle
        with open(context.artifacts["model_pickle"], "rb") as f:
            self.model = pickle.load(f)

    def predict(self, context, model_input):
        # ─── USER ACTION: Add preprocessing here if needed ───
        # Example: model_input = self.scaler.transform(model_input)
        return self.model.predict(model_input)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Log with Feature Engineering Client & Register
# MAGIC
# MAGIC `fe.log_model()` packages the model WITH feature lookups so that
# MAGIC `fe.score_batch()` and serving endpoints can auto-fetch features.

# COMMAND ----------

model_uc_name = f"{config['catalog']}.{config['schema']}.{config['model_name']}"
feature_lookups = get_feature_lookups(config)

with mlflow.start_run(run_name=f"migrate_{config['model_name']}") as run:
    fe.log_model(
        model=MigratedModel(),
        artifact_path="model",
        flavor=mlflow.pyfunc,
        training_set=None,
        artifacts={"model_pickle": config["migrate"]["model_path"]},
        registered_model_name=model_uc_name,
    )

from mlflow import MlflowClient
client = MlflowClient()

# Set Champion alias
latest = client.get_registered_model(model_uc_name).latest_versions[0]
client.set_registered_model_alias(model_uc_name, "Champion", latest.version)
print(f"Registered: {model_uc_name} v{latest.version} as Champion")

append_deployment_log(event="model_migrated", resource=model_uc_name, version=f"v{latest.version}")

