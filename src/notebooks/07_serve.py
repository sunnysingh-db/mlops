# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ml_v5"
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # 07 — Model Serving (Real-Time Endpoint)
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC Creates (or updates) a **Model Serving endpoint** so your model can be called
# MAGIC via REST API for real-time predictions. The endpoint automatically scales
# MAGIC based on traffic and can scale to zero when idle.
# MAGIC
# MAGIC ## Architecture
# MAGIC The framework registers **two models** in Unity Catalog:
# MAGIC - `{model_name}` — FE-wrapped model (used by `06_batch_inference` via `fe.score_batch()`, provides lineage)
# MAGIC - `{model_name}_serving` — raw sklearn pipeline (used here, clients send all features in request)
# MAGIC
# MAGIC This avoids any dependency on Lakebase / Online Feature Stores.
# MAGIC
# MAGIC ## How It Works
# MAGIC 1. Looks up `{model_name}_serving` from Unity Catalog (registered by `05_register`)
# MAGIC 2. Creates a serving endpoint with the specified workload size
# MAGIC 3. Enables AI Gateway inference tables (logs all requests/responses)
# MAGIC 4. Waits until the endpoint is ready
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - `05_register.py` has run (registers both FE model and serving model)
# MAGIC
# MAGIC ## Configuration (from config.yaml)
# MAGIC | Key | Description |
# MAGIC |-----|-------------|
# MAGIC | `serving.endpoint_name` | Custom name (auto-derived if null) |
# MAGIC | `serving.scale_to_zero` | Whether endpoint scales to 0 when idle (saves cost) |
# MAGIC | `serving.workload_size` | "Small", "Medium", or "Large" |
# MAGIC | `serving.workload_type` | "CPU" or "GPU" |
# MAGIC
# MAGIC ## Output
# MAGIC - A live serving endpoint accessible via REST API
# MAGIC - AI Gateway inference table logging all predictions
# MAGIC
# MAGIC ## Next Steps
# MAGIC → `08_monitor.py` (drift monitoring on inference table)
# MAGIC → `09_process_inference.py` (flatten inference table for analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install pyyaml "mlflow[databricks]" -q

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Setup
import sys
from pathlib import Path

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
framework_dir = str(Path(notebook_path).parent.parent.parent)
sys.path.insert(0, f"/Workspace{framework_dir}/src/notebooks")

from helpers import load_config, append_deployment_log
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput, ServedEntityInput
)

config = load_config()
w = WorkspaceClient()

# Self-skip gate
if config["inference_mode"] not in ["serving", "both"]:
    print("⏭️ Serving not enabled in config.yaml. Skipping.")
    dbutils.notebook.exit("skipped - serving not enabled")

model_uc_name = f"{config['catalog']}.{config['schema']}.{config['model_name']}"
serving_model_name = f"{model_uc_name}_serving"
endpoint_name = config["serving"].get("endpoint_name") or f"{config['model_name']}_serving"

print(f"✅ Config loaded")
print(f"   Endpoint:      {endpoint_name}")
print(f"   Model:         {serving_model_name}@Champion")
print(f"   Scale to zero: {config['serving']['scale_to_zero']}")
print(f"   Workload:      {config['serving']['workload_size']} / {config['serving']['workload_type']}")

# COMMAND ----------

# DBTITLE 1,Cell 6
# MAGIC %md
# MAGIC ## Step 2: Resolve Serving Model & Deploy Endpoint
# MAGIC
# MAGIC The serving model (`{model_name}_serving`) is a raw sklearn pipeline registered by `05_register`.
# MAGIC It accepts all features directly in the request — no Online Feature Store or Lakebase required.
# MAGIC
# MAGIC If the endpoint already exists, we update its config. Otherwise, we create it fresh.
# MAGIC **AI Gateway inference tables** are enabled — every request/response is logged to a Delta table.

# COMMAND ----------

# DBTITLE 1,Create or Update Endpoint
# Resolve serving model from Unity Catalog, then deploy endpoint.
import mlflow

client = mlflow.MlflowClient()
serving_model_version = None

try:
    serving_mv = client.get_model_version_by_alias(serving_model_name, "Champion")
    serving_model_version = serving_mv.version
except Exception:
    versions = client.search_model_versions(f"name='{serving_model_name}'")
    if versions:
        serving_model_version = max(versions, key=lambda v: int(v.version)).version

if not serving_model_version:
    print(f"❌ Serving model not found: {serving_model_name}")
    print(f"   Run 05_register first.")
    dbutils.notebook.exit("failed - serving model not registered")

print(f"Deploying: {serving_model_name} v{serving_model_version}")

# workload_type: CPU is the default (pass None); for GPU pass the string directly
wl_type = config["serving"]["workload_type"].upper()

served_entity = ServedEntityInput(
    entity_name=serving_model_name,
    entity_version=serving_model_version,
    scale_to_zero_enabled=config["serving"]["scale_to_zero"],
    workload_size=config["serving"]["workload_size"],
    workload_type=wl_type if wl_type != "CPU" else None,
)

# Check if endpoint already exists
existing_endpoints = [e.name for e in w.serving_endpoints.list()]

if endpoint_name in existing_endpoints:
    # If endpoint is in failed state, delete and recreate
    ep_state = w.serving_endpoints.get(endpoint_name)
    if ep_state.state and ep_state.state.config_update and "FAILED" in str(ep_state.state.config_update):
        print(f"Endpoint in failed state — deleting and recreating: {endpoint_name}")
        w.serving_endpoints.delete(endpoint_name)
        import time; time.sleep(5)
        w.serving_endpoints.create_and_wait(
            name=endpoint_name,
            config=EndpointCoreConfigInput(
                served_entities=[served_entity],
            ),
        )
        print(f"✅ Endpoint recreated")
    else:
        print(f"Updating existing endpoint: {endpoint_name}")
        w.serving_endpoints.update_config_and_wait(
            name=endpoint_name,
            served_entities=[served_entity],
        )
        print(f"✅ Endpoint updated")
else:
    print(f"Creating new endpoint: {endpoint_name}")
    w.serving_endpoints.create_and_wait(
        name=endpoint_name,
        config=EndpointCoreConfigInput(
            served_entities=[served_entity],
        ),
    )
    print(f"✅ Endpoint created")

# Enable AI Gateway inference tables (replaces deprecated auto_capture_config)
from databricks.sdk.service.serving import AiGatewayInferenceTableConfig

w.serving_endpoints.put_ai_gateway(
    name=endpoint_name,
    inference_table_config=AiGatewayInferenceTableConfig(
        catalog_name=config["catalog"],
        schema_name=config["schema"],
        table_name_prefix=config["model_name"],
        enabled=True,
    ),
)
print(f"✅ AI Gateway inference table enabled")

append_deployment_log(event="endpoint_deployed", resource=endpoint_name, version=serving_model_name)

# Print the endpoint page URL
workspace_host = spark.conf.get("spark.databricks.workspaceUrl")
endpoint_url = f"https://{workspace_host}/ml/endpoints/{endpoint_name}"
print(f"\n{'═' * 60}")
print(f"  Endpoint:  {endpoint_name}")
print(f"  Model:     {serving_model_name} v{serving_model_version}")
print(f"  Status:    READY")
print(f"  Page:      {endpoint_url}")
print(f"{'═' * 60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Test the Endpoint
# MAGIC Quick smoke test to confirm the endpoint is responding.

# COMMAND ----------

# DBTITLE 1,Test Endpoint with Sample Request
# Smoke test: send a sample request to the endpoint and verify it responds
import json, requests

# Get a sample row from the feature table
feature_table = config["feature_table_name"]
exclude_cols = [config["entity_key"], config["timestamp_key"], config.get("label_column", "")]
feature_cols = [c for c in spark.table(feature_table).columns if c not in exclude_cols]
sample_row = spark.table(feature_table).select(feature_cols).limit(1).toPandas().iloc[0].to_dict()

print(f"Sending test request to endpoint: {endpoint_name}")
print(f"Payload: {json.dumps(sample_row, indent=2)}\n")

# --- Method 1: Databricks SDK ---
print("=== SDK Method ===")
response = w.serving_endpoints.query(
    name=endpoint_name,
    dataframe_records=[sample_row],
)
print(f"✅ Prediction: {response.predictions}\n")

# --- Method 2: REST API ---
print("=== REST Method ===")
context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = context.apiUrl().get()
token = context.apiToken().get()

rest_url = f"{host}/serving-endpoints/{endpoint_name}/invocations"
rest_payload = {"dataframe_records": [sample_row]}
rest_response = requests.post(
    rest_url,
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    json=rest_payload,
)
print(f"✅ Status: {rest_response.status_code}")
print(f"✅ Prediction: {rest_response.json().get('predictions')}")

append_deployment_log(event="endpoint_smoke_test", resource=endpoint_name, version="passed")
print(f"\n{'═' * 50}")
print(f"  Endpoint: {endpoint_name}")
print(f"  Status:   READY")
print(f"  URL:      {rest_url}")
print(f"{'═' * 50}")

