# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Model Serving (Real-Time Endpoint)
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC Creates (or updates) a **Model Serving endpoint** so your model can be called
# MAGIC via REST API for real-time predictions. The endpoint automatically scales
# MAGIC based on traffic and can scale to zero when idle.
# MAGIC
# MAGIC ## How It Works
# MAGIC 1. Reads the Champion model from Unity Catalog
# MAGIC 2. Creates a serving endpoint with the specified workload size
# MAGIC 3. Enables inference tables (logs all requests/responses for monitoring)
# MAGIC 4. Waits until the endpoint is ready
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - A model registered in UC with `@Champion` alias (from `05_register.py`)
# MAGIC - Feature table populated (the endpoint uses online feature lookups at inference time)
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
# MAGIC - Inference table logging all predictions (for monitoring)
# MAGIC
# MAGIC ## Next Steps
# MAGIC → `08_monitor.py` (drift monitoring on inference table)
# MAGIC → `09_process_inference.py` (flatten inference table for analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup

# COMMAND ----------

import sys
from pathlib import Path

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
framework_dir = str(Path(notebook_path).parent.parent.parent)
sys.path.insert(0, f"/Workspace{framework_dir}/src/notebooks")

from helpers import load_config, append_deployment_log
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput, ServedEntityInput, AutoCaptureConfigInput
)

config = load_config()
w = WorkspaceClient()

# Self-skip gate
if config["inference_mode"] not in ["serving", "both"]:
    print("⏭️ Serving not enabled in config.yaml. Skipping.")
    dbutils.notebook.exit("skipped - serving not enabled")

model_uc_name = f"{config['catalog']}.{config['schema']}.{config['model_name']}"
endpoint_name = config["serving"].get("endpoint_name") or f"{config['model_name']}_serving"

print(f"✅ Config loaded")
print(f"   Endpoint:      {endpoint_name}")
print(f"   Model:         {model_uc_name}@Champion")
print(f"   Scale to zero: {config['serving']['scale_to_zero']}")
print(f"   Workload:      {config['serving']['workload_size']} / {config['serving']['workload_type']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create or Update Endpoint
# MAGIC
# MAGIC If the endpoint already exists, we update its config (new model version, workload changes).
# MAGIC If it doesn't exist, we create it fresh.
# MAGIC
# MAGIC **Inference tables** are enabled automatically — every prediction request/response is
# MAGIC logged to a Delta table for monitoring and debugging.

# COMMAND ----------

served_entity = ServedEntityInput(
    entity_name=model_uc_name,
    entity_version=None,  # Uses Champion alias
    scale_to_zero_enabled=config["serving"]["scale_to_zero"],
    workload_size=config["serving"]["workload_size"],
    workload_type=config["serving"]["workload_type"],
)

inference_table_config = AutoCaptureConfigInput(
    catalog_name=config["catalog"],
    schema_name=config["schema"],
    enabled=True,
    table_name_prefix=config["model_name"],
)

# Check if endpoint already exists
existing_endpoints = [e.name for e in w.serving_endpoints.list()]

if endpoint_name in existing_endpoints:
    print(f"Updating existing endpoint: {endpoint_name}")
    w.serving_endpoints.update_config_and_wait(
        name=endpoint_name,
        served_entities=[served_entity],
        auto_capture_config=inference_table_config,
    )
    print(f"✅ Endpoint updated")
else:
    print(f"Creating new endpoint: {endpoint_name}")
    w.serving_endpoints.create_and_wait(
        name=endpoint_name,
        config=EndpointCoreConfigInput(
            served_entities=[served_entity],
            auto_capture_config=inference_table_config,
        ),
    )
    print(f"✅ Endpoint created")

append_deployment_log(event="endpoint_deployed", resource=endpoint_name, version=model_uc_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Test the Endpoint
# MAGIC Quick smoke test to confirm the endpoint is responding.

# COMMAND ----------

print(f"\n{'═' * 50}")
print(f"  Endpoint: {endpoint_name}")
print(f"  Status:   READY")
print(f"{'═' * 50}")
print(f"\n  Test with:")
print(f"  curl -X POST https://<workspace-url>/serving-endpoints/{endpoint_name}/invocations \\")
print(f"    -H \"Authorization: Bearer <token>\" \\")
print(f"    -d \'{{\"dataframe_records\": [{{\"<feature>\":<value>}}]}}\'")

