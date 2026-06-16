# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ml_v5"
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 08 — Lakehouse Monitoring
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC Sets up **Lakehouse Monitoring** on your predictions table to track:
# MAGIC - **Data drift**: Are input features changing over time?
# MAGIC - **Prediction drift**: Are model outputs shifting?
# MAGIC - **Data quality**: Missing values, unexpected distributions
# MAGIC
# MAGIC ## How It Works
# MAGIC 1. Creates a monitor with `InferenceLog` profile type
# MAGIC 2. Configures automatic refresh on a schedule
# MAGIC 3. Metrics are written to system tables you can query/dashboard
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Predictions table exists (from `06_batch_inference.py` or serving inference table)
# MAGIC - Table must have timestamp column and model version column
# MAGIC
# MAGIC ## Configuration (from config.yaml)
# MAGIC | Key | Description |
# MAGIC |-----|-------------|
# MAGIC | `monitoring.enabled` | Toggle monitoring on/off |
# MAGIC | `monitoring.granularity` | "1 hour", "1 day", or "1 week" |
# MAGIC | `monitoring.refresh_schedule` | Quartz cron for auto-refresh |
# MAGIC
# MAGIC ## Next Steps
# MAGIC Build a dashboard on the monitor metrics tables

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
from databricks.sdk.service.catalog import (
    MonitorInferenceLog, MonitorInferenceLogProblemType, MonitorCronSchedule
)

config = load_config()
w = WorkspaceClient()

# Self-skip gate
if not config["monitoring"]["enabled"]:
    print("Monitoring disabled in config.yaml. Skipping.")
    dbutils.notebook.exit("skipped - monitoring disabled")

output_table = config["batch"]["output_table"]
print(f"Monitor table: {output_table}")
print(f"Granularity:   {config['monitoring']['granularity']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create or Update Monitor
# MAGIC The monitor uses `InferenceLog` profile to track prediction, timestamp, and model version.

# COMMAND ----------

problem_type = (
    MonitorInferenceLogProblemType.PROBLEM_TYPE_CLASSIFICATION
    if config["task_type"] == "classification"
    else MonitorInferenceLogProblemType.PROBLEM_TYPE_REGRESSION
)

inference_log = MonitorInferenceLog(
    problem_type=problem_type,
    prediction_col="prediction",
    timestamp_col="scored_at",
    model_id_col="model_version",
    granularities=[config["monitoring"]["granularity"]],
)

schedule = None
if config["monitoring"]["refresh_schedule"]:
    schedule = MonitorCronSchedule(
        quartz_cron_expression=config["monitoring"]["refresh_schedule"],
        timezone_id="UTC",
    )

try:
    w.quality_monitors.create(
        table_name=output_table,
        inference_log=inference_log,
        schedule=schedule,
        output_schema_name=f"{config['catalog']}.{config['schema']}",
    )
    print(f"Monitor CREATED on: {output_table}")
except Exception as e:
    if "already" in str(e).lower():
        w.quality_monitors.update(
            table_name=output_table,
            inference_log=inference_log,
            schedule=schedule,
            output_schema_name=f"{config['catalog']}.{config['schema']}",
        )
        print(f"Monitor UPDATED on: {output_table}")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Trigger Initial Refresh

# COMMAND ----------

w.quality_monitors.run_refresh(table_name=output_table)
print(f"Monitor refresh triggered")
append_deployment_log(event="monitor_created", resource=output_table, version="v1")
