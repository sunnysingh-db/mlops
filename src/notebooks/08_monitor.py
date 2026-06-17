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

# DBTITLE 1,Install dependencies
# MAGIC %pip install pyyaml databricks-sdk --quiet

# COMMAND ----------

# DBTITLE 1,Restart Python
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup

# COMMAND ----------

# DBTITLE 1,Setup
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
monitoring_cfg = config["monitoring"]
print(f"Monitor table: {output_table}")
print(f"Granularity:   {monitoring_cfg['granularity']}")

# --- Label column handling (config-driven) ---
# The monitor requires label_col and prediction_col to be the same type (DOUBLE).
# This section ensures the label column exists in the predictions table as DOUBLE.
#
# Control via config.yaml:
#   monitoring.label_col: "churned"   → use this column for quality metrics
#   monitoring.label_col: null/absent → skip labels, only drift/profile metrics
# Falls back to top-level label_column if monitoring.label_col is not set.
label_col_name = monitoring_cfg.get("label_col", config.get("label_column"))
training_table = config["train"].get("training_table")

if label_col_name and training_table:
    pred_columns = {f.name: f.dataType.simpleString() for f in spark.table(output_table).schema.fields}

    if label_col_name in pred_columns:
        # Label column exists — ensure it's DOUBLE (widen if needed)
        if pred_columns[label_col_name] == 'double':
            print(f"✅ Label column '{label_col_name}' present (DOUBLE)")
        else:
            print(f"⚠️  '{label_col_name}' is {pred_columns[label_col_name]} — widening to DOUBLE...")
            spark.sql(f"ALTER TABLE {output_table} SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
            spark.sql(f"ALTER TABLE {output_table} ALTER COLUMN {label_col_name} TYPE DOUBLE")
            print(f"✅ Widened to DOUBLE")
    else:
        # Label column missing — add it from training table
        print(f"Adding '{label_col_name}' from {training_table}...")
        spark.sql(f"ALTER TABLE {output_table} ADD COLUMN {label_col_name} DOUBLE")
        spark.sql(f"""
            MERGE INTO {output_table} AS p
            USING {training_table} AS l
            ON p.{config['entity_key']} = l.{config['entity_key']}
            WHEN MATCHED THEN UPDATE SET p.{label_col_name} = CAST(l.{label_col_name} AS DOUBLE)
        """)
        print(f"✅ Labels populated")
else:
    label_col_name = None
    print(f"⚠️  No label_column or training_table configured — model quality metrics disabled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create or Update Monitor
# MAGIC The monitor uses `InferenceLog` profile to track prediction, timestamp, and model version.

# COMMAND ----------

# DBTITLE 1,Create or Update Monitor
problem_type = (
    MonitorInferenceLogProblemType.PROBLEM_TYPE_CLASSIFICATION
    if config["task_type"] == "classification"
    else MonitorInferenceLogProblemType.PROBLEM_TYPE_REGRESSION
)

# Use label_col_name from setup cell (already validated as DOUBLE, or None)
if label_col_name:
    print(f"✅ Label column: '{label_col_name}' → model quality metrics enabled")
else:
    print(f"⚠️  No label column → only drift/profile metrics")

# Column names are config-driven with sensible defaults matching 06_batch_inference output
inference_log = MonitorInferenceLog(
    problem_type=problem_type,
    prediction_col=monitoring_cfg.get("prediction_col", "prediction"),
    timestamp_col=monitoring_cfg.get("timestamp_col", "scored_at"),
    model_id_col=monitoring_cfg.get("model_id_col", "model_version"),
    granularities=[monitoring_cfg["granularity"]],
    label_col=label_col_name,
)

schedule = None
refresh_schedule = config["monitoring"].get("refresh_schedule")
if refresh_schedule:
    schedule = MonitorCronSchedule(
        quartz_cron_expression=refresh_schedule,
        timezone_id="UTC",
    )

# assets_dir for monitor dashboard/notebook assets (derived from project root, not hardcoded)
assets_dir = f"/Workspace{framework_dir}/monitoring"

try:
    monitor_info = w.quality_monitors.create(
        table_name=output_table,
        inference_log=inference_log,
        schedule=schedule,
        output_schema_name=f"{config['catalog']}.{config['schema']}",
        assets_dir=assets_dir,
    )
    print(f"✅ Monitor CREATED on: {output_table}")
except Exception as e:
    if "already" in str(e).lower():
        monitor_info = w.quality_monitors.update(
            table_name=output_table,
            inference_log=inference_log,
            schedule=schedule,
            output_schema_name=f"{config['catalog']}.{config['schema']}",
        )
        print(f"✅ Monitor UPDATED on: {output_table}")
    else:
        raise

# Display monitor dashboard URL
import json
from databricks.sdk.service.dashboards import Dashboard as LakeviewDashboard

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host = context.apiUrl().get()
workspace_id = context.workspaceId().get()
dashboard_id = getattr(monitor_info, 'dashboard_id', None)
if dashboard_id:
    print(f"\n📊 Monitor Dashboard: {host}/dashboardsv3/{dashboard_id}/published?o={workspace_id}")

    # Fix auto-generated dashboard parameter defaults:
    # 1. "Model Id" must default to "*" (otherwise: "Missing selection for parameter")
    # 2. "Time Window End" must use "now+1d/d" (daily windows end at next-day midnight,
    #    which is > "now", so the default "now/s" filter excludes today's data)
    dash = w.lakeview.get(dashboard_id)
    dash_def = json.loads(dash.serialized_dashboard)

    default_model_id = {"values": {"dataType": "STRING", "values": [{"value": "*"}]}}
    default_time_end = {"values": {"dataType": "DATETIME", "values": [{"value": "now+1d/d"}]}}

    fixed_model = 0
    fixed_time = 0
    for ds in dash_def.get('datasets', []):
        for param in ds.get('parameters', []):
            kw = param.get('keyword', '')
            # Fix Model Id
            if kw == 'Model Id':
                if not param.get('defaultSelection') or param['defaultSelection'] == {}:
                    param['defaultSelection'] = default_model_id
                    fixed_model += 1
            # Fix Time Window End
            elif kw == 'Time Window End':
                current_val = (param.get('defaultSelection', {}).get('values', {}).get('values', [{}]) or [{}])[0].get('value', '')
                if current_val in ('now/s', 'now', ''):
                    param['defaultSelection'] = default_time_end
                    fixed_time += 1

    if fixed_model + fixed_time > 0:
        w.lakeview.update(dashboard_id=dashboard_id, dashboard=LakeviewDashboard(serialized_dashboard=json.dumps(dash_def)))
        w.lakeview.publish(dashboard_id=dashboard_id)
        if fixed_model:
            print(f"✅ Dashboard fixed: 'Model Id' default → '*' ({fixed_model} datasets)")
        if fixed_time:
            print(f"✅ Dashboard fixed: 'Time Window End' → 'now+1d/d' ({fixed_time} datasets)")

print(f"📈 Drift metrics table:  {output_table}_profile_metrics")
print(f"📉 Analysis table:       {output_table}_drift_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Trigger Initial Refresh

# COMMAND ----------

# DBTITLE 1,Trigger Refresh and Wait for Completion
import time

refresh = w.quality_monitors.run_refresh(table_name=output_table)
refresh_id = refresh.refresh_id
print(f"Monitor refresh triggered (ID: {refresh_id})")
print("Waiting for all refreshes to complete...\n")

# Poll until no active refreshes remain
while True:
    response = w.quality_monitors.list_refreshes(table_name=output_table)
    active = [r for r in response.refreshes if r.state.value in ("RUNNING", "PENDING")]
    if not active:
        break
    running = [r for r in active if r.state.value == "RUNNING"]
    pending = [r for r in active if r.state.value == "PENDING"]
    elapsed = (int(time.time() * 1000) - active[-1].start_time_ms) / 1000
    print(f"  ⏳ Running: {len(running)} | Pending: {len(pending)} | Elapsed: {elapsed:.0f}s", end="\r")
    time.sleep(30)

# Final status summary
response = w.quality_monitors.list_refreshes(table_name=output_table)
succeeded = [r for r in response.refreshes if r.state.value == "SUCCESS"]
failed = [r for r in response.refreshes if r.state.value == "FAILED"]
canceled = [r for r in response.refreshes if r.state.value == "CANCELED"]

print(f"\n\n🏁 All refreshes complete:")
print(f"   ✅ Succeeded: {len(succeeded)}")
if failed:
    print(f"   ❌ Failed:    {len(failed)}")
    for f in failed:
        print(f"      - {f.refresh_id}: {f.message}")
if canceled:
    print(f"   ⏭️  Canceled:  {len(canceled)}")

# Verify metrics tables were created
try:
    profile_count = spark.table(f"{output_table}_profile_metrics").count()
    print(f"\n📊 Profile metrics table: {profile_count} rows")
except Exception:
    print(f"\n⚠️ Profile metrics table not yet available")

try:
    drift_count = spark.table(f"{output_table}_drift_metrics").count()
    print(f"📉 Drift metrics table:   {drift_count} rows")
except Exception:
    print(f"📉 Drift metrics table:   not yet available (needs 2+ time windows)")

append_deployment_log(event="monitor_created", resource=output_table, version="v1")
print(f"\n✅ Monitoring setup complete.")
