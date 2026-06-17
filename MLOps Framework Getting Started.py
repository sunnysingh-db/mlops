# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Framework Visual Guide
from pathlib import Path

# Dynamically resolve all paths from THIS notebook's location — fully portable
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
nb_path = ctx.notebookPath().get()

# Use workspace-specific hostname (NOT regional URL which triggers workspace selector)
workspace_host = spark.conf.get("spark.databricks.workspaceUrl")  # e.g. adb-12345.17.azuredatabricks.net

fw_workspace_path = str(Path(nb_path).parent)  # framework root (workspace path)
fw_dir = f"/Workspace{fw_workspace_path}"       # filesystem path for file I/O

# Build full absolute URLs using /#workspace/ routing with workspace-specific host
base_url = f"https://{workspace_host}/#workspace"
nb_base = f"{base_url}{fw_workspace_path}/src/notebooks"
config_url = f"{base_url}{fw_workspace_path}/config.yaml"
bundle_url = f"{base_url}{fw_workspace_path}/databricks.yml"

# Load HTML template and inject resolved links
with open(f"{fw_dir}/src/notebooks/guide.html") as f:
    html = f.read()

html = html.replace("{{NB_BASE}}", nb_base)
html = html.replace("{{CONFIG_URL}}", config_url)
html = html.replace("{{BUNDLE_URL}}", bundle_url)

displayHTML(html)

# COMMAND ----------

# DBTITLE 1,Validate & Deploy
# Validate config.yaml and deploy as scheduled Databricks Jobs.
# Change target below: "dev", "staging", or "prod"

import yaml, subprocess
from pathlib import Path

target = "dev"

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
framework_dir = "/Workspace" + str(Path(notebook_path).parent)

# Validate config
with open(f"{framework_dir}/config.yaml", "r") as f:
    config = yaml.safe_load(f)

required = ["mode", "catalog", "schema", "model_name", "task_type", "entity_key", "label_column", "feature_table_name"]
missing = [k for k in required if not config.get(k)]
if missing:
    raise ValueError(f"Missing in config.yaml: {missing}")
print(f"Config: {config['mode']} | {config['catalog']}.{config['schema']}.{config['model_name']} | {config['train']['model_algorithm']}")

# Deploy bundle
result = subprocess.run(["databricks", "bundle", "deploy", "-t", target, "--auto-approve"],
    capture_output=True, text=True, cwd=framework_dir)
if result.returncode == 0:
    print(f"Deployed to: {target}")
else:
    print(f"Deploy failed:\n{result.stderr[:500]}")

# COMMAND ----------

# DBTITLE 1,E2E Test — Step 1: Create synthetic customer churn data
# Creates 3 tables in sunny_uc.demo needed for the end-to-end run:
#   customer_activity   — raw monthly behaviour (feature source)
#   churn_labels        — entity + timestamp + label (training input)
#   customers_to_score  — entities for batch inference
import pandas as pd
import numpy as np

np.random.seed(42)

N_CUSTOMERS = 1_000
N_MONTHS    = 6
dates = pd.date_range('2024-01-01', periods=N_MONTHS, freq='MS')

rows = []
for cid in range(1, N_CUSTOMERS + 1):
    customer_id = f"cust_{cid:04d}"
    for ed in dates:
        sessions      = max(0, int(np.random.normal(15, 6)))
        transactions  = max(0, int(np.random.poisson(3)))
        avg_order     = max(0.0, round(float(np.random.normal(65, 20)), 2))
        days_since    = max(0, int(np.random.exponential(10)))
        spend_90d     = max(0.0, round(float(np.random.normal(200, 80)), 2))
        tickets       = int(np.random.poisson(0.5))
        email_opens   = max(0, int(np.random.poisson(4)))
        sub_months    = int(np.random.randint(1, 37))
        rows.append(dict(
            customer_id=customer_id, event_date=ed,
            monthly_sessions=sessions, monthly_transactions=transactions,
            avg_order_value=avg_order, days_since_last_session=days_since,
            total_spend_90d=spend_90d, support_tickets=tickets,
            email_opens_30d=email_opens, subscription_months=sub_months,
        ))

activity_pdf = pd.DataFrame(rows)

# Derive churn: low engagement + high support → higher churn probability
activity_pdf['_p'] = (
    0.15
    + (1 - (activity_pdf['monthly_sessions'] / 25).clip(0, 1)) * 0.30
    - (activity_pdf['total_spend_90d']         / 400).clip(0, 1) * 0.20
    + (activity_pdf['support_tickets']          / 3  ).clip(0, 1) * 0.15
).clip(0.05, 0.85)
rng = np.random.RandomState(42)
activity_pdf['churned'] = (rng.random(len(activity_pdf)) < activity_pdf['_p']).astype(int)

# Write tables
act_df  = spark.createDataFrame(activity_pdf.drop(columns=['_p', 'churned']))
lbl_df  = spark.createDataFrame(activity_pdf[['customer_id', 'event_date', 'churned']])
last_dt = activity_pdf['event_date'].max()
scr_pdf = activity_pdf[activity_pdf['event_date'] == last_dt].head(200)[['customer_id', 'event_date']]
scr_df  = spark.createDataFrame(scr_pdf)

# Tables confirmed not to exist — saveAsTable defaults to errorIfExists (safe CREATE)
act_df.write.saveAsTable('sunny_uc.demo.customer_activity')
lbl_df.write.saveAsTable('sunny_uc.demo.churn_labels')
scr_df.write.saveAsTable('sunny_uc.demo.customers_to_score')

churn_pct = round(100 * activity_pdf['churned'].mean(), 1)
print(f"✅ customer_activity   : {act_df.count():,} rows ({N_CUSTOMERS} customers × {N_MONTHS} months)")
print(f"✅ churn_labels        : {lbl_df.count():,} rows | {churn_pct}% churn rate")
print(f"✅ customers_to_score : {scr_df.count():,} rows  (latest month)")

# COMMAND ----------

# DBTITLE 1,E2E Test — Step 2: Run full pipeline (01 → 08)
import time
from pathlib import Path

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
fw_dir  = str(Path(nb_path).parent)
nb_base = f"{fw_dir}/src/notebooks"

# Notebooks to run in order; 03b/04b/07/09 self-skip from config
pipeline = [
    ("01_eda",                 "EDA & data profile",           300),
    ("02_feature_engineering", "Feature engineering → store",  300),
    ("03_train_tune",          "Optuna HPO (5 trials, lgbm)",  1200),
    ("04_evaluate",            "Holdout evaluation",           300),
    ("05_register",            "Register Champion → UC",       300),
    ("06_batch_inference",     "Batch scoring",                300),
    ("08_monitor",             "Lakehouse monitor setup",      300),
]

results = []
overall_start = time.time()

for nb_name, desc, timeout in pipeline:
    print(f"\n{'\u2500' * 55}\n  \u25b6 {nb_name:<28} {desc}\n{'\u2500' * 55}")
    t0 = time.time()
    try:
        ret = dbutils.notebook.run(f"{nb_base}/{nb_name}", timeout_seconds=timeout)
        elapsed = time.time() - t0
        status = "SKIP" if ret and "skipped" in str(ret).lower() else "PASS"
        icon   = "⏭️" if status == "SKIP" else "✅"
        results.append((nb_name, status, elapsed, ""))
        print(f"  {icon}  {status}  ({elapsed:.0f}s)")
    except Exception as exc:
        elapsed = time.time() - t0
        results.append((nb_name, "FAIL", elapsed, str(exc)[:200]))
        print(f"  ❌  FAIL  ({elapsed:.0f}s)  →  {str(exc)[:180]}")

total = time.time() - overall_start
print(f"\n{'=' * 55}")
print(f"  Pipeline complete  |  {total:.0f}s total")
print(f"{'=' * 55}")
for nb, st, sec, _ in results:
    icon = "⏭️" if st == "SKIP" else ("✅" if st == "PASS" else "❌")
    print(f"  {icon}  {nb:<30} {st:<5} {sec:.0f}s")
fails = [r for r in results if r[1] == "FAIL"]
print(f"\n{'\U0001f389 All stages passed!' if not fails else f'⚠️  {len(fails)} stage(s) failed — see above'}")
