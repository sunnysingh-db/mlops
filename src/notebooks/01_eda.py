# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 — Exploratory Data Analysis
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC Performs **Exploratory Data Analysis** on your training data before you start building
# MAGIC features or models. Think of it as a "health check" on your data.
# MAGIC
# MAGIC **Questions it answers:**
# MAGIC - How big is the dataset? What types are the columns?
# MAGIC - Are there missing values that need imputation?
# MAGIC - Is the target variable balanced or heavily skewed?
# MAGIC - Are any features suspiciously correlated with the target (possible data leakage)?
# MAGIC - What tables are available in your schema for feature engineering?
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Config validated (INSTALL Cell 1)
# MAGIC - Training table exists and is accessible
# MAGIC
# MAGIC ## Outputs
# MAGIC - Data profile (shape, types, nulls)
# MAGIC - Target distribution + imbalance flag
# MAGIC - Correlation heatmap + leakage screen
# MAGIC - Available tables list (for feature engineering)
# MAGIC
# MAGIC ## Next Step
# MAGIC → `02_feature_engineering.py`

# COMMAND ----------

# Setup & Config
import sys
from pathlib import Path

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
framework_dir = str(Path(notebook_path).parent.parent.parent)
sys.path.insert(0, f"/Workspace{framework_dir}/src/notebooks")

from helpers import load_config, detect_class_imbalance
config = load_config()

training_table = config["train"]["training_table"]
label_column = config["label_column"]
task_type = config["task_type"]

print(f"Training table: {training_table}")
print(f"Label column:   {label_column}")
print(f"Task type:      {task_type}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Load & Profile Data
# MAGIC
# MAGIC **What this does:** Reads the training table and reports basic statistics.
# MAGIC
# MAGIC **What to look for (new users):**
# MAGIC - Row count: Does this match what you expect? Too few rows = weak model, too many = check for duplicates
# MAGIC - Column types: Numeric columns should be int/float, categories should be object/string
# MAGIC - Null counts: Columns with >50%% nulls may need to be dropped rather than imputed

# COMMAND ----------

df = spark.table(training_table)
pdf = df.toPandas()

print("═" * 50)
print(f"  Rows:    {pdf.shape[0]:,}")
print(f"  Columns: {pdf.shape[1]}")
print("═" * 50)
print()
print("Column Types:")
print(pdf.dtypes.value_counts().to_string())
print()
print("Null Counts (top 10):")
nulls = pdf.isnull().sum().sort_values(ascending=False)
print(nulls[nulls > 0].head(10).to_string() if nulls.sum() > 0 else "  No nulls detected ✅")

display(df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Target Distribution
# MAGIC **What this does:** Visualizes the label distribution. Flags class imbalance > 5:1.
# MAGIC **Why it matters:** Imbalanced classes require PR-AUC (not F1) and class_weight tuning.

# COMMAND ----------

import matplotlib.pyplot as plt

if task_type == "classification":
    counts = pdf[label_column].value_counts()
    _imb = detect_class_imbalance(pdf, label_column)
    is_imbalanced = bool(_imb["is_imbalanced"])
    ratio = float(_imb["ratio"])
    
    fig, ax = plt.subplots(figsize=(6, 4))
    counts.plot(kind="bar", ax=ax, color=["steelblue", "coral"])
    ax.set_title(f"Target Distribution (ratio: {ratio:.1f}:1)")
    ax.set_ylabel("Count")
    plt.tight_layout()
    plt.show()
    
    if is_imbalanced:
        print(f"⚠️  Class imbalance detected: {ratio:.1f}:1 ratio")
        print("   → Framework will use PR-AUC as tuning objective")
    else:
        print(f"✅ Classes are balanced ({ratio:.1f}:1 ratio)")
else:
    fig, ax = plt.subplots(figsize=(6, 4))
    pdf[label_column].hist(bins=50, ax=ax, color="steelblue")
    ax.set_title(f"Target Distribution: {label_column}")
    ax.set_xlabel(label_column)
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Feature Correlations
# MAGIC **What this does:** Computes correlogram and top correlations with target.
# MAGIC **What to look for:** Features strongly correlated with target (good predictors)
# MAGIC and features correlated with each other (potential redundancy).

# COMMAND ----------

import seaborn as sns
import numpy as np
import pandas as pd

numeric_cols = pdf.select_dtypes(include=[np.number]).columns.tolist()
target_corr = pd.Series(dtype=float)  # safe default if condition below is skipped

if len(numeric_cols) > 1 and label_column in numeric_cols:
    corr_matrix = pdf[numeric_cols].corr()
    
    # Top correlations with target
    target_corr = corr_matrix[label_column].drop(label_column).abs().sort_values(ascending=False)
    print("Top-10 correlations with target:")
    print(target_corr.head(10).to_string())
    print()
    
    # Heatmap (capped at 20 features for readability)
    plot_cols = target_corr.head(20).index.tolist() + [label_column]
    fig, ax = plt.subplots(figsize=(12, 10))
    sns.heatmap(pdf[plot_cols].corr(), annot=len(plot_cols) <= 12,
                cmap="RdBu_r", center=0, ax=ax, fmt=".2f")
    ax.set_title("Feature Correlation Heatmap")
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Target Leakage Screen
# MAGIC **What this does:** Flags features with |corr| > 0.95 to target.
# MAGIC **Why it matters:** Leaky features inflate metrics but FAIL in production.
# MAGIC If detected, the pipeline will STOP — do not proceed without resolving.

# COMMAND ----------

LEAKAGE_THRESHOLD = 0.95

if label_column in numeric_cols:
    suspects = target_corr[target_corr > LEAKAGE_THRESHOLD]
    if len(suspects) > 0:
        print("═" * 50)
        print("  🛑 POTENTIAL TARGET LEAKAGE DETECTED")
        print("═" * 50)
        for feat, corr_val in suspects.items():
            print(f"  • {feat}: |corr| = {corr_val:.3f}")
        print()
        print("  Action: Remove these features or confirm they are")
        print("  available at prediction time before proceeding.")
        print("═" * 50)
    else:
        print("✅ No target leakage detected (all |corr| < 0.95)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Available Source Tables
# MAGIC **Why:** Helps you identify which tables to use in `02_feature_engineering.py`.

# COMMAND ----------

catalog = config["catalog"]
schema = config["schema"]

tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
display(tables_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC EDA complete. Review the outputs above, then proceed to:
# MAGIC → `02_feature_engineering.py` to define your features.
