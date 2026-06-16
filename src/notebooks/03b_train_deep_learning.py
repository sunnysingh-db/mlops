# Databricks notebook source
# MAGIC %md
# MAGIC # 03b — Deep Learning Training (PyTorch)
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC Trains a **PyTorch neural network** on tabular data as an alternative (or addition)
# MAGIC to the tree-based models in `03_train_tune.py`. Uses Optuna for hyperparameter
# MAGIC optimization of learning rate, weight decay, and architecture.
# MAGIC
# MAGIC ## When To Use This
# MAGIC - Set `train.model_algorithm` to `"deep_learning"` or `"all"` in config.yaml
# MAGIC - Best for: very large datasets (>100K rows), complex non-linear relationships
# MAGIC - For most tabular data, tree-based models (LightGBM) outperform DL
# MAGIC
# MAGIC ## Architectures Available
# MAGIC - `tabular_mlp`: Standard Multi-Layer Perceptron (Linear → BN → ReLU → Dropout)
# MAGIC - `tabular_resnet`: ResNet-style with skip connections (better for deeper networks)
# MAGIC - `custom`: Define your own in `nn_architectures.py`
# MAGIC
# MAGIC ## Configuration (from config.yaml → train.deep_learning)
# MAGIC | Key | Description |
# MAGIC |-----|-------------|
# MAGIC | `architecture` | "tabular_mlp", "tabular_resnet", or "custom" |
# MAGIC | `hidden_layers` | Layer sizes, e.g., [256, 128, 64] |
# MAGIC | `epochs` | Max training epochs (early stopping may cut short) |
# MAGIC | `batch_size` | Samples per gradient update |
# MAGIC | `compute` | "cpu" or "gpu" (GPU recommended for large data) |
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Feature table populated (from `02_feature_engineering.py`)
# MAGIC - GPU compute recommended (set `compute: "gpu"` in config.yaml)
# MAGIC
# MAGIC ## Next Steps
# MAGIC → `04_evaluate.py` (compare DL results against tree models)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup & Self-Skip Gate

# COMMAND ----------

import sys
from pathlib import Path

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
framework_dir = str(Path(notebook_path).parent.parent.parent)
sys.path.insert(0, f"/Workspace{framework_dir}/src/notebooks")

from helpers import load_config, get_feature_lookups
config = load_config()

# Self-skip gate: only run if deep learning is enabled
if config["train"]["model_algorithm"] not in ["deep_learning", "all"]:
    print("Deep learning not enabled in config.yaml (model_algorithm != deep_learning/all).")
    print("To enable: set train.model_algorithm to deep_learning or all")
    dbutils.notebook.exit("skipped - DL not enabled")

dl_config = config["train"]["deep_learning"]
print(f"Architecture:  {dl_config['architecture']}")
print(f"Hidden layers: {dl_config['hidden_layers']}")
print(f"Epochs:        {dl_config.get('epochs', 100)}")
print(f"Compute:       {dl_config.get('compute', 'cpu')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load Data & Prepare Tensors
# MAGIC
# MAGIC Converts the training set to PyTorch tensors for GPU/CPU training.

# COMMAND ----------

import torch
import numpy as np
import mlflow
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

fe = FeatureEngineeringClient()
feature_lookups = get_feature_lookups(config)

# Create training set from Feature Store
training_set = fe.create_training_set(
    df=spark.table(config["train"]["training_table"]),
    feature_lookups=feature_lookups,
    label=config["label_column"],
    exclude_columns=[config["entity_key"]],
)

pdf = training_set.load_df().toPandas()
X = pdf.drop(columns=[config["label_column"]]).select_dtypes(include=[np.number]).values
y = pdf[config["label_column"]].values

# Split
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

# Scale
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_val = scaler.transform(X_val)

# Convert to tensors
device = torch.device("cuda" if dl_config.get("compute") == "gpu" and torch.cuda.is_available() else "cpu")
X_train_t = torch.FloatTensor(X_train).to(device)
y_train_t = torch.FloatTensor(y_train).to(device)
X_val_t = torch.FloatTensor(X_val).to(device)
y_val_t = torch.FloatTensor(y_val).to(device)

print(f"Training samples: {len(X_train):,}")
print(f"Input features:   {X_train.shape[1]}")
print(f"Device:           {device}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Train with Optuna HPO
# MAGIC
# MAGIC Searches for optimal learning rate and weight decay.
# MAGIC Early stopping prevents overfitting on each trial.

# COMMAND ----------

# Full implementation in next build phase
# (Architecture loading, training loop, Optuna objective, model logging)
print("03b Deep Learning — implementation coming in next phase")
print("For now, use tree-based models in 03_train_tune.py")

