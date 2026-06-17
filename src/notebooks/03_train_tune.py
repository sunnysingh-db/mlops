# Databricks notebook source
# /// script
# [tool.databricks.environment]
# base_environment = "databricks_ml_v5"
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03 — Train & Tune Model
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC Trains ML models and finds the best hyperparameters using **Optuna** (a modern HPO framework).
# MAGIC
# MAGIC **Key principles:**
# MAGIC - The **test set is NEVER used** here — only train/validation splits (test is reserved for `04_evaluate.py`)
# MAGIC - **All trials** get their params + metrics logged to MLflow (for reproducibility)
# MAGIC - **Only the winning model** gets its artifact logged (saves storage)
# MAGIC - The Feature Engineering Client is used to maintain feature lineage
# MAGIC
# MAGIC ## Algorithms Supported
# MAGIC Configured via `config.yaml` → `train.model_algorithm`:
# MAGIC - `lightgbm` — fast gradient boosting (default, good for most tabular data)
# MAGIC - `xgboost` — robust gradient boosting
# MAGIC - `random_forest` — ensemble of decision trees (more interpretable)
# MAGIC - `all` — tries all algorithms, picks the best across all
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - `02_feature_engineering.py` has run (feature table populated)
# MAGIC - Config loaded with valid algorithm, n_trials, split_ratios
# MAGIC
# MAGIC ## Outputs
# MAGIC - MLflow experiment with all trials (params + metrics)
# MAGIC - Best model artifact logged via FE Client
# MAGIC - Test split saved for 04_evaluate.py
# MAGIC
# MAGIC ## Next Step
# MAGIC → `04_evaluate.py`

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install optuna lightgbm xgboost catboost databricks-feature-engineering --quiet

# COMMAND ----------

# DBTITLE 1,Restart Python kernel
# MAGIC %restart_python

# COMMAND ----------

# Setup & Config
import sys
from pathlib import Path

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
framework_dir = str(Path(notebook_path).parent.parent.parent)
sys.path.insert(0, f"/Workspace{framework_dir}/src/notebooks")

from helpers import load_config, get_feature_lookups, detect_class_imbalance, get_environment_params, append_deployment_log

config = load_config()

training_table = config["train"]["training_table"]
feature_table_name = config["feature_table_name"]
entity_key = config["entity_key"]
timestamp_key = config.get("timestamp_key")
label_column = config["label_column"]
task_type = config["task_type"]
model_algorithm = config["train"]["model_algorithm"]
n_trials = config["train"]["n_trials"]
positive_label = config["train"].get("positive_label", 1)
split_ratios = config["train"]["split_ratios"]

print(f"Task:        {task_type}")
print(f"Algorithm:   {model_algorithm}")
print(f"Trials:      {n_trials}")
print(f"Splits:      {split_ratios}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Training Set via Feature Engineering Client
# MAGIC
# MAGIC **What happens here:**
# MAGIC - `fe.create_training_set()` joins your labeled data (training_table) with features
# MAGIC   from the Feature Store, using `entity_key` as the join column
# MAGIC - If `timestamp_key` is set, it uses **point-in-time correctness** (only features
# MAGIC   that existed BEFORE each label timestamp are used — prevents data leakage)
# MAGIC - This is the Databricks-recommended way to create training data
# MAGIC **What this does:** Uses the FE Client to join features with labels,
# MAGIC ensuring point-in-time correctness via timestamp_lookup_key.

# COMMAND ----------

# DBTITLE 1,Create Training Set via FE Client
from databricks.feature_engineering import FeatureEngineeringClient
import mlflow

fe = FeatureEngineeringClient()

# Build feature lookups from config
feature_lookups = get_feature_lookups(config)

# Load labels from the source table (only entity_key + timestamp_key + label).
# The source table may contain features too, but we only need labels here —
# the FE Client joins features from the Feature Store via feature_lookups.
labels_df = spark.table(training_table).select(entity_key, timestamp_key, label_column)

# For incremental retraining, filter labels by time instead:
# from pyspark.sql import functions as F
# labels_df = spark.table(training_table).select(entity_key, timestamp_key, label_column).where(F.col(timestamp_key) > "2026-05-01")

# Create training set (point-in-time join)
training_set = fe.create_training_set(
    df=labels_df,
    feature_lookups=feature_lookups,
    label=label_column,
)

training_df = training_set.load_df()
print(f"Training set: {training_df.count():,} rows, {len(training_df.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Train/Val/Test Split
# MAGIC
# MAGIC **What happens here:**
# MAGIC - Data is split into train (70%), validation (15%), and test (15%) — configurable
# MAGIC - **Train**: used to fit the model
# MAGIC - **Validation**: used during HPO to evaluate each trial (prevents overfitting)
# MAGIC - **Test**: held out entirely — used ONLY in `04_evaluate.py` for final assessment
# MAGIC **Why:** Strict isolation — Optuna sees ONLY train+val. Test is locked.

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split

# Convert to pandas for scikit-learn
pdf = training_df.toPandas()

y = pdf[label_column]
X = pdf.drop(columns=[label_column, entity_key] + ([timestamp_key] if timestamp_key else []))

# Two-stage split: train+val vs test, then train vs val
train_ratio = split_ratios["train"]
val_ratio = split_ratios["val"]
test_ratio = split_ratios["test"]

X_trainval, X_test, y_trainval, y_test = train_test_split(
    X, y, test_size=test_ratio, random_state=42, stratify=y if task_type == "classification" else None
)
val_frac = val_ratio / (train_ratio + val_ratio)
X_train, X_val, y_train, y_val = train_test_split(
    X_trainval, y_trainval, test_size=val_frac, random_state=42,
    stratify=y_trainval if task_type == "classification" else None
)

print(f"Train: {X_train.shape[0]:,} | Val: {X_val.shape[0]:,} | Test: {X_test.shape[0]:,}")

# Save test set for 04_evaluate.py (as temp table)
test_pdf = pd.concat([X_test.reset_index(drop=True),
                      y_test.reset_index(drop=True).rename(label_column)], axis=1)
spark.createDataFrame(test_pdf).write.mode("overwrite").saveAsTable(
    f"{config['catalog']}.{config['schema']}.{config['model_name']}_test_split"
)
print("✅ Test split saved (locked for 04_evaluate.py)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Preprocessing Pipeline
# MAGIC
# MAGIC **What happens here:**
# MAGIC - Builds a `ColumnTransformer` that handles numeric and categorical features differently:
# MAGIC   - **Numeric**: impute missing values → standard scaling
# MAGIC   - **Categorical**: impute → one-hot encoding
# MAGIC - This preprocessing is applied consistently to train, validation, and test data
# MAGIC **What this does:** ColumnTransformer that auto-detects numeric vs categorical.
# MAGIC This pipeline is part of the model artifact — same transforms in train/serve.

# COMMAND ----------

# DBTITLE 1,Cell 10
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.pipeline import Pipeline

# Auto-detect column types
numeric_features = X_train.select_dtypes(include=[np.number]).columns.tolist()
categorical_features = X_train.select_dtypes(include=["object", "category"]).columns.tolist()

print(f"Numeric features:     {len(numeric_features)}")
print(f"Categorical features: {len(categorical_features)}")

preprocessor = ColumnTransformer(
    transformers=[
        ("num", StandardScaler(), numeric_features),
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), categorical_features),
    ],
    remainder="drop"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Baselines
# MAGIC
# MAGIC **What happens here:**
# MAGIC - Trains simple baseline models to establish a minimum performance bar:
# MAGIC   - Classification: DummyClassifier (majority vote) + Logistic Regression
# MAGIC   - Regression: DummyRegressor (mean prediction) + Ridge Regression
# MAGIC - If your tuned model can not beat these baselines, something is wrong with the data
# MAGIC **What this does:** Trains simple baselines (no tuning) to establish a floor.
# MAGIC Any tuned model should beat these — if it doesn't, something is wrong.

# COMMAND ----------

# DBTITLE 1,Step 4: Baselines
from sklearn.dummy import DummyClassifier, DummyRegressor
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import f1_score, average_precision_score, mean_squared_error, r2_score

_imb = detect_class_imbalance(pd.DataFrame({label_column: y_train}), label_column)
is_imbalanced = bool(_imb["is_imbalanced"])
imbalance_ratio = float(_imb["ratio"])

# Set experiment
mlflow.set_experiment(f"/Users/{config.get('_current_user', 'shared')}/{config['model_name']}_experiment")

if task_type == "classification":
    baselines = [
        ("dummy_most_frequent", DummyClassifier(strategy="most_frequent")),
        ("logistic_regression", LogisticRegression(C=1.0, class_weight="balanced", max_iter=1000)),
    ]
else:
    baselines = [
        ("dummy_mean", DummyRegressor(strategy="mean")),
        ("ridge_default", Ridge(alpha=1.0)),
    ]

for name, model in baselines:
    pipe = Pipeline([("preprocessor", preprocessor), ("model", model)])
    pipe.fit(X_train, y_train)
    y_pred = pipe.predict(X_val)
    
    with mlflow.start_run(run_name=f"baseline_{name}"):
        if task_type == "classification":
            f1 = f1_score(y_val, y_pred, pos_label=positive_label)
            mlflow.log_metric("val_f1", f1)
            print(f"  {name}: val_f1 = {f1:.4f}")
        else:
            rmse = mean_squared_error(y_val, y_pred, squared=False)
            mlflow.log_metric("val_rmse", rmse)
            print(f"  {name}: val_rmse = {rmse:.4f}")
        mlflow.log_params({"model_type": name, "is_baseline": True})

print("\n✅ Baselines logged")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Optuna Hyperparameter Optimization
# MAGIC
# MAGIC **What happens here:**
# MAGIC - Optuna searches the hyperparameter space intelligently (not random — uses TPE algorithm)
# MAGIC - Each trial trains a model with different hyperparameters and evaluates on validation set
# MAGIC - Parallel trials (`n_jobs`) run simultaneously for speed
# MAGIC - MedianPruner stops bad trials early (saves compute time)
# MAGIC - `n_trials` (from config.yaml) controls how many combinations to try
# MAGIC **What this does:** Parallel Optuna trials searching the best hyperparameters.
# MAGIC Only params + metrics are logged per trial. The full model artifact is logged
# MAGIC ONLY for the best trial (saves storage).
# MAGIC
# MAGIC **Objective:** val_f1 (balanced), val_pr_auc (imbalanced), val_rmse (regression)

# COMMAND ----------

# DBTITLE 1,Cell 14
import optuna
from optuna.samplers import TPESampler
from optuna.pruners import MedianPruner
import os

optuna.logging.set_verbosity(optuna.logging.WARNING)

# Determine parallelism
parallel_config = config["train"].get("parallel_trials", "auto")
if parallel_config == "auto":
    n_jobs = min(4, max(1, os.cpu_count() // 2))
else:
    n_jobs = int(parallel_config)
cores_per_trial = max(1, os.cpu_count() // n_jobs)

print(f"Parallel trials: {n_jobs} ({cores_per_trial} cores each)")

# Determine objective metric
if task_type == "classification":
    if is_imbalanced:
        objective_metric = "val_pr_auc"
        objective_direction = "maximize"
    else:
        objective_metric = "val_f1"
        objective_direction = "maximize"
else:
    objective_metric = "val_rmse"
    objective_direction = "minimize"

print(f"Objective: {objective_direction} {objective_metric}")

# Determine which algorithms to try
algorithms_to_run = []
if model_algorithm in ["lightgbm", "all"]:
    algorithms_to_run.append("lightgbm")
if model_algorithm in ["xgboost", "all"]:
    algorithms_to_run.append("xgboost")
if model_algorithm in ["catboost", "all"]:
    algorithms_to_run.append("catboost")
if model_algorithm in ["random_forest", "all"]:
    algorithms_to_run.append("random_forest")
if model_algorithm in ["elasticnet", "all"]:
    algorithms_to_run.append("elasticnet")

print(f"Algorithms: {algorithms_to_run}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Define Optuna Objective

# COMMAND ----------

# DBTITLE 1,Cell 16
from sklearn.metrics import f1_score, average_precision_score, mean_squared_error

def create_objective(algorithm: str):
    """Factory: returns an Optuna objective function for the given algorithm."""
    
    def objective(trial):
        if algorithm == "lightgbm":
            import lightgbm as lgb
            params = {
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
                "n_estimators": trial.suggest_int("n_estimators", 100, 1000, step=50),
                "max_depth": trial.suggest_int("max_depth", 3, 12),
                "num_leaves": trial.suggest_int("num_leaves", 20, 150),
                "min_child_samples": trial.suggest_int("min_child_samples", 5, 100),
                "subsample": trial.suggest_float("subsample", 0.5, 1.0),
                "colsample_bytree": trial.suggest_float("colsample_bytree", 0.4, 1.0),
                "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
                "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
                "random_state": 42,
                "n_jobs": cores_per_trial,
                "verbosity": -1,
            }
            if task_type == "classification":
                params["class_weight"] = trial.suggest_categorical("class_weight", ["balanced", None])
                model = lgb.LGBMClassifier(**params)
            else:
                model = lgb.LGBMRegressor(**params)

        elif algorithm == "xgboost":
            import xgboost as xgb
            params = {
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
                "n_estimators": trial.suggest_int("n_estimators", 100, 1000, step=50),
                "max_depth": trial.suggest_int("max_depth", 3, 12),
                "min_child_weight": trial.suggest_int("min_child_weight", 1, 20),
                "gamma": trial.suggest_float("gamma", 0.0, 5.0),
                "subsample": trial.suggest_float("subsample", 0.5, 1.0),
                "colsample_bytree": trial.suggest_float("colsample_bytree", 0.4, 1.0),
                "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
                "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
                "random_state": 42,
                "n_jobs": cores_per_trial,
                "verbosity": 0,
            }
            if task_type == "classification":
                model = xgb.XGBClassifier(**params, eval_metric="logloss")
            else:
                model = xgb.XGBRegressor(**params, eval_metric="rmse")

        elif algorithm == "catboost":
            from catboost import CatBoostClassifier, CatBoostRegressor
            params = {
                "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
                "iterations": trial.suggest_int("iterations", 100, 1000, step=50),
                "depth": trial.suggest_int("depth", 3, 10),
                "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1e-3, 10.0, log=True),
                "bagging_temperature": trial.suggest_float("bagging_temperature", 0.0, 1.0),
                "random_strength": trial.suggest_float("random_strength", 1e-3, 10.0, log=True),
                "border_count": trial.suggest_int("border_count", 32, 255),
                "random_seed": 42,
                "verbose": 0,
                "thread_count": cores_per_trial,
            }
            if task_type == "classification":
                params["auto_class_weights"] = trial.suggest_categorical("auto_class_weights", ["Balanced", "SqrtBalanced", None])
                model = CatBoostClassifier(**params)
            else:
                model = CatBoostRegressor(**params)

        elif algorithm == "random_forest":
            from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
            params = {
                "n_estimators": trial.suggest_int("n_estimators", 100, 500, step=50),
                "max_depth": trial.suggest_int("max_depth", 3, 25),
                "min_samples_split": trial.suggest_int("min_samples_split", 2, 20),
                "min_samples_leaf": trial.suggest_int("min_samples_leaf", 1, 20),
                "max_features": trial.suggest_categorical("max_features", ["sqrt", "log2"]),
                "random_state": 42,
                "n_jobs": cores_per_trial,
            }
            if task_type == "classification":
                params["class_weight"] = trial.suggest_categorical("class_weight", ["balanced", "balanced_subsample", None])
                model = RandomForestClassifier(**params)
            else:
                model = RandomForestRegressor(**params)

        elif algorithm == "elasticnet":
            from sklearn.linear_model import LogisticRegression, ElasticNet as ElasticNetRegressor
            if task_type == "classification":
                # LogisticRegression with elasticnet penalty (covers L1, L2, and mix)
                params = {
                    "C": trial.suggest_float("C", 1e-4, 100.0, log=True),
                    "l1_ratio": trial.suggest_float("l1_ratio", 0.0, 1.0),
                    "penalty": "elasticnet",
                    "solver": "saga",
                    "max_iter": 2000,
                    "class_weight": trial.suggest_categorical("class_weight", ["balanced", None]),
                    "random_state": 42,
                    "n_jobs": cores_per_trial,
                }
                model = LogisticRegression(**params)
            else:
                params = {
                    "alpha": trial.suggest_float("alpha", 1e-5, 10.0, log=True),
                    "l1_ratio": trial.suggest_float("l1_ratio", 0.0, 1.0),
                    "max_iter": 2000,
                    "random_state": 42,
                }
                model = ElasticNetRegressor(**params)

        # Build pipeline with preprocessing
        pipe = Pipeline([("preprocessor", preprocessor), ("model", model)])
        pipe.fit(X_train, y_train)
        y_pred = pipe.predict(X_val)

        # Compute metric
        if objective_metric == "val_f1":
            score = f1_score(y_val, y_pred, pos_label=positive_label)
        elif objective_metric == "val_pr_auc":
            y_proba = pipe.predict_proba(X_val)[:, 1]
            score = average_precision_score(y_val, y_proba)
        elif objective_metric == "val_rmse":
            score = mean_squared_error(y_val, y_pred, squared=False)

        # Log to MLflow
        with mlflow.start_run(run_name=f"{algorithm}_trial_{trial.number}", nested=True):
            mlflow.log_params(trial.params)
            mlflow.log_params({"algorithm": algorithm, "trial_number": trial.number})
            mlflow.log_metric(objective_metric, score)

        return score

    return objective

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Run HPO

# COMMAND ----------

# DBTITLE 1,Run HPO
import warnings
warnings.filterwarnings("ignore")
optuna.logging.set_verbosity(optuna.logging.ERROR)

best_results = {}  # algorithm -> (study, best_score, best_params)

def trial_callback(study, trial):
    """Print progress after each completed trial."""
    best_so_far = study.best_value
    marker = " ⭐ NEW BEST" if trial.value == best_so_far else ""
    print(f"  Trial {trial.number + 1:>3}/{n_trials} | {objective_metric}: {trial.value:.4f} | best so far: {best_so_far:.4f}{marker}")

for algorithm in algorithms_to_run:
    print(f"\n{'=' * 60}")
    print(f"  🚀 HPO: {algorithm.upper()}")
    print(f"     Trials: {n_trials} | Parallel: {n_jobs} | Metric: {objective_metric}")
    print(f"{'=' * 60}")

    with mlflow.start_run(run_name=f"hpo_{algorithm}"):
        study = optuna.create_study(
            direction=objective_direction,
            sampler=TPESampler(seed=42),
            pruner=MedianPruner(n_warmup_steps=10),
        )
        study.optimize(
            create_objective(algorithm),
            n_trials=n_trials,
            n_jobs=n_jobs,
            callbacks=[trial_callback],
        )

        best_score = study.best_value
        best_params = study.best_params
        mlflow.log_metric(f"best_{objective_metric}", best_score)
        mlflow.log_params({"algorithm": algorithm, "n_trials": n_trials})

    best_results[algorithm] = (study, best_score, best_params)
    print(f"\n  ✅ {algorithm} complete | Best {objective_metric}: {best_score:.4f}")
    print(f"     Params: {best_params}")

# Summary table
print(f"\n{'=' * 60}")
print(f"  HPO SUMMARY")
print(f"{'=' * 60}")
for algo, (_, score, _) in sorted(best_results.items(), key=lambda x: x[1][1], reverse=(objective_direction == "maximize")):
    print(f"  {algo:20s} {objective_metric} = {score:.4f}")
print(f"{'=' * 60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Select Overall Winner & Log Full Model
# MAGIC **What this does:** Picks the best algorithm overall, retrains with best params,
# MAGIC and logs the FULL model artifact via FE Client (only for this winner).

# COMMAND ----------

# DBTITLE 1,Cell 20
import warnings
warnings.filterwarnings("ignore")
from mlflow.models import infer_signature

# Pick overall best
if objective_direction == "maximize":
    winner = max(best_results, key=lambda k: best_results[k][1])
else:
    winner = min(best_results, key=lambda k: best_results[k][1])

winner_score = best_results[winner][1]
winner_params = best_results[winner][2]

# Retrain winner on full train set with best params
print(f"Retraining {winner} on full training set...")
if winner == "lightgbm":
    import lightgbm as lgb
    ModelClass = lgb.LGBMClassifier if task_type == "classification" else lgb.LGBMRegressor
    final_model = ModelClass(**winner_params, random_state=42, n_jobs=-1, verbose=-1)
elif winner == "xgboost":
    import xgboost as xgb
    ModelClass = xgb.XGBClassifier if task_type == "classification" else xgb.XGBRegressor
    final_model = ModelClass(**winner_params, random_state=42, n_jobs=-1, verbosity=0)
elif winner == "catboost":
    from catboost import CatBoostClassifier, CatBoostRegressor
    ModelClass = CatBoostClassifier if task_type == "classification" else CatBoostRegressor
    final_model = ModelClass(**winner_params, random_seed=42, verbose=0)
elif winner == "random_forest":
    from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
    ModelClass = RandomForestClassifier if task_type == "classification" else RandomForestRegressor
    final_model = ModelClass(**winner_params, random_state=42, n_jobs=-1)
elif winner == "elasticnet":
    from sklearn.linear_model import LogisticRegression, ElasticNet as ElasticNetRegressor
    if task_type == "classification":
        final_model = LogisticRegression(**winner_params, random_state=42)
    else:
        final_model = ElasticNetRegressor(**winner_params, random_state=42)

final_pipeline = Pipeline([("preprocessor", preprocessor), ("model", final_model)])
final_pipeline.fit(X_train, y_train)

# Compute comprehensive metrics on both train and validation sets
from sklearn.metrics import (
    f1_score, precision_score, recall_score, accuracy_score,
    roc_auc_score, average_precision_score, log_loss,
    mean_squared_error, mean_absolute_error, r2_score,
)

def compute_metrics(pipeline, X, y, prefix, task_type, pos_label):
    """Compute all relevant metrics for a dataset split."""
    y_pred = pipeline.predict(X)
    metrics = {}
    if task_type == "classification":
        metrics[f"{prefix}_accuracy"] = accuracy_score(y, y_pred)
        metrics[f"{prefix}_f1"] = f1_score(y, y_pred, pos_label=pos_label)
        metrics[f"{prefix}_precision"] = precision_score(y, y_pred, pos_label=pos_label, zero_division=0)
        metrics[f"{prefix}_recall"] = recall_score(y, y_pred, pos_label=pos_label, zero_division=0)
        if hasattr(pipeline, "predict_proba"):
            y_proba = pipeline.predict_proba(X)[:, 1]
            metrics[f"{prefix}_roc_auc"] = roc_auc_score(y, y_proba)
            metrics[f"{prefix}_pr_auc"] = average_precision_score(y, y_proba)
            metrics[f"{prefix}_log_loss"] = log_loss(y, y_proba)
    else:
        from sklearn.metrics import median_absolute_error, mean_absolute_percentage_error, explained_variance_score
        metrics[f"{prefix}_rmse"] = mean_squared_error(y, y_pred, squared=False)
        metrics[f"{prefix}_mae"] = mean_absolute_error(y, y_pred)
        metrics[f"{prefix}_median_ae"] = median_absolute_error(y, y_pred)
        metrics[f"{prefix}_mape"] = mean_absolute_percentage_error(y, y_pred)
        metrics[f"{prefix}_r2"] = r2_score(y, y_pred)
        metrics[f"{prefix}_explained_var"] = explained_variance_score(y, y_pred)
    return metrics

train_metrics = compute_metrics(final_pipeline, X_train, y_train, "train", task_type, positive_label)
val_metrics = compute_metrics(final_pipeline, X_val, y_val, "val", task_type, positive_label)
all_metrics = {**train_metrics, **val_metrics}

# Log via FE Client
with mlflow.start_run(run_name=f"best_model_{winner}") as run:
    env_params = get_environment_params(config)
    mlflow.log_params(env_params)
    mlflow.log_params({
        "algorithm": winner,
        "task_type": task_type,
        "n_train_rows": len(X_train),
        "n_features": X_train.shape[1],
        "training_table": training_table,
        "feature_table": feature_table_name,
    })
    mlflow.log_params(winner_params)
    mlflow.log_metrics(all_metrics)

    signature = infer_signature(X_train.head(5), final_pipeline.predict(X_train.head(5)))
    input_example = X_train.head(3)

    fe.log_model(
        model=final_pipeline,
        artifact_path="model",
        flavor=mlflow.sklearn,
        training_set=training_set,
        signature=signature,
        input_example=input_example,
    )
    best_run_id = run.info.run_id

# Clean summary
print(f"\n{'=' * 60}")
print(f"  TRAINING COMPLETE")
print(f"{'=' * 60}")
print(f"  Algorithm:   {winner}")
print(f"  Run ID:      {best_run_id}")
print(f"{'\u2500' * 60}")
print(f"  {'Metric':<20s} {'Train':>10s} {'Validation':>10s} {'Overfit?':>10s}")
print(f"  {'\u2500' * 52}")

# Metrics where higher = better (overfit = train >> val)
higher_is_better = {"accuracy", "f1", "precision", "recall", "roc_auc", "pr_auc", "r2", "explained_var"}

for key in sorted(train_metrics.keys()):
    metric_name = key.replace("train_", "")
    t_val = train_metrics[key]
    v_key = f"val_{metric_name}"
    v_val = val_metrics.get(v_key, None)
    if v_val is not None:
        # Detect overfitting using relative gap
        if metric_name in higher_is_better:
            # Higher = better: overfit if train >> val
            gap = t_val - v_val
            flag = "⚠️" if gap > 0.10 else ""
        else:
            # Lower = better (rmse, mae, log_loss, mape): overfit if train << val
            rel_gap = (v_val - t_val) / max(abs(v_val), 1e-8)
            flag = "⚠️" if rel_gap > 0.20 else ""  # 20% relative degradation
        print(f"  {metric_name:<20s} {t_val:>10.4f} {v_val:>10.4f} {flag:>10s}")
print(f"{'=' * 60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Save Run Context for Downstream Notebooks

# COMMAND ----------

import json

# Save context for 04_evaluate and 05_register
run_context = {
    "best_run_id": best_run_id,
    "best_algorithm": winner,
    "best_score": winner_score,
    "objective_metric": objective_metric,
    "n_trials": n_trials,
    "task_type": task_type,
}

try:
    dbutils.jobs.taskValues.set(key="train_context", value=json.dumps(run_context))
    print(f"Task context saved (jobs mode): {run_context}")
except Exception:
    # Interactive mode — taskValues not available; downstream notebooks use MLflow search
    print(f"Interactive mode: task context not persisted via taskValues.")
    print(f"Downstream notebooks will use MLflow to locate run_id={best_run_id}")
print(f"best_run_id = {best_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Training complete.
# MAGIC - Baselines: logged (floor metrics established)
# MAGIC - HPO: {n_trials} trials per algorithm, best = {winner} ({objective_metric}={winner_score:.4f})
# MAGIC - Model artifact: logged via FE Client with full lineage
# MAGIC - Test split: saved for holdout evaluation
# MAGIC
# MAGIC → **Next:** `04_evaluate.py`

# COMMAND ----------

append_deployment_log(
    event="model_trained",
    resource=f"{config['catalog']}.{config['schema']}.{config['model_name']}",
    version=f"run_id={best_run_id}",
    notes=f"{winner}, {objective_metric}={winner_score:.4f}, {n_trials} trials"
)
