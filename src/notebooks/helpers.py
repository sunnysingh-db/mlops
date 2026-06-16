"""
helpers.py — Shared Utility Functions for the MLOps Framework
=============================================================

This module provides utility functions used across all notebooks in the framework.
You should NOT need to edit this file — it reads from config.yaml automatically.

Functions:
    load_config()          — Reads config.yaml and returns the config dict
    get_current_user()     — Returns the current Databricks user email
    append_deployment_log() — Appends an event to DEPLOYMENT_LOG.md
    get_feature_lookups()  — Builds FeatureLookup list for FE Client
    detect_class_imbalance() — Checks if target is imbalanced (>5:1 ratio)
    get_environment_params() — Returns environment-specific params (dev/staging/prod)
"""

import yaml
from pathlib import Path


def load_config() -> dict:
    """Load framework configuration from config.yaml.

    Automatically finds config.yaml relative to this file's location.
    Returns the full config dictionary with all settings.

    Example:
        config = load_config()
        print(config["model_name"])  # "churn_model"
        print(config["train"]["n_trials"])  # 50
    """
    # Navigate from src/notebooks/helpers.py → framework root
    framework_dir = str(Path(__file__).parent.parent.parent)
    config_path = f"{framework_dir}/config.yaml"

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Validation
    required = ["mode", "catalog", "schema", "model_name", "task_type",
                "entity_key", "label_column", "feature_table_name"]
    missing = [k for k in required if not config.get(k)]
    if missing:
        raise ValueError(f"Missing required config fields: {missing}")

    # Inject runtime-resolved current user for MLflow experiment naming
    if '_current_user' not in config:
        try:
            from databricks.sdk import WorkspaceClient
            config['_current_user'] = WorkspaceClient().current_user.me().user_name
        except Exception:
            config['_current_user'] = 'shared'

    return config


def get_current_user() -> str:
    """Get the current Databricks user's email address.

    Useful for creating user-specific experiment paths or logging.
    """
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    return w.current_user.me().user_name


def append_deployment_log(event: str, resource: str, version: str, notes: str = ""):
    """Append a row to the deployment log (DEPLOYMENT_LOG.md).

    Args:
        event: What happened (e.g., "model_registered", "endpoint_deployed")
        resource: What was affected (e.g., table name, endpoint name)
        version: Version or model URI

    Example:
        append_deployment_log("model_registered", "catalog.schema.model", "v3")
    """
    from datetime import datetime

    framework_dir = str(Path(__file__).parent.parent.parent)
    log_path = f"{framework_dir}/deployment/DEPLOYMENT_LOG.md"

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    user = get_current_user()
    row = f"| {timestamp} | {event} | {resource} | {version} | {notes} | {user} |\n"

    try:
        with open(log_path, "a") as f:
            f.write(row)
    except FileNotFoundError:
        # Create the log file with header
        header = "| Timestamp | Event | Resource | Version | Notes | User |\n"
        header += "| --- | --- | --- | --- | --- | --- |\n"
        with open(log_path, "w") as f:
            f.write(header + row)


def get_feature_lookups(config: dict) -> list:
    """Build FeatureLookup list for the Feature Engineering Client.

    This tells the FE Client which features to fetch and how to join them.
    Used in create_training_set() and score_batch().

    Args:
        config: The framework config dict

    Returns:
        List of FeatureLookup objects

    Example:
        lookups = get_feature_lookups(config)
        training_set = fe.create_training_set(df=labels_df, feature_lookups=lookups, ...)
    """
    from databricks.feature_engineering import FeatureLookup

    return [
        FeatureLookup(
            table_name=config["feature_table_name"],
            lookup_key=config["entity_key"],
            timestamp_lookup_key=config.get("timestamp_key"),
        )
    ]


def detect_class_imbalance(pdf, label_column: str = None, threshold: float = 5.0) -> dict:
    """Detect if the target variable is imbalanced.

    Args:
        pdf: Pandas DataFrame with the label column
        label_column: Name of the target column
        threshold: Ratio above which we flag imbalance (default: 5:1)

    Returns:
        Dict with keys: is_imbalanced (bool), ratio (float), majority_class, minority_class

    Example:
        result = detect_class_imbalance(df, "churned")
        if result["is_imbalanced"]:
            print(f"Warning: {result['ratio']:.1f}:1 imbalance")
    """
    import pandas as pd
    if isinstance(pdf, pd.Series):
        counts = pdf.value_counts()
    elif label_column is not None:
        counts = pdf[label_column].value_counts()
    else:
        raise ValueError("Pass a pandas Series, or a DataFrame + label_column")
    majority = counts.iloc[0]
    minority = counts.iloc[-1]
    ratio = majority / minority if minority > 0 else float("inf")

    return {
        "is_imbalanced": ratio >= threshold,
        "ratio": ratio,
        "majority_class": counts.index[0],
        "minority_class": counts.index[-1],
        "distribution": counts.to_dict(),
    }


def get_environment_params(config: dict) -> dict:
    """Get environment-specific parameters based on the target (dev/staging/prod).

    Returns different settings per environment:
    - dev: smaller data samples, fewer trials, local experiment
    - staging: full data, full trials, shared experiment
    - prod: full data, full trials, production experiment

    Example:
        env = get_environment_params(config)
        print(env["experiment_prefix"])  # "/dev/" or "/prod/"
    """
    target = config.get("target", "dev")

    params = {
        "dev": {
            "experiment_prefix": "/dev/",
            "sample_fraction": 0.1,
            "n_trials_multiplier": 0.5,
        },
        "staging": {
            "experiment_prefix": "/staging/",
            "sample_fraction": 1.0,
            "n_trials_multiplier": 1.0,
        },
        "prod": {
            "experiment_prefix": "/prod/",
            "sample_fraction": 1.0,
            "n_trials_multiplier": 1.0,
        },
    }

    return params.get(target, params["dev"])
