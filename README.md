# Databricks MLOps Framework
.
A **reusable, config-driven framework** for building production ML pipelines on Databricks.
Edit one config file, define your features, and the framework handles everything else:
training, evaluation, registration, serving, and monitoring.

---

## Quick Start (5 Minutes)

```
1. Edit config.yaml           ← Set your catalog, schema, model name, algorithm
2. Edit 02_feature_engineering.py  ← Define your feature transformations
3. Run notebooks in order     ← 01 → 02 → 03 → 04 → 05
4. (Optional) Run INSTALL     ← Deploys as scheduled Databricks Jobs
```

---
..
## Directory Structure

```
mlops-framework/
├── config.yaml              ← THE ONLY CONFIG FILE (edit this)
├── INSTALL.py               ← Optional: generates DAB job configs + deploys
├── README.md                ← You are here
├── src/
│   ├── generator.py         ← DAB config generation logic
│   └── notebooks/           ← All ML notebooks (static, always present)
│       ├── helpers.py       ← Shared utilities (load_config, etc.)
│       ├── nn_architectures.py  ← PyTorch models (for deep learning mode)
│       ├── 01_eda.py
│       ├── 02_feature_engineering.py  ← USER EDITS THIS
│       ├── 03_train_tune.py
│       ├── 03b_train_deep_learning.py
│       ├── 04_evaluate.py
│       ├── 04b_explainability.py
│       ├── 05_register.py
│       ├── 01_validate_model.py    (migrate mode)
│       ├── 03_wrap_and_register.py (migrate mode)
│       ├── 06_batch_inference.py
│       ├── 07_serve.py
│       ├── 08_monitor.py
│       └── 09_process_inference.py
├── resources/               ← Generated DAB job YAML files
├── deployment/
│   └── DEPLOYMENT_LOG.md    ← Auto-maintained deployment history
└── databricks.yml           ← Generated DAB bundle config
```

---

## Two Modes

### Train Mode (build a new model)
```
01_eda → 02_feature_engineering → 03_train_tune → 04_evaluate → 05_register
                                                                      ↓
                                            06_batch_inference / 07_serve → 08_monitor
```

### Migrate Mode (bring an existing pickle)
```
01_validate_model → 02_feature_engineering → 03_wrap_and_register
                                                        ↓
                                 06_batch_inference / 07_serve → 08_monitor
```

---

## What Each File Does

| File | What It Does | User Action |
|------|-------------|-------------|
| `config.yaml` | All settings (catalog, schema, algorithm, etc.) | **EDIT THIS** |
| `01_eda.py` | Profiles data: shape, nulls, correlations, leakage | Run & review |
| `02_feature_engineering.py` | Template for computing features | **EDIT THIS** |
| `03_train_tune.py` | HPO with Optuna (LightGBM/XGBoost/RF) | Just run |
| `03b_train_deep_learning.py` | PyTorch tabular models (auto-skips if disabled) | Just run |
| `04_evaluate.py` | Holdout test metrics + diagnostic plots | Run & review |
| `04b_explainability.py` | SHAP values + feature importance | Just run |
| `05_register.py` | Registers Champion model in Unity Catalog | Just run |
| `06_batch_inference.py` | Scores new data with Champion model | Just run |
| `07_serve.py` | Creates real-time serving endpoint | Just run |
| `08_monitor.py` | Sets up drift monitoring | Just run |
| `09_process_inference.py` | Flattens serving logs | Just run |

---

## Feature Engineering Flow

The framework uses the **Databricks Feature Engineering Client** for end-to-end lineage:

```
┌─────────────────────────┐     ┌──────────────────────────────┐
│ 02_feature_engineering  │     │ Feature Store (UC Table)     │
│ - Raw data transforms   │ ──→ │ - Primary key: entity_key    │
│ - Aggregations          │     │ - Timestamp: timestamp_key   │
│ - fe.create_table()     │     │ - All computed features      │
└─────────────────────────┘     └──────────────────────────────┘
                                              │
                         ┌────────────────────┼────────────────────┐
                         ↓                    ↓                    ↓
              ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
              │ 03_train_tune    │ │ 06_batch_infer.  │ │ 07_serve         │
              │ fe.create_       │ │ fe.score_batch() │ │ Endpoint auto-   │
              │ training_set()   │ │ (auto-fetches    │ │ fetches features │
              │ + fe.log_model() │ │  features)       │ │ at request time  │
              └──────────────────┘ └──────────────────┘ └──────────────────┘
```

**Key insight**: After `02_feature_engineering.py` writes to the Feature Store, you NEVER
manually join features again. The FE Client handles all joins automatically during training,
batch inference, and serving.

---

## Configuration Reference

See `config.yaml` for the full schema with inline comments. Key fields:

| Field | Description | Example |
|-------|-------------|---------|
| `mode` | "train" or "migrate" | "train" |
| `catalog` | Unity Catalog name | "ml_prod" |
| `schema` | Schema name | "churn" |
| `model_name` | Model identifier | "churn_lgbm" |
| `task_type` | "classification" or "regression" | "classification" |
| `entity_key` | Primary key for feature lookups | "customer_id" |
| `train.model_algorithm` | Which algorithm(s) to try | "lightgbm" |
| `train.n_trials` | Number of HPO trials | 50 |
| `inference_mode` | "batch", "serving", or "both" | "both" |

---

## Deploying as Scheduled Jobs

The `INSTALL` notebook generates Databricks Asset Bundle configs and deploys them:

1. Run `INSTALL` Cell 1 → generates `databricks.yml` + `resources/*.yml`
2. Run `INSTALL` Cell 2 → validates and deploys to your target environment

This creates two jobs:
- **Training pipeline**: Runs all training notebooks in sequence (01→02→03→04→05)
- **Batch inference**: Runs scoring + monitoring on a schedule

---

## FAQ

**Q: Do I need to run INSTALL before using the notebooks?**
No. Notebooks are static and can be run directly. INSTALL is only for deploying as scheduled jobs.

**Q: What if I re-run 02_feature_engineering after changing my features?**
It uses `fe.write_table(mode="overwrite")` on subsequent runs, so your feature table is updated cleanly.

**Q: Can I add more algorithms?**
Yes — edit `03_train_tune.py` to add your own Optuna objective function. The framework
is designed to be extended.

**Q: What happens if a notebook doesn't apply to my config?**
Conditional notebooks (03b, 04b, 07, 08, 09) have "self-skip gates" — they check
config.yaml at the top and exit gracefully if they're not relevant.
