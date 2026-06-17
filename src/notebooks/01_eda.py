# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # 01 — Comprehensive Exploratory Data Analysis
# MAGIC
# MAGIC ## Purpose
# MAGIC A rigorous, production-grade EDA that a tenured data scientist would run before
# MAGIC building any model. This is NOT a quick glance — it's a systematic diagnostic that
# MAGIC identifies data issues, informs feature engineering, and documents assumptions.
# MAGIC
# MAGIC ## Diagnostic Sections
# MAGIC | # | Section | Key Questions |
# MAGIC |---|---------|---------------|
# MAGIC | 1 | **Data Profiling** | Shape, types, memory, cardinality, duplicates |
# MAGIC | 2 | **Missing Value Analysis** | Patterns, mechanisms (MCAR/MAR/MNAR), imputation strategy |
# MAGIC | 3 | **Target Analysis** | Distribution, class balance, statistical tests |
# MAGIC | 4 | **Univariate Distributions** | Skewness, kurtosis, outliers, transformation candidates |
# MAGIC | 5 | **Outlier Detection** | IQR + z-score flagging, percentage of extreme values |
# MAGIC | 6 | **Bivariate Analysis** | Feature vs target, discrimination power, effect size |
# MAGIC | 7 | **Multicollinearity** | Correlation matrix, VIF, redundant feature groups |
# MAGIC | 8 | **Target Leakage Screen** | High-correlation flags, temporal leakage checks |
# MAGIC | 9 | **Temporal Patterns** | Data freshness, time-based drift, seasonality |
# MAGIC | 10 | **Data Quality Scorecard** | Overall pass/fail summary with actionable recommendations |
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - `config.yaml` configured with training table and label column
# MAGIC
# MAGIC ## Next Step
# MAGIC → `02_feature_engineering.py`

# COMMAND ----------

# DBTITLE 1,Setup & Config
# Setup & Config
import sys
from pathlib import Path
import warnings
warnings.filterwarnings("ignore")

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
framework_dir = str(Path(notebook_path).parent.parent.parent)
sys.path.insert(0, f"/Workspace{framework_dir}/src/notebooks")

from helpers import load_config, detect_class_imbalance
config = load_config()

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

# Configure plot aesthetics
sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)
plt.rcParams["figure.dpi"] = 120
plt.rcParams["axes.titleweight"] = "bold"

# Extract config
training_table = config["train"]["training_table"]
label_column = config["label_column"]
task_type = config["task_type"]
entity_key = config["entity_key"]
timestamp_key = config["timestamp_key"]
feature_table = config.get("feature_table_name", "")

print(f"═" * 60)
print(f"  EDA Configuration")
print(f"═" * 60)
print(f"  Training table:  {training_table}")
print(f"  Feature table:   {feature_table}")
print(f"  Label column:    {label_column}")
print(f"  Entity key:      {entity_key}")
print(f"  Timestamp key:   {timestamp_key}")
print(f"  Task type:       {task_type}")
print(f"═" * 60)

# COMMAND ----------

# DBTITLE 1,Section 1
# MAGIC %md
# MAGIC ## 1. Data Profiling & Structural Diagnostics
# MAGIC Comprehensive structural analysis: shape, types, memory footprint, cardinality, duplicates, and sample size adequacy.

# COMMAND ----------

# DBTITLE 1,Data Profiling
# Load training data + feature table (if available) for full-picture EDA
df_train = spark.table(training_table)
pdf_train = df_train.toPandas()

# If feature table exists, join for complete feature analysis
try:
    df_features = spark.table(feature_table)
    pdf_features = df_features.toPandas()
    # Merge on entity key for full feature + label view
    pdf = pdf_train.merge(pdf_features, on=entity_key, how="inner", suffixes=("", "_feat"))
    # Drop duplicate timestamp columns from join
    dup_cols = [c for c in pdf.columns if c.endswith("_feat")]
    pdf = pdf.drop(columns=dup_cols)
    print(f"✅ Joined training labels with feature table ({len(pdf):,} rows)")
except Exception:
    pdf = pdf_train.copy()
    print(f"ℹ️  Using training table only ({len(pdf):,} rows)")

# Identify column roles
numeric_cols = pdf.select_dtypes(include=[np.number]).columns.tolist()
categorical_cols = pdf.select_dtypes(include=["object", "category"]).columns.tolist()
datetime_cols = pdf.select_dtypes(include=["datetime64"]).columns.tolist()
feature_cols = [c for c in numeric_cols if c not in [entity_key, timestamp_key, label_column]]

# --- Structural Profile ---
print(f"\n{'═' * 60}")
print(f"  STRUCTURAL PROFILE")
print(f"{'═' * 60}")
print(f"  Rows:              {pdf.shape[0]:,}")
print(f"  Columns:           {pdf.shape[1]}")
print(f"  Memory footprint:  {pdf.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
print(f"  Numeric features:  {len(feature_cols)}")
print(f"  Categorical cols:  {len(categorical_cols)}")
print(f"  Datetime cols:     {len(datetime_cols)}")
print(f"{'─' * 60}")

# Duplicates
n_exact_dupes = pdf.duplicated().sum()
n_entity_dupes = pdf.duplicated(subset=[entity_key]).sum() if entity_key in pdf.columns else 0
print(f"  Exact duplicates:  {n_exact_dupes} {'⚠️' if n_exact_dupes > 0 else '✅'}")
print(f"  Entity key dupes:  {n_entity_dupes} {'(multiple events per entity)' if n_entity_dupes > 0 else '✅ unique entities'}")

# Sample size adequacy (rule of thumb: >= 10 events per feature for stable estimates)
events_per_feature = pdf.shape[0] / max(len(feature_cols), 1)
print(f"  Events/feature:    {events_per_feature:.0f} {'✅ adequate' if events_per_feature >= 10 else '⚠️ low — risk of overfitting'}")
print(f"{'═' * 60}")

# Column-level profile table
profile_data = []
for col in pdf.columns:
    col_data = pdf[col]
    profile_data.append({
        "column": col,
        "dtype": str(col_data.dtype),
        "nulls": col_data.isnull().sum(),
        "null_%": round(col_data.isnull().mean() * 100, 1),
        "unique": col_data.nunique(),
        "cardinality_%": round(col_data.nunique() / len(col_data) * 100, 1),
    })
profile_df = pd.DataFrame(profile_data).sort_values("null_%", ascending=False)
display(spark.createDataFrame(profile_df))

# COMMAND ----------

# DBTITLE 1,Section 2
# MAGIC %md
# MAGIC ## 2. Missing Value Analysis
# MAGIC Beyond counting nulls — we analyze the **pattern** of missingness to determine if it's random (MCAR),
# MAGIC conditional (MAR), or systematic (MNAR). This directly informs imputation strategy.

# COMMAND ----------

# DBTITLE 1,Missing Value Analysis
# Missing value analysis
null_counts = pdf.isnull().sum()
null_pct = pdf.isnull().mean() * 100

if null_counts.sum() == 0:
    print("✅ No missing values detected in any column.")
    print("   Imputation: Not required.")
else:
    # Missingness summary
    missing_cols = null_counts[null_counts > 0].sort_values(ascending=False)
    print(f"⚠️  {len(missing_cols)} columns have missing values:\n")
    for col in missing_cols.index:
        pct = null_pct[col]
        strategy = "DROP column" if pct > 50 else "IMPUTE (median/mode)" if pct < 5 else "IMPUTE (model-based)"
        print(f"  {col:30s} {missing_cols[col]:5d} ({pct:5.1f}%) → {strategy}")
    
    # Missingness heatmap (sample for performance)
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Left: Missing value bar chart
    missing_cols.plot(kind="barh", ax=axes[0], color="coral")
    axes[0].set_title("Missing Values by Column")
    axes[0].set_xlabel("Count")
    
    # Right: Missingness pattern matrix (random sample of rows)
    sample_size = min(200, len(pdf))
    sns.heatmap(pdf[missing_cols.index].sample(sample_size).isnull().T, 
                cbar=False, cmap="YlOrRd", ax=axes[1])
    axes[1].set_title("Missingness Pattern (sampled rows)")
    axes[1].set_xlabel("Row index")
    plt.tight_layout()
    plt.show()
    
    # Little's MCAR test approximation (pairwise null correlation)
    if len(missing_cols) > 1:
        null_corr = pdf[missing_cols.index].isnull().corr()
        high_corr = (null_corr.abs() > 0.5).sum().sum() - len(missing_cols)  # exclude diagonal
        if high_corr > 0:
            print(f"\n  ⚠️  Correlated missingness detected → likely MAR (Missing At Random)")
            print(f"     Consider model-based imputation (IterativeImputer, MissForest)")
        else:
            print(f"\n  ✅ Missingness appears random (MCAR) → simple imputation is safe")

# COMMAND ----------

# DBTITLE 1,Section 3
# MAGIC %md
# MAGIC ## 3. Target Variable Analysis
# MAGIC Deep dive into the target: distribution shape, class balance, statistical properties,
# MAGIC and what this means for model selection and evaluation metrics.

# COMMAND ----------

# DBTITLE 1,Target Analysis
fig, axes = plt.subplots(1, 3, figsize=(16, 5))

if task_type == "classification":
    counts = pdf[label_column].value_counts().sort_index()
    _imb = detect_class_imbalance(pdf, label_column)
    is_imbalanced = bool(_imb["is_imbalanced"])
    ratio = float(_imb["ratio"])
    majority_class = counts.idxmax()
    minority_class = counts.idxmin()
    
    # Plot 1: Class distribution bar
    colors = ["#2196F3", "#FF5722"] if len(counts) == 2 else sns.color_palette("muted", len(counts))
    counts.plot(kind="bar", ax=axes[0], color=colors, edgecolor="black", linewidth=0.5)
    axes[0].set_title(f"Target Distribution")
    axes[0].set_ylabel("Count")
    axes[0].set_xlabel(label_column)
    for i, (idx, val) in enumerate(counts.items()):
        axes[0].text(i, val + len(pdf)*0.01, f"{val:,}\n({val/len(pdf)*100:.1f}%)", 
                     ha="center", fontsize=9)
    
    # Plot 2: Pie chart showing proportions
    axes[1].pie(counts.values, labels=[f"Class {c}" for c in counts.index], 
                colors=colors, autopct="%1.1f%%", startangle=90,
                explode=[0.05]*len(counts))
    axes[1].set_title("Class Proportions")
    
    # Plot 3: Imbalance context — where does this fall?
    imbalance_zones = [1, 2, 5, 10, 20, 50, 100]
    zone_labels = ["Balanced", "Mild", "Moderate", "Severe", "Extreme", "Critical"]
    zone_colors = ["#4CAF50", "#8BC34A", "#FFC107", "#FF9800", "#FF5722", "#B71C1C"]
    ax3 = axes[2]
    for i, (low, high) in enumerate(zip(imbalance_zones[:-1], imbalance_zones[1:])):
        ax3.barh(0, high-low, left=low, color=zone_colors[i], alpha=0.6, height=0.5)
        ax3.text((low+high)/2, 0.35, zone_labels[i], ha="center", fontsize=8)
    ax3.axvline(x=ratio, color="black", linewidth=2, linestyle="--")
    ax3.text(ratio, -0.35, f"YOUR DATA\n{ratio:.1f}:1", ha="center", fontweight="bold", fontsize=9)
    ax3.set_xlim(0.5, 50)
    ax3.set_xscale("log")
    ax3.set_title("Imbalance Severity Scale")
    ax3.set_xlabel("Majority:Minority Ratio")
    ax3.set_yticks([])
    
    plt.tight_layout()
    plt.show()
    
    # Diagnostic summary
    print(f"\n{'─' * 60}")
    print(f"  TARGET DIAGNOSTIC SUMMARY")
    print(f"{'─' * 60}")
    print(f"  Classes:         {len(counts)} (binary)" if len(counts) == 2 else f"  Classes: {len(counts)} (multiclass)")
    print(f"  Majority class:  {majority_class} ({counts[majority_class]:,} samples)")
    print(f"  Minority class:  {minority_class} ({counts[minority_class]:,} samples)")
    print(f"  Imbalance ratio: {ratio:.2f}:1")
    print(f"  Status:          {'\u26a0\ufe0f IMBALANCED' if is_imbalanced else '\u2705 BALANCED'}")
    if is_imbalanced:
        print(f"\n  Recommendations:")
        print(f"  • Use PR-AUC as primary tuning metric (not accuracy/F1)")
        print(f"  • Apply class_weight='balanced' or SMOTE")
        print(f"  • Stratify all train/val/test splits")
        print(f"  • Report precision-recall curve alongside ROC")
    else:
        print(f"\n  Recommendations:")
        print(f"  • F1-score is appropriate as primary metric")
        print(f"  • Standard stratified splits are sufficient")
    print(f"{'─' * 60}")

else:
    # Regression: distribution shape analysis
    target_data = pdf[label_column].dropna()
    skew = target_data.skew()
    kurt = target_data.kurtosis()
    
    axes[0].hist(target_data, bins=50, color="steelblue", edgecolor="black", linewidth=0.5)
    axes[0].axvline(target_data.mean(), color="red", linestyle="--", label=f"Mean: {target_data.mean():.2f}")
    axes[0].axvline(target_data.median(), color="orange", linestyle="--", label=f"Median: {target_data.median():.2f}")
    axes[0].legend()
    axes[0].set_title(f"Target Distribution: {label_column}")
    
    stats.probplot(target_data, plot=axes[1])
    axes[1].set_title("Q-Q Plot (normality check)")
    
    if skew > 1:
        axes[2].hist(np.log1p(target_data), bins=50, color="seagreen", edgecolor="black", linewidth=0.5)
        axes[2].set_title(f"Log-transformed (skew: {np.log1p(target_data).skew():.2f})")
    else:
        sns.boxplot(y=target_data, ax=axes[2])
        axes[2].set_title("Box Plot")
    
    plt.tight_layout()
    plt.show()
    print(f"  Skewness: {skew:.3f} {'(transform recommended)' if abs(skew) > 1 else '(acceptable)'}")
    print(f"  Kurtosis: {kurt:.3f}")

# COMMAND ----------

# DBTITLE 1,Section 4
# MAGIC %md
# MAGIC ## 4. Univariate Feature Distributions
# MAGIC Analyze each feature's shape: skewness, kurtosis, identify log-transform candidates,
# MAGIC and flag zero-variance features that add no predictive value.

# COMMAND ----------

# DBTITLE 1,Univariate Distributions
# Univariate distribution analysis for all numeric features
if len(feature_cols) > 0:
    # Compute distribution statistics
    dist_stats = []
    for col in feature_cols:
        col_data = pdf[col].dropna()
        if len(col_data) == 0:
            continue
        dist_stats.append({
            "feature": col,
            "mean": col_data.mean(),
            "std": col_data.std(),
            "skewness": col_data.skew(),
            "kurtosis": col_data.kurtosis(),
            "min": col_data.min(),
            "max": col_data.max(),
            "zeros_%": (col_data == 0).mean() * 100,
            "unique_values": col_data.nunique(),
        })
    
    dist_df = pd.DataFrame(dist_stats).sort_values("skewness", key=abs, ascending=False)
    
    # Flag problematic features
    high_skew = dist_df[dist_df["skewness"].abs() > 2]
    zero_var = dist_df[dist_df["std"] == 0]
    
    print(f"Feature Distribution Summary:")
    print(f"  Total numeric features: {len(feature_cols)}")
    print(f"  Highly skewed (|skew|>2): {len(high_skew)} → {'consider log/sqrt transform' if len(high_skew) > 0 else 'none'}")
    print(f"  Zero variance: {len(zero_var)} → {'DROP these features' if len(zero_var) > 0 else 'none'}")
    
    # Distribution grid plot
    n_features = min(len(feature_cols), 12)  # Cap at 12 for readability
    n_cols_plot = 4
    n_rows_plot = (n_features + n_cols_plot - 1) // n_cols_plot
    
    fig, axes = plt.subplots(n_rows_plot, n_cols_plot, figsize=(16, 3.5 * n_rows_plot))
    axes = axes.flatten() if n_features > 1 else [axes]
    
    for i, col in enumerate(feature_cols[:n_features]):
        ax = axes[i]
        col_data = pdf[col].dropna()
        
        # Histogram with KDE overlay
        ax.hist(col_data, bins=30, density=True, alpha=0.6, color="steelblue", edgecolor="black", linewidth=0.3)
        try:
            col_data.plot.kde(ax=ax, color="darkred", linewidth=1.5)
        except Exception:
            pass
        
        skew_val = col_data.skew()
        ax.set_title(f"{col}\nskew={skew_val:.2f}", fontsize=9)
        ax.tick_params(labelsize=8)
        
        # Color border by skewness severity
        if abs(skew_val) > 2:
            for spine in ax.spines.values():
                spine.set_edgecolor("red")
                spine.set_linewidth(2)
    
    # Hide unused axes
    for j in range(n_features, len(axes)):
        axes[j].set_visible(False)
    
    plt.suptitle("Feature Distributions (red border = high skewness, consider transform)", 
                 fontsize=12, fontweight="bold", y=1.01)
    plt.tight_layout()
    plt.show()
    
    # Display stats table
    print(f"\nDistribution Statistics:")
    display(spark.createDataFrame(dist_df.round(3)))
else:
    print("⚠️  No numeric feature columns found for distribution analysis.")

# COMMAND ----------

# DBTITLE 1,Section 5
# MAGIC %md
# MAGIC ## 5. Outlier Detection
# MAGIC Identifies extreme values using both IQR (robust) and z-score methods.
# MAGIC Outliers can destabilize tree-based models less, but heavily impact linear models and distance-based algorithms.

# COMMAND ----------

# DBTITLE 1,Outlier Detection
# Outlier detection using IQR method (robust to non-normal distributions)
if len(feature_cols) > 0:
    outlier_summary = []
    for col in feature_cols:
        col_data = pdf[col].dropna()
        Q1, Q3 = col_data.quantile(0.25), col_data.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        n_outliers = ((col_data < lower_bound) | (col_data > upper_bound)).sum()
        outlier_summary.append({
            "feature": col,
            "outliers": n_outliers,
            "outlier_%": round(n_outliers / len(col_data) * 100, 2),
            "lower_bound": round(lower_bound, 3),
            "upper_bound": round(upper_bound, 3),
        })
    
    outlier_df = pd.DataFrame(outlier_summary).sort_values("outlier_%", ascending=False)
    high_outlier_cols = outlier_df[outlier_df["outlier_%"] > 5]
    
    print(f"Outlier Summary (IQR method, 1.5x):")
    print(f"  Features with >5% outliers: {len(high_outlier_cols)}")
    
    # Box plot grid for top outlier features
    plot_features = outlier_df.head(8)["feature"].tolist()
    if len(plot_features) > 0:
        fig, axes = plt.subplots(2, 4, figsize=(16, 8))
        axes = axes.flatten()
        
        for i, col in enumerate(plot_features):
            if i >= 8:
                break
            sns.boxplot(y=pdf[col], ax=axes[i], color="steelblue", flierprops={"marker": "o", "markersize": 3})
            pct = outlier_df[outlier_df["feature"] == col]["outlier_%"].values[0]
            axes[i].set_title(f"{col}\n({pct:.1f}% outliers)", fontsize=9)
        
        for j in range(len(plot_features), 8):
            axes[j].set_visible(False)
        
        plt.suptitle("Box Plots — Features Ranked by Outlier %", fontweight="bold", y=1.01)
        plt.tight_layout()
        plt.show()
    
    # Recommendations
    if len(high_outlier_cols) > 0:
        print(f"\n  Recommendations for high-outlier features:")
        print(f"  • Tree-based models (LightGBM/XGBoost): generally robust, no action needed")
        print(f"  • Linear models: consider winsorization or robust scaling")
        print(f"  • Verify outliers are real (not data errors) before removing")
    
    display(spark.createDataFrame(outlier_df))

# COMMAND ----------

# DBTITLE 1,Section 6
# MAGIC %md
# MAGIC ## 6. Bivariate Analysis — Feature vs Target
# MAGIC How does each feature relate to the target? This identifies the most discriminative features
# MAGIC and reveals non-linear relationships that correlation alone would miss.

# COMMAND ----------

# DBTITLE 1,Bivariate Analysis
# Bivariate analysis: feature discrimination power against target
if task_type == "classification" and len(feature_cols) > 0:
    # For each feature: compare distributions across target classes
    n_plot = min(len(feature_cols), 8)
    fig, axes = plt.subplots(2, 4, figsize=(18, 10))
    axes = axes.flatten()
    
    # Rank features by discrimination power (AUC of univariate classifier)
    from sklearn.metrics import roc_auc_score
    discrimination = []
    for col in feature_cols:
        col_data = pdf[[col, label_column]].dropna()
        if col_data[col].std() == 0:
            discrimination.append((col, 0.5))
            continue
        try:
            auc = roc_auc_score(col_data[label_column], col_data[col])
            auc = max(auc, 1 - auc)  # Flip if negatively correlated
            discrimination.append((col, auc))
        except Exception:
            discrimination.append((col, 0.5))
    
    disc_df = pd.DataFrame(discrimination, columns=["feature", "univariate_auc"]).sort_values("univariate_auc", ascending=False)
    top_features = disc_df.head(n_plot)["feature"].tolist()
    
    for i, col in enumerate(top_features):
        ax = axes[i]
        for cls in sorted(pdf[label_column].unique()):
            subset = pdf[pdf[label_column] == cls][col].dropna()
            ax.hist(subset, bins=25, alpha=0.5, label=f"Class {cls}", density=True)
        ax.set_title(f"{col}\nAUC={disc_df[disc_df['feature']==col]['univariate_auc'].values[0]:.3f}", fontsize=9)
        ax.legend(fontsize=7)
        ax.tick_params(labelsize=8)
    
    for j in range(n_plot, 8):
        axes[j].set_visible(False)
    
    plt.suptitle("Feature Distributions by Target Class (ranked by discrimination power)",
                 fontweight="bold", y=1.01)
    plt.tight_layout()
    plt.show()
    
    # Feature ranking table
    print(f"\nFeature Discrimination Ranking (Univariate AUC):")
    print(f"{'─' * 45}")
    for _, row in disc_df.iterrows():
        bar = '█' * int(row['univariate_auc'] * 20) + '░' * (20 - int(row['univariate_auc'] * 20))
        quality = "STRONG" if row['univariate_auc'] > 0.7 else "MODERATE" if row['univariate_auc'] > 0.6 else "WEAK"
        print(f"  {row['feature']:25s} {bar} {row['univariate_auc']:.3f} ({quality})")
    print(f"{'─' * 45}")

elif task_type == "regression" and len(feature_cols) > 0:
    # Scatter plots for regression
    n_plot = min(len(feature_cols), 8)
    fig, axes = plt.subplots(2, 4, figsize=(18, 10))
    axes = axes.flatten()
    corrs = pdf[feature_cols + [label_column]].corr()[label_column].drop(label_column).abs().sort_values(ascending=False)
    for i, col in enumerate(corrs.head(n_plot).index):
        ax = axes[i]
        ax.scatter(pdf[col], pdf[label_column], alpha=0.3, s=10)
        ax.set_xlabel(col, fontsize=8)
        ax.set_ylabel(label_column, fontsize=8)
        ax.set_title(f"r={corrs[col]:.3f}", fontsize=9)
    for j in range(n_plot, 8):
        axes[j].set_visible(False)
    plt.suptitle("Feature vs Target (ranked by |correlation|)", fontweight="bold")
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# DBTITLE 1,Section 7
# MAGIC %md
# MAGIC ## 7. Multicollinearity & Correlation Analysis
# MAGIC Beyond simple correlation: VIF (Variance Inflation Factor) quantifies how much a feature
# MAGIC is linearly predicted by OTHER features. VIF > 10 means severe multicollinearity.

# COMMAND ----------

# DBTITLE 1,Multicollinearity Analysis
# Correlation matrix + VIF analysis
if len(feature_cols) > 1:
    corr_matrix = pdf[feature_cols].corr()
    
    # Correlation heatmap
    fig, axes = plt.subplots(1, 2, figsize=(18, 8))
    
    # Left: Full correlation heatmap
    mask = np.triu(np.ones_like(corr_matrix, dtype=bool))  # Upper triangle mask
    sns.heatmap(corr_matrix, mask=mask, annot=len(feature_cols) <= 10,
                cmap="RdBu_r", center=0, ax=axes[0], fmt=".2f",
                square=True, linewidths=0.5, vmin=-1, vmax=1)
    axes[0].set_title("Feature Correlation Matrix")
    
    # Right: Target correlation bar chart
    if label_column in pdf.select_dtypes(include=[np.number]).columns:
        target_corr = pdf[feature_cols + [label_column]].corr()[label_column].drop(label_column).sort_values()
        colors = ["#FF5722" if v < 0 else "#2196F3" for v in target_corr.values]
        target_corr.plot(kind="barh", ax=axes[1], color=colors)
        axes[1].axvline(x=0, color="black", linewidth=0.5)
        axes[1].set_title(f"Correlation with Target ({label_column})")
        axes[1].set_xlabel("Pearson r")
    
    plt.tight_layout()
    plt.show()
    
    # VIF Calculation
    from sklearn.preprocessing import StandardScaler
    from numpy.linalg import LinAlgError
    
    print(f"\nVariance Inflation Factor (VIF):")
    print(f"{'─' * 50}")
    print(f"  VIF > 5: moderate multicollinearity")
    print(f"  VIF > 10: severe → consider dropping or combining features")
    print(f"{'─' * 50}")
    
    try:
        X = pdf[feature_cols].dropna()
        X_scaled = StandardScaler().fit_transform(X)
        corr_inv = np.linalg.inv(np.corrcoef(X_scaled.T))
        vif_data = []
        for i, col in enumerate(feature_cols):
            vif_val = corr_inv[i, i]
            vif_data.append({"feature": col, "VIF": round(vif_val, 2)})
        
        vif_df = pd.DataFrame(vif_data).sort_values("VIF", ascending=False)
        for _, row in vif_df.iterrows():
            flag = "🛑 SEVERE" if row["VIF"] > 10 else "⚠️  MODERATE" if row["VIF"] > 5 else "✅"
            print(f"  {row['feature']:25s} VIF = {row['VIF']:8.2f}  {flag}")
        
        severe_vif = vif_df[vif_df["VIF"] > 10]
        if len(severe_vif) > 0:
            print(f"\n  ⚠️  {len(severe_vif)} features have severe multicollinearity.")
            print(f"  Consider: PCA, dropping redundant features, or regularization (L1/L2).")
    except (LinAlgError, np.linalg.LinAlgError):
        print("  ⚠️  Correlation matrix is singular — perfectly correlated features exist.")
    
    # Highly correlated feature pairs
    print(f"\nHighly Correlated Feature Pairs (|r| > 0.8):")
    high_corr_pairs = []
    for i in range(len(feature_cols)):
        for j in range(i+1, len(feature_cols)):
            r = corr_matrix.iloc[i, j]
            if abs(r) > 0.8:
                high_corr_pairs.append((feature_cols[i], feature_cols[j], round(r, 3)))
    
    if high_corr_pairs:
        for f1, f2, r in sorted(high_corr_pairs, key=lambda x: abs(x[2]), reverse=True):
            print(f"  {f1} ↔ {f2}: r = {r}")
    else:
        print("  ✅ No highly correlated pairs found.")

# COMMAND ----------

# DBTITLE 1,Section 8
# MAGIC %md
# MAGIC ## 8. Target Leakage Screen
# MAGIC Critical safety check: features with near-perfect correlation to target are almost always
# MAGIC leaked (computed FROM the target). These inflate training metrics but produce garbage in production.

# COMMAND ----------

# DBTITLE 1,Target Leakage Screen
# Target leakage detection: correlation + temporal checks
LEAKAGE_THRESHOLD = 0.95
SUSPICIOUS_THRESHOLD = 0.85

if label_column in pdf.select_dtypes(include=[np.number]).columns and len(feature_cols) > 0:
    target_corr_abs = pdf[feature_cols + [label_column]].corr()[label_column].drop(label_column).abs().sort_values(ascending=False)
    
    leaky = target_corr_abs[target_corr_abs > LEAKAGE_THRESHOLD]
    suspicious = target_corr_abs[(target_corr_abs > SUSPICIOUS_THRESHOLD) & (target_corr_abs <= LEAKAGE_THRESHOLD)]
    
    if len(leaky) > 0:
        print(f"{'═' * 60}")
        print(f"  🛑 CRITICAL: TARGET LEAKAGE DETECTED")
        print(f"{'═' * 60}")
        for feat, corr_val in leaky.items():
            print(f"  • {feat}: |correlation| = {corr_val:.4f}")
        print(f"\n  ACTION REQUIRED:")
        print(f"  These features are almost certainly computed FROM the target.")
        print(f"  Remove them before training or your model will fail in production.")
        print(f"{'═' * 60}")
    elif len(suspicious) > 0:
        print(f"⚠️  Suspicious features (|corr| > {SUSPICIOUS_THRESHOLD}):")
        for feat, corr_val in suspicious.items():
            print(f"  • {feat}: |correlation| = {corr_val:.4f}")
        print(f"\n  These may be legitimate strong predictors OR subtle leakage.")
        print(f"  Verify: are these features available BEFORE the target is known?")
    else:
        print(f"✅ No target leakage detected (all |corr| < {SUSPICIOUS_THRESHOLD})")
        print(f"   Highest correlation: {target_corr_abs.index[0]} = {target_corr_abs.iloc[0]:.4f}")
else:
    print("⚠️  Cannot check leakage: label column is not numeric or no features found.")

# COMMAND ----------

# DBTITLE 1,Section 9
# MAGIC %md
# MAGIC ## 9. Temporal Patterns & Data Freshness
# MAGIC If your data has timestamps, this reveals time-based patterns: seasonality, trends, and
# MAGIC whether your training data is representative of current conditions.

# COMMAND ----------

# DBTITLE 1,Temporal Analysis
# Temporal analysis (if timestamp column exists)
if timestamp_key in pdf.columns:
    ts_col = pd.to_datetime(pdf[timestamp_key], errors="coerce")
    valid_ts = ts_col.dropna()
    
    if len(valid_ts) > 0:
        print(f"Temporal Coverage:")
        print(f"  Earliest:  {valid_ts.min()}")
        print(f"  Latest:    {valid_ts.max()}")
        print(f"  Span:      {(valid_ts.max() - valid_ts.min()).days} days")
        print(f"  Freshness: data is {(pd.Timestamp.now() - valid_ts.max()).days} days old")
        
        fig, axes = plt.subplots(1, 3, figsize=(18, 5))
        
        # Plot 1: Events over time
        events_by_date = ts_col.dt.date.value_counts().sort_index()
        axes[0].plot(events_by_date.index, events_by_date.values, color="steelblue", linewidth=1)
        axes[0].fill_between(events_by_date.index, events_by_date.values, alpha=0.2)
        axes[0].set_title("Event Volume Over Time")
        axes[0].set_xlabel("Date")
        axes[0].set_ylabel("Events")
        axes[0].tick_params(axis="x", rotation=45)
        
        # Plot 2: Target rate over time (classification)
        if task_type == "classification":
            temp_df = pd.DataFrame({"date": ts_col.dt.to_period("W").dt.start_time, "target": pdf[label_column]})
            weekly_rate = temp_df.groupby("date")["target"].mean()
            axes[1].plot(weekly_rate.index, weekly_rate.values, color="coral", linewidth=1.5, marker="o", markersize=3)
            axes[1].axhline(y=pdf[label_column].mean(), color="gray", linestyle="--", label="Overall mean")
            axes[1].set_title(f"Target Rate Over Time (weekly)")
            axes[1].set_ylabel(f"P({label_column}=1)")
            axes[1].legend()
            axes[1].tick_params(axis="x", rotation=45)
            
            # Check for temporal drift in target rate
            first_half_rate = pdf[label_column][ts_col <= ts_col.median()].mean()
            second_half_rate = pdf[label_column][ts_col > ts_col.median()].mean()
            drift_pct = abs(second_half_rate - first_half_rate) / max(first_half_rate, 0.001) * 100
        else:
            temp_df = pd.DataFrame({"date": ts_col.dt.to_period("W").dt.start_time, "target": pdf[label_column]})
            weekly_mean = temp_df.groupby("date")["target"].mean()
            axes[1].plot(weekly_mean.index, weekly_mean.values, color="coral", linewidth=1.5)
            axes[1].set_title(f"Target Mean Over Time (weekly)")
            axes[1].tick_params(axis="x", rotation=45)
            drift_pct = 0
        
        # Plot 3: Day-of-week / monthly seasonality
        dow_counts = ts_col.dt.day_name().value_counts()
        dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        dow_counts = dow_counts.reindex([d for d in dow_order if d in dow_counts.index])
        axes[2].bar(range(len(dow_counts)), dow_counts.values, color="seagreen", alpha=0.7)
        axes[2].set_xticks(range(len(dow_counts)))
        axes[2].set_xticklabels([d[:3] for d in dow_counts.index], fontsize=9)
        axes[2].set_title("Events by Day of Week")
        axes[2].set_ylabel("Count")
        
        plt.tight_layout()
        plt.show()
        
        # Temporal drift warning
        if drift_pct > 20:
            print(f"\n  ⚠️  Target rate drifted {drift_pct:.0f}% between first and second half of data.")
            print(f"     Consider: time-based splitting instead of random split.")
        else:
            print(f"\n  ✅ Target rate is stable over time (drift: {drift_pct:.0f}%).")
    else:
        print("⚠️  Timestamp column exists but contains no valid dates.")
else:
    print(f"ℹ️  No timestamp column '{timestamp_key}' found. Temporal analysis skipped.")

# COMMAND ----------

# DBTITLE 1,Section 10
# MAGIC %md
# MAGIC ## 10. Data Quality Scorecard
# MAGIC Final summary: a pass/fail scorecard that tells you whether this data is ready for modeling,
# MAGIC and specific actions to take before proceeding to feature engineering.

# COMMAND ----------

# DBTITLE 1,Data Quality Scorecard
# Comprehensive Data Quality Scorecard
print(f"{'═' * 70}")
print(f"  DATA QUALITY SCORECARD")
print(f"{'═' * 70}")

checks = []

# Check 1: Sample size
min_samples = max(len(feature_cols) * 20, 500)  # At least 20x features or 500
check_pass = len(pdf) >= min_samples
checks.append(("Sample size", check_pass, f"{len(pdf):,} rows (need >={min_samples:,})"))

# Check 2: Missing values
max_null_pct = pdf.isnull().mean().max() * 100
check_pass = max_null_pct < 50
checks.append(("Missing values", check_pass, f"Max {max_null_pct:.1f}% (threshold: <50%)"))

# Check 3: Duplicates
check_pass = n_exact_dupes == 0
checks.append(("No exact duplicates", check_pass, f"{n_exact_dupes} duplicates found"))

# Check 4: Target leakage
if label_column in pdf.select_dtypes(include=[np.number]).columns and len(feature_cols) > 0:
    max_corr = pdf[feature_cols + [label_column]].corr()[label_column].drop(label_column).abs().max()
    check_pass = max_corr < LEAKAGE_THRESHOLD
    checks.append(("No target leakage", check_pass, f"Max |corr| = {max_corr:.4f} (threshold: <{LEAKAGE_THRESHOLD})"))

# Check 5: Zero-variance features
n_zero_var = sum(1 for col in feature_cols if pdf[col].std() == 0)
check_pass = n_zero_var == 0
checks.append(("No zero-variance features", check_pass, f"{n_zero_var} constant features"))

# Check 6: Class balance (classification only)
if task_type == "classification":
    check_pass = not is_imbalanced
    checks.append(("Class balance", check_pass, f"Ratio: {ratio:.1f}:1 (warn if >5:1)"))

# Check 7: Data freshness (if timestamp available)
if timestamp_key in pdf.columns:
    ts_max = pd.to_datetime(pdf[timestamp_key], errors="coerce").max()
    if pd.notna(ts_max):
        days_old = (pd.Timestamp.now() - ts_max).days
        check_pass = days_old < 90
        checks.append(("Data freshness", check_pass, f"{days_old} days old (warn if >90)"))

# Check 8: Feature diversity
if len(feature_cols) > 0:
    check_pass = len(feature_cols) >= 3
    checks.append(("Feature diversity", check_pass, f"{len(feature_cols)} features (need >=3)"))

# Print scorecard
passed = sum(1 for _, p, _ in checks if p)
total = len(checks)

for name, passed_check, detail in checks:
    status = "✅ PASS" if passed_check else "❌ FAIL"
    print(f"  {status}  {name:30s} {detail}")

print(f"\n{'─' * 70}")
print(f"  Score: {passed}/{total} checks passed")

if passed == total:
    print(f"  \n  🎉 DATA IS READY FOR MODELING")
    print(f"  Proceed to → 02_feature_engineering.py")
elif passed >= total - 2:
    print(f"  \n  ⚠️  DATA NEEDS MINOR ATTENTION")
    print(f"  Address the failed checks above, then proceed to feature engineering.")
else:
    print(f"  \n  🛑 DATA NEEDS SIGNIFICANT WORK")
    print(f"  Fix the critical issues above before building models.")

print(f"{'═' * 70}")

# Available tables for feature engineering reference
print(f"\n\nAvailable tables in {config['catalog']}.{config['schema']}:")
tables_df = spark.sql(f"SHOW TABLES IN {config['catalog']}.{config['schema']}")
display(tables_df)
