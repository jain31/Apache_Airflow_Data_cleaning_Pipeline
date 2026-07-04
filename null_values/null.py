"""
null.py - null_values/null.py
Null-value imputation and validation for the data-cleaning pipeline.
All activity logs to logs/null_log.log (+Airflow StreamHandler).

PUBLIC API
_____
    handle_nulls(df, original_filename, date_cols, continuous_cols, categorical_cols)
       -> (clean_df, quarantine_df, report_dict)
"""

import os
import uuid
from datetime import datetime
from pathlib import Path
import numpy as np
import pandas as pd
from scipy import stats

# Integrate with your centralized Airflow-ready logger setup
from logger_file.logger import get_logger

log = get_logger("null")

# _______Paths_________
_ROOT = Path(__file__).resolve().parent.parent
QUARANTINE_DIR = _ROOT / "quarantine" / "null_quarantine"
QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

# _____Thresholds_____________
NULL_QUARANTINE_THRESHOLD = 0.25    # NULL % > 25% rows go to quarantine
SKEW_THRESHOLD = 0.5                # |SKEW| > 0.5 -> MEDIAN ELSE MEAN
P_VALUE_THRESHOLD = 0.05            # VALIDATION: P < 0.05 raises a warning

# PRIVATE HELPERS

def _null_pct(series: pd.Series) -> float:
    return float(series.isna().sum() / len(series)) if len(series) > 0 else 0.0

def _quarantine_rows(df: pd.DataFrame, mask: pd.Series, col: str, original_filename: str) -> pd.DataFrame:
    quarantine_df = df[mask].copy()
    if quarantine_df.empty:
        return quarantine_df
        
    date_str = datetime.now().strftime("%Y-%m-%d")
    uid = uuid.uuid4().hex[:8]
    stem = Path(original_filename).stem
    filename = f"{uid}_null_{date_str}_{stem}.csv"
    dest = QUARANTINE_DIR / filename

    quarantine_df.to_csv(dest, index=False)
    log.warning(
        f"action=quarantine_rows | col={col} "
        f"| rows_quarantined={len(quarantine_df)} "
        f"| null_pct={mask.mean():.1%} > {NULL_QUARANTINE_THRESHOLD:.0%} threshold "
        f"| dest={dest}"
    )
    return quarantine_df

# PUBLIC CORE API ENTRY POINT FOR AIRFLOW PIPELINE

def handle_nulls(df: pd.DataFrame, original_filename: str, date_cols: list, continuous_cols: list, categorical_cols: list):
    """
    Orchestrates the isolation of heavy-null rows followed by targeted imputation
    and analytical post-validation checks.
    """
    log.info(f"Starting null handling workflow for file: {original_filename}")
    
    work_df = df.copy()
    all_quarantined = []
    report_dict = {"imputations": {}, "validation_warnings": []}

    # Step 1: Null Threshold Isolation & Quarantine Inspection Loop
    all_cols = list(date_cols) + list(continuous_cols) + list(categorical_cols)
    
    for col in all_cols:
        if col not in work_df.columns:
            continue
            
        # Calculate percentage of missing values per individual column row mapping
        null_mask = work_df[col].isna()
        null_ratio = _null_pct(work_df[col])
        
        if null_ratio > NULL_QUARANTINE_THRESHOLD:
            q_df = _quarantine_rows(work_df, null_mask, col, original_filename)
            if not q_df.empty:
                all_quarantined.append(q_df)
                # Eliminate quarantined records from downstream imputation processing
                work_df = work_df[~null_mask].copy()

    # Consolidate separated quarantine slices
    quarantine_df = pd.concat(all_quarantined, ignore_index=False) if all_quarantined else pd.DataFrame(columns=df.columns)

    if work_df.empty:
        log.warning("All records dropped into quarantine due to high null density.")
        return work_df, quarantine_df, report_dict

    # Step 2: Date Columns Imputation (Forward-fill -> Backward-fill)
    for col in date_cols:
        if col in work_df.columns and work_df[col].isna().sum() > 0:
            pre_nulls = work_df[col].isna().sum()
            work_df[col] = work_df[col].ffill().bfill()
            report_dict["imputations"][col] = {"strategy": "ffill_bfill", "count": int(pre_nulls)}
            log.info(f"col={col} | strategy=ffill_bfill | imputed={pre_nulls}")

    # Step 3: Continuous Columns Imputation with Skewness Checking
    for col in continuous_cols:
        if col in work_df.columns and work_df[col].isna().sum() > 0:
            pre_nulls = work_df[col].isna().sum()
            original_non_null = work_df[col].dropna().copy()
            
            if original_non_null.empty:
                continue

            # Calculate mathematical data skewness
            skew_val = float(stats.skew(original_non_null))
            strategy = "mean" if abs(skew_val) <= SKEW_THRESHOLD else "median"
            impute_value = float(original_non_null.mean() if strategy == "mean" else original_non_null.median())
            
            # Perform Imputation
            work_df[col] = work_df[col].fillna(impute_value)
            report_dict["imputations"][col] = {"strategy": strategy, "value": impute_value, "count": int(pre_nulls)}
            log.info(f"col={col} | strategy={strategy} | skew={skew_val:.3f} | imputed={pre_nulls}")

            # Statistical Post-Imputation Validation: Welch's Two-Sample T-Test
            post_values = work_df[col].values
            try:
                t_stat, p_val = stats.ttest_ind(original_non_null.values, post_values, equal_var=False)
                if p_val < P_VALUE_THRESHOLD:
                    warn_msg = f"Continuous distribution shift detected in col={col} (Welch p-value={p_val:.4f} < {P_VALUE_THRESHOLD})"
                    report_dict["validation_warnings"].append(warn_msg)
                    log.warning(warn_msg)
            except Exception as e:
                log.error(f"Failed to execute Welch t-test validation on col={col}: {str(e)}")

    # Step 4: Categorical Columns Imputation (Mode Imputation)
    for col in categorical_cols:
        if col in work_df.columns and work_df[col].isna().sum() > 0:
            pre_nulls = work_df[col].isna().sum()
            original_non_null = work_df[col].dropna().copy()
            
            if original_non_null.empty:
                continue

            mode_series = original_non_null.mode()
            mode_value = mode_series.iloc[0] if not mode_series.empty else "Unknown"
            
            # Pre-imputation distribution tracking for validation
            pre_dist = original_non_null.value_counts(normalize=True)

            # Perform Imputation
            work_df[col] = work_df[col].fillna(mode_value)
            report_dict["imputations"][col] = {"strategy": "mode", "value": str(mode_value), "count": int(pre_nulls)}
            log.info(f"col={col} | strategy=mode | value={mode_value} | imputed={pre_nulls}")

            # Statistical Post-Imputation Validation: Chi-Square Goodness-of-Fit Test
            try:
                post_counts = work_df[col].value_counts()
                # Reindex to ensure category align perfectly
                expected_counts = pre_dist.reindex(post_counts.index, fill_value=0.0) * len(work_df)
                
                # Add tiny variance epsilon factor to safeguard against zero frequency divisions
                f_obs = post_counts.values
                f_exp = expected_counts.values + 1e-5
                
                # Normalize frequencies to match exact shapes
                f_exp = f_exp * (f_obs.sum() / f_exp.sum())

                chi2_stat, p_val = stats.chisquare(f_obs=f_obs, f_exp=f_exp)
                if p_val < P_VALUE_THRESHOLD:
                    warn_msg = f"Categorical distribution shift detected in col={col} (Chi2 p-value={p_val:.4f} < {P_VALUE_THRESHOLD})"
                    report_dict["validation_warnings"].append(warn_msg)
                    log.warning(warn_msg)
            except Exception as e:
                log.error(f"Failed to execute Chi-Square validation on col={col}: {str(e)}")

    log.info(f"Null handling workflow finished successfully. Cleaned records remaining: {len(work_df)}")
    return work_df, quarantine_df, report_dict