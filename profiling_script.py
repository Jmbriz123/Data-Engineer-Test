"""
Springer Capital — Data Profiling Script
=========================================
Profiles all source CSV tables and exports results to
profiling/data_profiling_report.csv  and  profiling/data_profiling_report.xlsx

Metrics per column:
  - data_type
  - null_count / null_pct
  - distinct_count
  - min_value / max_value
  - max_actual_length
"""

import os
import pandas as pd
import numpy as np

DATA_DIR     = os.getenv("DATA_DIR",     "data")
PROFILING_DIR = os.getenv("PROFILING_DIR", "profiling")
os.makedirs(PROFILING_DIR, exist_ok=True)

FILES = {
    "lead_logs":              "lead_log.csv",
    "paid_transactions":      "paid_transactions.csv",
    "referral_rewards":       "referral_rewards.csv",
    "user_logs":              "user_logs.csv",
    "user_referral_logs":     "user_referral_logs.csv",
    "user_referral_statuses": "user_referral_statuses.csv",
    "user_referrals":         "user_referrals.csv",
}


def profile_dataframe(table_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Return a profiling summary DataFrame for a single table."""
    total_rows = len(df)
    rows = []

    for col in df.columns:
        series = df[col]

        # null counting (treat literal 'null' strings as nulls)
        null_mask  = series.isna() | series.astype(str).str.lower().eq("null")
        null_count = int(null_mask.sum())
        null_pct   = round(null_count / total_rows * 100, 2) if total_rows else 0

        non_null   = series[~null_mask]
        distinct   = int(non_null.nunique())

        # min / max – attempt numeric cast, else lexicographic
        try:
            numeric = pd.to_numeric(non_null, errors="raise")
            min_val = numeric.min() if len(numeric) else None
            max_val = numeric.max() if len(numeric) else None
        except (ValueError, TypeError):
            min_val = non_null.min() if len(non_null) else None
            max_val = non_null.max() if len(non_null) else None

        max_len = int(non_null.astype(str).str.len().max()) if len(non_null) else 0

        rows.append({
            "table_name":       table_name,
            "column_name":      col,
            "data_type":        str(series.dtype),
            "total_rows":       total_rows,
            "null_count":       null_count,
            "null_pct":         null_pct,
            "populated_pct":    round(100 - null_pct, 2),
            "distinct_count":   distinct,
            "min_value":        min_val,
            "max_value":        max_val,
            "max_actual_length": max_len,
        })

    return pd.DataFrame(rows)


def main():
    print("=" * 60)
    print("  Springer Capital — Data Profiling")
    print("=" * 60)

    all_profiles = []

    for table_name, filename in FILES.items():
        path = os.path.join(DATA_DIR, filename)
        df   = pd.read_csv(path, dtype=str)
        print(f"\n  Profiling {table_name} ({len(df)} rows × {len(df.columns)} cols) …")
        profile = profile_dataframe(table_name, df)
        all_profiles.append(profile)

        # Console summary
        print(profile[["column_name", "null_count", "distinct_count", "max_actual_length"]].to_string(index=False))

    combined = pd.concat(all_profiles, ignore_index=True)

    # Save CSV
    csv_path = os.path.join(PROFILING_DIR, "data_profiling_report.csv")
    combined.to_csv(csv_path, index=False)
    print(f"\n✓ Profiling CSV saved → {csv_path}")

    # Save Excel (one sheet per table)
    xlsx_path = os.path.join(PROFILING_DIR, "data_profiling_report.xlsx")
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        # Summary sheet
        combined.to_excel(writer, sheet_name="All Tables", index=False)
        # Individual sheets
        for table_name in FILES:
            subset = combined[combined["table_name"] == table_name]
            sheet  = table_name[:31]  # Excel sheet name limit
            subset.to_excel(writer, sheet_name=sheet, index=False)

    print(f"✓ Profiling Excel saved → {xlsx_path}")
    print("\nDone.")


if __name__ == "__main__":
    main()
