"""
Springer Capital — Referral Program Data Pipeline
===================================================
Loads, cleans, processes, and validates referral data to produce a
fraud-detection report with business-logic validity flags.

Author  : Data Engineering
Version : 1.0.0
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# 0. Configuration
# ---------------------------------------------------------------------------
DATA_DIR   = os.getenv("DATA_DIR",   "data")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# 1. Data Loading
# ---------------------------------------------------------------------------
def load_data(data_dir: str) -> dict[str, pd.DataFrame]:
    """Load all CSV source files into a dictionary of DataFrames."""
    files = {
        "lead_logs":              "lead_log.csv",
        "paid_transactions":      "paid_transactions.csv",
        "referral_rewards":       "referral_rewards.csv",
        "user_logs":              "user_logs.csv",
        "user_referral_logs":     "user_referral_logs.csv",
        "user_referral_statuses": "user_referral_statuses.csv",
        "user_referrals":         "user_referrals.csv",
    }
    dfs = {}
    for key, filename in files.items():
        path = os.path.join(data_dir, filename)
        dfs[key] = pd.read_csv(path, dtype=str)  # load everything as str first
        print(f"  Loaded {key}: {len(dfs[key])} rows")
    return dfs


# ---------------------------------------------------------------------------
# 2. Data Cleaning helpers
# ---------------------------------------------------------------------------
def replace_null_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Replace literal 'null' strings with proper NaN."""
    return df.replace({"null": np.nan, "NULL": np.nan, "": np.nan})


def parse_timestamps(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Parse ISO-8601 timestamp columns to UTC-aware datetime."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    return df


def to_local(ts: pd.Timestamp, tz_str: str) -> pd.Timestamp | None:
    """Convert a UTC-aware Timestamp to the given local timezone."""
    if pd.isna(ts) or pd.isna(tz_str):
        return pd.NaT
    try:
        return ts.astimezone(ZoneInfo(tz_str)).replace(tzinfo=None)
    except Exception:
        return pd.NaT


def initcap(series: pd.Series) -> pd.Series:
    """Apply title-case to a string series, leaving NaN intact."""
    return series.where(series.isna(), series.str.title())


def clean_reward_value(series: pd.Series) -> pd.Series:
    """Extract numeric day count from strings like '10 days'."""
    return (
        series.str.extract(r"(\d+)")[0]
              .astype(float)
              .astype("Int64")   # nullable integer
    )


# ---------------------------------------------------------------------------
# 3. Per-table cleaning
# ---------------------------------------------------------------------------
def clean_lead_logs(df: pd.DataFrame) -> pd.DataFrame:
    df = replace_null_strings(df)
    df = parse_timestamps(df, ["created_at"])
    df["id"] = df["id"].astype("Int64")
    df["source_category"] = initcap(df["source_category"])
    df["current_status"]  = initcap(df["current_status"])
    return df.drop_duplicates()


def clean_paid_transactions(df: pd.DataFrame) -> pd.DataFrame:
    df = replace_null_strings(df)
    df = parse_timestamps(df, ["transaction_at"])
    df["transaction_status"] = initcap(df["transaction_status"])
    df["transaction_type"]   = initcap(df["transaction_type"])
    # Convert transaction_at to local time using timezone_transaction
    df["transaction_at_local"] = df.apply(
        lambda r: to_local(r["transaction_at"], r["timezone_transaction"]), axis=1
    )
    return df.drop_duplicates(subset=["transaction_id"])


def clean_referral_rewards(df: pd.DataFrame) -> pd.DataFrame:
    df = replace_null_strings(df)
    df = parse_timestamps(df, ["created_at"])
    df["id"]           = df["id"].astype("Int64")
    df["reward_value"] = clean_reward_value(df["reward_value"])
    df["reward_type"]  = df["reward_type"].astype("Int64")
    return df.drop_duplicates(subset=["id"])


def clean_user_logs(df: pd.DataFrame) -> pd.DataFrame:
    df = replace_null_strings(df)
    df["id"]                     = df["id"].astype("Int64")
    df["membership_expired_date"] = pd.to_datetime(
        df["membership_expired_date"], errors="coerce"
    )
    df["is_deleted"] = df["is_deleted"].str.lower().map({"true": True, "false": False})
    # homeclub keeps its original casing per spec
    return df.drop_duplicates(subset=["user_id"])


def clean_user_referral_logs(df: pd.DataFrame) -> pd.DataFrame:
    df = replace_null_strings(df)
    df = parse_timestamps(df, ["created_at"])
    df["id"]              = df["id"].astype("Int64")
    df["is_reward_granted"] = (
        df["is_reward_granted"].str.upper().map({"TRUE": True, "FALSE": False})
    )
    return df.drop_duplicates()


def clean_user_referral_statuses(df: pd.DataFrame) -> pd.DataFrame:
    df = replace_null_strings(df)
    df = parse_timestamps(df, ["created_at"])
    df["id"] = df["id"].astype("Int64")
    return df.drop_duplicates(subset=["id"])


def clean_user_referrals(df: pd.DataFrame) -> pd.DataFrame:
    df = replace_null_strings(df)
    df = parse_timestamps(df, ["referral_at", "updated_at"])
    df["user_referral_status_id"] = df["user_referral_status_id"].astype("Int64")
    df["referral_reward_id"]      = pd.to_numeric(
        df["referral_reward_id"], errors="coerce"
    ).astype("Int64")
    df["referee_name"] = initcap(df["referee_name"])
    return df.drop_duplicates(subset=["referral_id"])


# ---------------------------------------------------------------------------
# 4. Time adjustment: convert referral_at to local time
#    Timezone must be sourced from referrer's homeclub timezone
# ---------------------------------------------------------------------------
def adjust_referral_times(
    user_referrals: pd.DataFrame,
    user_logs: pd.DataFrame,
) -> pd.DataFrame:
    """
    user_referrals has no timezone column directly.
    Join with user_logs on referrer_id → user_id to get timezone_homeclub,
    then convert referral_at and updated_at.
    """
    tz_map = (
        user_logs[["user_id", "timezone_homeclub"]]
        .drop_duplicates(subset=["user_id"])
    )
    df = user_referrals.merge(
        tz_map, left_on="referrer_id", right_on="user_id", how="left"
    ).drop(columns=["user_id"])

    # Fallback: if referrer timezone is unknown, use Asia/Jakarta (platform default)
    df["tz_resolved"] = df["timezone_homeclub"].fillna("Asia/Jakarta")
    df["referral_at_local"] = df.apply(
        lambda r: to_local(r["referral_at"], r["tz_resolved"]), axis=1
    )
    df["updated_at_local"] = df.apply(
        lambda r: to_local(r["updated_at"], r["tz_resolved"]), axis=1
    )
    return df


# ---------------------------------------------------------------------------
# 5. Source category logic
# ---------------------------------------------------------------------------
def assign_source_category(
    user_referrals: pd.DataFrame,
    lead_logs: pd.DataFrame,
) -> pd.DataFrame:
    """
    CASE
      WHEN referral_source = 'User Sign Up'       THEN 'Online'
      WHEN referral_source = 'Draft Transaction'  THEN 'Offline'
      WHEN referral_source = 'Lead'               THEN leads.source_category
    END
    """
    lead_cat = lead_logs[["lead_id", "source_category"]].drop_duplicates("lead_id")

    df = user_referrals.copy()

    # For Lead referrals, referee_id is the lead_id
    df = df.merge(
        lead_cat.rename(columns={"lead_id": "referee_id", "source_category": "lead_source_cat"}),
        on="referee_id",
        how="left",
    )

    conditions = [
        df["referral_source"] == "User Sign Up",
        df["referral_source"] == "Draft Transaction",
        df["referral_source"] == "Lead",
    ]
    choices = ["Online", "Offline", df["lead_source_cat"]]
    df["referral_source_category"] = np.select(conditions, choices, default=np.nan)

    return df.drop(columns=["lead_source_cat"])


# ---------------------------------------------------------------------------
# 6. Build master report table
# ---------------------------------------------------------------------------
def build_report(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Merge all cleaned tables into a single denormalised report DataFrame.
    """
    ur   = dfs["user_referrals"]
    url  = dfs["user_referral_logs"]
    urs  = dfs["user_referral_statuses"]
    rr   = dfs["referral_rewards"]
    pt   = dfs["paid_transactions"]
    ul   = dfs["user_logs"]

    # --- user_referral_logs: keep latest log per referral ---
    url_latest = (
        url.sort_values("created_at")
           .drop_duplicates(subset=["user_referral_id"], keep="last")
    )

    # --- join referrals → latest log ---
    df = ur.merge(
        url_latest[["user_referral_id", "source_transaction_id",
                    "created_at", "is_reward_granted"]].rename(
            columns={
                "user_referral_id": "referral_id",
                "created_at": "reward_granted_at",
            }
        ),
        on="referral_id",
        how="left",
    )

    # --- join referral statuses ---
    df = df.merge(
        urs[["id", "description"]].rename(
            columns={"id": "user_referral_status_id", "description": "referral_status"}
        ),
        on="user_referral_status_id",
        how="left",
    )

    # --- join referral rewards ---
    df = df.merge(
        rr[["id", "reward_value"]].rename(
            columns={"id": "referral_reward_id", "reward_value": "num_reward_days"}
        ),
        on="referral_reward_id",
        how="left",
    )

    # --- resolve transaction_id: prefer source_transaction_id from logs ---
    df["effective_transaction_id"] = df["source_transaction_id"].combine_first(
        df["transaction_id"]
    )

    # --- join paid transactions ---
    df = df.merge(
        pt[["transaction_id", "transaction_status", "transaction_at_local",
            "transaction_location", "transaction_type"]].rename(
            columns={
                "transaction_id":     "effective_transaction_id",
                "transaction_at_local": "transaction_at",
            }
        ),
        on="effective_transaction_id",
        how="left",
    )

    # --- join referrer info from user_logs ---
    referrer_info = (
        ul[["user_id", "name", "phone_number", "homeclub",
            "membership_expired_date", "is_deleted"]]
        .rename(columns={
            "user_id":      "referrer_id",
            "name":         "referrer_name",
            "phone_number": "referrer_phone_number",
            "homeclub":     "referrer_homeclub",
        })
        .drop_duplicates(subset=["referrer_id"])
    )
    df = df.merge(referrer_info, on="referrer_id", how="left")

    return df


# ---------------------------------------------------------------------------
# 7. Business Logic Validation (fraud detection)
# ---------------------------------------------------------------------------
def apply_business_logic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add is_business_logic_valid column based on defined valid/invalid conditions.
    True  → referral passes all business rules (not fraud)
    False → referral violates at least one business rule (potential fraud)
    """
    today = pd.Timestamp(datetime.now(timezone.utc).date())

    has_reward       = df["num_reward_days"].notna() & (df["num_reward_days"] > 0)
    is_berhasil      = df["referral_status"] == "Berhasil"
    is_pending_fail  = df["referral_status"].isin(["Menunggu", "Tidak Berhasil"])
    has_txn          = df["effective_transaction_id"].notna()
    txn_paid         = df["transaction_status"].str.upper().eq("Paid") if "transaction_status" in df.columns else pd.Series(False, index=df.index)
    txn_paid         = df["transaction_status"].fillna("").str.lower() == "paid"
    txn_new          = df["transaction_type"].fillna("").str.lower() == "new"
    txn_after_ref    = df["transaction_at"] > df["referral_at_local"]
    txn_same_month   = (
        df["transaction_at"].dt.to_period("M") ==
        df["referral_at_local"].dt.to_period("M")
    )
    membership_valid = df["membership_expired_date"] >= today
    not_deleted      = df["is_deleted"].fillna(True) == False
    reward_granted   = df["is_reward_granted"].fillna(False) == True

    # Valid Condition 1: successful referral with all checks passing
    valid_c1 = (
        has_reward &
        is_berhasil &
        has_txn &
        txn_paid &
        txn_new &
        txn_after_ref &
        txn_same_month &
        membership_valid &
        not_deleted &
        reward_granted
    )

    # Valid Condition 2: pending/failed referral with no reward
    valid_c2 = is_pending_fail & ~has_reward

    # Invalid Condition 1: reward exists but status is not Berhasil
    invalid_c1 = has_reward & ~is_berhasil

    # Invalid Condition 2: reward exists but no transaction
    invalid_c2 = has_reward & ~has_txn

    # Invalid Condition 3: no reward but has paid transaction after referral
    invalid_c3 = ~has_reward & has_txn & txn_paid & txn_after_ref

    # Invalid Condition 4: status Berhasil but no/zero reward
    invalid_c4 = is_berhasil & ~has_reward

    # Invalid Condition 5: transaction occurred before referral
    invalid_c5 = has_txn & ~txn_after_ref

    is_invalid = invalid_c1 | invalid_c2 | invalid_c3 | invalid_c4 | invalid_c5
    is_valid   = (valid_c1 | valid_c2) & ~is_invalid

    df["is_business_logic_valid"] = is_valid
    return df


# ---------------------------------------------------------------------------
# 8. Finalise output columns
# ---------------------------------------------------------------------------
def finalise_report(df: pd.DataFrame) -> pd.DataFrame:
    """Select, rename, and order the output columns per specification."""
    df = df.reset_index(drop=True)
    df["referral_details_id"] = df.index + 101  # starts at 101 per sample

    col_map = {
        "referral_details_id":       "referral_details_id",
        "referral_id":               "referral_id",
        "referral_source":           "referral_source",
        "referral_source_category":  "referral_source_category",
        "referral_at_local":         "referral_at",
        "referrer_id":               "referrer_id",
        "referrer_name":             "referrer_name",
        "referrer_phone_number":     "referrer_phone_number",
        "referrer_homeclub":         "referrer_homeclub",
        "referee_id":                "referee_id",
        "referee_name":              "referee_name",
        "referee_phone":             "referee_phone",
        "referral_status":           "referral_status",
        "num_reward_days":           "num_reward_days",
        "effective_transaction_id":  "transaction_id",
        "transaction_status":        "transaction_status",
        "transaction_at":            "transaction_at",
        "transaction_location":      "transaction_location",
        "transaction_type":          "transaction_type",
        "updated_at_local":          "updated_at",
        "reward_granted_at":         "reward_granted_at",
        "is_business_logic_valid":   "is_business_logic_valid",
    }

    out = df[[c for c in col_map if c in df.columns]].rename(columns=col_map)

    # Format datetime columns
    for col in ["referral_at", "transaction_at", "updated_at", "reward_granted_at"]:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce").dt.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

    # Apply initcap only to human-readable categorical text fields
    initcap_cols = [
        "referral_source", "referral_source_category", "referral_status",
        "transaction_status", "transaction_type",
    ]
    for col in initcap_cols:
        if col in out.columns:
            out[col] = initcap(out[col])

    return out


# ---------------------------------------------------------------------------
# 9. Main entrypoint
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("  Springer Capital — Referral Pipeline")
    print("=" * 60)

    # --- Load ---
    print("\n[1/5] Loading source data …")
    dfs = load_data(DATA_DIR)

    # --- Clean ---
    print("\n[2/5] Cleaning tables …")
    dfs["lead_logs"]              = clean_lead_logs(dfs["lead_logs"])
    dfs["paid_transactions"]      = clean_paid_transactions(dfs["paid_transactions"])
    dfs["referral_rewards"]       = clean_referral_rewards(dfs["referral_rewards"])
    dfs["user_logs"]              = clean_user_logs(dfs["user_logs"])
    dfs["user_referral_logs"]     = clean_user_referral_logs(dfs["user_referral_logs"])
    dfs["user_referral_statuses"] = clean_user_referral_statuses(dfs["user_referral_statuses"])
    dfs["user_referrals"]         = clean_user_referrals(dfs["user_referrals"])

    # --- Process ---
    print("\n[3/5] Processing data …")
    dfs["user_referrals"] = adjust_referral_times(dfs["user_referrals"], dfs["user_logs"])
    dfs["user_referrals"] = assign_source_category(dfs["user_referrals"], dfs["lead_logs"])

    # --- Build report ---
    print("\n[4/5] Building master report …")
    report = build_report(dfs)

    # --- Business logic ---
    print("\n[5/5] Applying business logic validation …")
    report = apply_business_logic(report)
    report = finalise_report(report)

    # --- Output ---
    out_path = os.path.join(OUTPUT_DIR, "referral_report.csv")
    report.to_csv(out_path, index=False)
    print(f"\n✓ Report saved → {out_path}")
    print(f"  Total rows : {len(report)}")
    print(f"  Valid      : {report['is_business_logic_valid'].sum()}")
    print(f"  Invalid    : {(~report['is_business_logic_valid']).sum()}")
    print("\nDone.")


if __name__ == "__main__":
    main()
