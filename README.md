# Springer Capital – Referral Program Data Pipeline

> **Data Engineer Take-Home Assessment**
> Pipeline: referral data profiling + fraud-detection report (46-row CSV output)

---

## Table of Contents
1. [Project Structure](#project-structure)
2. [Quick Start (Local)](#quick-start-local)
3. [Quick Start (Docker)](#quick-start-docker)
4. [Script Details](#script-details)
5. [Output](#output)
6. [Business Logic Summary](#business-logic-summary)
7. [Cloud Storage (Optional)](#cloud-storage-optional)

---

## Project Structure

```
springer-capital-referral/
├── data/
│   ├── raw/                  ← Place all 7 CSV source files here
│   └── output/               ← Generated report lands here (git-ignored)
├── docs/
│   └── data_dictionary.xlsx  ← Business-facing data dictionary (4 sheets)
├── profiling/                ← Auto-generated profiling outputs (git-ignored)
├── src/
│   ├── data_profiling.py     ← Step 1: Profile all source tables
│   └── pipeline.py           ← Step 2: Full ETL + fraud detection
├── .dockerignore
├── .gitignore
├── Dockerfile
├── README.md
└── requirements.txt
```

---

## Quick Start (Local)

### Prerequisites
- Python 3.11+
- pip

### 1. Clone & enter the repo
```bash
git clone <https://github.com/Jmbriz123/Data-Engineer-Test.git>
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Add source data
Place the seven CSV files into `data/raw/`:
```
data/raw/lead_logs.csv
data/raw/user_referrals.csv
data/raw/user_referral_logs.csv
data/raw/user_logs.csv
data/raw/user_referral_statuses.csv
data/raw/referral_rewards.csv
data/raw/paid_transactions.csv
```

### 4. Run data profiling
```bash
python src/data_profiling.py
```
Outputs:
- `profiling/profiling_report.xlsx` — one Excel sheet per table
- `profiling/profiling_summary.csv` — combined CSV view

### 5. Run the pipeline
```bash
python src/pipeline.py
```
Output:
- `data/output/referral_report.csv` — 46-row fraud-detection report

---

## Quick Start (Docker)

### Prerequisites
- Docker Desktop (or Docker Engine on Linux)

### 1. Build the image
```bash
docker build -t springer-referral-pipeline .
```

### 2. Run the pipeline and export the report
The report is written to `/app/data/output/` inside the container.
Use a bind-mount so the file appears on your host machine:

```bash
# On Linux / macOS
docker run --rm \
  -v "$(pwd)/data/raw:/app/data/raw" \
  -v "$(pwd)/data/output:/app/data/output" \
  springer-referral-pipeline

# On Windows (PowerShell)
docker run --rm `
  -v "${PWD}/data/raw:/app/data/raw" `
  -v "${PWD}/data/output:/app/data/output" `
  springer-referral-pipeline
```

After the container exits, the report will be at:
```
data/output/referral_report.csv
```

### 3. Run profiling inside Docker
Override the default command:
```bash
docker run --rm \
  -v "$(pwd)/data/raw:/app/data/raw" \
  -v "$(pwd)/profiling:/app/profiling" \
  springer-referral-pipeline \
  python src/data_profiling.py
```

---

## Script Details

### `src/data_profiling.py`
| Step | What it does |
|------|-------------|
| Load | Reads each CSV from `data/raw/` |
| Profile | Counts nulls, distinct values, min/max, sample values per column |
| Export | Writes `profiling_report.xlsx` (per-table sheets) + `profiling_summary.csv` |

### `src/pipeline.py`
| Step | What it does |
|------|-------------|
| 1. Load | Reads all 7 CSVs into pandas DataFrames |
| 2. Clean | Parses timestamps (UTC-aware), strips whitespace, coerces types, normalises booleans |
| 3. Process | Joins all tables, converts timestamps to local timezone, applies initcap, derives `referral_source_category` |
| 4. Business Logic | Evaluates each row against valid/invalid fraud conditions → `is_business_logic_valid` |
| 5. Output | Selects final 22 columns, drops duplicates, writes `referral_report.csv` |

---

## Output

### `data/output/referral_report.csv`
22 columns, 46 rows.

| Column | Type | Description |
|--------|------|-------------|
| referral_details_id | INTEGER | Sequential report row ID starting at 101 |
| referral_id | TEXT | Original referral identifier |
| referral_source | TEXT | User Sign Up / Draft Transaction / Lead |
| referral_source_category | TEXT | Online / Offline / (lead source) |
| referral_at | DATETIME | Referral creation time (local tz) |
| referrer_id | TEXT | ID of the referring member |
| referrer_name | TEXT | Name of referrer (Title Case) |
| referrer_phone_number | TEXT | Referrer contact number |
| referrer_homeclub | TEXT | Referrer's gym location |
| referee_id | TEXT | ID of referred person (Lead only) |
| referee_name | TEXT | Name of referred person |
| referee_phone | TEXT | Referred person contact |
| referral_status | TEXT | Berhasil / Menunggu / Tidak Berhasil |
| num_reward_days | INTEGER | Reward value |
| transaction_id | TEXT | Linked transaction ID |
| transaction_status | TEXT | PAID / etc. |
| transaction_at | DATETIME | Transaction time (local tz) |
| transaction_location | TEXT | Transaction club/location |
| transaction_type | TEXT | NEW / etc. |
| updated_at | DATETIME | Last update time (local tz) |
| reward_granted_at | DATETIME | When reward was disbursed |
| is_business_logic_valid | BOOLEAN | TRUE = valid, FALSE = suspicious |

---

## Business Logic Summary

| Code | Result | Condition |
|------|--------|-----------|
| V1 | ✅ Valid | reward>0, Berhasil, PAID NEW transaction, post-referral in same month, active membership, reward granted |
| V2 | ✅ Valid | Menunggu or Tidak Berhasil with no reward |
| I1 | ❌ Invalid | reward>0 but status ≠ Berhasil |
| I2 | ❌ Invalid | reward>0 but no transaction ID |
| I3 | ❌ Invalid | no reward but PAID transaction exists after referral |
| I4 | ❌ Invalid | Berhasil but no/zero reward |
| I5 | ❌ Invalid | transaction_at < referral_at |

See `docs/data_dictionary.xlsx` → "Business_Rules" sheet for full plain-English descriptions.

---

## Cloud Storage (Optional)

To upload the output report to cloud storage, use environment variables — **never hard-code credentials**.

### AWS S3 (example)
```bash
# Set credentials via environment (never put keys in code)
export AWS_ACCESS_KEY_ID=<your_key>
export AWS_SECRET_ACCESS_KEY=<your_secret>
export AWS_DEFAULT_REGION=ap-southeast-1
export S3_BUCKET=<your-bucket-name>

pip install boto3
python scripts/upload_s3.py          # see scripts/upload_s3.py
```

### GCS
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
export GCS_BUCKET=<your-bucket-name>
pip install google-cloud-storage
python scripts/upload_gcs.py
```

> ⚠️ Never commit `.env` files or credential JSON files to the repository.
> Add them to `.gitignore` and use a secrets manager in production.