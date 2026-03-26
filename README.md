# Springer Capital — Referral Program Data Pipeline

> A Python/Pandas data pipeline that profiles, cleans, processes, and
> validates referral data to produce a fraud-detection report.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Repository Structure](#repository-structure)
3. [Quick Start (Local)](#quick-start-local)
4. [Quick Start (Docker)](#quick-start-docker)
5. [Pipeline Walkthrough](#pipeline-walkthrough)
6. [Business Logic — Fraud Detection](#business-logic--fraud-detection)
7. [Output Schema](#output-schema)
8. [Data Profiling](#data-profiling)
9. [Environment Variables](#environment-variables)

---

## Project Overview

The Referral Program Pipeline ingests seven CSV source tables, applies
cleaning and timezone-normalisation, joins them into a single denormalised
report, then flags each referral as **valid** or **potentially fraudulent**
using a set of defined business rules.

**Key deliverables**

| File | Description |
|---|---|
| `pipeline.py` | Main ETL + fraud-detection script |
| `profiling_script.py` | Data profiling script |
| `Dockerfile` | Container definition |
| `output/referral_report.csv` | Final fraud-detection report (46 rows) |
| `profiling/data_profiling_report.xlsx` | Profiling report (all tables) |
| `docs/data_dictionary.md` | Business-user data dictionary |

---

## Repository Structure

```
springer-referral-pipeline/
├── src/                           # Python source scripts
│   ├── pipeline.py                # Main ETL script
│   └── profiling_script.py        # Data profiling script
│
├── data/                          # Source CSV files (mount or place here)
│   ├── lead_log.csv
│   ├── paid_transactions.csv
│   ├── referral_rewards.csv
│   ├── user_logs.csv
│   ├── user_referral_logs.csv
│   ├── user_referral_statuses.csv
│   └── user_referrals.csv
│
├── output/                        # Generated report (git-ignored)
│   └── referral_report.csv
│
├── profiling/                     # Generated profiling artefacts (git-ignored)
│   ├── data_profiling_report.csv
│   └── data_profiling_report.xlsx
│
├── docs/
│   └── data_dictionary.md         # Non-technical data dictionary
│
├── requirements.txt               # Python dependencies
├── Dockerfile                     # Docker container definition
├── .dockerignore
├── .gitignore
└── README.md
```

---

## Quick Start (Local)

### Prerequisites

- Python 3.12+
- pip

### Steps

```bash
# 1. Clone the repository
git clone https://github.com/your-org/springer-referral-pipeline.git
cd springer-referral-pipeline

# 2. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Place the source CSV files inside data/
#    (already present if you cloned with sample data)

# 5. Run data profiling
python src/profiling_script.py

# 6. Run the main pipeline
python src/pipeline.py
```

Output files will be written to:
- `output/referral_report.csv`
- `profiling/data_profiling_report.csv`
- `profiling/data_profiling_report.xlsx`

---

## Quick Start (Docker)

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or Docker Engine on Linux)

### Build the image

```bash
docker build -t springer-referral-pipeline .
```

### Run the container

```bash
docker run --rm \
  -v "$(pwd)/data":/app/data:ro \
  -v "$(pwd)/output":/app/output \
  -v "$(pwd)/profiling":/app/profiling \
  springer-referral-pipeline
```

> **Windows (PowerShell)**
> ```powershell
> docker run --rm `
>   -v "${PWD}/data:/app/data:ro" `
>   -v "${PWD}/output:/app/output" `
>   -v "${PWD}/profiling:/app/profiling" `
>   springer-referral-pipeline
> ```

The container mounts three host directories:

| Host path | Container path | Access |
|---|---|---|
| `./data` | `/app/data` | read-only |
| `./output` | `/app/output` | read-write |
| `./profiling` | `/app/profiling` | read-write |

Generated files appear in `./output/` and `./profiling/` on your host machine immediately after the container exits.

### Run only one script

```bash
# Profiling only
docker run --rm \
  -v "$(pwd)/data":/app/data:ro \
  -v "$(pwd)/profiling":/app/profiling \
  springer-referral-pipeline \
  python src/profiling_script.py

# Pipeline only
docker run --rm \
  -v "$(pwd)/data":/app/data:ro \
  -v "$(pwd)/output":/app/output \
  springer-referral-pipeline \
  python src/pipeline.py
```

---

## Pipeline Walkthrough

```
Load CSVs
   │
   ▼
Clean each table
  ├─ Replace 'null' strings with NaN
  ├─ Parse & type-cast timestamps (UTC)
  ├─ Deduplicate
  └─ Apply InitCap to string fields (except club names)
   │
   ▼
Time Adjustment
  └─ Convert all UTC timestamps to local time via
     timezone columns (joined from user_logs when absent)
   │
   ▼
Source Category Assignment
  ├─ 'User Sign Up'      → 'Online'
  ├─ 'Draft Transaction' → 'Offline'
  └─ 'Lead'              → lead_logs.source_category
   │
   ▼
Build Master Report
  └─ Join: user_referrals ← user_referral_logs
                          ← user_referral_statuses
                          ← referral_rewards
                          ← paid_transactions
                          ← user_logs (referrer info)
   │
   ▼
Business Logic Validation (is_business_logic_valid)
   │
   ▼
Export → output/referral_report.csv  (46 rows)
```

---

## Business Logic — Fraud Detection

Each referral is marked `TRUE` (valid) or `FALSE` (potential fraud) in the
`is_business_logic_valid` column.

### ✅ Valid Conditions

| # | Rule |
|---|------|
| V1 | Reward > 0, status = Berhasil, transaction exists, status = PAID, type = NEW, transaction after referral, same month, referrer membership active, referrer account not deleted, reward granted to referee |
| V2 | Status is Menunggu or Tidak Berhasil **and** no reward assigned |

### ❌ Invalid Conditions (Potential Fraud)

| # | Rule |
|---|------|
| I1 | Reward > 0 but referral status is **not** Berhasil |
| I2 | Reward > 0 but **no transaction ID** linked |
| I3 | No reward but transaction is PAID and occurred after referral |
| I4 | Status = Berhasil but reward is null or 0 |
| I5 | Transaction date is **before** referral creation date |

---

## Output Schema

Full column definitions are in [`docs/data_dictionary.md`](docs/data_dictionary.md).

| Column | Type | Example |
|---|---|---|
| referral_details_id | INTEGER | 101 |
| referral_id | STRING | 9331c8f1… |
| referral_source | STRING | User Sign Up |
| referral_source_category | STRING | Online |
| referral_at | DATETIME | 2024-05-01 12:17:31 |
| referrer_id | STRING | 2c71c5d6… |
| referrer_name | STRING | John Doe |
| referrer_phone_number | STRING | abc123hash… |
| referrer_homeclub | STRING | BENHIL |
| referee_id | STRING | f1327c9d… |
| referee_name | STRING | Jane Doe |
| referee_phone | STRING | 5ba638fe… |
| referral_status | STRING | Berhasil |
| num_reward_days | INTEGER | 20 |
| transaction_id | STRING | 1d1eb8a9… |
| transaction_status | STRING | Paid |
| transaction_at | DATETIME | 2024-05-02 11:49:01 |
| transaction_location | STRING | ARTERI PONDOK INDAH |
| transaction_type | STRING | New |
| updated_at | DATETIME | 2024-05-01 12:17:31 |
| reward_granted_at | DATETIME | 2024-06-02 20:42:09 |
| is_business_logic_valid | BOOLEAN | True |

---

## Data Profiling

The profiling script calculates per-column statistics for every source table:

| Metric | Description |
|---|---|
| `data_type` | Pandas inferred dtype |
| `total_rows` | Row count of the table |
| `null_count` | Count of null / 'null' values |
| `null_pct` | Percentage of null values |
| `populated_pct` | Percentage of non-null values |
| `distinct_count` | Number of unique non-null values |
| `min_value` | Minimum value (numeric or lexicographic) |
| `max_value` | Maximum value |
| `max_actual_length` | Maximum string length |

Results are saved as both CSV and Excel (one sheet per table).

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DATA_DIR` | `data` | Path to folder containing source CSV files |
| `OUTPUT_DIR` | `output` | Path where `referral_report.csv` is written |
| `PROFILING_DIR` | `profiling` | Path where profiling reports are written |

Override at runtime:

```bash
# Local
DATA_DIR=/mnt/lake/raw OUTPUT_DIR=/mnt/lake/curated python pipeline.py

# Docker
docker run --rm \
  -e DATA_DIR=/app/data \
  -e OUTPUT_DIR=/app/output \
  -v "$(pwd)/data":/app/data:ro \
  -v "$(pwd)/output":/app/output \
  springer-referral-pipeline
```

---

> **Note on credentials** — No API keys or database passwords are stored in
> this repository. If the pipeline is extended to read from or write to cloud
> storage, credentials must be injected via environment variables or a secrets
> manager (e.g. AWS Secrets Manager, GCP Secret Manager) and never hard-coded.