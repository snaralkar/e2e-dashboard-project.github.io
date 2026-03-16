"""
DataPulse -- ETL Pipeline
=========================
Handles: Extraction -> Validation -> Transformation -> Loading

Author : Shubham Naralkar
Stack  : Python, Pandas, SQLAlchemy, MySQL

Install:
    pip install pandas numpy sqlalchemy pymysql cryptography python-dotenv
"""

import sys, io, os, logging
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ── ENV ──────────────────────────────────────────────────
load_dotenv()

DB_CONFIG = {
    "host"    : os.getenv("DB_HOST",     "sql12.freesqldatabase.com"),
    "port"    : int(os.getenv("DB_PORT", "3306")),
    "user"    : os.getenv("DB_USER",     "sql12820146"),
    "password": os.getenv("DB_PASSWORD", "j6Ep5EicwM"),
    "database": os.getenv("DB_NAME",     "sql12820146"),
}

# ── LOGGING (Windows UTF-8 fix) ───────────────────────────
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("etl_pipeline.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger("DataPulse_ETL")

# ── PIPELINE CONFIG ───────────────────────────────────────
CONFIG = {
    "input_file"      : r"C:\Users\Shubham Naralkar\OneDrive\E2E_Project\DataPulse\sales_data.csv",
    "table_name"      : "sales",
    "required_cols"   : ["order_id","customer_id","product","category",
                          "quantity","unit_price","sales","profit","date","region"],
    "valid_categories": ["Electronics","Clothing","Home & Kitchen","Books","Sports"],
    "valid_regions"   : ["North","South","East","West","Central"],
}


# ═══════════════════════════════════════════════
# STEP 1 -- EXTRACT
# ═══════════════════════════════════════════════
def extract(filepath: str) -> pd.DataFrame:
    log.info(f"[EXTRACT] Reading: {filepath}")
    if not Path(filepath).exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    df = pd.read_csv(filepath)
    log.info(f"[EXTRACT] Loaded {len(df):,} rows x {len(df.columns)} columns")
    return df


# ═══════════════════════════════════════════════
# STEP 2 -- VALIDATE
# ═══════════════════════════════════════════════
def validate(df: pd.DataFrame) -> pd.DataFrame:
    log.info("[VALIDATE] Starting data quality checks...")
    issues, original_len = [], len(df)

    missing = [c for c in CONFIG["required_cols"] if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    log.info("  [OK] All required columns present")

    dupes = df.duplicated(subset="order_id", keep="first")
    if dupes.sum():
        issues.append(f"Dropped {dupes.sum()} duplicate order_ids")
        df = df[~dupes]

    null_cols = df[CONFIG["required_cols"]].isnull().sum()
    null_cols = null_cols[null_cols > 0]
    if not null_cols.empty:
        issues.append(f"Nulls found: {null_cols.to_dict()}")
        df = df.dropna(subset=CONFIG["required_cols"])

    bad = (df["sales"] <= 0) | (df["quantity"] <= 0) | (df["unit_price"] <= 0)
    if bad.sum():
        issues.append(f"Dropped {bad.sum()} rows with invalid numeric values")
        df = df[~bad]

    bad_cat = ~df["category"].isin(CONFIG["valid_categories"])
    bad_reg = ~df["region"].isin(CONFIG["valid_regions"])
    if bad_cat.sum():
        issues.append(f"Dropped {bad_cat.sum()} unknown categories")
        df = df[~bad_cat]
    if bad_reg.sum():
        issues.append(f"Dropped {bad_reg.sum()} unknown regions")
        df = df[~bad_reg]

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    bad_dates = df["date"].isna()
    if bad_dates.sum():
        issues.append(f"Dropped {bad_dates.sum()} bad dates")
        df = df[~bad_dates]

    log.info(f"[VALIDATE] {len(issues)} issue(s), {original_len - len(df)} rows dropped")
    for i in issues:
        log.warning(f"  [WARN] {i}")
    log.info(f"  [OK] Clean rows: {len(df):,}")
    return df


# ═══════════════════════════════════════════════
# STEP 3 -- TRANSFORM
# ═══════════════════════════════════════════════
def transform(df: pd.DataFrame) -> pd.DataFrame:
    log.info("[TRANSFORM] Applying transformations...")

    df["year"]        = df["date"].dt.year
    df["month"]       = df["date"].dt.month
    df["quarter"]     = df["date"].dt.quarter
    df["month_name"]  = df["date"].dt.strftime("%b")
    df["day_of_week"] = df["date"].dt.day_name()

    df["profit_margin_pct"] = (df["profit"] / df["sales"] * 100).round(2)
    df["revenue_per_unit"]  = (df["sales"]  / df["quantity"]).round(2)

    df["profit_tier"] = pd.cut(
        df["profit_margin_pct"],
        bins=[-np.inf, 0, 15, 30, np.inf],
        labels=["Loss", "Low", "Medium", "High"]
    )

    order_counts = df.groupby("customer_id")["order_id"].transform("count")
    df["customer_segment"] = pd.cut(
        order_counts, bins=[0, 3, 7, np.inf],
        labels=["One-time", "Returning", "Loyal"]
    )

    mu, sigma        = df["sales"].mean(), df["sales"].std()
    df["is_outlier"] = (df["sales"] > mu + 3 * sigma).astype(int)
    df["year_month"] = df["date"].dt.to_period("M").astype(str)
    df["date"]       = df["date"].dt.strftime("%Y-%m-%d")

    log.info("  [OK] Derived: profit_margin_pct, profit_tier, customer_segment, is_outlier, year_month")
    log.info(f"  [OK] Final shape: {df.shape}")
    return df


# ═══════════════════════════════════════════════
# STEP 4 -- LOAD
# ═══════════════════════════════════════════════
def get_engine():
    url = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        f"?charset=utf8mb4"
    )
    return create_engine(url, echo=False)


def ensure_database_exists():
    root_url = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}?charset=utf8mb4"
    )
    engine = create_engine(root_url, echo=False)
    with engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{DB_CONFIG['database']}`"))
    engine.dispose()
    log.info(f"  [OK] Database `{DB_CONFIG['database']}` ready")


def load(df: pd.DataFrame, table: str) -> None:
    log.info(f"[LOAD] Connecting -> {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    ensure_database_exists()
    engine = get_engine()

    df["loaded_at"]        = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["profit_tier"]      = df["profit_tier"].astype(str)
    df["customer_segment"] = df["customer_segment"].astype(str)

    log.info(f"[LOAD] Writing {len(df):,} rows -> `{table}`")
    df.to_sql(name=table, con=engine, if_exists="replace", index=False, chunksize=500)

    with engine.connect() as conn:
        count = conn.execute(text(f"SELECT COUNT(*) FROM `{table}`")).scalar()
    engine.dispose()
    log.info(f"  [OK] MySQL record count: {count:,}")


# ═══════════════════════════════════════════════
# STEP 5 -- AUDIT
# ═══════════════════════════════════════════════
def log_audit(df: pd.DataFrame) -> None:
    log.info("=" * 55)
    log.info("DATAPULSE ETL -- AUDIT SUMMARY")
    log.info("=" * 55)
    log.info(f"  Total Orders    : {len(df):,}")
    log.info(f"  Total Sales     : INR {df['sales'].sum():,.0f}")
    log.info(f"  Total Profit    : INR {df['profit'].sum():,.0f}")
    log.info(f"  Avg Margin      : {df['profit_margin_pct'].mean():.1f}%")
    log.info(f"  Unique Customers: {df['customer_id'].nunique():,}")
    log.info(f"  Date Range      : {df['date'].min()} to {df['date'].max()}")
    log.info(f"  Outlier Rows    : {df['is_outlier'].sum()}")
    log.info(f"  Categories      : {df['category'].nunique()}")
    log.info("=" * 55)


# ═══════════════════════════════════════════════
# ORCHESTRATOR
# ═══════════════════════════════════════════════
def run_pipeline():
    start = datetime.now()
    log.info(">>> DataPulse ETL Pipeline -- STARTED")
    try:
        raw_df   = extract(CONFIG["input_file"])
        clean_df = validate(raw_df)
        final_df = transform(clean_df)
        load(final_df, CONFIG["table_name"])
        log_audit(final_df)
        log.info(f"[DONE] Pipeline completed in {(datetime.now()-start).total_seconds():.2f}s")
    except Exception as e:
        log.error(f"[FAILED] {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run_pipeline()
