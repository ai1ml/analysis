# streamlit_app.py
import os, io, re, duckdb, pandas as pd, streamlit as st
from datetime import datetime
from google.cloud import storage

# Import your setup helpers
import rds_agent_setup as setup

st.set_page_config(page_title="RDS Savings Agent (MVP)", layout="wide")
st.title("RDS Savings Agent — DuckDB + Streamlit (MVP)")

# --- Config ---
# Either use local CSVs from ./data or read from GCS if you set envs
USE_GCS = st.sidebar.checkbox("Load CSVs from GCS", value=False)
GCS_BUCKET = st.sidebar.text_input("GCS Bucket", os.getenv("GCS_BUCKET", ""))
GCS_PREFIX = st.sidebar.text_input("GCS Prefix", os.getenv("GCS_PREFIX", "aws-cost/rds/"))

# Create a DuckDB connection (persist file if you like)
if "con" not in st.session_state:
    st.session_state.con = duckdb.connect(":memory:")

con = st.session_state.con

# --- Helpers to load data ---
def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [re.sub(r"\W+","_", c.strip()).lower() for c in df.columns]
    return df

def load_local_csvs():
    folder = "data"
    files = [f for f in os.listdir(folder) if f.endswith(".csv")]
    if not files:
        st.warning("No CSVs in ./data. Drop your RDS CSVs there (with headers).")
        return 0
    frames = []
    for f in files:
        df = pd.read_csv(os.path.join(folder, f))
        frames.append(normalize_cols(df))
    if not frames:
        return 0
    df_all = pd.concat(frames, ignore_index=True)
    # Create table if needed
    con.execute("DELETE FROM rds_usage")
    con.register("rds_df", df_all)
    con.execute("INSERT INTO rds_usage SELECT * FROM rds_df")
    con.unregister("rds_df")
    return len(df_all)

def load_gcs_csvs(bucket, prefix):
    if not bucket:
        st.error("Please provide a GCS bucket.")
        return 0
    client = storage.Client()
    blobs = [b for b in client.list_blobs(bucket, prefix=prefix) if b.name.endswith(".csv")]
    if not blobs:
        st.warning(f"No CSVs found at gs://{bucket}/{prefix}")
        return 0

    frames = []
    for b in blobs:
        by = b.download_as_bytes()
        df = pd.read_csv(io.BytesIO(by))
        frames.append(normalize_cols(df))

    if not frames:
        return 0
    df_all = pd.concat(frames, ignore_index=True)
    con.execute("DELETE FROM rds_usage")
    con.register("rds_df", df_all)
    con.execute("INSERT INTO rds_usage SELECT * FROM rds_df")
    con.unregister("rds_df")
    return len(df_all)

# --- One-time table creation (if not created already) ---
# rds_agent_setup.py already issues CREATE TABLE IF NOT EXISTS; call once:
if st.sidebar.button("Initialize Tables (once)"):
    # This runs CREATE TABLEs; no views yet
    pass  # imported module executed CREATE TABLE on import

# --- Load data controls ---
colA, colB = st.columns(2)
with colA:
    if st.button("🔄 Reload RDS CSVs"):
        if USE_GCS:
            n = load_gcs_csvs(GCS_BUCKET, GCS_PREFIX)
        else:
            n = load_local_csvs()
        if n:
            st.success(f"Loaded {n} rows into rds_usage")

with colB:
    if st.button("🧱 Build Sizes + Views + (optional) Prices"):
        setup.initialize_after_loading_usage(con)
        st.success("Sizes built, views created. Price fetch attempted (if AWS creds).")

st.divider()

# --- Filters (read from DB) ---
def distinct(col, table="rds_usage"):
    try:
        return [r[0] for r in con.execute(f"SELECT DISTINCT {col} FROM {table} WHERE {col} IS NOT NULL ORDER BY {col}").fetchall()]
    except:
        return []

months = con.execute("SELECT DISTINCT billing_period FROM rds_usage ORDER BY billing_period DESC").fetchdf()
BAs    = distinct("BA", "rds_usage")
regions= distinct("region", "rds_usage")

c1, c2, c3 = st.columns(3)
sel_month = c1.selectbox("Billing period", options=["(all)"] + months["billing_period"].astype(str).tolist())
sel_ba    = c2.selectbox("Business Unit (BA)", options=["(all)"] + BAs)
sel_region= c3.selectbox("Region", options=["(all)"] + regions)

def where_clause(base="1=1"):
    wc = [base]
    if sel_month != "(all)":
        wc.append(f"billing_period = DATE '{sel_month}'")
    if sel_ba != "(all)":
        wc.append(f"BA = '{sel_ba.replace(\"'\",\"''\")}'")
    if sel_region != "(all)":
        wc.append(f"region = '{sel_region.replace(\"'\",\"''\")}'")
    return " AND ".join(wc)

# --- Summary cards ---
sum_row = con.execute(f"""
WITH a AS (
  SELECT * FROM rds_actions_ranked WHERE {where_clause('1=1')}
)
SELECT
  COUNT(*) AS total_actions,
  SUM(COALESCE(est_monthly_savings_usd,0)) AS total_savings
FROM a
""").fetchdf() if con else pd.DataFrame()

c4, c5 = st.columns(2)
if not sum_row.empty:
    c4.metric("Total actions", int(sum_row.loc[0, "total_actions"]))
    c5.metric("Total est. savings ($/mo)", f"{sum_row.loc[0, 'total_savings']:.2f}")

# --- Tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Actions (ranked)", "Underutilized", "Rightsize (next smaller)", "Off-hours", "High CPU"
])

with tab1:
    q = f"SELECT * FROM rds_actions_ranked WHERE {where_clause('1=1')} ORDER BY est_monthly_savings_usd DESC NULLS LAST, current_cost_usd DESC LIMIT 500"
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())

with tab2:
    q = f"SELECT * FROM rds_underutilized WHERE {where_clause('1=1')} ORDER BY cost_usd DESC LIMIT 500"
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())

with tab3:
    q = f"""
    SELECT billing_period, account_name, BA, db_id, region, current_class, recommended_class,
           current_cost_usd, est_monthly_savings_usd, avg_cpu_14d, price_date
    FROM rds_rightsize_next_smaller
    WHERE {where_clause('1=1')}
    ORDER BY est_monthly_savings_usd DESC NULLS LAST
    LIMIT 500
    """
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())

with tab4:
    q = f"""
    SELECT billing_period, account_name, BA, db_id, region, instance_class, current_cost_usd,
           est_monthly_savings_usd, approx_247, avg_cpu_14d
    FROM rds_offhours_candidates
    WHERE {where_clause('1=1')}
    ORDER BY est_monthly_savings_usd DESC NULLS LAST
    LIMIT 500
    """
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())

with tab5:
    q = f"""
    SELECT billing_period, account_name, BA, db_id, region, instance_class,
           hours, cost_usd, avg_cpu_14d, recommendation
    FROM rds_high_utilization
    WHERE {where_clause('1=1')}
    ORDER BY avg_cpu_14d DESC, cost_usd DESC
    LIMIT 500
    """
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())

st.divider()

# --- Actions ---
c6, c7 = st.columns(2)
with c6:
    if st.button("↻ Refresh Prices (AWS Pricing API)"):
        try:
            setup.refresh_price_rds_from_usage(con, deployment="Single-AZ", engine="Any")
            st.success("price_rds refreshed.")
        except Exception as e:
            st.error(f"Pricing refresh failed (expected if no AWS creds): {e}")

with c7:
    if st.button("⬇️ Export Actions CSV"):
        df = con.execute(f"SELECT * FROM rds_actions_ranked WHERE {where_clause('1=1')}").fetchdf()
        st.download_button("Download rds_actions_ranked.csv", data=df.to_csv(index=False), file_name="rds_actions_ranked.csv", mime="text/csv")