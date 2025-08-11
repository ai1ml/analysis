import os, io, re, duckdb, pandas as pd, streamlit as st
from datetime import datetime

# Import your setup modules
import rds_agent_setup as rds
import ec2_ta_setup as ec2

st.set_page_config(page_title="Cloud Savings — RDS + EC2 (MVP)", layout="wide")
st.title("Cloud Savings — RDS + EC2 (MVP)")

# Create or reuse a DuckDB connection
if "con" not in st.session_state:
    st.session_state.con = duckdb.connect(":memory:")
con = st.session_state.con

# -------- Helpers ----------
def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [re.sub(r"\W+","_", c.strip()).lower() for c in df.columns]
    return df

def load_local_rds_csvs(folder="data"):
    if not os.path.isdir(folder):
        os.makedirs(folder, exist_ok=True)
        return 0
    files = [f for f in os.listdir(folder) if f.lower().endswith(".csv")]
    if not files:
        return 0
    frames = []
    for f in files:
        df = pd.read_csv(os.path.join(folder, f))
        frames.append(normalize_cols(df))
    if not frames:
        return 0
    df_all = pd.concat(frames, ignore_index=True)
    con.execute("DELETE FROM rds_usage")
    con.register("rds_df", df_all)
    con.execute("INSERT INTO rds_usage SELECT * FROM rds_df")
    con.unregister("rds_df")
    return len(df_all)

def load_local_ec2_csvs(folder="data_ec2"):
    return ec2.load_ec2_ta_csvs_from_folder(con, folder=folder)

def distinct(col, table):
    try:
        return [r[0] for r in con.execute(
            f"SELECT DISTINCT {col} FROM {table} WHERE {col} IS NOT NULL ORDER BY {col}"
        ).fetchall()]
    except:
        return []

# ---------------- Sidebar actions ----------------
st.sidebar.header("Data loading")
colA, colB = st.sidebar.columns(2)
with colA:
    if st.button("Reload RDS CSVs"):
        n = load_local_rds_csvs("data")
        st.success(f"RDS: loaded {n} rows" if n else "RDS: no CSVs found in ./data/")
with colB:
    if st.button("Reload EC2 TA CSVs"):
        n = load_local_ec2_csvs("data_ec2")
        st.success(f"EC2: loaded {n} rows" if n else "EC2: no CSVs found in ./data_ec2/")

if st.sidebar.button("Build RDS sizes + views (+prices if creds)"):
    rds.initialize_after_loading_usage(con)
    st.success("RDS: sizes/views ready (pricing refreshed if AWS creds configured).")

if st.sidebar.button("Build EC2 views"):
    ec2.create_views_ec2(con)
    st.success("EC2: TA views created.")

st.sidebar.divider()
if st.sidebar.button("Refresh AWS prices now (RDS)"):
    try:
        rds.refresh_price_rds_from_usage(con, deployment="Single-AZ", engine="Any")
        st.success("price_rds refreshed.")
    except Exception as e:
        st.error(f"Pricing refresh failed (expected if no AWS creds): {e}")

st.divider()

# ---------------- Filters (RDS) ----------------
st.subheader("RDS — Optimizations")
rds_months = con.execute("SELECT DISTINCT billing_period FROM rds_usage ORDER BY billing_period DESC").fetchdf()
rds_BAs    = distinct("BA", "rds_usage")
rds_regions= distinct("region", "rds_usage")

fc1, fc2, fc3 = st.columns(3)
rds_sel_month = fc1.selectbox("RDS: Billing period", options=["(all)"] + rds_months["billing_period"].astype(str).tolist())
rds_sel_ba    = fc2.selectbox("RDS: Business Unit (BA)", options=["(all)"] + rds_BAs)
rds_sel_region= fc3.selectbox("RDS: Region", options=["(all)"] + rds_regions)

def rds_where(base="1=1"):
    wc = [base]
    if rds_sel_month != "(all)":
        wc.append(f"billing_period = DATE '{rds_sel_month}'")
    if rds_sel_ba != "(all)":
        wc.append(f"BA = '{rds_sel_ba.replace(\"'\",\"''\")}'")
    if rds_sel_region != "(all)":
        wc.append(f"region = '{rds_sel_region.replace(\"'\",\"''\")}'")
    return " AND ".join(wc)

tabR1, tabR2, tabR3, tabR4 = st.tabs(["Actions (ranked)", "Underutilized", "Rightsize", "High CPU"])

with tabR1:
    q = f"SELECT * FROM rds_actions_ranked WHERE {rds_where('1=1')} ORDER BY est_monthly_savings_usd DESC NULLS LAST, current_cost_usd DESC LIMIT 500"
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())
    df = con.execute(f"SELECT BA, action, SUM(COALESCE(est_monthly_savings_usd,0)) AS total_savings FROM rds_actions_ranked WHERE {rds_where('1=1')} GROUP BY 1,2 ORDER BY total_savings DESC").fetchdf()
    st.write("RDS Savings by BA & Action")
    st.dataframe(df)

with tabR2:
    q = f"SELECT * FROM rds_underutilized WHERE {rds_where('1=1')} ORDER BY cost_usd DESC LIMIT 500"
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())

with tabR3:
    q = f"""
    SELECT billing_period, account_name, BA, db_id, region, current_class, recommended_class,
           current_cost_usd, est_monthly_savings_usd, avg_cpu_14d, price_date
    FROM rds_rightsize_next_smaller
    WHERE {rds_where('1=1')}
    ORDER BY est_monthly_savings_usd DESC NULLS LAST
    LIMIT 500
    """
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())

with tabR4:
    q = f"""
    SELECT billing_period, account_name, BA, db_id, region, instance_class,
           hours, cost_usd, avg_cpu_14d, recommendation
    FROM rds_high_utilization
    WHERE {rds_where('1=1')}
    ORDER BY avg_cpu_14d DESC, cost_usd DESC
    LIMIT 500
    """
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())

st.divider()

# ---------------- EC2 TA Section ----------------
st.subheader("EC2 (Trusted Advisor) — RI Opportunities")
ec2_BAs     = distinct("BA", "ec2_reserved_recs")
ec2_regions = distinct("region", "ec2_reserved_recs")
ec2_plats   = distinct("platform", "ec2_reserved_recs")

e1, e2, e3 = st.columns(3)
ec2_sel_ba     = e1.selectbox("EC2: Business Unit (BA)", options=["(all)"] + ec2_BAs)
ec2_sel_region = e2.selectbox("EC2: Region", options=["(all)"] + ec2_regions)
ec2_sel_plat   = e3.selectbox("EC2: Platform", options=["(all)"] + ec2_plats)

def ec2_where(base="1=1"):
    wc = [base]
    if ec2_sel_ba != "(all)":
        wc.append(f"BA = '{ec2_sel_ba.replace(\"'\",\"''\")}'")
    if ec2_sel_region != "(all)":
        wc.append(f"region = '{ec2_sel_region.replace(\"'\",\"''\")}'")
    if ec2_sel_plat != "(all)":
        wc.append(f"platform = '{ec2_sel_plat.replace(\"'\",\"''\")}'")
    return " AND ".join(wc)

tabE1, tabE2, tabE3, tabE4, tabE5 = st.tabs([
    "Ranked (ROI + Risk)", "By BA (coverage%)", "By Platform", "By Region", "Pareto Top"
])

with tabE1:
    q = f"""
    SELECT *
    FROM ec2_ri_ranked
    WHERE {ec2_where('1=1')}
    ORDER BY savings_per_instance DESC NULLS LAST, est_monthly_savings_usd DESC NULLS LAST
    LIMIT 500
    """
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())
    # Quick filters users often ask for:
    st.markdown("**Quick filters:**")
    colq1, colq2 = st.columns(2)
    if colq1.button("High-Util ≥ 80% & ≤ 2 instances suggested"):
        q2 = f"""
        SELECT *
        FROM ec2_ri_ranked
        WHERE {ec2_where("1=1")} AND avg_util_6m >= 80 AND num_instances_to_purchase <= 2
        ORDER BY est_monthly_savings_usd DESC NULLS LAST
        LIMIT 200
        """
        st.caption(q2); st.dataframe(con.execute(q2).fetchdf())
    if colq2.button("Purchase candidates (util ≥50% & $/inst ≥25)"):
        q3 = f"""
        SELECT *
        FROM ec2_ri_ranked
        WHERE {ec2_where("1=1")} AND avg_util_6m >= 50 AND savings_per_instance >= 25
        ORDER BY savings_per_instance DESC NULLS LAST
        LIMIT 200
        """
        st.caption(q3); st.dataframe(con.execute(q3).fetchdf())

with tabE2:
    q = f"SELECT * FROM ec2_ri_by_ba WHERE {ec2_where('1=1')} ORDER BY total_savings_usd DESC"
    st.caption(q); st.dataframe(con.execute(q).fetchdf())

with tabE3:
    q = f"SELECT * FROM ec2_ri_by_platform WHERE {ec2_where('1=1')} ORDER BY total_savings_usd DESC NULLS LAST"
    st.caption(q); st.dataframe(con.execute(q).fetchdf())

with tabE4:
    q = f"SELECT * FROM ec2_ri_by_region WHERE {ec2_where('1=1')} ORDER BY total_savings_usd DESC NULLS LAST"
    st.caption(q); st.dataframe(con.execute(q).fetchdf())

with tabE5:
    q = f"SELECT * FROM ec2_ri_top_pareto WHERE {ec2_where('1=1')} ORDER BY est_monthly_savings_usd DESC NULLS LAST"
    st.caption(q); st.dataframe(con.execute(q).fetchdf())

st.divider()

# Downloads
c6, c7 = st.columns(2)
with c6:
    if st.button("⬇️ Download RDS Actions"):
        df = con.execute(f"SELECT * FROM rds_actions_ranked WHERE {rds_where('1=1')}").fetchdf()
        st.download_button("rds_actions_ranked.csv", data=df.to_csv(index=False), file_name="rds_actions_ranked.csv", mime="text/csv")
with c7:
    if st.button("⬇️ Download EC2 Ranked"):
        df = con.execute(f"SELECT * FROM ec2_ri_ranked WHERE {ec2_where('1=1')}").fetchdf()
        st.download_button("ec2_ri_ranked.csv", data=df.to_csv(index=False), file_name="ec2_ri_ranked.csv", mime="text/csv")