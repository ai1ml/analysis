import os, io, re, duckdb, pandas as pd, streamlit as st
from datetime import datetime

# Import your setup modules
import rds_agent_setup as rds
import ec2_ta_setup as ec2
import snapshots_setup as snaps
import ebs_setup as ebs
import ec2_ops_setup as ec2ops

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

st.sidebar.subheader("Snapshots")
colS1, colS2 = st.sidebar.columns(2)
with colS1:
    if st.button("Reload Snapshot CSVs"):
        n = snaps.load_snapshots_csvs_from_folder(con, folder="data_snapshots")
        st.success(f"Snapshots: loaded {n} rows" if n else "Snapshots: no CSVs in ./data_snapshots/")
with colS2:
    if st.button("Build Snapshot views"):
        snaps.initialize(con)
        st.success("Snapshots: views created.")

st.sidebar.subheader("EBS")
c_ebs1, c_ebs2 = st.sidebar.columns(2)
with c_ebs1:
    if st.button("Reload EBS CSVs"):
        n = ebs.load_ebs_csvs_from_folder(con, folder="data_ebs")
        st.success(f"EBS: loaded {n} rows" if n else "EBS: no CSVs in ./data_ebs/")
with c_ebs2:
    if st.button("Build EBS views"):
        ebs.initialize(con)
        st.success("EBS: views created.")      

st.sidebar.subheader("EC2 Ops (On-Demand → Spot / Schedule / Rightsize)")
c_ops1, c_ops2 = st.sidebar.columns(2)
with c_ops1:
    if st.button("Reload EC2 Ops CSVs"):
        n = ec2ops.load_ops_csvs_from_folder(con, folder="data_ec2_ops")
        st.success(f"EC2 Ops: loaded {n} rows" if n else "EC2 Ops: no CSVs in ./data_ec2_ops/")
with c_ops2:
    if st.button("Build EC2 Ops views"):
        ec2ops.initialize_after_load(con)
        st.success("EC2 Ops: size map & views created.")          

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


st.divider()
st.subheader("Snapshots — Cost & Cleanup Opportunities")

# Filters
snap_BAs     = [r[0] for r in con.execute("SELECT DISTINCT business_area FROM snapshots_parsed ORDER BY 1").fetchall()] if con else []
snap_regions = [r[0] for r in con.execute("SELECT DISTINCT region FROM snapshots_parsed ORDER BY 1").fetchall()] if con else []
snap_types   = [r[0] for r in con.execute("SELECT DISTINCT snapshot_type FROM snapshots_parsed ORDER BY 1").fetchall()] if con else []

s1, s2, s3 = st.columns(3)
snap_sel_ba    = s1.selectbox("Business Area", options=["(all)"] + snap_BAs)
snap_sel_region= s2.selectbox("Region", options=["(all)"] + snap_regions)
snap_sel_type  = s3.selectbox("Snapshot Type", options=["(all)"] + snap_types)

def sw(base="1=1"):
    wc = [base]
    if snap_sel_ba != "(all)":
        wc.append(f"business_area = '{snap_sel_ba.replace(\"'\",\"''\")}'")
    if snap_sel_region != "(all)":
        wc.append(f"region = '{snap_sel_region.replace(\"'\",\"''\")}'")
    if snap_sel_type != "(all)":
        wc.append(f"snapshot_type = '{snap_sel_type.replace(\"'\",\"''\")}'")
    return " AND ".join(wc)

tabS1, tabS2, tabS3, tabS4, tabS5 = st.tabs([
    "Hotspots (BA/Region/Type)", "Archive Opportunity", "Sprawl — Top", "Sprawl — Clusters", "BA Roll-up"
])

with tabS1:
    q = f"SELECT * FROM snapshots_by_ba_region WHERE {sw('1=1')} ORDER BY total_cost_usd DESC LIMIT 500"
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())

with tabS2:
    q = f"""
    SELECT business_area, region, snapshot_id, gb_standard, cost_standard,
           price_snapshot_gb, price_archive_gb, est_monthly_savings_usd
    FROM snapshots_archive_opportunity
    WHERE {sw('1=1')}
    ORDER BY est_monthly_savings_usd DESC NULLS LAST, gb_standard DESC
    LIMIT 500
    """
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())

with tabS3:
    q = f"SELECT * FROM snapshots_sprawl_top WHERE {sw('1=1')} ORDER BY public_cost_usd DESC LIMIT 500"
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())

with tabS4:
    # clusters ignore snapshot_type filter by design; apply BA/region only
    q = f"""
    SELECT * FROM snapshots_sprawl_clusters
    WHERE {sw('1=1').replace(" AND snapshot_type = ", " AND 1=1 /*type ignored*/ AND ")}
    ORDER BY snapshot_count DESC, total_cost_usd DESC
    LIMIT 200
    """
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())

with tabS5:
    q = f"SELECT * FROM snapshots_by_ba WHERE {sw('1=1').replace(' AND snapshot_type = ', ' AND 1=1 /*type ignored*/ AND ')} ORDER BY total_cost_usd DESC"
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())


st.divider()
st.subheader("EBS — Unattached cleanup, gp2→gp3, io1 review")

# Filters
ebs_BAs    = [r[0] for r in con.execute("SELECT DISTINCT business_area FROM ebs_volumes_usage ORDER BY 1").fetchall()] if con else []
ebs_types  = [r[0] for r in con.execute("SELECT DISTINCT volume_type FROM ebs_volumes_usage ORDER BY 1").fetchall()] if con else []
ebs_states = [r[0] for r in con.execute("SELECT DISTINCT volume_state FROM ebs_volumes_usage ORDER BY 1").fetchall()] if con else []

e1, e2, e3 = st.columns(3)
ebs_sel_ba    = e1.selectbox("Business Area", options=["(all)"] + ebs_BAs)
ebs_sel_type  = e2.selectbox("Volume Type",  options=["(all)"] + ebs_types)
ebs_sel_state = e3.selectbox("State",        options=["(all)"] + ebs_states)

def ebs_where(base="1=1"):
    wc = [base]
    if ebs_sel_ba   != "(all)": wc.append(f"business_area = '{ebs_sel_ba.replace(\"'\",\"''\")}'")
    if ebs_sel_type != "(all)": wc.append(f"volume_type = '{ebs_sel_type.replace(\"'\",\"''\")}'")
    if ebs_sel_state!= "(all)": wc.append(f"volume_state = '{ebs_sel_state.replace(\"'\",\"''\")}'")
    return " AND ".join(wc)

tabB1, tabB2, tabB3, tabB4, tabB5 = st.tabs([
    "Actions (ranked)", "Unattached ≥30d", "gp2→gp3 (attached)", "io1 low-IOPS review", "Leadership rollups"
])

with tabB1:
    q = f"SELECT * FROM ebs_actions_ranked WHERE {ebs_where('1=1')} ORDER BY est_monthly_savings_usd DESC NULLS LAST, current_cost_usd DESC LIMIT 500"
    st.caption(q); st.dataframe(con.execute(q).fetchdf())

with tabB2:
    q = f"SELECT * FROM ebs_unattached_long_idle WHERE {ebs_where('1=1')} ORDER BY current_monthly_cost_usd DESC LIMIT 500"
    st.caption(q); st.dataframe(con.execute(q).fetchdf())

with tabB3:
    q = f"SELECT * FROM ebs_gp2_to_gp3_opportunity WHERE {ebs_where(\"volume_state='in use'\")} ORDER BY est_monthly_savings_usd DESC NULLS LAST LIMIT 500"
    st.caption(q); st.dataframe(con.execute(q).fetchdf())

with tabB4:
    q = f"SELECT * FROM ebs_io1_low_iops_review WHERE {ebs_where(\"volume_state='in use'\")} ORDER BY est_monthly_savings_usd DESC NULLS LAST LIMIT 500"
    st.caption(q); st.dataframe(con.execute(q).fetchdf())

with tabB5:
    colL, colR = st.columns(2)
    q1 = f"SELECT * FROM ebs_cost_by_ba_attached_state ORDER BY total_cost_usd DESC"
    colL.caption(q1); colL.dataframe(con.execute(q1).fetchdf())
    q2 = f"SELECT * FROM ebs_attached_summary ORDER BY total_cost_usd DESC"
    colR.caption(q2); colR.dataframe(con.execute(q2).fetchdf())

# Optional: downloads
d1, d2 = st.columns(2)
with d1:
    if st.button("⬇️ Download EBS Actions"):
        df = con.execute(f"SELECT * FROM ebs_actions_ranked WHERE {ebs_where('1=1')}").fetchdf()
        st.download_button("ebs_actions_ranked.csv", data=df.to_csv(index=False), file_name="ebs_actions_ranked.csv", mime="text/csv")
with d2:
    if st.button("⬇️ Download EBS BA Rollup"):
        df = con.execute("SELECT * FROM ebs_cost_by_ba_attached_state").fetchdf()
        st.download_button("ebs_ba_rollup.csv", data=df.to_csv(index=False), file_name="ebs_ba_rollup.csv", mime="text/csv")

st.divider()
st.subheader("EC2 Ops — Spot, Scheduling, Rightsizing (1-month)")

# Filters
ops_BAs     = [r[0] for r in con.execute("SELECT DISTINCT business_area FROM ec2_ops_usage ORDER BY 1").fetchall()] if con else []
ops_regions = [r[0] for r in con.execute("SELECT DISTINCT region FROM ec2_ops_usage ORDER BY 1").fetchall()] if con else []
ops_opts    = [r[0] for r in con.execute("SELECT DISTINCT purchase_option FROM ec2_ops_usage ORDER BY 1").fetchall()] if con else []

o1, o2, o3 = st.columns(3)
ops_sel_ba     = o1.selectbox("Business Area", options=["(all)"] + ops_BAs)
ops_sel_region = o2.selectbox("Region", options=["(all)"] + ops_regions)
ops_sel_opt    = o3.selectbox("Purchase Option", options=["(all)"] + ops_opts)

def ow(base="1=1"):
    wc = [base]
    if ops_sel_ba     != "(all)": wc.append(f"business_area = '{ops_sel_ba.replace(\"'\",\"''\")}'")
    if ops_sel_region != "(all)": wc.append(f"region = '{ops_sel_region.replace(\"'\",\"''\")}'")
    if ops_sel_opt    != "(all)": wc.append(f"purchase_option = '{ops_sel_opt.replace(\"'\",\"''\")}'")
    return " AND ".join(wc)

tabO1, tabO2, tabO3, tabO4, tabO5, tabO6 = st.tabs([
    "Actions (ranked)", "Spot candidates", "Schedule candidates",
    "Rightsize (ours)", "TA comparison", "BA rollup"
])

with tabO1:
    q = f"SELECT * FROM ec2_ops_actions_ranked WHERE {ow('1=1')} ORDER BY est_monthly_savings_usd DESC NULLS LAST, current_cost_usd DESC LIMIT 500"
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())

with tabO2:
    q = f"SELECT * FROM ec2_spot_candidates WHERE {ow(\"purchase_option ILIKE 'ondemand'\")} ORDER BY est_monthly_savings_usd DESC NULLS LAST LIMIT 500"
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())

with tabO3:
    q = f"SELECT * FROM ec2_schedule_candidates WHERE {ow(\"purchase_option ILIKE 'ondemand'\")} ORDER BY est_monthly_savings_usd DESC NULLS LAST LIMIT 500"
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())

with tabO4:
    q = f\"\"\"
    SELECT *
    FROM ec2_rightsize_candidates
    WHERE {ow("1=1")} 
    ORDER BY est_monthly_savings_usd DESC NULLS LAST, total_cost_usd DESC
    LIMIT 500
    \"\"\"
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())

with tabO5:
    q = f"SELECT * FROM ec2_ta_rightsize_comparison WHERE {ow('1=1')} ORDER BY comparison, ours_est_monthly_savings_usd DESC NULLS LAST LIMIT 500"
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())

with tabO6:
    q = "SELECT * FROM ec2_ops_ba_summary ORDER BY ondemand_cost_usd DESC"
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf())

# Optional downloads
dops1, dops2 = st.columns(2)
with dops1:
    if st.button("⬇️ Download EC2 Ops Actions"):
        df = con.execute(f"SELECT * FROM ec2_ops_actions_ranked WHERE {ow('1=1')}").fetchdf()
        st.download_button("ec2_ops_actions_ranked.csv", data=df.to_csv(index=False), file_name="ec2_ops_actions_ranked.csv", mime="text/csv")
with dops2:
    if st.button("⬇️ Download EC2 Ops BA Rollup"):
        df = con.execute("SELECT * FROM ec2_ops_ba_summary ORDER BY ondemand_cost_usd DESC").fetchdf()
        st.download_button("ec2_ops_ba_summary.csv", data=df.to_csv(index=False), file_name="ec2_ops_ba_summary.csv", mime="text/csv")