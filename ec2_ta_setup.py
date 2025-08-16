# ec2_ta_setup.py
# EC2 Trusted Advisor (TA) analysis — platform-aware, agent-friendly views
# Usage:
#   import duckdb, ec2_ta_setup as ta
#   con = duckdb.connect()
#   ta.load_ta_csv(con, "path/to/ec2_ta.csv")
#   ta.create_views(con)

import re
import duckdb
import pandas as pd

# -----------------------------
# Helpers
# -----------------------------
def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [re.sub(r"\W+", "_", c.strip()).lower() for c in df.columns]

    # map common aliases -> canonical field names we use in SQL
    alias_map = {
        "number_of_instances_to_purchase": "ta_rec_instances",
        "existing_savings_usd": "ta_est_savings",
        "rightsize_cost_avoidance_usd": "ta_rightsize_savings",
        "recommended_instance_type": "recommended_instance_type",
        "instance_type": "instance_type",
        "avg_cpu_14d": "avg_cpu_14d",
        "current_cost_usd": "current_cost_usd",
        "usage_pattern": "usage_pattern",
    }

    # ensure required columns exist
    want_cols = [
        "billing_period","account_name","business_area","instance_id","region","platform",
        "instance_type","ta_rec_instances","ta_est_savings","recommended_instance_type",
        "ta_rightsize_savings","current_cost_usd","avg_cpu_14d","usage_pattern"
    ]
    # rename where possible
    for k, v in alias_map.items():
        if k in df.columns and v not in df.columns:
            df.rename(columns={k: v}, inplace=True)
    # add missing as None
    for c in want_cols:
        if c not in df.columns:
            df[c] = None

    return df[want_cols]

# -----------------------------
# Load TA CSV → ec2_ta table
# -----------------------------
def load_ta_csv(con: duckdb.DuckDBPyConnection, csv_path: str) -> int:
    """Loads & normalizes a TA CSV into table `ec2_ta`. Returns row count."""
    df = pd.read_csv(csv_path)
    df = _norm_cols(df)
    con.execute("CREATE TABLE IF NOT EXISTS ec2_ta AS SELECT * FROM df WHERE 1=0")  # schema only
    con.execute("DELETE FROM ec2_ta")
    con.register("df", df)
    con.execute("INSERT INTO ec2_ta SELECT * FROM df")
    con.unregister("df")
    return len(df)

# -----------------------------
# Views (platform-aware + agent features)
# -----------------------------
def create_views(con):
    # 0) Normalize platform to family + flavor
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_norm AS
    SELECT
      e.*,

      -- High-level family (simple)
      CASE
        WHEN platform ILIKE '%win%'                          THEN 'Windows'
        WHEN platform ILIKE '%linux%' OR platform ILIKE '%unix%' THEN 'Linux'
        ELSE 'Other'
      END AS platform_family,

      -- More detailed flavor (keeps Windows variants + common Linux flavors)
      CASE
        WHEN platform ILIKE '%windows%sql%enterprise%'       THEN 'Windows SQL Ent'
        WHEN platform ILIKE '%windows%sql%standard%'         THEN 'Windows SQL Std'
        WHEN platform ILIKE '%windows%sql%'                  THEN 'Windows SQL'
        WHEN platform ILIKE '%windows%'                      THEN 'Windows'

        WHEN platform ILIKE '%red hat%' OR platform ILIKE '%rhel%' THEN 'RHEL'
        WHEN platform ILIKE '%suse%'                          THEN 'SUSE'
        WHEN platform ILIKE '%amazon linux%'                  THEN 'Amazon Linux'
        WHEN platform ILIKE '%ubuntu%'                        THEN 'Ubuntu'

        WHEN platform IS NULL OR TRIM(platform) = ''         THEN 'Unknown'
        ELSE platform
      END AS platform_flavor

    FROM ec2_ta e;
    """)

    # 1) Rollup for dashboards: BA × Region × platform_family
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_by_ba_region_platform AS
    WITH base AS (
      SELECT
        recommendation_date, business_area, region, platform_family AS platform,
        SUM(COALESCE(ta_rec_instances,0))               AS rec_instances,
        SUM(COALESCE(ta_est_savings_usd,0))             AS total_ta_savings_usd,
        AVG(NULLIF(avg_util_6mo_pct,0))                 AS avg_util_6mo_pct
      FROM ec2_ta_norm
      GROUP BY 1,2,3,4
    ),
    tot AS (
      SELECT recommendation_date, SUM(total_ta_savings_usd) AS grand_total
      FROM base GROUP BY 1
    )
    SELECT
      b.*,
      ROUND(100.0 * b.total_ta_savings_usd / NULLIF(t.grand_total,0), 2) AS savings_share_pct,
      RANK() OVER (
        PARTITION BY b.recommendation_date
        ORDER BY b.total_ta_savings_usd DESC
      ) AS savings_rank
    FROM base b
    JOIN tot t USING (recommendation_date)
    ORDER BY total_ta_savings_usd DESC NULLS LAST, rec_instances DESC NULLS LAST;
    """)

    # 2) Drill-down rollup: BA × Region × platform_flavor (richer)
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_by_ba_region_flavor AS
    SELECT
      recommendation_date, business_area, region, platform_flavor,
      SUM(COALESCE(ta_rec_instances,0))               AS rec_instances,
      SUM(COALESCE(ta_est_savings_usd,0))             AS total_ta_savings_usd,
      AVG(NULLIF(avg_util_6mo_pct,0))                 AS avg_util_6mo_pct
    FROM ec2_ta_norm
    GROUP BY 1,2,3,4
    ORDER BY total_ta_savings_usd DESC NULLS LAST, rec_instances DESC NULLS LAST;
    """)

    # 3) Detail table: include both family + flavor for agent answers
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_recommendations_detail AS
    SELECT
      recommendation_date,
      business_area,
      region,
      platform_family  AS platform,   -- keep 'platform' = family for simpler filters
      platform_flavor,
      instance_type,
      avg_util_6mo_pct,
      recurring_monthly_cost_usd,
      ta_rec_instances,
      ta_est_savings_usd
    FROM ec2_ta_norm
    ORDER BY ta_est_savings_usd DESC NULLS LAST;
    """)

    # 4) Opinionated actions (Buy RIs) — include both platform levels
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_actions_explain AS
    SELECT
      recommendation_date,
      business_area,
      region,
      platform_family  AS platform,
      platform_flavor,
      instance_type,
      ta_rec_instances,
      ta_est_savings_usd               AS best_action_savings,
      'buy_reserved_instances'         AS best_action,
      CASE
        WHEN avg_util_6mo_pct >= 60 THEN 'High'
        WHEN avg_util_6mo_pct BETWEEN 30 AND 60 THEN 'Medium'
        ELSE 'Low'
      END AS confidence,
      CONCAT(
        'TA suggests ', COALESCE(CAST(ta_rec_instances AS VARCHAR), '0'),
        ' RIs; est. saving $', COALESCE(CAST(ROUND(ta_est_savings_usd,2) AS VARCHAR),'0'),
        '/mo. 6-mo util ', COALESCE(CAST(ROUND(avg_util_6mo_pct,1) AS VARCHAR),'NA'),
        '%. Platform flavor: ', platform_flavor, '.'
      ) AS reason
    FROM ec2_ta_norm
    WHERE ta_rec_instances IS NOT NULL AND ta_rec_instances > 0
    ORDER BY best_action_savings DESC NULLS LAST;
    """)

    # 5) Hotspots (BA × Region) keyed by recommendation_date (unchanged)
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_hotspots AS
    SELECT
      recommendation_date,
      business_area,
      region,
      SUM(COALESCE(ta_est_savings_usd,0)) AS total_savings_usd,
      SUM(COALESCE(ta_rec_instances,0))   AS total_rec_instances,
      RANK() OVER (
        PARTITION BY recommendation_date
        ORDER BY SUM(COALESCE(ta_est_savings_usd,0)) DESC
      ) AS rank_in_period
    FROM ec2_ta_norm
    GROUP BY 1,2,3
    ORDER BY total_savings_usd DESC;
    """)

# (optional quick run)
if __name__ == "__main__":
    con = duckdb.connect()
    # n = load_ta_csv(con, "ec2_ta.csv"); print("rows:", n)
    # create_views(con)
    # print(con.execute("SELECT * FROM ec2_ta_actions_explain LIMIT 5").fetchdf())


tab1, tab2, tab3, tab4 = st.tabs(["Hotspots", "Recs", "Actions", "Spot/Schedule"])

with tab1:  # BA×Region×Platform rollup
    v = "ec2_ta_by_ba_region_platform"
    q = f"SELECT * FROM {v} WHERE {sw_for_view(v)} ORDER BY total_savings_all DESC LIMIT 500"
    st.caption(q); st.dataframe(con.execute(q).fetchdf())

with tab2:  # Detailed TA recs
    v = "ec2_ta_recommendations_detail"
    q = f"SELECT * FROM {v} WHERE {sw_for_view(v)} ORDER BY COALESCE(ta_est_savings,0)+COALESCE(ta_rightsize_savings,0) DESC NULLS LAST LIMIT 500"
    st.caption(q); st.dataframe(con.execute(q).fetchdf())

with tab3:  # Unified actions (ranked & explained)
    v = "ec2_ta_actions_explain"
    q = f"SELECT * FROM {v} WHERE {sw_for_view(v)} ORDER BY best_action_savings DESC NULLS LAST LIMIT 500"
    st.caption(q); st.dataframe(con.execute(q).fetchdf())

with tab4:  # Optional: Spot & Scheduling candidates
    v1, v2 = "ec2_ta_spot_candidates", "ec2_ta_scheduling_candidates"
    q1 = f"SELECT * FROM {v1} WHERE {sw_for_view(v1)} ORDER BY est_spot_savings DESC NULLS LAST LIMIT 500"
    q2 = f"SELECT * FROM {v2} WHERE {sw_for_view(v2)} ORDER BY est_sched_savings DESC NULLS LAST LIMIT 500"
    st.caption(q1); st.dataframe(con.execute(q1).fetchdf())
    st.caption(q2); st.dataframe(con.execute(q2).fetchdf())
