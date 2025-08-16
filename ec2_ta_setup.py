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
def create_views(con: duckdb.DuckDBPyConnection):
    # 0) Normalize platform labels once → use ec2_ta_norm everywhere
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_norm AS
    SELECT
      e.*,
      CASE
        WHEN platform ILIKE '%win%'                    THEN 'Windows'
        WHEN platform ILIKE '%linux%' OR platform ILIKE '%unix%' THEN 'Linux'
        WHEN platform IS NULL OR TRIM(platform) = ''   THEN 'Unknown'
        ELSE platform
      END AS platform_norm
    FROM ec2_ta e;
    """)

    # 1) Main rollup: BA × Region × Platform (+ share % and rank)
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_by_ba_region_platform AS
    WITH base AS (
      SELECT
        billing_period, business_area, region, platform_norm AS platform,
        COUNT(DISTINCT instance_id) AS instance_count,
        SUM(COALESCE(ta_est_savings,0))               AS total_ta_savings,
        SUM(COALESCE(ta_rightsize_savings,0))         AS total_ta_rightsize_savings,
        SUM(COALESCE(ta_est_savings,0) + COALESCE(ta_rightsize_savings,0)) AS total_savings_all
      FROM ec2_ta_norm
      GROUP BY 1,2,3,4
    ),
    tot AS (
      SELECT billing_period, SUM(total_savings_all) AS grand_total
      FROM base
      GROUP BY 1
    )
    SELECT
      b.*,
      ROUND(100.0 * b.total_savings_all / NULLIF(t.grand_total,0), 2) AS savings_share_pct,
      RANK() OVER (PARTITION BY b.billing_period ORDER BY b.total_savings_all DESC) AS savings_rank
    FROM base b
    JOIN tot t USING (billing_period)
    ORDER BY b.total_savings_all DESC NULLS LAST, b.instance_count DESC;
    """)

    # 2) Detailed TA recommendations (drilldown)
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_recommendations_detail AS
    SELECT
      billing_period,
      account_name,
      business_area,
      region,
      platform_norm AS platform,
      instance_id,
      instance_type           AS current_instance_type,
      recommended_instance_type,
      ta_rec_instances,
      ta_est_savings,
      ta_rightsize_savings,
      current_cost_usd,
      avg_cpu_14d
    FROM ec2_ta_norm
    ORDER BY COALESCE(ta_est_savings,0) + COALESCE(ta_rightsize_savings,0) DESC NULLS LAST;
    """)

    # 3) Spot candidates (heuristic): Linux + has cost → ~60% saving vs OD
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_spot_candidates AS
    SELECT
      billing_period,
      business_area,
      region,
      platform_norm AS platform,
      instance_id,
      instance_type           AS current_instance_type,
      current_cost_usd,
      CASE
        WHEN platform_norm = 'Linux' AND current_cost_usd IS NOT NULL AND current_cost_usd > 0
          THEN ROUND(current_cost_usd * 0.60, 2)
      END AS est_spot_savings,
      CASE
        WHEN platform_norm = 'Linux' THEN 'Linux OK for Spot (validate interruption tolerance)'
        WHEN platform_norm = 'Windows' THEN 'Windows licensing/eligibility often limits Spot'
        ELSE 'Unknown platform'
      END AS reason
    FROM ec2_ta_norm
    ORDER BY est_spot_savings DESC NULLS LAST;
    """)

    # 4) Scheduling candidates (heuristic): likely non-prod 24×7 → switch to 5×12 (~65%)
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_scheduling_candidates AS
    SELECT
      billing_period,
      business_area,
      region,
      platform_norm AS platform,
      instance_id,
      instance_type           AS current_instance_type,
      current_cost_usd,
      CASE
        WHEN (COALESCE(usage_pattern,'') ILIKE '%24x7%' OR usage_pattern IS NULL)
         AND (
              business_area ILIKE '%dev%' OR business_area ILIKE '%test%' OR business_area ILIKE '%stage%'
              OR instance_id  ILIKE '%dev%' OR instance_id  ILIKE '%test%' OR instance_id  ILIKE '%stage%'
             )
         AND current_cost_usd IS NOT NULL AND current_cost_usd > 0
        THEN ROUND(current_cost_usd * 0.65, 2)
      END AS est_sched_savings,
      'Non-prod 24×7 → schedule 5×12 (~65% saving assumed)' AS reason
    FROM ec2_ta_norm
    ORDER BY est_sched_savings DESC NULLS LAST;
    """)

    # 5) High utilization (possible upsize / reserved capacity)
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_high_utilization AS
    SELECT
      billing_period,
      business_area,
      region,
      platform_norm AS platform,
      instance_id,
      instance_type           AS current_instance_type,
      avg_cpu_14d,
      current_cost_usd
    FROM ec2_ta_norm
    WHERE avg_cpu_14d IS NOT NULL AND avg_cpu_14d >= 90
    ORDER BY avg_cpu_14d DESC, current_cost_usd DESC NULLS LAST;
    """)

    # 6) Top drivers (which types drive savings within BA/Region/Platform)
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_top_drivers AS
    SELECT
      billing_period,
      business_area,
      region,
      platform_norm AS platform,
      instance_type,
      COUNT(*) AS instances,
      SUM(COALESCE(ta_est_savings,0) + COALESCE(ta_rightsize_savings,0)) AS total_savings
    FROM ec2_ta_norm
    GROUP BY 1,2,3,4,5
    ORDER BY total_savings DESC, instances DESC
    LIMIT 200;
    """)

    # 7) Platform share within BA (helps narrative; % + rank)
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_ba_platform_share AS
    WITH base AS (
      SELECT
        billing_period, business_area, platform_norm AS platform,
        SUM(COALESCE(ta_est_savings,0) + COALESCE(ta_rightsize_savings,0)) AS platform_savings
      FROM ec2_ta_norm
      GROUP BY 1,2,3
    ),
    tot AS (
      SELECT billing_period, business_area, SUM(platform_savings) AS ba_total
      FROM base GROUP BY 1,2
    )
    SELECT
      b.billing_period,
      b.business_area,
      b.platform,
      b.platform_savings,
      t.ba_total,
      ROUND(100.0 * b.platform_savings / NULLIF(t.ba_total,0), 2) AS platform_share_pct,
      RANK() OVER (PARTITION BY b.billing_period, b.business_area ORDER BY b.platform_savings DESC) AS platform_rank_in_ba
    FROM base b
    JOIN tot t USING (billing_period, business_area)
    ORDER BY b.business_area, platform_share_pct DESC NULLS LAST;
    """)

    # 8) Unified actions (per instance) + best action selection
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_actions AS
    SELECT
      billing_period, business_area, region, platform_norm AS platform,
      instance_id, instance_type AS current_instance_type,
      COALESCE(ta_rightsize_savings,0) AS rightsize_savings,
      current_cost_usd,
      avg_cpu_14d
    FROM ec2_ta_norm;
    """)

    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_actions_ranked AS
    SELECT
      a.*,
      COALESCE(s.est_spot_savings, 0)  AS spot_savings,
      COALESCE(sc.est_sched_savings, 0) AS schedule_savings,
      -- choose best action by $ (simple rule; ties broken by order below)
      CASE
        WHEN COALESCE(s.est_spot_savings,0) >= GREATEST(COALESCE(a.rightsize_savings,0), COALESCE(sc.est_sched_savings,0))
          THEN 'spot'
        WHEN COALESCE(sc.est_sched_savings,0) >= GREATEST(COALESCE(a.rightsize_savings,0), COALESCE(s.est_spot_savings,0))
          THEN 'schedule'
        WHEN COALESCE(a.rightsize_savings,0) > 0
          THEN 'rightsize'
        ELSE 'none'
      END AS best_action,
      GREATEST(COALESCE(s.est_spot_savings,0), COALESCE(sc.est_sched_savings,0), COALESCE(a.rightsize_savings,0)) AS best_action_savings
    FROM ec2_ta_actions a
    LEFT JOIN ec2_ta_spot_candidates       s  USING (billing_period,business_area,region,platform,instance_id)
    LEFT JOIN ec2_ta_scheduling_candidates sc USING (billing_period,business_area,region,platform,instance_id)
    """)

    # 9) Scoring (confidence) + explanation text (reason)
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_actions_scored AS
    SELECT
      ar.*,
      CASE ar.best_action
        WHEN 'spot' THEN CASE WHEN ar.platform = 'Linux' THEN 'High' ELSE 'Low' END
        WHEN 'schedule' THEN CASE
              WHEN (business_area ILIKE '%dev%' OR instance_id ILIKE '%dev%' OR business_area ILIKE '%test%' OR instance_id ILIKE '%test%' OR business_area ILIKE '%stage%' OR instance_id ILIKE '%stage%')
                THEN 'High' ELSE 'Medium' END
        WHEN 'rightsize' THEN CASE
              WHEN ar.avg_cpu_14d IS NOT NULL AND ar.avg_cpu_14d < 30 THEN 'High'
              WHEN ar.avg_cpu_14d IS NULL THEN 'Medium'
              ELSE 'Low' END
        ELSE 'Low'
      END AS confidence
    FROM ec2_ta_actions_ranked ar;
    """)

    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_actions_explain AS
    SELECT
      sc.*,
      CASE sc.best_action
        WHEN 'spot' THEN 'Linux workload: consider Spot (validate interruption tolerance).'
        WHEN 'schedule' THEN 'Likely non-prod 24×7: consider 5×12 schedule.'
        WHEN 'rightsize' THEN 'Rightsize recommended by TA / low CPU headroom.'
        ELSE 'No clear action.'
      END AS reason
    FROM ec2_ta_actions_scored sc
    ORDER BY best_action_savings DESC NULLS LAST, rightsize_savings DESC NULLS LAST;
    """)

    # 10) Hotspots (BA×Region totals with rank)
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_hotspots AS
    SELECT
      billing_period,
      business_area,
      region,
      SUM(COALESCE(ta_est_savings,0) + COALESCE(ta_rightsize_savings,0)) AS total_savings,
      RANK() OVER (PARTITION BY billing_period ORDER BY SUM(COALESCE(ta_est_savings,0) + COALESCE(ta_rightsize_savings,0)) DESC) AS rank_in_period
    FROM ec2_ta_norm
    GROUP BY 1,2,3
    ORDER BY total_savings DESC;
    """)

# (optional quick run)
if __name__ == "__main__":
    con = duckdb.connect()
    # n = load_ta_csv(con, "ec2_ta.csv"); print("rows:", n)
    # create_views(con)
    # print(con.execute("SELECT * FROM ec2_ta_actions_explain LIMIT 5").fetchdf())
