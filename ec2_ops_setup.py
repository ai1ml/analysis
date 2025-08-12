"""
EC2 Operational Optimization (1-month):
- OnDemand → Spot candidates
- OnDemand → Schedule (off-hours)
- Rightsizing (next smaller) using our CPU rule
- Validation vs TA fields (if present)

CSV expected (normalize to snake_case):
- billing_period (DATE)
- account_id (TEXT)
- business_area (TEXT)
- resource_id (TEXT)
- purchase_option (TEXT)                # 'OnDemand' | 'Spot' | 'Reserved' ...
- region (TEXT)
- current_instance_type (TEXT)          # e.g., m5.large
- usage_quantity_hours (DOUBLE)         # hours in the month (per instance id)
- total_cost_usd (DOUBLE)
- fourteen_day_average_cpu_utilization (DOUBLE)  # 0..100 (name can be avg_cpu_14d)
- number_days_of_consistent_data (INT)  # optional (ignored)
- recommended_instance_type (TEXT)      # TA (optional)
- rightsize_monthly_cost_avoidance (DOUBLE)  # TA (optional)
"""

import os, re, duckdb, pandas as pd

# -------------------
# Connect is done in your main app
# -------------------

def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [re.sub(r"\W+","_", c.strip()).lower() for c in df.columns]
    # common aliases
    rename = {
        "fourteendayaveragecpuutilization": "avg_cpu_14d",
        "fourteen_day_average_cpu_utilization": "avg_cpu_14d",
        "rightsizemonthlycostavoidance": "ta_rightsize_savings_usd",
        "rightsize_monthly_cost_avoidance": "ta_rightsize_savings_usd",
    }
    for k,v in rename.items():
        if k in df.columns and v not in df.columns:
            df.rename(columns={k:v}, inplace=True)
    return df

# -------------------
# Tables
# -------------------
def create_tables(con: duckdb.DuckDBPyConnection):
    con.execute("""
    CREATE TABLE IF NOT EXISTS ec2_ops_usage (
      billing_period DATE,
      account_id TEXT,
      business_area TEXT,
      resource_id TEXT,
      purchase_option TEXT,
      region TEXT,
      current_instance_type TEXT,
      usage_quantity_hours DOUBLE,
      total_cost_usd DOUBLE,
      avg_cpu_14d DOUBLE,
      number_days_of_consistent_data INTEGER,
      recommended_instance_type TEXT,
      ta_rightsize_savings_usd DOUBLE
    );
    """)
    # instance size ordering per family (built from data)
    con.execute("""
    CREATE TABLE IF NOT EXISTS ec2_sizes (
      family TEXT,    -- e.g., m5
      size   TEXT,    -- large, xlarge, 2xlarge, ...
      size_rank INTEGER  -- 1=smallest
    );
    """)

# -------------------
# Loader
# -------------------
def load_ops_csvs_from_folder(con: duckdb.DuckDBPyConnection, folder="data_ec2_ops") -> int:
    if not os.path.isdir(folder):
        os.makedirs(folder, exist_ok=True); return 0
    files = [f for f in os.listdir(folder) if f.lower().endswith(".csv")]
    if not files: return 0
    frames = []
    for f in files:
        frames.append(_normalize_cols(pd.read_csv(os.path.join(folder, f))))
    df_all = pd.concat(frames, ignore_index=True)

    required = [
      "billing_period","account_id","business_area","resource_id","purchase_option",
      "region","current_instance_type","usage_quantity_hours","total_cost_usd","avg_cpu_14d",
      "number_days_of_consistent_data","recommended_instance_type","ta_rightsize_savings_usd"
    ]
    for c in required:
        if c not in df_all.columns: df_all[c] = None

    con.execute("DELETE FROM ec2_ops_usage")
    con.register("ops_df", df_all[required])
    con.execute("""
      INSERT INTO ec2_ops_usage
      SELECT
        CAST(billing_period AS DATE),
        account_id, business_area, resource_id, purchase_option, region,
        current_instance_type,
        CAST(usage_quantity_hours AS DOUBLE),
        CAST(total_cost_usd AS DOUBLE),
        CAST(avg_cpu_14d AS DOUBLE),
        CAST(number_days_of_consistent_data AS INTEGER),
        recommended_instance_type,
        CAST(ta_rightsize_savings_usd AS DOUBLE)
      FROM ops_df
    """)
    con.unregister("ops_df")
    return len(df_all)

# -------------------
# Helper: build ec2_sizes from observed types
# -------------------
_SIZE_ORDER = {
    "nano":1, "micro":2, "small":3, "medium":4, "large":5, "xlarge":6,
    "2xlarge":7, "3xlarge":8, "4xlarge":9, "6xlarge":10, "8xlarge":11,
    "9xlarge":12, "10xlarge":13, "12xlarge":14, "16xlarge":15, "18xlarge":16,
    "24xlarge":17, "32xlarge":18
}

def refresh_ec2_sizes(con: duckdb.DuckDBPyConnection):
    df = con.execute("SELECT DISTINCT current_instance_type FROM ec2_ops_usage WHERE current_instance_type IS NOT NULL").fetchdf()
    rows=[]
    for t in df["current_instance_type"].dropna().unique():
        m = re.match(r"^([a-z0-9]+)\\.([^.]+)$", t)  # e.g., m5.large
        if not m: continue
        family, size = m.group(1), m.group(2).lower()
        r = _SIZE_ORDER.get(size)
        if r: rows.append((family, size, r))
    if not rows: return
    out = pd.DataFrame(rows, columns=["family","size","size_rank"]).drop_duplicates()
    out["size_rank"] = out.groupby("family")["size_rank"].rank(method="dense").astype(int)
    con.execute("DELETE FROM ec2_sizes")
    con.register("sz", out)
    con.execute("INSERT INTO ec2_sizes SELECT * FROM sz")
    con.unregister("sz")

# -------------------
# Views
# -------------------
def create_views(con: duckdb.DuckDBPyConnection, assumed_spot_discount=0.6):
    # Observed on-demand hourly by (type, region) from your file
    con.execute(f"""
    CREATE OR REPLACE VIEW ec2_od_hourly AS
    SELECT
      region, current_instance_type AS instance_type,
      SUM(total_cost_usd) / NULLIF(SUM(usage_quantity_hours),0) AS hourly_usd
    FROM ec2_ops_usage
    WHERE purchase_option ILIKE 'ondemand'
    GROUP BY region, instance_type;
    """)

    # Month length
    con.execute("""
    CREATE OR REPLACE VIEW ec2_month_len AS
    SELECT billing_period,
           (julianday(date_trunc('month', billing_period, 'start') + INTERVAL 1 MONTH)
            - julianday(date_trunc('month', billing_period, 'start')))::INT AS days_in_month
    FROM (SELECT DISTINCT billing_period FROM ec2_ops_usage);
    """)

    # 24x7 detector
    con.execute("""
    CREATE OR REPLACE VIEW ec2_usage_flags AS
    SELECT
      u.*,
      m.days_in_month,
      CASE WHEN u.usage_quantity_hours >= 0.98 * (m.days_in_month*24) THEN 1 ELSE 0 END AS approx_247,
      lower(coalesce(u.resource_id,'')) AS name_l
    FROM ec2_ops_usage u
    JOIN ec2_month_len m USING (billing_period);
    """)

    # A) Spot candidates (OnDemand, low CPU)
    con.execute(f"""
    CREATE OR REPLACE VIEW ec2_spot_candidates AS
    SELECT
      e.billing_period, e.account_id, e.business_area, e.resource_id, e.region,
      e.current_instance_type, e.usage_quantity_hours, e.total_cost_usd, e.avg_cpu_14d,
      'Spot' AS target_purchase_option,
      ROUND(e.total_cost_usd * {assumed_spot_discount}, 2) AS est_monthly_savings_usd,
      CASE
        WHEN e.name_l LIKE '%dev%' OR e.name_l LIKE '%test%' OR e.name_l LIKE '%staging%' THEN 'High'
        WHEN e.avg_cpu_14d IS NOT NULL AND e.avg_cpu_14d < 10 THEN 'Medium'
        ELSE 'Low'
      END AS confidence,
      'OnDemand → Spot (assume ~60% saving) — validate interruption tolerance' AS reason
    FROM ec2_usage_flags e
    WHERE e.purchase_option ILIKE 'ondemand'
      AND e.avg_cpu_14d IS NOT NULL AND e.avg_cpu_14d < 20;
    """)

    # B) Schedule candidates
    # Two flavors:
    #   (1) Non-prod running 24x7 → recommend 5x12 (≈65% saving)
    #   (2) Already not 24x7 → suggest aligning schedule to observed duty cycle
    con.execute("""
    CREATE OR REPLACE VIEW ec2_schedule_candidates AS
    WITH sched_nonprod_247 AS (
      SELECT
        e.billing_period, e.account_id, e.business_area, e.resource_id, e.region,
        e.current_instance_type, e.usage_quantity_hours, e.total_cost_usd, e.avg_cpu_14d,
        ROUND(e.total_cost_usd * 0.65, 2) AS est_monthly_savings_usd,
        'High' AS confidence,
        'Non-prod 24x7 → 5x12 schedule (~65% saving)' AS reason
      FROM ec2_usage_flags e
      WHERE e.purchase_option ILIKE 'ondemand'
        AND e.approx_247 = 1
        AND (e.name_l LIKE '%dev%' OR e.name_l LIKE '%test%' OR e.name_l LIKE '%staging%')
    ),
    sched_align_to_observed AS (
      SELECT
        e.billing_period, e.account_id, e.business_area, e.resource_id, e.region,
        e.current_instance_type, e.usage_quantity_hours, e.total_cost_usd, e.avg_cpu_14d,
        ROUND(e.total_cost_usd * (1 - (e.usage_quantity_hours / NULLIF((e.days_in_month*24),0))), 2) AS est_monthly_savings_usd,
        'Medium' AS confidence,
        'Observed hours << month — align schedule to actual duty cycle' AS reason
      FROM ec2_usage_flags e
      WHERE e.purchase_option ILIKE 'ondemand'
        AND e.approx_247 = 0
    )
    SELECT * FROM sched_nonprod_247
    UNION ALL
    SELECT * FROM sched_align_to_observed
    ORDER BY est_monthly_savings_usd DESC NULLS LAST, total_cost_usd DESC;
    """)

    # Helper: next smaller type in same family (from observed)
    con.execute("""
    CREATE OR REPLACE VIEW ec2_with_family AS
    SELECT
      e.*,
      regexp_extract(current_instance_type, '^([a-z0-9]+)\\.', 1) AS family,
      regexp_extract(current_instance_type, '\\.([^.]+)$', 1)     AS size
    FROM ec2_ops_usage e;
    """)

    # C) Rightsize candidates — our CPU rule (<30%), next smaller
    con.execute("""
    CREATE OR REPLACE VIEW ec2_rightsize_candidates AS
    WITH ranked AS (
      SELECT w.*, s.size_rank
      FROM ec2_with_family w
      JOIN ec2_sizes s ON w.family = s.family AND lower(w.size) = s.size
      WHERE w.purchase_option ILIKE 'ondemand'
        AND w.avg_cpu_14d IS NOT NULL AND w.avg_cpu_14d < 30
    ),
    tgt AS (
      SELECT r.*, (r.size_rank - 1) AS target_rank
      FROM ranked r WHERE r.size_rank > 1
    ),
    t2 AS (
      SELECT t.*, s2.size AS target_size
      FROM tgt t
      JOIN ec2_sizes s2 ON t.family = s2.family AND t.target_rank = s2.size_rank
    ),
    priced AS (
      SELECT
        x.*,
        od.hourly_usd AS current_hourly,
        od2.hourly_usd AS target_hourly
      FROM t2 x
      LEFT JOIN ec2_od_hourly od  ON od.region = x.region AND od.instance_type  = x.current_instance_type
      LEFT JOIN ec2_od_hourly od2 ON od2.region= x.region AND od2.instance_type = (x.family || '.' || x.target_size)
    )
    SELECT
      p.billing_period, p.account_id, p.business_area, p.resource_id, p.region,
      p.current_instance_type,
      (p.family || '.' || p.target_size) AS recommended_instance_type_ours,
      p.usage_quantity_hours, p.total_cost_usd, p.avg_cpu_14d,
      p.current_hourly, p.target_hourly,
      CASE
        WHEN p.current_hourly IS NOT NULL AND p.target_hourly IS NOT NULL
        THEN ROUND(p.usage_quantity_hours * (p.current_hourly - p.target_hourly), 2)
      END AS est_monthly_savings_usd,
      'OnDemand rightsizing: CPU<30% → next smaller' AS reason,
      CASE
        WHEN p.current_hourly IS NOT NULL AND p.target_hourly IS NOT NULL THEN 'High'
        ELSE 'Medium'
      END AS confidence
    FROM priced p
    ORDER BY est_monthly_savings_usd DESC NULLS LAST, p.total_cost_usd DESC;
    """)

    # D) Compare our rightsizing vs TA (if TA fields exist)
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ta_rightsize_comparison AS
    SELECT
      r.billing_period, r.account_id, r.business_area, r.resource_id, r.region,
      r.current_instance_type,
      r.recommended_instance_type_ours,
      u.recommended_instance_type AS ta_recommended_instance_type,
      u.ta_rightsize_savings_usd  AS ta_est_monthly_savings_usd,
      r.est_monthly_savings_usd   AS ours_est_monthly_savings_usd,
      r.avg_cpu_14d,
      CASE
        WHEN u.recommended_instance_type IS NULL THEN 'TA: none'
        WHEN lower(u.recommended_instance_type) = lower(r.recommended_instance_type_ours) THEN 'Agree'
        ELSE 'Different'
      END AS comparison
    FROM ec2_rightsize_candidates r
    LEFT JOIN ec2_ops_usage u USING (billing_period, account_id, business_area, resource_id, region, current_instance_type);
    """)

    # Unified actions, de-duped (priority: schedule > rightsize > spot)
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ops_actions_ranked AS
    WITH all_actions AS (
      SELECT 'schedule' AS action, 'ec2' AS service, resource_id, business_area, region,
             current_instance_type, total_cost_usd AS current_cost_usd,
             est_monthly_savings_usd, reason, confidence, 3 AS priority
      FROM ec2_schedule_candidates
      UNION ALL
      SELECT 'rightsize' AS action, 'ec2' AS service, resource_id, business_area, region,
             current_instance_type, total_cost_usd AS current_cost_usd,
             est_monthly_savings_usd, reason, confidence, 2 AS priority
      FROM ec2_rightsize_candidates
      UNION ALL
      SELECT 'spot' AS action, 'ec2' AS service, resource_id, business_area, region,
             current_instance_type, total_cost_usd AS current_cost_usd,
             est_monthly_savings_usd, reason, confidence, 1 AS priority
      FROM ec2_spot_candidates
    ),
    d AS (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY resource_id
               ORDER BY priority DESC, est_monthly_savings_usd DESC NULLS LAST
             ) AS rn
      FROM all_actions
    )
    SELECT action, service, resource_id, business_area, region, current_instance_type,
           current_cost_usd, est_monthly_savings_usd, reason, confidence
    FROM d
    WHERE rn = 1
    ORDER BY est_monthly_savings_usd DESC NULLS LAST, current_cost_usd DESC;
    """)

    # BA rollup (current spend + potential savings)
    con.execute("""
    CREATE OR REPLACE VIEW ec2_ops_ba_summary AS
    SELECT
      u.business_area,
      SUM(CASE WHEN u.purchase_option ILIKE 'ondemand' THEN u.total_cost_usd ELSE 0 END) AS ondemand_cost_usd,
      SUM(CASE WHEN u.purchase_option ILIKE 'spot' THEN u.total_cost_usd ELSE 0 END)     AS spot_cost_usd,
      SUM(COALESCE(a.est_monthly_savings_usd,0)) AS potential_savings_usd,
      ROUND(SUM(COALESCE(a.est_monthly_savings_usd,0)) / NULLIF(SUM(u.total_cost_usd),0) * 100, 2) AS potential_savings_pct
    FROM ec2_ops_usage u
    LEFT JOIN ec2_ops_actions_ranked a ON a.resource_id = u.resource_id
    GROUP BY u.business_area
    ORDER BY ondemand_cost_usd DESC;
    """)

# -------------------
# One-shot init
# -------------------
def initialize_after_load(con: duckdb.DuckDBPyConnection):
    refresh_ec2_sizes(con)
    create_views(con)