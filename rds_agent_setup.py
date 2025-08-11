"""
RDS cost agent setup (DuckDB): tables, helpers, and optimized views.

What you do:
1) Load your RDS CSV(s) into the rds_usage table (headers snake_case; includes BA).
2) Run this file to create/refresh helper tables and views.
3) Query rds_actions_ranked for prioritized, deduped, confidence-scored actions.

MVP note: Ignores NumberDaysOfConsistentData; uses avg_cpu_14d if present.
"""

import re, json
import duckdb, pandas as pd

# =========================
# 0) Connect DuckDB (edit)
# =========================
con = duckdb.connect(":memory:")  # use 'rds.duckdb' to persist

# ======================
# 1) Tables (create)
# ======================
con.execute("""
CREATE TABLE IF NOT EXISTS rds_usage (
  billing_period   DATE,
  account_id       VARCHAR,
  account_name     VARCHAR,
  BA               VARCHAR,      -- business unit
  db_id            VARCHAR,
  region           VARCHAR,      -- e.g., us-east-1
  instance_class   VARCHAR,      -- e.g., db.r5.xlarge
  hours            DOUBLE,
  cost_usd         DOUBLE,
  avg_cpu_14d      DOUBLE,       -- 0..100 (nullable)
  consistent_days  INTEGER
);

CREATE TABLE IF NOT EXISTS price_rds (
  instance_class VARCHAR,
  region         VARCHAR,
  deployment     VARCHAR,        -- 'Single-AZ' / 'Multi-AZ' (optional)
  engine         VARCHAR,        -- e.g., 'Any' (optional)
  hourly_usd     DOUBLE,
  price_date     DATE
);

CREATE TABLE IF NOT EXISTS rds_sizes (         -- generated from usage
  family    VARCHAR,                            -- e.g., db.r5
  size      VARCHAR,                            -- e.g., xlarge, 2xlarge
  size_rank INTEGER                             -- 1 = smallest
);

CREATE TABLE IF NOT EXISTS rds_class_specs (    -- optional (fill later if needed)
  instance_class  VARCHAR,
  vcpu            INTEGER,
  ram_gib         DOUBLE,
  max_connections INTEGER
);

-- Patterns to exclude from actions (empty by default)
CREATE TABLE IF NOT EXISTS rds_exclusions (pattern VARCHAR);
""")

# =====================================================
# (Load your CSVs into rds_usage BEFORE initializing.)
# Example:
#   df = pd.read_csv("rds_2025-07.csv")
#   con.register("rds_df", df)
#   con.execute("INSERT INTO rds_usage SELECT * FROM rds_df")
#   con.unregister("rds_df")
# =====================================================

# ===========================
# 2) Helper: build rds_sizes
# ===========================
_SIZE_ORDER = {
    "nano":1, "micro":2, "small":3, "medium":4, "large":5, "xlarge":6,
    "2xlarge":7, "3xlarge":8, "4xlarge":9, "6xlarge":10, "8xlarge":11,
    "9xlarge":12, "10xlarge":13, "12xlarge":14, "16xlarge":15, "18xlarge":16,
    "24xlarge":17, "32xlarge":18
}

def refresh_rds_sizes_from_usage(con: duckdb.DuckDBPyConnection):
    df = con.execute("""
        SELECT DISTINCT instance_class
        FROM rds_usage
        WHERE instance_class IS NOT NULL
    """).fetchdf()
    rows = []
    for ic in df["instance_class"].dropna().unique():
        m = re.match(r"^(db\.[^.]+)\.(.+)$", ic)
        if not m:
            continue
        fam, size = m.group(1), m.group(2)
        rank = _SIZE_ORDER.get(size.lower())
        if rank is not None:
            rows.append((fam, size, rank))
    if not rows:
        return
    tmp = pd.DataFrame(rows, columns=["family","size","size_rank"]).drop_duplicates()
    tmp = tmp.sort_values(["family","size_rank"])
    # Normalize ranks per family (dense): next smaller = rank-1
    tmp["size_rank"] = tmp.groupby("family")["size_rank"].rank(method="dense").astype(int)
    con.execute("DELETE FROM rds_sizes")
    con.register("sz_df", tmp)
    con.execute("INSERT INTO rds_sizes SELECT * FROM sz_df")
    con.unregister("sz_df")

# =========================================
# 3) Pricing API helpers (AWS creds needed)
# =========================================
def fetch_rds_price(instance_class: str, region_code: str, deployment="Single-AZ", engine="Any"):
    import boto3  # local import to keep file import-safe without boto3 preinstalled
    pricing = boto3.client("pricing", region_name="us-east-1")
    filters = [
        {"Type":"TERM_MATCH","Field":"servicecode","Value":"AmazonRDS"},
        {"Type":"TERM_MATCH","Field":"instanceType","Value":instance_class},
        {"Type":"TERM_MATCH","Field":"regionCode","Value":region_code},
        {"Type":"TERM_MATCH","Field":"deploymentOption","Value":deployment},
        {"Type":"TERM_MATCH","Field":"tenancy","Value":"Shared"},
        {"Type":"TERM_MATCH","Field":"termType","Value":"OnDemand"},
    ]
    resp = pricing.get_products(ServiceCode="AmazonRDS", Filters=filters, MaxResults=100)
    if not resp.get("PriceList"):
        return None
    for item in resp["PriceList"]:
        data = json.loads(item)
        for term in data.get("terms", {}).get("OnDemand", {}).values():
            for pd_ in term.get("priceDimensions", {}).values():
                if pd_.get("unit") == "Hrs":
                    usd = pd_.get("pricePerUnit", {}).get("USD")
                    if usd:
                        try:
                            return float(usd)
                        except:
                            pass
    return None

def refresh_price_rds_from_usage(con: duckdb.DuckDBPyConnection, deployment="Single-AZ", engine="Any"):
    need = con.execute("""
        SELECT DISTINCT instance_class, region
        FROM rds_usage
        WHERE instance_class IS NOT NULL AND region IS NOT NULL
    """).fetchdf()
    have = con.execute("SELECT instance_class, region FROM price_rds").fetchdf()
    have_set = set(map(tuple, have.to_records(index=False))) if not have.empty else set()
    rows = []
    for _, r in need.iterrows():
        key = (r["instance_class"], r["region"])
        if key in have_set:
            continue
        usd = fetch_rds_price(r["instance_class"], r["region"], deployment=deployment, engine=engine)
        if usd is not None:
            rows.append((r["instance_class"], r["region"], deployment, engine, usd))
    if rows:
        df_new = pd.DataFrame(rows, columns=["instance_class","region","deployment","engine","hourly_usd"])
        con.register("pr_new", df_new)
        con.execute("""
            INSERT INTO price_rds(instance_class, region, deployment, engine, hourly_usd)
            SELECT instance_class, region, deployment, engine, hourly_usd FROM pr_new
        """)
        con.unregister("pr_new")

# =========================
# 4) Views (create/refresh)
# =========================
def create_views(con: duckdb.DuckDBPyConnection):
    # Underutilized (ignore consistent_days for MVP)
    con.execute("""
    CREATE OR REPLACE VIEW rds_underutilized AS
    SELECT
      billing_period, account_id, account_name, BA, db_id, region, instance_class,
      hours, cost_usd, avg_cpu_14d
    FROM rds_usage
    WHERE avg_cpu_14d IS NOT NULL
      AND avg_cpu_14d < 10
    ORDER BY cost_usd DESC;
    """)

    # Rightsize (next smaller) with price snapshot & sanity cap
    con.execute("""
    CREATE OR REPLACE VIEW rds_rightsize_next_smaller AS
    WITH parsed AS (
      SELECT
        ru.*,
        regexp_extract(instance_class,'^(db\\.[^\\.]+)',1) AS family,
        regexp_extract(instance_class,'\\.([^.]+)$',1)     AS size
      FROM rds_usage ru
      WHERE avg_cpu_14d IS NOT NULL AND avg_cpu_14d < 10
    ),
    ranked AS (
      SELECT p.*, s.size_rank
      FROM parsed p
      JOIN rds_sizes s ON p.family = s.family AND p.size = s.size
    ),
    eligible AS (              -- smaller size must exist
      SELECT * FROM ranked WHERE size_rank > 1
    ),
    target AS (
      SELECT e.*, (e.size_rank - 1) AS target_rank
      FROM eligible e
    ),
    t_size AS (
      SELECT t.*, s2.size AS target_size
      FROM target t
      JOIN rds_sizes s2 ON t.family = s2.family AND t.target_rank = s2.size_rank
    ),
    priced AS (
      SELECT
        x.*,
        prc.hourly_usd AS current_hourly, prc.price_date AS current_price_date,
        prt.hourly_usd AS target_hourly,  prt.price_date AS target_price_date
      FROM t_size x
      JOIN price_rds prc ON prc.instance_class = x.instance_class AND prc.region = x.region
      JOIN price_rds prt ON prt.instance_class = (x.family || '.' || x.target_size) AND prt.region = x.region
    )
    SELECT
      billing_period, account_id, account_name, BA, db_id, region,
      instance_class AS current_class,
      (family || '.' || target_size) AS recommended_class,
      hours,
      cost_usd AS current_cost_usd,
      current_hourly, target_hourly,
      COALESCE(current_price_date, target_price_date) AS price_date,
      ROUND(hours * (current_hourly - target_hourly), 2) AS est_monthly_savings_usd,
      avg_cpu_14d
    FROM priced
    ORDER BY est_monthly_savings_usd DESC;
    """)

    # Off-hours (non-prod by name), CPU not a gate
    con.execute("""
    CREATE OR REPLACE VIEW rds_offhours_candidates AS
    WITH base AS (
      SELECT *,
             lower(coalesce(account_name,'')) AS acct,
             lower(coalesce(db_id,''))        AS name_like
      FROM rds_usage
    ),
    nonprod AS (
      SELECT *
      FROM base
      WHERE acct LIKE '%dev%' OR acct LIKE '%test%' OR acct LIKE '%staging%'
         OR name_like LIKE '%dev%' OR name_like LIKE '%test%' OR name_like LIKE '%staging%'
    ),
    month_len AS (
      SELECT billing_period,
             (julianday(date_trunc('month', billing_period, 'start') + INTERVAL 1 MONTH)
              - julianday(date_trunc('month', billing_period, 'start')))::INT AS days_in_month
      FROM (SELECT DISTINCT billing_period FROM nonprod)
    )
    SELECT
      n.billing_period, n.account_id, n.account_name, n.BA, n.db_id, n.region, n.instance_class,
      n.hours AS current_hours,
      n.cost_usd AS current_cost_usd,
      CASE WHEN n.hours >= 0.98 * (m.days_in_month * 24) THEN 1 ELSE 0 END AS approx_247,
      ROUND(n.cost_usd * 0.65, 2) AS est_monthly_savings_usd,
      'Assume 5x12 schedule (~65% savings)' AS assumption,
      n.avg_cpu_14d
    FROM nonprod n
    JOIN month_len m USING (billing_period)
    WHERE n.hours IS NOT NULL
    ORDER BY est_monthly_savings_usd DESC;
    """)

    # High utilization (performance risk)
    con.execute("""
    CREATE OR REPLACE VIEW rds_high_utilization AS
    WITH month_len AS (
      SELECT billing_period,
             (julianday(date_trunc('month', billing_period, 'start') + INTERVAL 1 MONTH)
              - julianday(date_trunc('month', billing_period, 'start')))::INT AS days_in_month
      FROM (SELECT DISTINCT billing_period FROM rds_usage)
    )
    SELECT
      u.billing_period, u.account_id, u.account_name, u.BA, u.db_id, u.region, u.instance_class,
      u.hours, u.cost_usd, u.avg_cpu_14d,
      'High CPU; consider upsizing' AS recommendation
    FROM rds_usage u
    JOIN month_len m USING (billing_period)
    WHERE u.avg_cpu_14d IS NOT NULL
      AND u.avg_cpu_14d >= 90
      AND u.hours >= 0.98 * (m.days_in_month * 24)
    ORDER BY u.avg_cpu_14d DESC, u.cost_usd DESC;
    """)

    # ---------- Optimized unified actions ----------
    con.execute("""
    CREATE OR REPLACE VIEW rds_actions_ranked AS
    WITH excl AS (
      SELECT LOWER(pattern) AS pat FROM rds_exclusions
    ),
    month_len AS (
      SELECT billing_period,
             (julianday(date_trunc('month', billing_period, 'start') + INTERVAL 1 MONTH)
              - julianday(date_trunc('month', billing_period, 'start')))::INT AS days_in_month
      FROM (SELECT DISTINCT billing_period FROM rds_usage)
    ),
    flags AS (
      SELECT
        u.*,
        ml.days_in_month,
        CASE WHEN u.hours >= 0.98 * (ml.days_in_month * 24) THEN 1 ELSE 0 END AS approx_247,
        LOWER(COALESCE(u.account_name,'')) AS acct_l,
        LOWER(COALESCE(u.db_id,''))        AS name_l
      FROM rds_usage u
      JOIN month_len ml USING (billing_period)
    ),
    not_excluded AS (
      SELECT f.*
      FROM flags f
      LEFT JOIN excl e1 ON f.acct_l LIKE '%' || e1.pat || '%'
      LEFT JOIN excl e2 ON f.name_l LIKE '%' || e2.pat || '%'
      WHERE e1.pat IS NULL AND e2.pat IS NULL
    ),
    -- A) kill/merge: CPU < 5%
    kill_merge AS (
      SELECT
        'kill/merge' AS action, 'rds' AS service, db_id AS resource_id, account_name, BA, region,
        instance_class AS current_config,
        cost_usd AS current_cost_usd,
        ROUND(cost_usd, 2) AS est_monthly_savings_usd,
        'CPU<5%, 24x7=' || approx_247 || ', hours=' || CAST(hours AS VARCHAR) AS reason,
        'Assumes decommission; savings ≈ current monthly cost' AS assumptions,
        CASE
          WHEN avg_cpu_14d IS NOT NULL AND avg_cpu_14d < 5 THEN 'High'
          ELSE 'Medium'
        END AS confidence,
        3 AS priority,
        billing_period
      FROM not_excluded
      WHERE avg_cpu_14d IS NOT NULL AND avg_cpu_14d < 5
    ),
    -- B) downsize: 5–10% CPU using next-smaller pricing
    downsize AS (
      SELECT
        'downsize' AS action, 'rds' AS service, r.db_id AS resource_id, r.account_name, r.BA, r.region,
        r.current_class AS current_config,
        r.current_cost_usd AS current_cost_usd,
        r.est_monthly_savings_usd,
        'CPU5–10%, next size down; avg_cpu_14d=' || CAST(r.avg_cpu_14d AS VARCHAR) AS reason,
        'On-demand price delta × hours; price_date=' || COALESCE(CAST(r.price_date AS VARCHAR), 'unknown') AS assumptions,
        CASE
          WHEN r.current_hourly IS NOT NULL AND r.target_hourly IS NOT NULL THEN 'High'
          ELSE 'Medium'
        END AS confidence,
        2 AS priority,
        r.billing_period
      FROM rds_rightsize_next_smaller r
      WHERE r.avg_cpu_14d >= 5 AND r.avg_cpu_14d < 10
    ),
    -- C) offhours: non-prod & 24x7 (CPU ignored)
    offhours AS (
      WITH nonprod AS (
        SELECT *
        FROM not_excluded
        WHERE acct_l LIKE '%dev%' OR acct_l LIKE '%test%' OR acct_l LIKE '%staging%'
           OR name_l LIKE '%dev%' OR name_l LIKE '%test%' OR name_l LIKE '%staging%'
      )
      SELECT
        'offhours' AS action, 'rds' AS service, n.db_id AS resource_id, n.account_name, n.BA, n.region,
        n.instance_class AS current_config,
        n.cost_usd AS current_cost_usd,
        ROUND(n.cost_usd * 0.65, 2) AS est_monthly_savings_usd,
        'Non-prod & 24x7; hours=' || CAST(n.hours AS VARCHAR) AS reason,
        'Assume 5x12 schedule (~65% savings)' AS assumptions,
        CASE WHEN n.approx_247=1 THEN 'High' ELSE 'Medium' END AS confidence,
        1 AS priority,
        n.billing_period
      FROM nonprod n
      WHERE n.approx_247 = 1
    ),
    all_actions AS (
      SELECT * FROM kill_merge
      UNION ALL
      SELECT * FROM downsize
      UNION ALL
      SELECT * FROM offhours
    ),
    -- Economic threshold: >= $25
    filtered AS (
      SELECT *
      FROM all_actions
      WHERE COALESCE(est_monthly_savings_usd, 0) >= 25
    ),
    -- De-duplicate by (billing_period, resource): keep highest priority, then largest savings
    deduped AS (
      SELECT *
      FROM (
        SELECT
          f.*,
          ROW_NUMBER() OVER (
            PARTITION BY billing_period, resource_id
            ORDER BY priority DESC, est_monthly_savings_usd DESC NULLS LAST
          ) AS rn
        FROM filtered f
      )
      WHERE rn = 1
    )
    SELECT
      action, service, resource_id, account_name, BA, region, current_config,
      current_cost_usd, est_monthly_savings_usd, reason, assumptions, confidence
    FROM deduped
    ORDER BY est_monthly_savings_usd DESC NULLS LAST, current_cost_usd DESC;
    """)

# ========================
# 5) Initialize everything
# ========================
def initialize_after_loading_usage(con: duckdb.DuckDBPyConnection):
    # Build size map (no AWS required)
    refresh_rds_sizes_from_usage(con)
    # Optional: fetch prices for seen pairs (requires AWS creds)
    try:
      refresh_price_rds_from_usage(con, deployment="Single-AZ", engine="Any")
    except Exception as e:
      print(f"[warn] price_rds refresh skipped: {e}")
    # Create views
    create_views(con)

# =========================
# 6) Quick sanity previews
# =========================
def sanity_checks(con: duckdb.DuckDBPyConnection):
    print(con.execute("SELECT COUNT(*) AS rows FROM rds_usage").fetchdf())
    print(con.execute("SELECT * FROM rds_actions_ranked LIMIT 10").fetchdf())
    print(con.execute("""
        SELECT BA, action, SUM(COALESCE(est_monthly_savings_usd,0)) AS total_savings
        FROM rds_actions_ranked
        GROUP BY 1,2
        ORDER BY total_savings DESC
    """).fetchdf())

# -------------
# Entry point
# -------------
if __name__ == "__main__":
    # 1) Load your rds_usage FIRST (see note above).
    # 2) Then:
    initialize_after_loading_usage(con)
    sanity_checks(con)