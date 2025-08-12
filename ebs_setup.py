"""
EBS one-month savings analysis — DuckDB tables + views

CSV expected (snake_case):
- billing_period (DATE), linked_account_id, business_area, resource_id (vol-xxxx)
- volume_type (gp2,gp3,io1,sc1,st1)
- volume_state ('available' | 'in use')
- days_since_last_attachment (INTEGER; 0 when 'in use')
- usage_storage_gb_mo (DOUBLE), usage_iops_mo (DOUBLE, optional), usage_throughput_gibps_mo (DOUBLE, optional)
- cost_mo_usd (DOUBLE)
"""

import os, re, duckdb, pandas as pd

def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [re.sub(r"\W+","_", c.strip()).lower() for c in df.columns]
    return df

def create_tables(con: duckdb.DuckDBPyConnection):
    con.execute("""
    CREATE TABLE IF NOT EXISTS ebs_volumes_usage (
      billing_period              DATE,
      linked_account_id           VARCHAR,
      business_area               VARCHAR,
      resource_id                 VARCHAR,
      volume_type                 VARCHAR,
      volume_state                VARCHAR,
      days_since_last_attachment  INTEGER,
      usage_storage_gb_mo         DOUBLE,
      usage_iops_mo               DOUBLE,
      usage_throughput_gibps_mo   DOUBLE,
      cost_mo_usd                 DOUBLE
    );
    """)

def load_ebs_csvs_from_folder(con: duckdb.DuckDBPyConnection, folder="data_ebs") -> int:
    if not os.path.isdir(folder):
        os.makedirs(folder, exist_ok=True); return 0
    files = [f for f in os.listdir(folder) if f.lower().endswith(".csv")]
    if not files: return 0
    frames = []
    for f in files:
        frames.append(_normalize_cols(pd.read_csv(os.path.join(folder, f))))
    df_all = pd.concat(frames, ignore_index=True)

    required = ["billing_period","linked_account_id","business_area","resource_id",
                "volume_type","volume_state","days_since_last_attachment",
                "usage_storage_gb_mo","usage_iops_mo","usage_throughput_gibps_mo","cost_mo_usd"]
    for c in required:
        if c not in df_all.columns: df_all[c] = None

    con.execute("DELETE FROM ebs_volumes_usage")
    con.register("ebs_df", df_all[required])
    con.execute("""
        INSERT INTO ebs_volumes_usage
        SELECT
          CAST(billing_period AS DATE), linked_account_id, business_area, resource_id, volume_type,
          volume_state, CAST(days_since_last_attachment AS INT),
          CAST(usage_storage_gb_mo AS DOUBLE),
          CAST(usage_iops_mo AS DOUBLE),
          CAST(usage_throughput_gibps_mo AS DOUBLE),
          CAST(cost_mo_usd AS DOUBLE)
        FROM ebs_df
    """)
    con.unregister("ebs_df")
    return len(df_all)

def create_views(con: duckdb.DuckDBPyConnection):
    # Observed $/GB for the month
    con.execute("""
    CREATE OR REPLACE VIEW ebs_price_per_gb AS
    SELECT volume_type,
           SUM(cost_mo_usd) / NULLIF(SUM(usage_storage_gb_mo),0) AS avg_cost_per_gb
    FROM ebs_volumes_usage
    GROUP BY volume_type;
    """)

    # Unattached ≥30d → delete
    con.execute("""
    CREATE OR REPLACE VIEW ebs_unattached_long_idle AS
    SELECT billing_period, business_area, resource_id, volume_type,
           days_since_last_attachment,
           usage_storage_gb_mo AS size_gb,
           cost_mo_usd AS current_monthly_cost_usd,
           'Delete unattached volume' AS action,
           CASE WHEN days_since_last_attachment >= 90 THEN 'High'
                WHEN days_since_last_attachment >= 30 THEN 'Medium'
                ELSE 'Low' END AS confidence
    FROM ebs_volumes_usage
    WHERE volume_state='available' AND days_since_last_attachment >= 30
    ORDER BY current_monthly_cost_usd DESC;
    """)

    # Unattached (<30d) → quarantine/recheck
    con.execute("""
    CREATE OR REPLACE VIEW ebs_unattached_recent AS
    SELECT billing_period, business_area, resource_id, volume_type,
           days_since_last_attachment,
           usage_storage_gb_mo AS size_gb,
           cost_mo_usd AS current_monthly_cost_usd,
           'Quarantine tag & delete after 30d' AS action, 'Low' AS confidence
    FROM ebs_volumes_usage
    WHERE volume_state='available' AND days_since_last_attachment < 30
    ORDER BY current_monthly_cost_usd DESC;
    """)

    # gp2 → gp3 (attached only)
    con.execute("""
    CREATE OR REPLACE VIEW ebs_gp2_to_gp3_opportunity AS
    WITH price AS (
      SELECT
        (SELECT avg_cost_per_gb FROM ebs_price_per_gb WHERE volume_type='gp2') AS p_gp2,
        (SELECT avg_cost_per_gb FROM ebs_price_per_gb WHERE volume_type='gp3') AS p_gp3
    ),
    cands AS (
      SELECT e.*,
             (SELECT p_gp2 FROM price) AS price_gp2,
             (SELECT p_gp3 FROM price) AS price_gp3
      FROM ebs_volumes_usage e
      WHERE e.volume_type='gp2' AND e.volume_state='in use'
    )
    SELECT billing_period, business_area, resource_id, volume_type,
           usage_storage_gb_mo AS size_gb, cost_mo_usd AS current_monthly_cost_usd,
           price_gp2, price_gp3,
           CASE WHEN price_gp2 IS NOT NULL AND price_gp3 IS NOT NULL
                THEN ROUND(usage_storage_gb_mo * (price_gp2 - price_gp3), 2) END AS est_monthly_savings_usd,
           'Migrate gp2 → gp3 (storage delta only)' AS action, 'Medium' AS confidence
    FROM cands
    WHERE price_gp3 IS NOT NULL
    ORDER BY est_monthly_savings_usd DESC NULLS LAST, size_gb DESC;
    """)

    # io1 with very low IOPS (attached) → review to gp3
    con.execute("""
    CREATE OR REPLACE VIEW ebs_io1_low_iops_review AS
    WITH price AS (
      SELECT
        (SELECT avg_cost_per_gb FROM ebs_price_per_gb WHERE volume_type='io1') AS p_io1,
        (SELECT avg_cost_per_gb FROM ebs_price_per_gb WHERE volume_type='gp3') AS p_gp3
    )
    SELECT e.billing_period, e.business_area, e.resource_id, e.volume_type,
           e.usage_storage_gb_mo AS size_gb, e.cost_mo_usd AS current_monthly_cost_usd,
           e.usage_iops_mo,
           (SELECT p_io1 FROM price) AS price_io1_gb,
           (SELECT p_gp3 FROM price) AS price_gp3_gb,
           CASE WHEN (SELECT p_io1 FROM price) IS NOT NULL AND (SELECT p_gp3 FROM price) IS NOT NULL
                THEN ROUND(e.usage_storage_gb_mo * ((SELECT p_io1 FROM price) - (SELECT p_gp3 FROM price)), 2) END AS est_monthly_savings_usd,
           'Consider io1 → gp3 if performance OK (very low IOPS observed)' AS action,
           'Low' AS confidence
    FROM ebs_volumes_usage e
    WHERE e.volume_type='io1' AND COALESCE(e.usage_iops_mo,0) <= 100 AND e.volume_state='in use'
    ORDER BY est_monthly_savings_usd DESC NULLS LAST, e.cost_mo_usd DESC;
    """)

    # Leadership rollups
    con.execute("""
    CREATE OR REPLACE VIEW ebs_cost_by_ba_attached_state AS
    SELECT business_area,
           SUM(cost_mo_usd) AS total_cost_usd,
           SUM(CASE WHEN volume_state='in use' THEN cost_mo_usd END) AS attached_cost_usd,
           SUM(CASE WHEN volume_state='available' THEN cost_mo_usd END) AS unattached_cost_usd,
           SUM(usage_storage_gb_mo) AS total_gb,
           SUM(CASE WHEN volume_state='available' THEN usage_storage_gb_mo END) AS unattached_gb,
           ROUND(COALESCE(SUM(CASE WHEN volume_state='available' THEN cost_mo_usd END),0)
                 / NULLIF(SUM(cost_mo_usd),0) * 100, 2) AS unattached_cost_pct
    FROM ebs_volumes_usage
    GROUP BY business_area
    ORDER BY total_cost_usd DESC;
    """)

    con.execute("""
    CREATE OR REPLACE VIEW ebs_attached_summary AS
    SELECT business_area, volume_type,
           COUNT(*) AS vol_count,
           SUM(usage_storage_gb_mo) AS total_gb,
           SUM(cost_mo_usd) AS total_cost_usd
    FROM ebs_volumes_usage
    WHERE volume_state='in use'
    GROUP BY business_area, volume_type
    ORDER BY total_cost_usd DESC;
    """)

    # Unified actions with priority & de-dup
    con.execute("""
    CREATE OR REPLACE VIEW ebs_actions_ranked AS
    WITH A AS (
      SELECT resource_id, business_area, volume_type, days_since_last_attachment,
             cost_mo_usd AS current_cost_usd, ROUND(cost_mo_usd,2) AS est_monthly_savings_usd,
             'delete' AS action, 'ebs' AS service, 'Unattached ≥30d' AS reason, confidence, 3 AS priority
      FROM ebs_unattached_long_idle
      UNION ALL
      SELECT resource_id, business_area, volume_type, NULL,
             current_monthly_cost_usd, est_monthly_savings_usd,
             'convert' AS action, 'ebs' AS service,
             'gp2 → gp3 (storage delta only)' AS reason, 'Medium' AS confidence, 2 AS priority
      FROM ebs_gp2_to_gp3_opportunity
      UNION ALL
      SELECT resource_id, business_area, volume_type, NULL,
             current_monthly_cost_usd, est_monthly_savings_usd,
             'review' AS action, 'ebs' AS service,
             'io1 with very low IOPS — consider gp3' AS reason, 'Low' AS confidence, 1 AS priority
      FROM ebs_io1_low_iops_review
    ),
    D AS (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY resource_id
               ORDER BY priority DESC, est_monthly_savings_usd DESC NULLS LAST
             ) AS rn
      FROM A
    )
    SELECT action, service, resource_id, business_area, volume_type,
           days_since_last_attachment, current_cost_usd, est_monthly_savings_usd,
           reason, confidence
    FROM D
    WHERE rn = 1
    ORDER BY est_monthly_savings_usd DESC NULLS LAST, current_cost_usd DESC;
    """)

def initialize(con: duckdb.DuckDBPyConnection):
    create_tables(con)
    create_views(con)