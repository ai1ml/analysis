"""
Snapshots (EBS / RDS) monthly analysis — DuckDB tables + views.

Expected CSV columns (normalize to snake_case before insert):
- billing_period (DATE like 2021-01-01)
- linked_account_id (TEXT)
- business_area (TEXT)
- resource_id (TEXT ARN)
- snapshot_type (TEXT: 'Snapshot' | 'Snapshot_Archive')
- usage_quantity_gb (DOUBLE)   -- GB-month billed
- public_cost_usd (DOUBLE)
"""

import os, re, duckdb, pandas as pd

def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [re.sub(r"\W+","_", c.strip()).lower() for c in df.columns]
    return df

def create_tables(con: duckdb.DuckDBPyConnection):
    con.execute("""
    CREATE TABLE IF NOT EXISTS snapshots_usage (
      billing_period      DATE,
      linked_account_id   VARCHAR,
      business_area       VARCHAR,
      resource_id         VARCHAR,
      snapshot_type       VARCHAR,
      usage_quantity_gb   DOUBLE,
      public_cost_usd     DOUBLE
    );
    """)

def load_snapshots_csvs_from_folder(con: duckdb.DuckDBPyConnection, folder="data_snapshots") -> int:
    if not os.path.isdir(folder):
        os.makedirs(folder, exist_ok=True)
        return 0
    files = [f for f in os.listdir(folder) if f.lower().endswith(".csv")]
    if not files:
        return 0
    frames = []
    for f in files:
        df = pd.read_csv(os.path.join(folder, f))
        frames.append(_normalize_cols(df))
    if not frames:
        return 0
    df_all = pd.concat(frames, ignore_index=True)

    # Make sure expected columns exist
    required = ["billing_period","linked_account_id","business_area","resource_id",
                "snapshot_type","usage_quantity_gb","public_cost_usd"]
    for col in required:
        if col not in df_all.columns:
            df_all[col] = None

    con.execute("DELETE FROM snapshots_usage")
    con.register("snap_df", df_all[required])
    con.execute("""
    INSERT INTO snapshots_usage
    SELECT
      CAST(billing_period AS DATE),
      linked_account_id, business_area, resource_id, snapshot_type,
      CAST(usage_quantity_gb AS DOUBLE), CAST(public_cost_usd AS DOUBLE)
    FROM snap_df
    """)
    con.unregister("snap_df")
    return len(df_all)

def create_views(con: duckdb.DuckDBPyConnection):
    # Parsed view (region, snapshot_id, cost_per_gb)
    con.execute("""
    CREATE OR REPLACE VIEW snapshots_parsed AS
    SELECT
      s.*,
      regexp_extract(resource_id, '^arn:[^:]+:[^:]+:([a-z0-9-]+):', 1)  AS region,
      regexp_extract(resource_id, '([^/]+)$', 1)                        AS snapshot_id,
      CASE WHEN usage_quantity_gb > 0 THEN public_cost_usd / usage_quantity_gb END AS cost_per_gb
    FROM snapshots_usage s;
    """)

    # Observed price/GB by region+type
    con.execute("""
    CREATE OR REPLACE VIEW snap_price_per_gb AS
    SELECT
      region,
      snapshot_type,
      SUM(public_cost_usd) / NULLIF(SUM(usage_quantity_gb),0) AS avg_cost_per_gb
    FROM snapshots_parsed
    GROUP BY region, snapshot_type;
    """)

    # BA/Region hotspots
    con.execute("""
    CREATE OR REPLACE VIEW snapshots_by_ba_region AS
    SELECT
      business_area,
      region,
      snapshot_type,
      COUNT(DISTINCT snapshot_id)                AS snapshot_count,
      SUM(usage_quantity_gb)                     AS total_gb,
      SUM(public_cost_usd)                       AS total_cost_usd,
      SUM(public_cost_usd) / NULLIF(SUM(usage_quantity_gb),0) AS blended_cost_per_gb
    FROM snapshots_parsed
    GROUP BY business_area, region, snapshot_type
    ORDER BY total_cost_usd DESC;
    """)

    # Archive opportunity (Standard -> Archive)
    con.execute("""
    CREATE OR REPLACE VIEW snapshots_archive_opportunity AS
    WITH price AS (
      SELECT
        p1.region,
        p1.avg_cost_per_gb AS price_snapshot_gb,
        p2.avg_cost_per_gb AS price_archive_gb
      FROM snap_price_per_gb p1
      LEFT JOIN snap_price_per_gb p2
        ON p1.region = p2.region AND p2.snapshot_type = 'Snapshot_Archive'
      WHERE p1.snapshot_type = 'Snapshot'
    ),
    std AS (
      SELECT business_area, region, snapshot_id,
             SUM(usage_quantity_gb) AS gb_standard,
             SUM(public_cost_usd)   AS cost_standard
      FROM snapshots_parsed
      WHERE snapshot_type = 'Snapshot'
      GROUP BY business_area, region, snapshot_id
    )
    SELECT
      s.business_area,
      s.region,
      s.snapshot_id,
      s.gb_standard,
      s.cost_standard,
      pr.price_snapshot_gb,
      pr.price_archive_gb,
      CASE
        WHEN pr.price_archive_gb IS NOT NULL AND pr.price_snapshot_gb IS NOT NULL
        THEN ROUND(s.gb_standard * (pr.price_snapshot_gb - pr.price_archive_gb), 2)
      END AS est_monthly_savings_usd
    FROM std s
    LEFT JOIN price pr USING (region)
    WHERE pr.price_archive_gb IS NOT NULL
      AND s.gb_standard >= 100
    ORDER BY est_monthly_savings_usd DESC NULLS LAST, s.gb_standard DESC;
    """)

    # Sprawl top (largest single snapshots by $)
    con.execute("""
    CREATE OR REPLACE VIEW snapshots_sprawl_top AS
    SELECT
      business_area, region, snapshot_id, snapshot_type,
      usage_quantity_gb, public_cost_usd, cost_per_gb
    FROM snapshots_parsed
    ORDER BY public_cost_usd DESC
    LIMIT 500;
    """)

    # Sprawl clusters (BA/region combos with high counts)
    con.execute("""
    CREATE OR REPLACE VIEW snapshots_sprawl_clusters AS
    WITH counts AS (
      SELECT
        billing_period, business_area, region,
        COUNT(DISTINCT snapshot_id) AS snapshot_count,
        SUM(public_cost_usd)        AS total_cost_usd,
        SUM(usage_quantity_gb)      AS total_gb
      FROM snapshots_parsed
      GROUP BY billing_period, business_area, region
    ),
    pct AS (
      SELECT
        c.*,
        PERCENTILE_CONT(c.snapshot_count, 0.90) OVER () AS p90_count
      FROM counts c
    )
    SELECT *
    FROM pct
    WHERE snapshot_count >= p90_count
    ORDER BY snapshot_count DESC, total_cost_usd DESC;
    """)

    # BA rollup
    con.execute("""
    CREATE OR REPLACE VIEW snapshots_by_ba AS
    SELECT
      business_area,
      SUM(public_cost_usd)   AS total_cost_usd,
      SUM(usage_quantity_gb) AS total_gb,
      AVG(cost_per_gb)       AS avg_cost_per_gb,
      SUM(CASE WHEN snapshot_type='Snapshot' THEN public_cost_usd ELSE 0 END) AS cost_standard_usd,
      SUM(CASE WHEN snapshot_type='Snapshot_Archive' THEN public_cost_usd ELSE 0 END) AS cost_archive_usd
    FROM snapshots_parsed
    GROUP BY business_area
    ORDER BY total_cost_usd DESC;
    """)

def initialize(con: duckdb.DuckDBPyConnection):
    create_tables(con)
    create_views(con)