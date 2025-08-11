# Minimal demo: analyze RDS CSVs on GCS with DuckDB
# - Lists CSVs in a GCS prefix
# - Loads into DuckDB
# - Creates views for underutilized, rightsizing, and off-hours
# - Prints top results

import io, os, re
import duckdb
import pandas as pd
from google.cloud import storage

# >>>>>> EDIT THESE <<<<<<
BUCKET = "your-bucket-name"
PREFIX = "aws-cost/rds/"        # folder where your CSVs live (with header row)
CPU_THRESHOLD = 10.0            # underutilized if avg_cpu_14d < this
MIN_CONSISTENT_DAYS = 14        # require at least this many days of CPU metric

# ---------------- Helpers ----------------
def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [re.sub(r"\W+", "_", c.strip().lower()) for c in df.columns]
    return df

def list_csvs(bucket: str, prefix: str):
    gcs = storage.Client()
    return [b for b in gcs.list_blobs(bucket, prefix=prefix) if b.name.endswith(".csv")]

# ---------------- Load data ----------------
def load_to_duckdb(con: duckdb.DuckDBPyConnection):
    blobs = list_csvs(BUCKET, PREFIX)
    if not blobs:
        raise SystemExit(f"No CSVs found under gs://{BUCKET}/{PREFIX}")

    # Split helper files if you keep them as CSVs in the same folder
    usage_blobs = [b for b in blobs if "price_rds" not in b.name and "rds_sizes" not in b.name]
    price_blob  = next((b for b in blobs if "price_rds" in b.name), None)
    sizes_blob  = next((b for b in blobs if "rds_sizes" in b.name), None)

    # Load RDS usage CSVs -> single DataFrame
    frames = []
    for b in usage_blobs:
        byts = b.download_as_bytes()
        df = pd.read_csv(io.BytesIO(byts))
        df = normalize_cols(df)
        frames.append(df)
    if not frames:
        raise SystemExit("Found CSVs, but none looked like RDS usage.")
    rds = pd.concat(frames, ignore_index=True)

    # Create main table with light type coercion
    con.execute("DROP TABLE IF EXISTS rds_usage")
    con.register("rds_df", rds)
    con.execute(f"""
        CREATE TABLE rds_usage AS
        SELECT
          CAST(billing_period AS DATE)                  AS billing_period,
          CAST(account_id AS VARCHAR)                   AS account_id,
          CAST(account_name AS VARCHAR)                 AS account_name,
          CAST(db_id AS VARCHAR)                        AS db_id,
          CAST(region AS VARCHAR)                       AS region,
          CAST(instance_class AS VARCHAR)               AS instance_class,
          CAST(hours AS DOUBLE)                         AS hours,
          CAST(cost_usd AS DOUBLE)                      AS cost_usd,
          CAST(NULLIF(avg_cpu_14d,'') AS DOUBLE)        AS avg_cpu_14d,
          CAST(NULLIF(consistent_days,'') AS INTEGER)   AS consistent_days
        FROM rds_df
    """)
    con.unregister("rds_df")

    # Price table (load if provided; else seed a few rows so demo runs)
    con.execute("DROP TABLE IF EXISTS price_rds")
    if price_blob:
        dfp = pd.read_csv(io.BytesIO(price_blob.download_as_bytes()))
        dfp = normalize_cols(dfp)
        con.register("pr_df", dfp)
        con.execute("CREATE TABLE price_rds AS SELECT * FROM pr_df")
        con.unregister("pr_df")
    else:
        con.execute("""
            CREATE TABLE price_rds(instance_class VARCHAR, region VARCHAR, hourly_usd DOUBLE);
            INSERT INTO price_rds VALUES
              ('db.r5.large','us-east-1',0.24),
              ('db.r5.xlarge','us-east-1',0.48),
              ('db.r5.2xlarge','us-east-1',0.96);
        """)

    # Size ordering table (same: load or seed)
    con.execute("DROP TABLE IF EXISTS rds_sizes")
    if sizes_blob:
        dfs = pd.read_csv(io.BytesIO(sizes_blob.download_as_bytes()))
        dfs = normalize_cols(dfs)
        con.register("sz_df", dfs)
        con.execute("CREATE TABLE rds_sizes AS SELECT * FROM sz_df")
        con.unregister("sz_df")
    else:
        con.execute("""
            CREATE TABLE rds_sizes(family VARCHAR, size VARCHAR, size_rank INT);
            INSERT INTO rds_sizes VALUES
              ('db.r5','large',1),
              ('db.r5','xlarge',2),
              ('db.r5','2xlarge',3),
              ('db.r5','4xlarge',4),
              ('db.r5','8xlarge',5);
        """)

# ---------------- Views ----------------
def create_views(con: duckdb.DuckDBPyConnection):
    con.execute(f"""
    CREATE OR REPLACE VIEW rds_underutilized AS
    SELECT billing_period, account_id, account_name, db_id, region, instance_class,
           hours, cost_usd, avg_cpu_14d
    FROM rds_usage
    WHERE avg_cpu_14d < {CPU_THRESHOLD}
      AND COALESCE(consistent_days,0) >= {MIN_CONSISTENT_DAYS}
    ORDER BY cost_usd DESC
    """)

    con.execute("""
    CREATE OR REPLACE VIEW rds_rightsize_next_smaller AS
    WITH parsed AS (
      SELECT ru.*,
             regexp_extract(instance_class,'^(db\\.[^\\.]+)',1) AS family,
             regexp_extract(instance_class,'\\.([^.]+)$',1)     AS size
      FROM rds_usage ru
    ),
    ranked AS (
      SELECT p.*, s.size_rank
      FROM parsed p JOIN rds_sizes s ON p.family=s.family AND p.size=s.size
    ),
    candidates AS (
      SELECT *, (size_rank-1) AS target_rank
      FROM ranked
      WHERE avg_cpu_14d < {thr} AND COALESCE(consistent_days,0) >= {days} AND size_rank > 1
    ),
    target AS (
      SELECT c.*, s2.size AS target_size
      FROM candidates c JOIN rds_sizes s2 ON c.family=s2.family AND c.target_rank=s2.size_rank
    ),
    priced AS (
      SELECT t.*,
             prc.hourly_usd AS current_hourly,
             prt.hourly_usd AS target_hourly
      FROM target t
      JOIN price_rds prc ON prc.instance_class=t.instance_class AND prc.region=t.region
      JOIN price_rds prt ON prt.instance_class=(t.family||'.'||t.target_size) AND prt.region=t.region
    )
    SELECT billing_period, account_id, account_name, db_id, region,
           instance_class AS current_class,
           (family||'.'||target_size) AS recommended_class,
           hours, cost_usd AS current_cost_usd,
           current_hourly, target_hourly,
           round(hours*(current_hourly-target_hourly),2) AS est_monthly_savings_usd,
           avg_cpu_14d
    FROM priced
    ORDER BY est_monthly_savings_usd DESC
    """.format(thr=CPU_THRESHOLD, days=MIN_CONSISTENT_DAYS))

    con.execute("""
    CREATE OR REPLACE VIEW rds_offhours_candidates AS
    WITH base AS (
      SELECT *, lower(coalesce(account_name,'')) acct, lower(coalesce(db_id,'')) name_like
      FROM rds_usage
    ),
    nonprod AS (
      SELECT * FROM base
      WHERE acct LIKE '%dev%' OR acct LIKE '%test%'
         OR name_like LIKE '%dev%' OR name_like LIKE '%test%' OR name_like LIKE '%staging%'
    )
    SELECT billing_period, account_id, account_name, db_id, region, instance_class,
           hours AS current_hours, cost_usd AS current_cost_usd,
           round(cost_usd*0.65,2) AS est_monthly_savings_usd,
           'Assumes 24x7→5x12 schedule' AS assumption
    FROM nonprod
    ORDER BY est_monthly_savings_usd DESC
    """)

# ---------------- Main ----------------
def main():
    # Use an in-memory DuckDB; change to 'rds.duckdb' to persist to a file.
    con = duckdb.connect(":memory:")

    print(f"Loading CSVs from gs://{BUCKET}/{PREFIX} …")
    load_to_duckdb(con)
    create_views(con)

    print("\nTop Underutilized (CPU < {:.0f}%, ≥ {} days):".format(CPU_THRESHOLD, MIN_CONSISTENT_DAYS))
    print(con.execute("SELECT * FROM rds_underutilized LIMIT 10").fetchdf())

    print("\nTop Rightsize Candidates (next smaller class):")
    print(con.execute("SELECT * FROM rds_rightsize_next_smaller LIMIT 10").fetchdf())

    print("\nNon-prod Off-hours Candidates (assume 24x7 → 5x12):")
    print(con.execute("SELECT * FROM rds_offhours_candidates LIMIT 10").fetchdf())

if __name__ == "__main__":
    # Make sure your local account is authenticated:
    #   gcloud auth application-default login
    # And BUCKET/PREFIX above are set correctly.
    main()