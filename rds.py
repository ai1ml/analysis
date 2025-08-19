# rds_agent_setup.py
# End-to-end RDS analysis (single-month CSV) with dynamic AWS pricing, env detection,
# size ladder, off-hours, and priced rightsizing actions.

from __future__ import annotations
import re, json
from typing import List, Dict, Set, Tuple, Optional

import pandas as pd
import boto3
from botocore.config import Config


# =========================================================
# 0) PRICING CLIENT (Pricing is in us-east-1)
# =========================================================
def _pricing_client():
    return boto3.client(
        "pricing",
        region_name="us-east-1",
        config=Config(retries={"max_attempts": 8, "mode": "standard"})
    )


# =========================================================
# 1) CORE NORMALIZATION (from your single-month CSV)
# =========================================================
def create_rds_core_views(con, source_table: str = "rds_raw") -> None:
    """
    Expected columns in source_table (from your screenshot):
      account_id, business_area, resource_id, region, instance_type,
      fourteenDayAverageCPUUtilization%, usage_quantity, public_cost
    """
    con.execute(f"""
    CREATE OR REPLACE VIEW rds_norm AS
    SELECT
      CAST(account_id     AS VARCHAR)                              AS account_id,
      CAST(business_area  AS VARCHAR)                              AS business_area,
      CAST(resource_id    AS VARCHAR)                              AS db_id,
      CAST(region         AS VARCHAR)                              AS region,
      LOWER(TRIM(CAST(instance_type AS VARCHAR)))                  AS current_class,
      CAST(NULLIF(REPLACE(REPLACE(CAST("fourteenDayAverageCPUUtilization%" AS VARCHAR), '%',''), ',', ''), '') AS DOUBLE)
        AS avg_cpu_14d,
      CAST(usage_quantity AS DOUBLE)                               AS hours,
      CAST(REPLACE(REPLACE(CAST(public_cost AS VARCHAR), '$',''), ',', '') AS DOUBLE)
        AS cost_usd
    FROM {source_table};
    """)

    con.execute("""
    CREATE OR REPLACE VIEW rds_clean AS
    SELECT
      account_id, business_area, db_id, region, current_class,
      CASE WHEN avg_cpu_14d BETWEEN 0 AND 100 THEN avg_cpu_14d END AS avg_cpu_14d,
      COALESCE(hours, 0)   AS hours,
      COALESCE(cost_usd,0) AS cost_usd
    FROM rds_norm
    WHERE current_class IS NOT NULL AND current_class <> '';
    """)


# =========================================================
# 2) ENVIRONMENT DETECTION (dev/test/staging/qa/perf/uat)
# =========================================================
def ensure_env_tables(con) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS rds_env_patterns (
      env        VARCHAR,   -- 'nonprod' or 'prod' (we use 'nonprod')
      pattern    VARCHAR,   -- lowercase substring to match
      priority   INT DEFAULT 0,
      UNIQUE(env, pattern)
    );
    """)
    con.execute("""
    INSERT INTO rds_env_patterns(env, pattern, priority) VALUES
      ('nonprod','dev',1),('nonprod','test',1),('nonprod','staging',1),
      ('nonprod','qa',1), ('nonprod','perf',1), ('nonprod','uat',1)
    ON CONFLICT DO NOTHING;
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS rds_exclusions (
      pattern VARCHAR UNIQUE  -- lowercase substring to exclude
    );
    """)

def create_env_detect_view(con) -> None:
    # patternized detector; matches db_id and business_area
    con.execute("""
    CREATE OR REPLACE VIEW rds_env_detect AS
    WITH base AS (
      SELECT
        rc.*,
        LOWER(COALESCE(rc.db_id,''))         AS name_l,
        LOWER(COALESCE(rc.business_area,'')) AS ba_l
      FROM rds_clean rc
    ),
    hits AS (
      SELECT
        b.*,
        MAX(CASE WHEN b.name_l LIKE '%'||p.pattern||'%' OR b.ba_l LIKE '%'||p.pattern||'%' THEN 1 ELSE 0 END)
          AS nonprod_hit
      FROM base b
      LEFT JOIN rds_env_patterns p ON p.env='nonprod'
      GROUP BY ALL
    ),
    not_excl AS (
      SELECT h.*
      FROM hits h
      LEFT JOIN rds_exclusions x1 ON h.name_l LIKE '%'||x1.pattern||'%'
      LEFT JOIN rds_exclusions x2 ON h.ba_l   LIKE '%'||x2.pattern||'%'
      WHERE x1.pattern IS NULL AND x2.pattern IS NULL
    )
    SELECT
      *,
      CASE WHEN nonprod_hit=1 THEN 'nonprod' ELSE 'prod' END AS env_guess
    FROM not_excl;
    """)


# =========================================================
# 3) SIZE LADDER (data-driven; no big CASE)
# =========================================================
_SIZE_ORDER = [
    "nano","micro","small","medium","large","xlarge",
    "2xlarge","3xlarge","4xlarge","6xlarge","8xlarge","9xlarge",
    "10xlarge","12xlarge","16xlarge","18xlarge","24xlarge","32xlarge","48xlarge","56xlarge"
]

def ensure_rds_sizes(con) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS rds_sizes (
      family     VARCHAR,   -- 'db.r5'
      size       VARCHAR,   -- 'large'
      size_rank  INTEGER,   -- dense rank within the family
      PRIMARY KEY (family, size)
    );
    """)

def refresh_rds_sizes_from_usage(con) -> None:
    con.execute("""
      CREATE OR REPLACE VIEW _rds_classes_seen AS
      SELECT DISTINCT
        REGEXP_EXTRACT(LOWER(TRIM(current_class)), '^((db\\.[^.]+))\\.(.+)$', 1) AS family,
        REGEXP_EXTRACT(LOWER(TRIM(current_class)), '^((db\\.[^.]+))\\.(.+)$', 3) AS size
      FROM rds_clean
      WHERE current_class IS NOT NULL AND current_class <> '';
    """)
    df = con.execute("SELECT family, size FROM _rds_classes_seen").fetchdf()
    if df.empty:
        ensure_rds_sizes(con)
        return
    order_map = {s:i for i,s in enumerate(_SIZE_ORDER, start=1)}
    df = df.dropna()
    df["size_order"] = df["size"].map(order_map)
    df = df[~df["size_order"].isna()]
    df["size_rank"] = df.groupby("family")["size_order"].rank(method="dense").astype(int)
    df = df[["family","size","size_rank"]].drop_duplicates().sort_values(["family","size_rank"])
    ensure_rds_sizes(con)
    con.execute("DELETE FROM rds_sizes")
    con.register("rds_sizes_df", df)
    con.execute("INSERT INTO rds_sizes SELECT * FROM rds_sizes_df")
    con.unregister("rds_sizes_df")

def create_rds_with_size_view(con) -> None:
    con.execute("""
    CREATE OR REPLACE VIEW rds_with_size AS
    WITH parts AS (
      SELECT
        rc.*,
        REGEXP_EXTRACT(current_class, '^((db\\.[^.]+))\\.(.+)$', 1) AS family,    -- 'db.r5'
        REGEXP_EXTRACT(current_class, '^((db\\.[^.]+))\\.(.+)$', 3) AS size_label -- 'large'
      FROM rds_env_detect rc
    )
    SELECT p.*, rs.size_rank
    FROM parts p
    LEFT JOIN rds_sizes rs
      ON rs.family = p.family AND rs.size = LOWER(p.size_label);
    """)


# =========================================================
# 4) ROLLUPS, HEURISTICS, OFF-HOURS
# =========================================================
def create_rollups_and_heuristics(con) -> None:
    con.execute("""
    CREATE OR REPLACE VIEW rds_by_ba AS
    SELECT
      business_area,
      COUNT(*)         AS db_count,
      SUM(cost_usd)    AS total_cost_usd,
      AVG(avg_cpu_14d) AS avg_cpu_14d
    FROM rds_with_size
    GROUP BY 1
    ORDER BY total_cost_usd DESC;
    """)
    con.execute("""
    CREATE OR REPLACE VIEW rds_by_ba_region AS
    SELECT
      business_area, region,
      COUNT(*)         AS db_count,
      SUM(cost_usd)    AS total_cost_usd,
      AVG(avg_cpu_14d) AS avg_cpu_14d
    FROM rds_with_size
    GROUP BY 1,2
    ORDER BY total_cost_usd DESC;
    """)
    con.execute("""
    CREATE OR REPLACE VIEW rds_by_class AS
    SELECT
      business_area, region, current_class,
      COUNT(*)         AS db_count,
      SUM(cost_usd)    AS total_cost_usd,
      AVG(avg_cpu_14d) AS avg_cpu_14d
    FROM rds_with_size
    GROUP BY 1,2,3
    ORDER BY total_cost_usd DESC;
    """)

    # Underutilized buckets (kill/merge uses <5%)
    con.execute("""
    CREATE OR REPLACE VIEW rds_kill_merge AS
    SELECT
      business_area, region, db_id, current_class,
      avg_cpu_14d, hours, cost_usd,
      cost_usd AS est_monthly_savings_usd,
      CASE
        WHEN avg_cpu_14d IS NOT NULL AND avg_cpu_14d < 5 THEN 'High'
        ELSE 'Medium'
      END AS confidence,
      'CPU < 5%; retire or merge' AS reason
    FROM rds_with_size
    WHERE avg_cpu_14d IS NOT NULL AND avg_cpu_14d < 5
    ORDER BY cost_usd DESC;
    """)

    # High utilization (≥90%)
    con.execute("""
    CREATE OR REPLACE VIEW rds_high_utilization AS
    SELECT
      business_area, region, db_id, current_class,
      avg_cpu_14d, hours, cost_usd
    FROM rds_with_size
    WHERE avg_cpu_14d >= 90
    ORDER BY avg_cpu_14d DESC, cost_usd DESC;
    """)

    # Off-hours: non-prod + ~24×7 (hours >= 672 ~ 28 days * 24)
    con.execute("""
    CREATE OR REPLACE VIEW rds_offhours_candidates AS
    SELECT
      business_area, region, db_id, current_class,
      env_guess,
      hours AS current_hours,
      cost_usd AS current_cost_usd,
      CASE WHEN hours >= 672 THEN 1 ELSE 0 END AS approx_247,
      ROUND(cost_usd * 0.65, 2)            AS est_monthly_savings_usd,
      'Assume 5×12 schedule (~65% savings)' AS assumption,
      avg_cpu_14d
    FROM rds_with_size
    WHERE env_guess = 'nonprod'
      AND hours >= 672
    ORDER BY est_monthly_savings_usd DESC;
    """)


# =========================================================
# 5) RIGHTSIZING (rank ± 1, unpriced)
# =========================================================
def create_rightsize_unpriced(con) -> None:
    # Next smaller → underutilized (5–10% CPU) + any NULL CPU
    con.execute("""
    CREATE OR REPLACE VIEW rds_rightsize_next_smaller AS
    WITH base AS (
      SELECT * FROM rds_with_size WHERE avg_cpu_14d IS NULL OR avg_cpu_14d < 10
    ),
    next_rank AS (
      SELECT b.*, (b.size_rank - 1) AS target_rank
      FROM base b
      WHERE b.size_rank IS NOT NULL AND b.size_rank > 1
    ),
    target AS (
      SELECT n.*, rs.size AS target_size
      FROM next_rank n
      JOIN rds_sizes rs
        ON rs.family = n.family AND rs.size_rank = n.target_rank
    )
    SELECT
      t.business_area, t.region, t.db_id,
      t.current_class,
      CONCAT(t.family, '.', t.size_label)   AS current_size_label,
      CONCAT(t.family, '.', t.target_size)  AS recommended_class,
      t.avg_cpu_14d, t.hours, t.cost_usd
    FROM target t
    ORDER BY t.cost_usd DESC NULLS LAST;
    """)

    # Next larger → hot DBs (≥90% CPU)
    con.execute("""
    CREATE OR REPLACE VIEW rds_rightsize_next_larger AS
    WITH base AS (
      SELECT * FROM rds_with_size WHERE avg_cpu_14d >= 90
    ),
    next_rank AS (
      SELECT b.*, (b.size_rank + 1) AS target_rank
      FROM base b
      WHERE b.size_rank IS NOT NULL
    ),
    target AS (
      SELECT n.*, rs.size AS target_size
      FROM next_rank n
      JOIN rds_sizes rs
        ON rs.family = n.family AND rs.size_rank = n.target_rank
    )
    SELECT
      t.business_area, t.region, t.db_id,
      t.current_class,
      CONCAT(t.family, '.', t.size_label)   AS current_size_label,
      CONCAT(t.family, '.', t.target_size)  AS recommended_class,
      t.avg_cpu_14d, t.hours, t.cost_usd
    FROM target t
    ORDER BY t.avg_cpu_14d DESC, t.cost_usd DESC;
    """)


# =========================================================
# 6) PRICING (dynamic discovery; GovCloud fallback)
# =========================================================
def ensure_price_table(con) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS price_rds (
      price_date           DATE DEFAULT CURRENT_DATE,
      region               VARCHAR,
      instance_class       VARCHAR,   -- 'db.r5.large'
      purchase_option      VARCHAR DEFAULT 'OnDemand',
      price_per_hour_usd   DOUBLE,
      PRIMARY KEY (region, instance_class, purchase_option)
    );
    """)

def discover_location_for_region(pricing_client, region_code: str) -> Optional[str]:
    """
    Use Pricing API to resolve a region code to a 'location' (display string) dynamically.
    Returns None for GovCloud/unsupported regions.
    """
    try:
        paginator = pricing_client.get_paginator("get_products")
        pages = paginator.paginate(
            ServiceCode="AmazonRDS",
            Filters=[{"Type":"TERM_MATCH","Field":"regionCode","Value":region_code}]
        )
        for page in pages:
            for raw in page.get("PriceList", []):
                data = json.loads(raw)
                loc = data.get("product", {}).get("attributes", {}).get("location")
                if loc:
                    return loc
    except Exception:
        pass
    return None

def build_region_location_map_from_csv(con, table: str = "rds_clean"
                                      ) -> Tuple[Dict[str,str], Set[str], Set[str]]:
    rows = con.execute(f"SELECT DISTINCT region FROM {table} WHERE region IS NOT NULL").fetchall()
    csv_regions = {r[0].strip() for r in rows if r and r[0]}
    govcloud = {r for r in csv_regions if r.startswith("us-gov-")}
    pricing = _pricing_client()
    region_to_location: Dict[str,str] = {}
    unknown: Set[str] = set()
    for r in (csv_regions - govcloud):
        loc = discover_location_for_region(pricing, r)
        if loc:
            region_to_location[r] = loc
        else:
            unknown.add(r)
    return region_to_location, govcloud, unknown

def upsert_price_rows(con, rows: List[dict]) -> None:
    if not rows:
        return
    ensure_price_table(con)
    df = pd.DataFrame(rows)
    if "purchase_option" not in df:
        df["purchase_option"] = "OnDemand"
    if "price_date" not in df:
        df["price_date"] = pd.Timestamp.today().date()
    con.register("price_upsert_df", df)
    con.execute("""
      INSERT OR REPLACE INTO price_rds
      (price_date, region, instance_class, purchase_option, price_per_hour_usd)
      SELECT price_date, region, instance_class, purchase_option, price_per_hour_usd
      FROM price_upsert_df
    """)
    con.unregister("price_upsert_df")

def _price_from_api(pricing_client, location: str, instance_class: str,
                    deployment="Single-AZ", engine="Any") -> Optional[float]:
    m = re.match(r"^db\.([^.]+)\.(.+)$", instance_class)
    if not m:
        return None
    instanceType = f"{m.group(1)}.{m.group(2)}"

    paginator = pricing_client.get_paginator("get_products")
    pages = paginator.paginate(
        ServiceCode="AmazonRDS",
        Filters=[
            {"Type":"TERM_MATCH","Field":"location","Value":location},
            {"Type":"TERM_MATCH","Field":"instanceType","Value":instanceType},
            {"Type":"TERM_MATCH","Field":"deploymentOption","Value":deployment},
            {"Type":"TERM_MATCH","Field":"databaseEngine","Value":engine},
            {"Type":"TERM_MATCH","Field":"preInstalledSw","Value":"NA"},
            {"Type":"TERM_MATCH","Field":"purchaseOption","Value":"OnDemand"},
        ]
    )
    for page in pages:
        for raw in page.get("PriceList", []):
            data = json.loads(raw)
            terms = data.get("terms", {}).get("OnDemand", {})
            for term in terms.values():
                for dim in term.get("priceDimensions", {}).values():
                    usd = dim.get("pricePerUnit", {}).get("USD")
                    if usd:
                        try:
                            return float(usd)
                        except:
                            return None
    return None

def classes_for_pricing(con) -> List[str]:
    df = con.execute("""
      WITH c AS (
        SELECT DISTINCT LOWER(TRIM(current_class)) AS cls FROM rds_with_size
        UNION
        SELECT DISTINCT LOWER(CONCAT(family, '.', size)) AS cls FROM rds_sizes
      )
      SELECT cls FROM c WHERE cls IS NOT NULL
    """).fetchdf()
    return sorted({c for c in df["cls"].dropna().tolist()})

def seed_prices_dynamic(con, deployment="Single-AZ", engine="Any") -> None:
    """
    Commercial regions → Pricing API (dynamic location discovery).
    GovCloud → observed pricing (cost/hour) fallback.
    """
    pricing = _pricing_client()
    region_to_location, govcloud, unknown = build_region_location_map_from_csv(con, "rds_clean")

    # 1) Commercial via API
    cls = classes_for_pricing(con)
    rows: List[dict] = []
    for region_code, location in region_to_location.items():
        for c in cls:
            price = _price_from_api(pricing, location, c, deployment=deployment, engine=engine)
            if price is not None:
                rows.append({
                    "region": region_code,
                    "instance_class": c,
                    "purchase_option": "OnDemand",
                    "price_per_hour_usd": price
                })
    upsert_price_rows(con, rows)

    # 2) GovCloud via observed (public Pricing API does not cover GovCloud)
    if govcloud:
        con.register("csv_regions_tbl", pd.DataFrame({"region": list(govcloud)}))
        con.execute("""
          INSERT OR REPLACE INTO price_rds (price_date, region, instance_class, purchase_option, price_per_hour_usd)
          SELECT
            CURRENT_DATE,
            rc.region,
            LOWER(TRIM(rc.current_class)) AS instance_class,
            'OnDemand',
            AVG(NULLIF(rc.cost_usd / NULLIF(rc.hours,0),0)) AS price_per_hour_usd
          FROM rds_clean rc
          JOIN csv_regions_tbl r ON r.region = rc.region
          WHERE rc.hours > 0 AND rc.cost_usd > 0 AND rc.current_class IS NOT NULL
          GROUP BY rc.region, instance_class
        """)
        con.unregister("csv_regions_tbl")

    if unknown:
        print("WARNING: Unknown regions (no pricing location discovered):", sorted(unknown))


# Optional stopgap if no API creds yet
def seed_price_from_observed(con) -> None:
    ensure_price_table(con)
    con.execute("""
    INSERT OR REPLACE INTO price_rds (price_date, region, instance_class, purchase_option, price_per_hour_usd)
    SELECT
      CURRENT_DATE,
      region,
      LOWER(TRIM(current_class)) AS instance_class,
      'OnDemand',
      AVG(NULLIF(cost_usd / NULLIF(hours,0),0)) AS price_per_hour_usd
    FROM rds_clean
    WHERE hours > 0 AND cost_usd > 0 AND current_class IS NOT NULL
    GROUP BY region, instance_class
    """)


# =========================================================
# 7) PRICED RIGHTSIZING & ACTIONS
# =========================================================
def create_priced_rightsizing_and_actions(con) -> None:
    # Next smaller (priced)
    con.execute("""
    CREATE OR REPLACE VIEW rds_rightsize_next_smaller_priced AS
    SELECT
      r.*, pc.price_per_hour_usd AS current_price_per_hr,
      pn.price_per_hour_usd AS rec_price_per_hr,
      CASE
        WHEN pn.price_per_hour_usd IS NULL OR r.hours IS NULL OR r.hours=0 THEN NULL
        ELSE r.cost_usd - (pn.price_per_hour_usd * r.hours)
      END AS est_monthly_savings_usd,
      'CPU 5–10% or unknown; next size down' AS reason
    FROM rds_rightsize_next_smaller r
    LEFT JOIN price_rds pc
      ON pc.region = r.region AND pc.instance_class = r.current_class AND pc.purchase_option='OnDemand'
    LEFT JOIN price_rds pn
      ON pn.region = r.region AND pn.instance_class = r.recommended_class AND pn.purchase_option='OnDemand'
    ORDER BY est_monthly_savings_usd DESC NULLS LAST, r.cost_usd DESC NULLS LAST;
    """)

    # Next larger (priced)
    con.execute("""
    CREATE OR REPLACE VIEW rds_rightsize_next_larger_priced AS
    SELECT
      r.*, pc.price_per_hour_usd AS current_price_per_hr,
      pn.price_per_hour_usd AS rec_price_per_hr,
      CASE
        WHEN pn.price_per_hour_usd IS NULL OR r.hours IS NULL OR r.hours=0 THEN NULL
        ELSE (pn.price_per_hour_usd * r.hours) - r.cost_usd
      END AS est_monthly_delta_usd,
      'CPU ≥ 90%; next size up' AS reason
    FROM rds_rightsize_next_larger r
    LEFT JOIN price_rds pc
      ON pc.region = r.region AND pc.instance_class = r.current_class AND pc.purchase_option='OnDemand'
    LEFT JOIN price_rds pn
      ON pn.region = r.region AND pn.instance_class = r.recommended_class AND pn.purchase_option='OnDemand'
    ORDER BY est_monthly_delta_usd DESC NULLS LAST, r.avg_cpu_14d DESC;
    """)

    # BA savings rollup (downsize only)
    con.execute("""
    CREATE OR REPLACE VIEW rds_savings_by_ba AS
    SELECT
      business_area,
      SUM(COALESCE(est_monthly_savings_usd,0)) AS potential_savings_usd
    FROM rds_rightsize_next_smaller_priced
    GROUP BY 1
    ORDER BY potential_savings_usd DESC;
    """)

    # Ranked actions list
    con.execute("""
    CREATE OR REPLACE VIEW rds_actions_ranked AS
    WITH all_actions AS (
      -- 3 = highest priority in this sort scheme
      SELECT 'kill/merge' AS action, business_area, region, db_id, current_class,
             avg_cpu_14d, hours, cost_usd,
             est_monthly_savings_usd AS est_delta_usd,
             reason, confidence, 3 AS priority
      FROM rds_kill_merge

      UNION ALL
      SELECT 'downsize', business_area, region, db_id, current_class,
             avg_cpu_14d, hours, cost_usd,
             est_monthly_savings_usd, reason,
             CASE WHEN current_price_per_hr IS NOT NULL AND rec_price_per_hr IS NOT NULL THEN 'High' ELSE 'Medium' END,
             2
      FROM rds_rightsize_next_smaller_priced

      UNION ALL
      SELECT 'offhours', business_area, region, db_id, current_class,
             avg_cpu_14d, current_hours AS hours, current_cost_usd AS cost_usd,
             est_monthly_savings_usd, assumption AS reason,
             CASE WHEN approx_247=1 THEN 'High' ELSE 'Medium' END,
             1
      FROM rds_offhours_candidates

      UNION ALL
      SELECT 'upsize', business_area, region, db_id, current_class,
             avg_cpu_14d, hours, cost_usd,
             est_monthly_delta_usd, reason,
             CASE WHEN current_price_per_hr IS NOT NULL AND rec_price_per_hr IS NOT NULL THEN 'High' ELSE 'Medium' END,
             0
      FROM rds_rightsize_next_larger_priced
    ),
    filtered AS (
      SELECT * FROM all_actions
      WHERE COALESCE(est_delta_usd,0) >= 25   -- economic floor
    )
    SELECT *
    FROM filtered
    ORDER BY priority DESC, est_delta_usd DESC NULLS LAST, cost_usd DESC NULLS LAST;
    """)


# =========================================================
# 8) ONE-CALL BUILDER (call this after loading rds_raw)
# =========================================================
def build_rds(con,
              source_table: str = "rds_raw",
              seed_prices: bool = True,
              deployment: str = "Single-AZ",
              engine: str = "Any") -> None:
    """
    Full build:
      - rds_clean, env detection, size ladder, rollups
      - off-hours, unpriced rightsizing
      - dynamic pricing (commercial via API, GovCloud via observed)
      - priced views and final rds_actions_ranked
    """
    create_rds_core_views(con, source_table=source_table)
    ensure_env_tables(con)
    create_env_detect_view(con)

    ensure_rds_sizes(con)
    refresh_rds_sizes_from_usage(con)
    create_rds_with_size_view(con)

    create_rollups_and_heuristics(con)
    create_rightsize_unpriced(con)

    if seed_prices:
        seed_prices_dynamic(con, deployment=deployment, engine=engine)
    else:
        # fallback if you prefer to avoid AWS Pricing during early tests:
        seed_price_from_observed(con)

    create_priced_rightsizing_and_actions(con)
