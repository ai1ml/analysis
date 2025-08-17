def create_views(con, source_table="ebs"):
    # ------------------ 0) Assumptions (single row) ------------------
    con.execute("""
    CREATE OR REPLACE TABLE ebs_assumptions AS
    SELECT
      0.20::DOUBLE AS pct_gp2_to_gp3,        -- gp2 → gp3: assume ~20% cheaper (cost basis)
      0.40::DOUBLE AS pct_standard_to_gp3,   -- legacy 'standard' → gp3 (conservative)
      0.30::DOUBLE AS pct_io1_to_gp3,        -- io1 downgrade placeholder when pricing absent
      30::INTEGER  AS long_idle_days,        -- Medium confidence threshold
      90::INTEGER  AS very_long_idle_days;   -- High confidence threshold
    """)

    # ------------------ 1) Normalize base (only your columns) ------------------
    con.execute(f"""
    CREATE OR REPLACE VIEW ebs_norm AS
    SELECT
      billing_period,
      linked_account_id,
      business_area,
      resource_id                              AS volume_id,
      LOWER(volume_type)                       AS volume_type_norm,
      CASE
        WHEN LOWER(COALESCE(volume_state,'')) IN ('available','detached') THEN 'Detached'
        WHEN LOWER(COALESCE(volume_state,'')) IN ('in use','in-use')      THEN 'Attached'
        ELSE COALESCE(volume_state,'Unknown')
      END                                      AS attach_state,
      CAST(days_since_last_attachment AS INTEGER)            AS days_since_last_attached,
      CAST(usage_storage_gb_mo AS DOUBLE)                    AS usage_storage_gb_mo,
      CAST(usage_iops_mo AS DOUBLE)                          AS usage_iops_mo,
      CAST(usage_throughput_gibps_mo AS DOUBLE)              AS usage_throughput_gibps_mo,
      CAST(REGEXP_REPLACE(CAST(cost_mo AS VARCHAR), '[^0-9\\.-]', '') AS DOUBLE) AS monthly_cost_usd
    FROM {source_table};
    """)

    # ------------------ 2) Leadership & Ops rollups (no region) ------------------
    con.execute("""
    CREATE OR REPLACE VIEW ebs_by_ba AS
    SELECT
      billing_period, business_area,
      COUNT(*)                 AS volume_count,
      SUM(usage_storage_gb_mo) AS total_usage_gb_mo,
      SUM(monthly_cost_usd)    AS total_cost_usd
    FROM ebs_norm
    GROUP BY 1,2
    ORDER BY total_cost_usd DESC;
    """)

    con.execute("""
    CREATE OR REPLACE VIEW ebs_by_account_type AS
    SELECT
      billing_period,
      linked_account_id,
      volume_type_norm AS volume_type,
      COUNT(*)                 AS volume_count,
      SUM(usage_storage_gb_mo) AS total_usage_gb_mo,
      SUM(monthly_cost_usd)    AS total_cost_usd
    FROM ebs_norm
    GROUP BY 1,2,3
    ORDER BY total_cost_usd DESC, total_usage_gb_mo DESC;
    """)

    # ------------------ 3) Opportunity views ------------------
    con.execute("""
    CREATE OR REPLACE VIEW ebs_unattached AS
    SELECT
      billing_period, business_area, linked_account_id, volume_id,
      volume_type_norm AS volume_type,
      days_since_last_attached,
      usage_storage_gb_mo, usage_iops_mo, usage_throughput_gibps_mo,
      monthly_cost_usd
    FROM ebs_norm
    WHERE attach_state = 'Detached'
    ORDER BY monthly_cost_usd DESC NULLS LAST, days_since_last_attached DESC NULLS LAST;
    """)

    con.execute("""
    CREATE OR REPLACE VIEW ebs_unattached_long_idle AS
    WITH a AS (
      SELECT u.*, s.long_idle_days, s.very_long_idle_days
      FROM ebs_unattached u
      CROSS JOIN ebs_assumptions s
    )
    SELECT
      a.*,
      CASE
        WHEN days_since_last_attached IS NULL                    THEN 'Low'
        WHEN days_since_last_attached >= a.very_long_idle_days   THEN 'High'
        WHEN days_since_last_attached >= a.long_idle_days        THEN 'Medium'
        ELSE 'Low'
      END AS confidence,
      'delete_idle' AS suggested_action
    FROM a
    ORDER BY monthly_cost_usd DESC NULLS LAST, days_since_last_attached DESC NULLS LAST;
    """)

    con.execute("""
    CREATE OR REPLACE VIEW ebs_gp2_to_gp3 AS
    SELECT
      n.billing_period, n.business_area, n.linked_account_id, n.volume_id,
      n.volume_type_norm AS volume_type,
      n.usage_storage_gb_mo, n.monthly_cost_usd,
      s.pct_gp2_to_gp3,
      ROUND(n.monthly_cost_usd * s.pct_gp2_to_gp3, 2) AS est_monthly_savings_usd,
      'migrate_gp2_to_gp3' AS suggested_action
    FROM ebs_norm n
    CROSS JOIN ebs_assumptions s
    WHERE n.volume_type_norm = 'gp2'
    ORDER BY est_monthly_savings_usd DESC NULLS LAST;
    """)

    con.execute("""
    CREATE OR REPLACE VIEW ebs_standard_to_gp3 AS
    SELECT
      n.billing_period, n.business_area, n.linked_account_id, n.volume_id,
      n.volume_type_norm AS volume_type,
      n.usage_storage_gb_mo, n.monthly_cost_usd,
      s.pct_standard_to_gp3,
      ROUND(n.monthly_cost_usd * s.pct_standard_to_gp3, 2) AS est_monthly_savings_usd,
      'migrate_standard_to_gp3' AS suggested_action
    FROM ebs_norm n
    CROSS JOIN ebs_assumptions s
    WHERE n.volume_type_norm = 'standard'
    ORDER BY est_monthly_savings_usd DESC NULLS LAST;
    """)

    con.execute("""
    CREATE OR REPLACE VIEW ebs_hdd_review AS
    SELECT
      billing_period, business_area, linked_account_id, volume_id,
      volume_type_norm AS volume_type, attach_state,
      usage_storage_gb_mo, monthly_cost_usd,
      CASE
        WHEN attach_state = 'Detached' THEN 'Delete if safe'
        ELSE 'Validate throughput; consider gp3 if SSD fits'
      END AS recommendation
    FROM ebs_norm
    WHERE volume_type_norm IN ('sc1','st1')
    ORDER BY monthly_cost_usd DESC NULLS LAST, usage_storage_gb_mo DESC NULLS LAST;
    """)

    # io1 downgrade without provisioned_iops: percentile heuristic on usage_iops_mo
    con.execute("""
    CREATE OR REPLACE VIEW ebs_io1_downgrade AS
    WITH io1 AS (
      SELECT * FROM ebs_norm
      WHERE volume_type_norm = 'io1' AND usage_iops_mo IS NOT NULL
    ),
    th AS (
      SELECT PERCENTILE_CONT(usage_iops_mo, 0.25) OVER () AS p25 FROM io1 LIMIT 1
    )
    SELECT
      i.billing_period, i.business_area, i.linked_account_id, i.volume_id,
      i.monthly_cost_usd, i.usage_iops_mo,
      CASE
        WHEN i.usage_iops_mo <= (SELECT p25 FROM th) THEN 'Medium'
        ELSE 'Low'
      END AS confidence,
      'downgrade_io1' AS suggested_action
    FROM io1 i
    ORDER BY i.monthly_cost_usd DESC NULLS LAST;
    """)

    # ------------------ 4) Sprawl clusters (BA × Account, top 10% by count) ------------------
    con.execute("""
    CREATE OR REPLACE VIEW ebs_sprawl_clusters AS
    WITH counts AS (
      SELECT
        billing_period, business_area, linked_account_id,
        COUNT(*)                 AS volume_count,
        SUM(monthly_cost_usd)    AS total_cost_usd,
        SUM(usage_storage_gb_mo) AS total_usage_gb_mo
      FROM ebs_norm
      GROUP BY 1,2,3
    ),
    pct AS (
      SELECT c.*, PERCENTILE_CONT(c.volume_count, 0.90) OVER () AS p90_count
      FROM counts c
    )
    SELECT *
    FROM pct
    WHERE volume_count >= p90_count
    ORDER BY volume_count DESC, total_cost_usd DESC;
    """)

    # ------------------ 5) Unified actions (ranked + explain) ------------------
    con.execute("""
    CREATE OR REPLACE VIEW ebs_actions_union AS
    SELECT billing_period,business_area,linked_account_id,volume_id,'delete_idle' AS action,
           monthly_cost_usd AS est_savings_usd, confidence, 'Long idle & detached' AS reason
    FROM ebs_unattached_long_idle

    UNION ALL
    SELECT billing_period,business_area,linked_account_id,volume_id,'migrate_gp2_to_gp3' AS action,
           est_monthly_savings_usd, 'High' AS confidence,
           'gp2 → gp3 cost delta (assumed %)' AS reason
    FROM ebs_gp2_to_gp3

    UNION ALL
    SELECT billing_period,business_area,linked_account_id,volume_id,'migrate_standard_to_gp3' AS action,
           est_monthly_savings_usd, 'High' AS confidence,
           'legacy magnetic → gp3 (assumed %)' AS reason
    FROM ebs_standard_to_gp3

    UNION ALL
    SELECT billing_period,business_area,linked_account_id,volume_id,'review_hdd' AS action,
           NULL::DOUBLE AS est_savings_usd,
           CASE WHEN attach_state='Detached' THEN 'High' ELSE 'Medium' END AS confidence,
           recommendation AS reason
    FROM ebs_hdd_review

    UNION ALL
    SELECT billing_period,business_area,linked_account_id,volume_id,'downgrade_io1' AS action,
           ROUND(monthly_cost_usd * (SELECT pct_io1_to_gp3 FROM ebs_assumptions), 2) AS est_savings_usd,
           confidence,
           'io1 usage low (p25); consider gp3/io2' AS reason
    FROM ebs_io1_downgrade
    """)

    con.execute("""
    CREATE OR REPLACE VIEW ebs_actions_ranked AS
    SELECT
      billing_period, business_area, linked_account_id, volume_id, action,
      est_savings_usd, confidence, reason,
      RANK() OVER (
        PARTITION BY billing_period
        ORDER BY COALESCE(est_savings_usd, 0) DESC, business_area, linked_account_id, volume_id
      ) AS rank_in_period
    FROM ebs_actions_union
    ORDER BY est_savings_usd DESC NULLS LAST, rank_in_period ASC;
    """)

    con.execute("""
    CREATE OR REPLACE VIEW ebs_actions_explain AS
    SELECT
      r.*,
      CASE action
        WHEN 'delete_idle'             THEN 'Delete unattached long-idle volume (snapshot first if required).'
        WHEN 'migrate_gp2_to_gp3'      THEN 'Migrate gp2 to gp3 for lower $/GB.'
        WHEN 'migrate_standard_to_gp3' THEN 'Migrate legacy magnetic to gp3.'
        WHEN 'downgrade_io1'           THEN 'Reduce IOPS tier or move to gp3/io2 per workload.'
        WHEN 'review_hdd'              THEN 'Review sc1/st1; keep only if throughput profile truly requires HDD.'
        ELSE 'Review volume.'
      END AS suggestion
    FROM ebs_actions_ranked r
    ORDER BY est_savings_usd DESC NULLS LAST, rank_in_period ASC;
    """)
