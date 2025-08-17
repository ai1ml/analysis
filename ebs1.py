def create_views(con):
    # -------------------------
    # 0) Assumptions (1 row)
    # -------------------------
    con.execute("""
    CREATE OR REPLACE TABLE ebs_assumptions AS
    SELECT
      0.20::DOUBLE AS pct_gp2_to_gp3,          -- assume ~20% cheaper on gp3 vs gp2 (monthly cost basis)
      0.40::DOUBLE AS pct_standard_to_gp3,     -- legacy magnetic → gp3 (conservative 40%)
      0.30::DOUBLE AS pct_io1_to_gp3,          -- if we lack IOPS price detail; adjust later if you add pricing
      30::INTEGER  AS long_idle_days,          -- start nudging at 30d
      90::INTEGER  AS very_long_idle_days;     -- strong confidence at 90d+
    """)

    # -------------------------
    # 1) Normalize base
    # -------------------------
    con.execute("""
    CREATE OR REPLACE VIEW ebs_norm AS
    SELECT
      e.*,
      CASE
        WHEN LOWER(volume_state) IN ('available','detached') THEN 'Detached'
        WHEN LOWER(volume_state) IN ('in-use','in use')      THEN 'Attached'
        ELSE COALESCE(volume_state,'Unknown')
      END AS attach_state,
      LOWER(volume_type) AS volume_type_norm,
      public_cost_usd    AS monthly_cost_usd
    FROM ebs e;
    """)

    # -------------------------
    # 2) Leadership & Ops rollups
    # -------------------------
    con.execute("""
    CREATE OR REPLACE VIEW ebs_by_ba AS
    SELECT
      billing_period, business_area,
      COUNT(*)             AS volume_count,
      SUM(size_gb)         AS total_gb,
      SUM(monthly_cost_usd) AS total_cost_usd
    FROM ebs_norm
    GROUP BY 1,2
    ORDER BY total_cost_usd DESC;
    """)

    con.execute("""
    CREATE OR REPLACE VIEW ebs_by_region_type AS
    SELECT
      billing_period, region, volume_type_norm AS volume_type,
      COUNT(*)              AS volume_count,
      SUM(size_gb)          AS total_gb,
      SUM(monthly_cost_usd) AS total_cost_usd
    FROM ebs_norm
    GROUP BY 1,2,3
    ORDER BY total_cost_usd DESC, total_gb DESC;
    """)

    # -------------------------
    # 3) Targeted opportunity views
    # -------------------------
    con.execute("""
    CREATE OR REPLACE VIEW ebs_unattached AS
    SELECT
      billing_period, business_area, region, volume_id,
      volume_type_norm AS volume_type, size_gb,
      days_since_last_attached, monthly_cost_usd
    FROM ebs_norm
    WHERE attach_state = 'Detached'
    ORDER BY monthly_cost_usd DESC NULLS LAST, size_gb DESC NULLS LAST;
    """)

    # Long-idle with confidence (High/Med/Low) and action=delete
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
        WHEN days_since_last_attached IS NULL THEN 'Low'
        WHEN days_since_last_attached >= a.very_long_idle_days THEN 'High'
        WHEN days_since_last_attached >= a.long_idle_days      THEN 'Medium'
        ELSE 'Low'
      END AS confidence,
      'delete_idle' AS suggested_action
    FROM a
    ORDER BY monthly_cost_usd DESC NULLS LAST, days_since_last_attached DESC NULLS LAST;
    """)

    # gp2 → gp3 (cost-ratio heuristic on monthly cost)
    con.execute("""
    CREATE OR REPLACE VIEW ebs_gp2_to_gp3 AS
    SELECT
      n.billing_period, n.business_area, n.region, n.volume_id,
      n.size_gb, n.monthly_cost_usd,
      s.pct_gp2_to_gp3,
      ROUND(n.monthly_cost_usd * s.pct_gp2_to_gp3, 2) AS est_monthly_savings_usd,
      'migrate_gp2_to_gp3' AS suggested_action
    FROM ebs_norm n
    CROSS JOIN ebs_assumptions s
    WHERE n.volume_type_norm = 'gp2'
    ORDER BY est_monthly_savings_usd DESC NULLS LAST;
    """)

    # Legacy magnetic → gp3
    con.execute("""
    CREATE OR REPLACE VIEW ebs_standard_to_gp3 AS
    SELECT
      n.billing_period, n.business_area, n.region, n.volume_id,
      n.size_gb, n.monthly_cost_usd,
      s.pct_standard_to_gp3,
      ROUND(n.monthly_cost_usd * s.pct_standard_to_gp3, 2) AS est_monthly_savings_usd,
      'migrate_standard_to_gp3' AS suggested_action
    FROM ebs_norm n
    CROSS JOIN ebs_assumptions s
    WHERE n.volume_type_norm = 'standard'
    ORDER BY est_monthly_savings_usd DESC NULLS LAST;
    """)

    # HDD (sc1/st1) review
    con.execute("""
    CREATE OR REPLACE VIEW ebs_hdd_review AS
    SELECT
      billing_period, business_area, region, volume_id,
      volume_type_norm AS volume_type, attach_state, size_gb, monthly_cost_usd,
      CASE
        WHEN attach_state = 'Detached' THEN 'Delete if safe'
        ELSE 'Validate throughput profile; consider gp3 if SSD fits'
      END AS recommendation
    FROM ebs_norm
    WHERE volume_type_norm IN ('sc1','st1')
    ORDER BY monthly_cost_usd DESC NULLS LAST, size_gb DESC NULLS LAST;
    """)

    # io1 downgrade (optional; only create if IOPS columns exist)
    cols = set(con.execute("PRAGMA table_info('ebs')").fetchdf()["name"].str.lower())
    if {"provisioned_iops", "avg_iops_14d"}.issubset(cols):
        con.execute("""
        CREATE OR REPLACE VIEW ebs_io1_downgrade AS
        SELECT
          n.billing_period, n.business_area, n.region, n.volume_id,
          n.size_gb, n.provisioned_iops, n.avg_iops_14d, n.monthly_cost_usd,
          CASE
            WHEN n.avg_iops_14d IS NULL THEN 'Low'
            WHEN n.provisioned_iops IS NULL OR n.provisioned_iops = 0 THEN 'Low'
            WHEN n.avg_iops_14d < 0.10 * n.provisioned_iops THEN 'High'
            WHEN n.avg_iops_14d < 0.25 * n.provisioned_iops THEN 'Medium'
            ELSE 'Low'
          END AS confidence,
          'downgrade_io1' AS suggested_action
        FROM ebs_norm n
        WHERE n.volume_type_norm = 'io1'
        ORDER BY n.monthly_cost_usd DESC NULLS LAST, n.size_gb DESC NULLS LAST;
        """)
    # else: quietly skip creating ebs_io1_downgrade

    # -------------------------
    # 4) Sprawl clusters (top 10% by volume_count)
    # -------------------------
    con.execute("""
    CREATE OR REPLACE VIEW ebs_sprawl_clusters AS
    WITH counts AS (
      SELECT
        billing_period, business_area, region,
        COUNT(*)              AS volume_count,
        SUM(monthly_cost_usd) AS total_cost_usd,
        SUM(size_gb)          AS total_gb
      FROM ebs_norm
      GROUP BY 1,2,3
    ),
    pct AS (
      SELECT
        c.*,
        PERCENTILE_CONT(c.volume_count, 0.90) OVER () AS p90_count
      FROM counts c
    )
    SELECT *
    FROM pct
    WHERE volume_count >= p90_count
    ORDER BY volume_count DESC, total_cost_usd DESC;
    """)

    # -------------------------
    # 5) Unified actions (rank, score, explain)
    # -------------------------
    # Collect candidates from all specialized views
    con.execute("""
    CREATE OR REPLACE VIEW ebs_actions_union AS
    SELECT billing_period,business_area,region,volume_id,'delete_idle'            AS action,
           monthly_cost_usd                             AS est_savings_usd,
           confidence, 'Long idle & detached'          AS reason
    FROM ebs_unattached_long_idle

    UNION ALL
    SELECT billing_period,business_area,region,volume_id,'migrate_gp2_to_gp3'     AS action,
           est_monthly_savings_usd                        AS est_savings_usd,
           'High'                                         AS confidence,
           'gp2 → gp3 cost delta (assumed ratio)'         AS reason
    FROM ebs_gp2_to_gp3

    UNION ALL
    SELECT billing_period,business_area,region,volume_id,'migrate_standard_to_gp3' AS action,
           est_monthly_savings_usd                         AS est_savings_usd,
           'High'                                          AS confidence,
           'legacy magnetic → gp3 (assumed ratio)'         AS reason
    FROM ebs_standard_to_gp3

    UNION ALL
    SELECT billing_period,business_area,region,volume_id,'review_hdd'            AS action,
           monthly_cost_usd                             AS est_savings_usd,
           CASE WHEN attach_state='Detached' THEN 'High' ELSE 'Medium' END AS confidence,
           recommendation                              AS reason
    FROM ebs_hdd_review
    """)

    # include io1 if view exists
    try:
        con.execute("SELECT 1 FROM ebs_io1_downgrade LIMIT 1")
        con.execute("""
        CREATE OR REPLACE VIEW ebs_actions_union_all AS
        SELECT * FROM ebs_actions_union
        UNION ALL
        SELECT billing_period,business_area,region,volume_id,'downgrade_io1' AS action,
               ROUND(monthly_cost_usd * (SELECT pct_io1_to_gp3 FROM ebs_assumptions), 2) AS est_savings_usd,
               confidence,
               'io1 provisioned >> usage; consider gp3/io2' AS reason
        FROM ebs_io1_downgrade
        """)
    except Exception:
        # fallback when io1 view doesn't exist
        con.execute("""
        CREATE OR REPLACE VIEW ebs_actions_union_all AS
        SELECT * FROM ebs_actions_union
        """)

    # Rank by savings within the period/BA/region (stable tie-breakers)
    con.execute("""
    CREATE OR REPLACE VIEW ebs_actions_ranked AS
    SELECT
      billing_period, business_area, region, volume_id, action,
      est_savings_usd,
      confidence,
      reason,
      RANK() OVER (
        PARTITION BY billing_period
        ORDER BY est_savings_usd DESC NULLS LAST, business_area, region, volume_id
      ) AS rank_in_period
    FROM ebs_actions_union_all
    ORDER BY est_savings_usd DESC NULLS LAST;
    """)

    # Final “explain” layer with human-readable suggestion
    con.execute("""
    CREATE OR REPLACE VIEW ebs_actions_explain AS
    SELECT
      r.*,
      CASE action
        WHEN 'delete_idle'             THEN 'Delete unattached, long-idle volume (snapshot if required).'
        WHEN 'migrate_gp2_to_gp3'      THEN 'Migrate gp2 to gp3 for lower $/GB.'
        WHEN 'migrate_standard_to_gp3' THEN 'Migrate legacy magnetic to gp3.'
        WHEN 'downgrade_io1'           THEN 'Reduce IOPS tier or move to gp3/io2 per workload.'
        WHEN 'review_hdd'              THEN 'Review sc1/st1 usage; move to gp3 if SSD fits.'
        ELSE 'Review volume.'
      END AS suggestion
    FROM ebs_actions_ranked r
    ORDER BY est_savings_usd DESC NULLS LAST, rank_in_period ASC;
    """)


import streamlit as st

def ebs_section(con):
    st.header("EBS Analysis")

    # Business Area filter
    ebs_BAs = [r[0] for r in con.execute(
        "SELECT DISTINCT business_area FROM ebs_norm ORDER BY 1"
    ).fetchall()]
    sel_ba = st.selectbox("Business Area", options=["(all)"] + ebs_BAs, key="ebs_ba")

    # Simple WHERE builder
    def ew(base="1=1"):
        wc = [base]
        if sel_ba != "(all)":
            wc.append(f"business_area = '{sel_ba.replace(\"'\",\"''\")}'")
        return " AND ".join(wc)

    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
        "By BA", "By Region/Type", "Unattached", "Long Idle", 
        "gp2→gp3", "standard→gp3", "HDD Review", "Actions Ranked"
    ])

    with tab1:
        q = f"SELECT * FROM ebs_by_ba WHERE {ew()} ORDER BY total_cost_usd DESC"
        st.caption(q)
        st.dataframe(con.execute(q).fetchdf())

    with tab2:
        q = f"SELECT * FROM ebs_by_region_type WHERE {ew()} ORDER BY total_cost_usd DESC"
        st.caption(q)
        st.dataframe(con.execute(q).fetchdf())

    with tab3:
        q = f"SELECT * FROM ebs_unattached WHERE {ew()} ORDER BY public_cost_usd DESC"
        st.caption(q)
        st.dataframe(con.execute(q).fetchdf())

    with tab4:
        q = f"SELECT * FROM ebs_unattached_long_idle WHERE {ew()} ORDER BY confidence_level DESC, public_cost_usd DESC"
        st.caption(q)
        st.dataframe(con.execute(q).fetchdf())

    with tab5:
        q = f"SELECT * FROM ebs_gp2_to_gp3 WHERE {ew()} ORDER BY est_monthly_savings_usd DESC"
        st.caption(q)
        st.dataframe(con.execute(q).fetchdf())

    with tab6:
        q = f"SELECT * FROM ebs_standard_to_gp3 WHERE {ew()} ORDER BY est_monthly_savings_usd DESC"
        st.caption(q)
        st.dataframe(con.execute(q).fetchdf())

    with tab7:
        q = f"SELECT * FROM ebs_hdd_review WHERE {ew()} ORDER BY public_cost_usd DESC"
        st.caption(q)
        st.dataframe(con.execute(q).fetchdf())

    with tab8:
        q = f"SELECT * FROM ebs_actions_ranked WHERE {ew()} ORDER BY est_monthly_savings_usd DESC"
        st.caption(q)
        st.dataframe(con.execute(q).fetchdf())
