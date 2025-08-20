# --- put near the top of your Streamlit file ---

import duckdb, streamlit as st

# use the same connection across the app
con = st.session_state.get("con") or duckdb.connect("cloud_savings.duckdb")
st.session_state["con"] = con

def _rds_build_pipeline(con, csv_path: str):
    # 0) Load CSV -> rds_raw
    con.execute("CREATE OR REPLACE TABLE rds_raw AS SELECT * FROM read_csv_auto(?, IGNORE_ERRORS=TRUE)", [csv_path])

    # 1) Build all views in the right order
    from rds_agent_setup import (
        create_rds_core_views,            # builds rds_clean, rds_by_ba_region, rds_by_class, etc.
        ensure_env_tables, create_env_detect_view,
        ensure_rds_sizes, refresh_rds_sizes_from_usage, create_rds_with_size_view,
        create_rollups_and_heuristics,    # kill/merge, high util, offhours (unpriced)
        create_rightsize_unpriced,        # next_smaller/next_larger recommendations (unpriced)
        create_priced_rightsizing_and_actions,  # joins price_rds -> priced savings + rds_actions_ranked
        seed_price_from_observed          # CSV-derived prices -> price_rds
    )

    create_rds_core_views(con, source_table="rds_raw")
    ensure_env_tables(con); create_env_detect_view(con)

    ensure_rds_sizes(con)
    refresh_rds_sizes_from_usage(con)     # populates rds_sizes from your data
    create_rds_with_size_view(con)        # adds family/size_rank columns

    create_rollups_and_heuristics(con)
    create_rightsize_unpriced(con)

    # Make sure price_rds exists and is seeded from CSV first (fast)
    seed_price_from_observed(con)

    # Build priced rightsizing + ranked actions (depends on price_rds)
    create_priced_rightsizing_and_actions(con)

def _rds_ensure_ready(con, csv_path: str):
    """Run the pipeline if core views are missing."""
    try:
        con.execute("SELECT 1 FROM rds_by_ba_region LIMIT 1")  # smoke test for a key view
    except Exception:
        _rds_build_pipeline(con, csv_path)

# call this ONCE at the top of the RDS page
csv_path = st.session_state.get("rds_csv_path", "gs://YOUR_BUCKET/rds.csv")  # set your default
_rds_ensure_ready(con, csv_path)



st.caption("Debug: current tables/views in DuckDB")
st.dataframe(con.execute("""
    SELECT table_name, table_type
    FROM information_schema.tables
    WHERE table_schema = 'main'
    ORDER BY table_type DESC, table_name
""").fetchdf(), hide_index=True, use_container_width=True)




# one button anywhere in the header area
if st.button("🔄 Update Prices (API)", use_container_width=True):
    from rds_agent_setup import seed_prices_dynamic as refresh_prices_via_api  # or seed_prices_incremental if you have it
    with st.status("Refreshing prices via AWS Pricing API…", expanded=True) as s:
        refresh_prices_via_api(con)           # upserts rows into price_rds
        # views like rds_rightsize_next_smaller_priced/rds_actions_ranked read price_rds -> auto-refresh
        s.update(label="Prices updated ✅", state="complete")
    st.rerun()
