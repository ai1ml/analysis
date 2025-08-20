# =========================
# RDS: one-time CSV pricing + API button
# =========================
import streamlit as st
import duckdb

# Reuse your existing DuckDB connection
con = st.session_state.get("con") or duckdb.connect("rds.db")
st.session_state["con"] = con

# --- pricing helpers (imported from your builder file) ---
from rds_agent_setup import seed_price_from_observed
try:
    from rds_agent_setup import seed_prices_incremental as _refresh_prices_via_api
except Exception:
    from rds_agent_setup import seed_prices_dynamic    as _refresh_prices_via_api

# Seed price_rds from CSV once per session so all views work w/o API
if "rds_prices_seeded" not in st.session_state:
    seed_price_from_observed(con)      # <-- CSV-derived prices
    st.session_state["rds_prices_seeded"] = True

# --- compact toolbar row with ONE API button (kept out of sidebar to match your style) ---
tb1, tb2 = st.columns([6, 2])
with tb2:
    if st.button("🔄 Update Prices (API)", use_container_width=True):
        with st.status("Refreshing prices via AWS Pricing API…", expanded=True) as s:
            _refresh_prices_via_api(con)    # upserts into price_rds
            s.update(label="Prices updated ✅", state="complete")
        st.rerun()

# =========================
# RDS: helpers + filters (mirrors EBS)
# =========================
def _rds_distinct(col, src="rds_with_size"):
    try:
        rows = con.execute(
            f"SELECT DISTINCT {col} FROM {src} WHERE {col} IS NOT NULL ORDER BY 1"
        ).fetchall()
        return [r[0] for r in rows if r and r[0] is not None]
    except Exception:
        return []

def _rds_q(x: str) -> str:
    return str(x).replace("'", "''")

def rds_where_for_view(view_name: str, base: str = "1=1") -> str:
    """Apply only filters the target view actually exposes (like your EBS helper)."""
    cols = set(con.execute(f"SELECT * FROM {view_name} LIMIT 0").fetchdf().columns)
    wc = [base]

    # BA
    sel_ba = st.session_state.get("rds_ba", "(all)")
    if "business_area" in cols and sel_ba != "(all)":
        wc.append(f"business_area = '{_rds_q(sel_ba)}'")

    # Region
    sel_region = st.session_state.get("rds_region", "(all)")
    if "region" in cols and sel_region != "(all)":
        wc.append(f"region = '{_rds_q(sel_region)}'")

    # Env (prod/nonprod)
    sel_env = st.session_state.get("rds_env", "All")
    if "env_guess" in cols and sel_env != "All":
        wc.append(f"env_guess = '{_rds_q(sel_env.lower())}'")

    # Family (db.r5, db.t3, …)
    fams = st.session_state.get("rds_families", [])
    fam_opts = st.session_state.get("rds_family_opts", [])
    if fams and fam_opts and len(fams) != len(fam_opts):
        if "family" in cols:
            wc.append("family IN (" + ", ".join([f"'{_rds_q(f)}'" for f in fams]) + ")")
        elif "current_class" in cols:
            fam_list = ", ".join([f"'{_rds_q(f)}'" for f in fams])
            wc.append(f"REGEXP_EXTRACT(current_class, '^((db\\.[^.]+))\\..+$', 1) IN ({fam_list})")

    # CPU %
    lo_cpu, hi_cpu = st.session_state.get("rds_cpu", (0, 100))
    if "avg_cpu_14d" in cols:
        wc.append(f"avg_cpu_14d BETWEEN {int(lo_cpu)} AND {int(hi_cpu)}")

    # Hours (handles either 'hours' or 'current_hours')
    lo_h, hi_h = st.session_state.get("rds_hours", (0, 800))
    if "hours" in cols:
        wc.append(f"hours BETWEEN {int(lo_h)} AND {int(hi_h)}")
    elif "current_hours" in cols:
        wc.append(f"current_hours BETWEEN {int(lo_h)} AND {int(hi_h)}")

    # Min cost
    min_cost = float(st.session_state.get("rds_min_cost", 0.0))
    if "cost_usd" in cols:
        wc.append(f"cost_usd >= {min_cost}")
    elif "current_cost_usd" in cols:
        wc.append(f"current_cost_usd >= {min_cost}")
    elif "total_cost_usd" in cols:
        wc.append(f"total_cost_usd >= {min_cost}")

    # Account search (optional)
    acct_q = st.session_state.get("rds_acct_search", "")
    if acct_q and "account_id" in cols:
        wc.append(f"CAST(account_id AS VARCHAR) ILIKE '%{_rds_q(acct_q)}%'")

    return " AND ".join(wc)

def _rds_defaults():
    # Options
    ba_opts     = ["(all)"] + _rds_distinct("business_area", "rds_clean")
    region_opts = ["(all)"] + _rds_distinct("region", "rds_clean")
    try:
        fam_opts = _rds_distinct("family", "rds_with_size")
        if not fam_opts:
            fam_opts = sorted(set([
                r[0] for r in con.execute("""
                    SELECT DISTINCT REGEXP_EXTRACT(current_class, '^((db\\.[^.]+))\\..+$', 1) AS fam
                    FROM rds_clean WHERE current_class IS NOT NULL
                """).fetchall() if r and r[0]
            ]))
    except Exception:
        fam_opts = []
    st.session_state["rds_family_opts"] = fam_opts

    # Ranges
    _max_cpu   = int(con.execute("SELECT COALESCE(CEIL(MAX(avg_cpu_14d)),100) FROM rds_clean").fetchone()[0] or 100)
    _max_hours = int(con.execute("SELECT COALESCE(CEIL(MAX(hours)),0) FROM rds_clean").fetchone()[0] or 0)
    _max_cost  = float(con.execute("SELECT COALESCE(MAX(cost_usd),0) FROM rds_clean").fetchone()[0] or 0.0)

    return {
        "rds_ba": "(all)",
        "rds_region": "(all)",
        "rds_env": "All",                    # All | prod | nonprod
        "rds_families": list(fam_opts),      # default to all
        "rds_cpu": (0, min(100, _max_cpu)),
        "rds_hours": (0, max(720, _max_hours)),  # include 24x7 range comfortably
        "rds_min_cost": 0.0,
        "rds_acct_search": "",
    }

def render_rds_filters():
    """Put this in the sidebar 'Filters — RDS' expander (same as EBS)."""
    if "rds_initialized" not in st.session_state:
        st.session_state.update(_rds_defaults())
        st.session_state["rds_initialized"] = True

    if st.button("Reset RDS Filters", use_container_width=True):
        st.session_state.update(_rds_defaults())
        st.rerun()

    ba_opts     = ["(all)"] + _rds_distinct("business_area", "rds_clean")
    region_opts = ["(all)"] + _rds_distinct("region", "rds_clean")
    fam_opts    = st.session_state.get("rds_family_opts") or _rds_defaults()["rds_families"]

    st.selectbox("Business Area", ba_opts,
                 index=ba_opts.index(st.session_state["rds_ba"]) if st.session_state["rds_ba"] in ba_opts else 0,
                 key="rds_ba")
    st.selectbox("Region", region_opts,
                 index=region_opts.index(st.session_state["rds_region"]) if st.session_state["rds_region"] in region_opts else 0,
                 key="rds_region")
    st.radio("Environment", ["All", "prod", "nonprod"],
             index=["All","prod","nonprod"].index(st.session_state["rds_env"]),
             horizontal=True, key="rds_env")
    st.multiselect("Instance Family (db.*)", fam_opts,
                   default=st.session_state["rds_families"] or fam_opts, key="rds_families")
    st.slider("CPU (avg 14d, %)", 0, 100,
              value=st.session_state["rds_cpu"], key="rds_cpu")
    data_max_h = int(con.execute("SELECT COALESCE(CEIL(MAX(hours)),0) FROM rds_clean").fetchone()[0] or 0)
    st.slider("Hours in month", 0, max(720, data_max_h),
              value=st.session_state["rds_hours"], key="rds_hours")
    st.slider("Min monthly cost (USD)", 0.0, float(max(1000.0, st.session_state.get("rds_min_cost", 0.0), 100.0)),
              value=float(st.session_state["rds_min_cost"]), step=5.0, key="rds_min_cost")
    st.text_input("Account ID contains (optional)", value=st.session_state["rds_acct_search"], key="rds_acct_search")

# =========================
# RDS: tabs (mirrors your EBS structure)
# =========================
def _rds_show_q(q: str):
    st.caption(q)
    st.dataframe(con.execute(q).fetchdf(), hide_index=True, use_container_width=True)

def _rds_filter_hint(view_name: str):
    cols = set(con.execute(f"SELECT * FROM {view_name} LIMIT 0").fetchdf().columns)
    badges = []
    if "business_area" in cols and st.session_state.get("rds_ba") != "(all)":
        badges.append(f"BA={st.session_state['rds_ba']}")
    if "region" in cols and st.session_state.get("rds_region") != "(all)":
        badges.append(f"Region={st.session_state['rds_region']}")
    if "env_guess" in cols and st.session_state.get("rds_env") != "All":
        badges.append(f"Env={st.session_state['rds_env']}")
    fams = st.session_state.get("rds_families") or []
    fam_opts = st.session_state.get("rds_family_opts") or []
    if ("family" in cols or "current_class" in cols) and fams and fam_opts and len(fams) != len(fam_opts):
        badges.append(f"Families={len(fams)}")
    if "avg_cpu_14d" in cols:
        lo, hi = st.session_state.get("rds_cpu", (0,100)); badges.append(f"CPU={lo}-{hi}%")
    if "hours" in cols or "current_hours" in cols:
        lo, hi = st.session_state.get("rds_hours", (0,720)); badges.append(f"Hrs={lo}-{hi}")
    if "cost_usd" in cols or "current_cost_usd" in cols or "total_cost_usd" in cols:
        mc = float(st.session_state.get("rds_min_cost", 0.0))
        if mc > 0: badges.append(f"Min$={mc:.0f}")
    if badges:
        st.caption("Active filters: " + " • ".join(badges))

def render_rds_section():
    st.markdown("### RDS Analysis")

    tabA, tabB, tabC, tabD, tabE = st.tabs([
        "Overview", "Rightsizing", "Scheduling", "Utilization", "Recommended Actions (ranked)"
    ])

    # 1) OVERVIEW
    with tabA:
        c1, c2 = st.columns(2)
        with c1:
            v = "rds_by_ba_region"
            q = f"SELECT * FROM {v} WHERE {rds_where_for_view(v)} ORDER BY total_cost_usd DESC"
            st.subheader("By Business Area × Region", divider=False)
            _rds_filter_hint(v); _rds_show_q(q)
        with c2:
            v = "rds_by_class"
            q = f"SELECT * FROM {v} WHERE {rds_where_for_view(v)} ORDER BY total_cost_usd DESC"
            st.subheader("By Class (family.size)", divider=False)
            _rds_filter_hint(v); _rds_show_q(q)

    # 2) RIGHTSIZING
    with tabB:
        sub1, sub2 = st.tabs(["Downsize (CPU 5–10%)", "Upsize (CPU ≥90%)"])
        with sub1:
            v = "rds_rightsize_next_smaller_priced"
            q = f"""
            SELECT business_area, region, db_id, current_class, recommended_class,
                   hours, cost_usd, avg_cpu_14d, est_monthly_savings_usd
            FROM {v}
            WHERE {rds_where_for_view(v)}
            ORDER BY est_monthly_savings_usd DESC NULLS LAST
            LIMIT 1000
            """
            _rds_filter_hint(v); _rds_show_q(q.strip())
        with sub2:
            v = "rds_rightsize_next_larger_priced"
            q = f"""
            SELECT business_area, region, db_id, current_class, recommended_class,
                   hours, cost_usd, avg_cpu_14d, est_monthly_delta_usd
            FROM {v}
            WHERE {rds_where_for_view(v)}
            ORDER BY est_monthly_delta_usd DESC NULLS LAST, avg_cpu_14d DESC
            LIMIT 1000
            """
            _rds_filter_hint(v); _rds_show_q(q.strip())

    # 3) SCHEDULING (off-hours)
    with tabC:
        v = "rds_offhours_candidates"
        q = f"""
        SELECT business_area, region, db_id, current_class, env_guess,
               current_hours, current_cost_usd, est_monthly_savings_usd
        FROM {v}
        WHERE {rds_where_for_view(v)}
        ORDER BY est_monthly_savings_usd DESC
        LIMIT 1000
        """
        st.subheader("Off-hours candidates (nonprod & ~24×7)", divider=False)
        _rds_filter_hint(v); _rds_show_q(q.strip())

    # 4) UTILIZATION
    with tabD:
        c1, c2 = st.columns(2)
        with c1:
            v = "rds_kill_merge"
            q = f"""
            SELECT business_area, region, db_id, current_class,
                   avg_cpu_14d, hours, cost_usd, est_monthly_savings_usd, confidence, reason
            FROM {v}
            WHERE {rds_where_for_view(v)}
            ORDER BY est_monthly_savings_usd DESC NULLS LAST, cost_usd DESC NULLS LAST
            LIMIT 1000
            """
            st.subheader("Kill/Merge (CPU < 5%)", divider=False)
            _rds_filter_hint(v); _rds_show_q(q.strip())
        with c2:
            v = "rds_high_utilization"
            q = f"""
            SELECT business_area, region, db_id, current_class,
                   avg_cpu_14d, hours, cost_usd
            FROM {v}
            WHERE {rds_where_for_view(v)}
            ORDER BY avg_cpu_14d DESC, cost_usd DESC
            LIMIT 1000
            """
            st.subheader("Hot DBs (CPU ≥ 90%)", divider=False)
            _rds_filter_hint(v); _rds_show_q(q.strip())

    # 5) RECOMMENDED ACTIONS
    with tabE:
        v = "rds_actions_ranked"
        q = f"""
        SELECT action, business_area, region, db_id, current_class,
               est_delta_usd, avg_cpu_14d, hours, cost_usd, reason, confidence
        FROM {v}
        WHERE {rds_where_for_view(v)}
        ORDER BY priority DESC, est_delta_usd DESC NULLS LAST, cost_usd DESC NULLS LAST
        LIMIT 2000
        """
        _rds_filter_hint(v); _rds_show_q(q.strip())
