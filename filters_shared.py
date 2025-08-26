# filters_shared.py
import streamlit as st

con = None
def set_connection(conn):  # call this once from main after you create `con`
    global con; con = conn

def _cols(view: str) -> set[str]:
    try:
        return set(con.execute(f"SELECT * FROM {view} LIMIT 0").fetchdf().columns)
    except Exception:
        return set()

def _q(x: str) -> str:
    return str(x).replace("'", "''")

def rds_where_for_view(view_name: str, base: str = "1=1") -> str:
    cols = _cols(view_name)
    wc = [base]

    # BA (supports BA or business_area)
    ba = st.session_state.get("rds_ba", "(all)")
    if ba != "(all)":
        if "BA" in cols:
            wc.append(f"BA = '{_q(ba)}'")
        elif "business_area" in cols:
            wc.append(f"business_area = '{_q(ba)}'")

    # Region
    region = st.session_state.get("rds_region", "(all)")
    if region != "(all)" and "region" in cols:
        wc.append(f"region = '{_q(region)}'")

    # CPU range
    cpu_lo, cpu_hi = st.session_state.get("rds_cpu", (0, 100))
    for c in ("avg_cpu_14d", "cpu_pct", "fourteen_day_average_cpu_utilization"):
        if c in cols:
            wc.append(f"{c} BETWEEN {float(cpu_lo)} AND {float(cpu_hi)}")
            break

    # Hours range
    hrs_lo, hrs_hi = st.session_state.get("rds_hours", (0, 720))
    for c in ("hours", "usage_quantity_hours"):
        if c in cols:
            wc.append(f"{c} BETWEEN {float(hrs_lo)} AND {float(hrs_hi)}")
            break

    # Min cost
    min_cost = float(st.session_state.get("rds_min_cost", 0.0) or 0.0)
    for c in ("monthly_cost_usd", "total_cost_usd", "cost_usd", "current_cost_usd"):
        if c in cols:
            wc.append(f"{c} >= {min_cost}")
            break

    # Account text search
    acct_like = st.session_state.get("rds_acct_search", "")
    if acct_like and "linked_account_id" in cols:
        wc.append(f"CAST(linked_account_id AS VARCHAR) ILIKE '%{_q(acct_like)}%'")

    return " AND ".join(wc)
