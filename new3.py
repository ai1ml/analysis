# --- View name resolver / aliases ---
VIEW_ALIASES = {
    # RDS
    "rds_cost_by_business_area": "rds_by_ba_region",
    "rds_by_business_area":      "rds_by_ba_region",
    # EBS
    "ebs_cost_by_business_area": "ebs_by_ba",
    "ebs_by_business_area":      "ebs_by_ba",
}

def _all_views():
    return [r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_type='VIEW'"
    ).fetchall()]

def _resolve_view(name: str) -> str:
    n = (name or "").strip()
    if not n:
        return n
    n_l = n.lower()
    # 1) alias map
    if n_l in VIEW_ALIASES:
        return VIEW_ALIASES[n_l]
    # 2) substring fallback
    for v in _all_views():
        if n_l in v.lower():
            return v
    return n  # unchanged if no match


def tool_run_view(name: str, filters: dict | None = None, limit: int = 500):
    orig = name
    name = _resolve_view(name)
    if not _exists(name):
        return {
            "status": "error",
            "error": f"view '{orig}' not found",
            "friendly": "I couldn't find that view. I can list available views or try: 'show RDS by business area' or 'show EBS by BA'."
        }
    where = _view_where(name, filters or {})
    q = f"SELECT * FROM {name} WHERE {where} LIMIT {int(limit)}"
    df = con.execute(q).fetchdf()
    rid = _cache_df(df)
    return {
        "status": "ok",
        "effective_sql": q,
        "row_count": len(df),
        "result_id": rid,
        "columns": list(df.columns),
        "preview": df.to_dict(orient="records")
    }



with st.chat_message("assistant"):
    if isinstance(out, dict) and out.get("status") == "error":
        st.markdown(
            f"**{name}**: {out.get('friendly') or 'Sorry, I could not complete that action.'}"
        )
    elif isinstance(out, dict) and "preview" in out:
        st.markdown(f"**{name}** ran. Showing a preview ({out.get('row_count', 0)} rows; SQL below).")
        try:
            import pandas as pd
            st.dataframe(pd.DataFrame(out["preview"]).head(50), hide_index=True, use_container_width=True)
        except Exception:
            pass
        if "effective_sql" in out:
            st.caption(out["effective_sql"])
    else:
        st.markdown(f"**{name}** completed.")




SYSTEM_PROMPT = """
You are a FinOps assistant over DuckDB.
Prefer curated views and these canonical names:
- RDS by BA/Region: rds_by_ba_region
- EBS by BA: ebs_by_ba
- EC2 actions: ec2_ops_actions_ranked
Never invent columns. When answering: (1) short takeaway, (2) call tools, (3) include SQL.
If a requested view name doesn't exist, try a close match or list available views.
Keep previews <= 500 rows.
"""
