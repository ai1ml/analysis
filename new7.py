def tool_refresh_prices_api():
    """
    Refresh RDS pricing. Tries AWS API first (if you defined refresh_rds_prices_from_aws),
    otherwise falls back to re-seeding prices from observed CSV (seed_price_from_observed).
    Returns a small status dict for the chat UI.
    """
    # Prefer the API refresh if your project defines it
    if "refresh_rds_prices_from_aws" in globals():
        try:
            updated = refresh_rds_prices_from_aws()  # your function
            return {"status": "ok", "updated_rows": int(updated or 0), "source": "aws_api"}
        except Exception as e:
            return {"status": "error", "message": f"AWS pricing refresh failed: {e}"}

    # Fallback: seed from CSV-derived observations so views keep working
    if "seed_price_from_observed" in globals():
        try:
            seed_price_from_observed(con)  # your function
            # You may not know exact rows—return generic success
            return {"status": "ok", "updated_rows": None, "source": "csv_observed"}
        except Exception as e:
            return {"status": "error", "message": f"CSV seed failed: {e}"}

    return {"status": "error", "message": "No pricing refresh function is available in this app."}

TOOL_IMPL = {
    "list_views": lambda args: tool_list_views(**args),
    "get_schema": lambda args: tool_get_schema(**args),
    "run_view":   lambda args: tool_run_view(**args),
    "run_sql_select": lambda args: tool_run_sql_select(**args),
    "top_ba_cost":      lambda args: tool_top_ba_cost(**args),
    "top_region_cost":  lambda args: tool_top_region_cost(**args),
    "top_account_cost": lambda args: tool_top_account_cost(**args),
    "top_actions":      lambda args: tool_top_actions(**args),
    "explain_view":     lambda args: tool_explain_view(**args),
    "export":           lambda args: tool_export(**args),
    "refresh_prices_api": lambda args: tool_refresh_prices_api(),   # <- keep this line
}


{"name": "refresh_prices_api", "description": "Refresh RDS pricing from AWS", "parameters": {"type":"object","properties":{},"required":[]}},

# --- Optional: view alias map for friendlier names ---
VIEW_ALIAS = {
    # user-asked -> real view in DuckDB
    "rds_cost_by_business_area": "rds_by_ba_region",
    "ebs_cost_by_business_area": "ebs_by_ba",
    "ec2_actions": "ec2_ops_actions_ranked",
}

def _resolve_view(name: str) -> str:
    # exact hit
    if _exists(name):
        return name
    # alias match
    real = VIEW_ALIAS.get(name.lower())
    if real and _exists(real):
        return real
    # prefix fallback (e.g., user types 'rds_by_ba' and real is 'rds_by_ba_region')
    for v in [name + "_region", name + "_summary"]:
        if _exists(v):
            return v
    return name  # return original; caller will still validate with _exists



def tool_run_view(name: str, filters: dict | None = None, limit: int = 500):
    view = _resolve_view(name)  # <- added
    if not _exists(view):
        return {"status":"error", "message": f"view '{name}' not found"}
    where = _view_where(view, filters or {})
    q = f"SELECT * FROM {view} WHERE {where} LIMIT {int(limit)}"
    df = con.execute(q).fetchdf()
    rid = _cache_df(df)
    return {"status":"ok", "effective_sql": q, "row_count": len(df), "result_id": rid,
            "columns": list(df.columns), "preview": df.to_dict(orient="records")}


with st.expander("Admin: Pricing"):
    if st.button("🔄 Refresh RDS Prices (API/CSV fallback)", use_container_width=True):
        out = tool_refresh_prices_api()
        if out.get("status") == "ok":
            st.success(f"Prices refreshed from {out.get('source')} ✓")
        else:
            st.error(out.get("message","Failed to refresh prices"))


  
