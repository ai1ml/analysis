# agent_tab.py (drop-in)
import os, re, json, uuid
from types import SimpleNamespace
import pandas as pd
import streamlit as st

# -------------------------------------------------------
# 0) Provider & keys (OpenAI default, Gemini supported)
# -------------------------------------------------------
PROVIDER       = st.secrets.get("LLM_PROVIDER", os.getenv("LLM_PROVIDER", "openai")).lower()
OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY", ""))
GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY", os.getenv("GEMINI_API_KEY", ""))

# Assumes a global DuckDB connection `con` already exists in your app.
# Also assumes (if available) these WHERE builders:
#   ebs_where_for_view(view, base="1=1")
#   rds_where_for_view(view, base="1=1")
#   ec2_where_for_view(view, base="1=1")
#   snap_where_for_view(view, base="1=1")

# -------------------------------------------------------
# 1) Small cache for last results (export)
# -------------------------------------------------------
RESULT_CACHE = {}
def _cache_df(df: pd.DataFrame) -> str:
    rid = str(uuid.uuid4())
    RESULT_CACHE[rid] = df
    return rid

# -------------------------------------------------------
# 2) DuckDB helpers
# -------------------------------------------------------
def _exists(obj: str) -> bool:
    try:
        con.execute(f"SELECT * FROM {obj} LIMIT 0")
        return True
    except Exception:
        return False

def _cols(obj: str) -> list[str]:
    try:
        return list(con.execute(f"SELECT * FROM {obj} LIMIT 0").fetchdf().columns)
    except Exception:
        return []

def _first_existing_view(candidates: list[str]) -> str | None:
    for v in candidates:
        if _exists(v):
            return v
    return None

def _find_first_col(view: str, candidates: list[str]) -> str | None:
    cs = {c.lower(): c for c in _cols(view)}
    for c in candidates:
        if c.lower() in cs:
            return cs[c.lower()]
    return None

# cost-column resolver (try common names)
_COST_CANDIDATES = ["total_cost_usd", "monthly_cost_usd", "cost_usd", "public_cost_usd"]
def _pick_cost_col(view: str) -> str | None:
    return _find_first_col(view, _COST_CANDIDATES)

# -------------------------------------------------------
# 3) Filter bridge (use your builders if present)
# -------------------------------------------------------
def _view_where(view: str, filters: dict | None = None) -> str:
    name = (view or "").lower()
    try:
        if name.startswith("ebs_")   and "ebs_where_for_view" in globals():  return ebs_where_for_view(view, base="1=1")
        if name.startswith("rds_")   and "rds_where_for_view" in globals():  return rds_where_for_view(view, base="1=1")
        if name.startswith("ec2_")   and "ec2_where_for_view" in globals():  return ec2_where_for_view(view, base="1=1")
        if name.startswith("snap")   and "snap_where_for_view" in globals(): return snap_where_for_view(view, base="1=1")
    except Exception:
        pass
    return "1=1"

# -------------------------------------------------------
# 4) Tools (safe, schema-aware)
# -------------------------------------------------------
_SELECT_ONLY = re.compile(r"^\s*select\b", re.IGNORECASE | re.DOTALL)

def tool_list_views(prefix: str | None = None):
    q = "SELECT table_name FROM information_schema.tables WHERE table_type='VIEW'"
    if prefix:
        q += f" AND LOWER(table_name) LIKE '{prefix.lower()}%'"
    names = [r[0] for r in con.execute(q).fetchall()]
    return {"status":"ok", "views": names}

def tool_get_schema(name: str):
    if not _exists(name):
        return {"status":"error", "message": f"view '{name}' not found"}
    cols = _cols(name)
    n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    return {"status":"ok", "name": name, "columns": cols, "rows": int(n or 0)}

def tool_run_view(name: str, filters: dict | None = None, limit: int = 500):
    if not _exists(name):
        return {"status":"error", "message": f"view '{name}' not found"}
    where = _view_where(name, filters or {})
    q = f"SELECT * FROM {name} WHERE {where} LIMIT {int(limit)}"
    df = con.execute(q).fetchdf()
    rid = _cache_df(df)
    return {"status":"ok", "effective_sql": q, "row_count": len(df), "result_id": rid,
            "columns": list(df.columns), "preview": df.to_dict(orient="records")}

def tool_run_sql_select(sql: str, limit: int = 500):
    if not _SELECT_ONLY.match(sql or ""):
        return {"status":"error", "message": "Only SELECT queries are allowed."}
    q = f"SELECT * FROM ({sql}) t LIMIT {int(limit)}"
    try:
        df = con.execute(q).fetchdf()
    except Exception as e:
        return {"status":"error", "message": str(e), "effective_sql": q}
    rid = _cache_df(df)
    return {"status":"ok", "effective_sql": q, "row_count": len(df), "result_id": rid,
            "columns": list(df.columns), "preview": df.to_dict(orient="records")}

def _top_group_cost(view: str, group_col_candidates: list[str], limit: int = 5):
    if not _exists(view):
        return {"status":"error", "message": f"view '{view}' not found"}
    group_col = _find_first_col(view, group_col_candidates)
    cost_col  = _pick_cost_col(view)
    if not group_col or not cost_col:
        return {"status":"error", "message": f"Columns not found. Available: {', '.join(_cols(view))}"}
    where = _view_where(view)
    sql = f"""
        SELECT {group_col} AS grp, SUM({cost_col}) AS total_cost_usd
        FROM {view}
        WHERE {where}
        GROUP BY 1
        ORDER BY total_cost_usd DESC
        LIMIT {int(limit)}
    """
    df = con.execute(sql).fetchdf()
    rid = _cache_df(df)
    return {"status":"ok", "effective_sql": sql.strip(), "row_count": len(df), "result_id": rid,
            "columns": list(df.columns), "preview": df.to_dict(orient="records")}

def tool_top_ba_cost(service: str | None = None, limit: int = 5):
    s = (service or "").lower()
    if s in ("", "rds"):
        v = _first_existing_view(["rds_by_ba_region", "rds_usage"])
        return _top_group_cost(v, ["business_area","BA"], limit) if v else {"status":"error","message":"No RDS view found"}
    if s == "ebs":
        v = _first_existing_view(["ebs_by_ba","ebs_norm"])
        return _top_group_cost(v, ["business_area","BA"], limit) if v else {"status":"error","message":"No EBS view found"}
    if s in ("ec2","ec2: ondemand","ondemand"):
        v = _first_existing_view(["ec2_ops_ba_summary","ec2_ops_usage"])
        return _top_group_cost(v, ["business_area","BA"], limit) if v else {"status":"error","message":"No EC2 view found"}
    return {"status":"error", "message": f"Unsupported service '{service}'"}

def tool_top_region_cost(service: str | None = None, limit: int = 5):
    s = (service or "").lower()
    if s in ("", "rds"):
        v = _first_existing_view(["rds_by_ba_region", "rds_usage"])
    elif s == "ebs":
        v = _first_existing_view(["ebs_by_account_type", "ebs_norm"])
    else:  # ec2
        v = _first_existing_view(["ec2_ops_usage","ec2_ops_ba_summary"])
    return _top_group_cost(v, ["region"], limit) if v else {"status":"error","message":"No suitable view found"}

def tool_top_account_cost(service: str | None = None, limit: int = 5):
    s = (service or "").lower()
    if s in ("", "rds"):
        v = _first_existing_view(["rds_usage"])
    elif s == "ebs":
        v = _first_existing_view(["ebs_by_account_type","ebs_norm"])
    else:
        v = _first_existing_view(["ec2_ops_usage"])
    return _top_group_cost(v, ["account_id","linked_account_id","account"], limit) if v else {"status":"error","message":"No suitable view found"}

def tool_top_actions(service: str | None = None, limit: int = 200):
    parts = []
    if _exists("rds_actions_ranked"):      parts.append("SELECT *,'rds'  as _svc FROM rds_actions_ranked")
    if _exists("ebs_actions_explain"):     parts.append("SELECT *,'ebs'  as _svc FROM ebs_actions_explain")
    if _exists("ec2_ops_actions_ranked"):  parts.append("SELECT *,'ec2'  as _svc FROM ec2_ops_actions_ranked")
    if not parts:
        return {"status":"error","message":"No actions view found"}
    base = " UNION ALL ".join(parts)
    where = f" WHERE LOWER(_svc) = '{service.lower()}' " if service else ""
    q = f"SELECT * FROM ({base}) a {where} ORDER BY est_monthly_savings_usd DESC NULLS LAST LIMIT {int(limit)}"
    df = con.execute(q).fetchdf()
    rid = _cache_df(df)
    return {"status":"ok","effective_sql":q,"row_count":len(df),"result_id":rid,
            "columns":list(df.columns),"preview":df.to_dict(orient="records")}

def tool_explain_view(name: str):
    summaries = {
        "rds_by_ba_region": "RDS cost & CPU by Business Area × Region.",
        "rds_rightsize_next_smaller": "Our RDS rightsizing to next smaller (with price deltas).",
        "rds_actions_ranked": "Ranked RDS actions (kill/merge, downsize, off-hours).",
        "ebs_by_ba": "EBS spend by Business Area.",
        "ebs_actions_explain": "Ranked EBS actions (unattached, long-idle, gp2→gp3, io1 downgrade).",
        "ec2_ops_actions_ranked": "Ranked EC2 actions (schedule, rightsize, spot).",
        "snapshots_archive_opportunity": "Snapshots that could be archived with estimated savings.",
    }
    if not _exists(name):
        return {"status":"error","message":f"view '{name}' not found"}
    return {"status":"ok","name":name,"summary":summaries.get(name,"No summary available.")}

def tool_export(result_id: str, fmt: str = "csv"):
    if result_id not in RESULT_CACHE:
        return {"status":"error","message":"Unknown result_id"}
    df = RESULT_CACHE[result_id]
    if fmt == "csv":
        return {"status":"ok","format":"csv","content": df.to_csv(index=False)}
    if fmt in ("md","markdown"):
        return {"status":"ok","format":"markdown","content": df.head(200).to_markdown(index=False)}
    return {"status":"error","message":"Unsupported format"}

# -------------------------------------------------------
# 5) Tool schemas (for function calling)
# -------------------------------------------------------
TOOLS = [
    {"name":"list_views", "description":"List available DuckDB views", "parameters":{"type":"object","properties":{"prefix":{"type":"string"}},"required":[]}},
    {"name":"get_schema","description":"Get schema + row count for a view", "parameters":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}},
    {"name":"run_view",  "description":"Run a curated view with safe filters", "parameters":{"type":"object","properties":{"name":{"type":"string"},"filters":{"type":"object"},"limit":{"type":"integer"}},"required":["name"]}},
    {"name":"run_sql_select","description":"Run read-only SELECT (guarded)", "parameters":{"type":"object","properties":{"sql":{"type":"string"},"limit":{"type":"integer"}},"required":["sql"]}},
    {"name":"top_ba_cost","description":"Top Business Areas by cost for a service (rds|ebs|ec2)", "parameters":{"type":"object","properties":{"service":{"type":"string"},"limit":{"type":"integer"}},"required":[]}},
    {"name":"top_region_cost","description":"Top Regions by cost for a service", "parameters":{"type":"object","properties":{"service":{"type":"string"},"limit":{"type":"integer"}},"required":[]}},
    {"name":"top_account_cost","description":"Top Accounts by cost for a service", "parameters":{"type":"object","properties":{"service":{"type":"string"},"limit":{"type":"integer"}},"required":[]}},
    {"name":"top_actions","description":"Get top actions across services", "parameters":{"type":"object","properties":{"service":{"type":"string"},"limit":{"type":"integer"}},"required":[]}},
    {"name":"explain_view","description":"Explain what a view does", "parameters":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}},
    {"name":"export","description":"Export last result as CSV/Markdown", "parameters":{"type":"object","properties":{"result_id":{"type":"string"},"fmt":{"type":"string"}},"required":["result_id"]}},
]

# -------------------------------------------------------
# 6) System prompt (tight & practical)
# -------------------------------------------------------
SYSTEM_PROMPT = """You are a FinOps assistant over DuckDB.

Follow this method:
1) Discover before acting: use list_views (with prefixes like 'rds_', 'ebs_', 'ec2_') and get_schema(view).
2) Use only real columns from get_schema. Never invent names.
3) Prefer curated views; if aggregation is needed, run_sql_select with correct columns.
4) For common questions (e.g., 'which BA costs most?'), call top_ba_cost/top_region_cost/top_account_cost.
5) Final answer format:
   - 1–2 line takeaway,
   - the exact SQL executed (if any),
   - data preview table (<=500 rows).

If a view/column does not exist, say so briefly and continue with the closest available alternative."""

# -------------------------------------------------------
# 7) LLM caller (OpenAI or Gemini) returning OpenAI-shaped object
# -------------------------------------------------------
def _call_llm(messages, tools):
    """
    Returns an object with:
      resp.choices[0].message.content (str)
      resp.choices[0].message.tool_calls -> list of {"type":"function","function":{"name","arguments"}}
    """
    if PROVIDER == "openai":
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        tool_spec = [{"type":"function","function":t} for t in tools]
        return client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            tools=tool_spec,
            tool_choice="auto",
            temperature=0.2,
        )

    # ---- Gemini branch ----
    import google.generativeai as genai
    genai.configure(api_key=GEMINI_API_KEY)

    func_decls = tools  # Gemini accepts OpenAPI-like JSON schemas
    gem_tools = [{"function_declarations": func_decls}]

    system_text = "\n".join(m["content"] for m in messages if m.get("role")=="system")
    user_texts = [m["content"] for m in messages if m.get("role")!="system"]
    user_blob  = "\n\n".join(user_texts).strip() or " "

    model = genai.GenerativeModel(
        model_name="gemini-1.5-pro",
        tools=gem_tools,
        system_instruction=system_text or None,
    )

    resp = model.generate_content(
        user_blob,
        tool_config={"function_calling_config":"AUTO"},
        generation_config={"temperature":0.2},
    )

    # Adapt Gemini response to OpenAI-like
    tool_calls, text_chunks = [], []
    try:
        cand = resp.candidates[0]
        parts = getattr(cand, "content", None) and getattr(cand.content, "parts", []) or []
        for p in parts:
            if getattr(p, "text", None):
                text_chunks.append(p.text)
            fc = getattr(p, "function_call", None)
            if fc:
                tool_calls.append({
                    "type": "function",
                    "function": {"name": fc.name, "arguments": json.dumps(dict(fc.args or {}))}
                })
    except Exception:
        if hasattr(resp, "text") and resp.text:
            text_chunks.append(resp.text)

    content = "\n".join([t for t in text_chunks if t]).strip()
    message = SimpleNamespace(content=content, tool_calls=tool_calls)
    choice  = SimpleNamespace(message=message)
    return SimpleNamespace(choices=[choice])

# -------------------------------------------------------
# 8) UI: Agent chat
# -------------------------------------------------------
def _render_tool_output(out: dict):
    if out.get("status") != "ok":
        st.warning(out.get("message","Something went wrong."))
        return
    sql = out.get("effective_sql")
    if sql:
        st.caption("SQL executed")
        st.code(sql, language="sql")
    rid = out.get("result_id")
    if rid and rid in RESULT_CACHE:
        st.dataframe(RESULT_CACHE[rid], hide_index=True, use_container_width=True)

TOOL_IMPL = {
    "list_views":        lambda args: tool_list_views(**args),
    "get_schema":        lambda args: tool_get_schema(**args),
    "run_view":          lambda args: tool_run_view(**args),
    "run_sql_select":    lambda args: tool_run_sql_select(**args),
    "top_ba_cost":       lambda args: tool_top_ba_cost(**args),
    "top_region_cost":   lambda args: tool_top_region_cost(**args),
    "top_account_cost":  lambda args: tool_top_account_cost(**args),
    "top_actions":       lambda args: tool_top_actions(**args),
    "explain_view":      lambda args: tool_explain_view(**args),
    "export":            lambda args: tool_export(**args),
}

def render_agent_tab():
    st.markdown("### Agent (Beta)")
    if "agent_msgs" not in st.session_state:
        st.session_state.agent_msgs = [{"role":"system","content":SYSTEM_PROMPT}]

    # show history (hide system)
    for m in st.session_state.agent_msgs[1:]:
        with st.chat_message(m["role"]):
            st.markdown(m.get("content",""))

    user_q = st.chat_input("Ask about EC2/EBS/RDS/Snapshots savings…")
    if not user_q:
        return

    st.session_state.agent_msgs.append({"role":"user","content":user_q})
    with st.chat_message("user"):
        st.markdown(user_q)

    # up to 2 tool-calling rounds (robust, prevents loops)
    rounds = 0
    messages = st.session_state.agent_msgs
    while rounds < 2:
        rounds += 1
        reply = _call_llm(messages, TOOLS)
        msg   = reply.choices[0].message

        # Tool calls?
        if getattr(msg, "tool_calls", None):
            # display planning text (if any)
            plan = (msg.content or "").strip()
            if plan:
                with st.chat_message("assistant"):
                    st.markdown(plan)

            # execute tools
            tool_outputs = []
            for tc in msg.tool_calls:
                name = tc["function"]["name"]
                args = json.loads(tc["function"].get("arguments") or "{}")
                out  = TOOL_IMPL[name](args)

                with st.chat_message("assistant"):
                    _render_tool_output(out)

                tool_outputs.append({
                    "role":"tool",
                    "tool_call_id": name + "-" + str(uuid.uuid4())[:8],
                    "name": name,
                    "content": json.dumps(out)[:50000]
                })

            # feed back tool results and continue one more round
            messages = messages + tool_outputs
            continue

        # no tool calls → final answer
        final_text = msg.content or "(no content)"
        with st.chat_message("assistant"):
            st.markdown(final_text)
        st.session_state.agent_msgs.append({"role":"assistant","content":final_text})
        return

    # safety net if we exit loop without a final message
    with st.chat_message("assistant"):
        st.info("I’ve shared results above. Ask another question anytime!")
