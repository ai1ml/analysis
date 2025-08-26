# agent_tab.py
import json, re, uuid
import pandas as pd
import streamlit as st

# assumes a global DuckDB connection `con` exists in your app
# and you already have WHERE builders like:
#   ebs_where_for_view, rds_where_for_view, ec2_where_for_view, snap_where_for_view

# ---------- Provider switch (OpenAI default) ----------
PROVIDER = st.secrets.get("LLM_PROVIDER", "openai")  # "openai" or "gemini"
OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY", "")
GEMINI_API_KEY = st.secrets.get("GEMINI_API_KEY", "")

# ---------- Small registry for last results (for export) ----------
RESULT_CACHE = {}

def _cache_df(df: pd.DataFrame) -> str:
    rid = str(uuid.uuid4())
    RESULT_CACHE[rid] = df
    return rid

# ---------- Utility ----------
def _exists(obj: str) -> bool:
    try:
        con.execute(f"SELECT * FROM {obj} LIMIT 0")
        return True
    except Exception:
        return False

def _cols(obj: str):
    try:
        return list(con.execute(f"SELECT * FROM {obj} LIMIT 0").fetchdf().columns)
    except Exception:
        return []

def _view_where(view: str, filters: dict) -> str:
    # route to the correct builder by service/view name
    name = view.lower()
    if name.startswith("ebs_"):
        return ebs_where_for_view(view, base="1=1")
    if name.startswith("rds_"):
        return rds_where_for_view(view, base="1=1")
    if name.startswith("ec2_"):
        return ec2_where_for_view(view, base="1=1")
    if name.startswith("snapshot"):
        return snap_where_for_view(view, base="1=1")
    # default (no extra filters)
    return "1=1"

# Guard: only allow SELECT
_SELECT_ONLY = re.compile(r"^\s*select\b", re.IGNORECASE | re.DOTALL)

# ---------- Tools the model can call ----------
def tool_list_views(prefix: str | None = None):
    q = "SELECT table_name FROM information_schema.tables WHERE table_type='VIEW'"
    if prefix:
        q += f" AND LOWER(table_name) LIKE '{prefix.lower()}%'"
    names = [r[0] for r in con.execute(q).fetchall()]
    return {"views": names}

def tool_get_schema(name: str):
    if not _exists(name):
        return {"error": f"view '{name}' not found"}
    cols = _cols(name)
    n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    return {"name": name, "columns": cols, "rows": int(n or 0)}

def tool_run_view(name: str, filters: dict | None = None, limit: int = 500):
    if not _exists(name):
        return {"error": f"view '{name}' not found"}
    where = _view_where(name, filters or {})
    q = f"SELECT * FROM {name} WHERE {where} LIMIT {int(limit)}"
    df = con.execute(q).fetchdf()
    rid = _cache_df(df)
    return {"effective_sql": q, "row_count": len(df), "result_id": rid, "columns": list(df.columns), "preview": df.to_dict(orient="records")}

def tool_run_sql_select(sql: str, limit: int = 500):
    if not _SELECT_ONLY.match(sql or ""):
        return {"error": "Only SELECT queries are allowed."}
    q = f"SELECT * FROM ({sql}) t LIMIT {int(limit)}"
    df = con.execute(q).fetchdf()
    rid = _cache_df(df)
    return {"effective_sql": q, "row_count": len(df), "result_id": rid, "columns": list(df.columns), "preview": df.to_dict(orient="records")}

def tool_top_actions(service: str | None = None, limit: int = 200):
    # union common actions views if they exist
    parts = []
    if _exists("rds_actions_ranked"):      parts.append("SELECT *,'rds'  as _svc FROM rds_actions_ranked")
    if _exists("ebs_actions_explain"):     parts.append("SELECT *,'ebs'  as _svc FROM ebs_actions_explain")
    if _exists("ec2_ops_actions_ranked"):  parts.append("SELECT *,'ec2'  as _svc FROM ec2_ops_actions_ranked")
    if not parts:
        return {"error": "No actions view found"}
    base = " UNION ALL ".join(parts)
    where = ""
    if service:
        where = f" WHERE LOWER(_svc) = '{service.lower()}' "
    q = f"SELECT * FROM ({base}) a {where} ORDER BY est_monthly_savings_usd DESC NULLS LAST LIMIT {int(limit)}"
    df = con.execute(q).fetchdf()
    rid = _cache_df(df)
    return {"effective_sql": q, "row_count": len(df), "result_id": rid, "columns": list(df.columns), "preview": df.to_dict(orient="records")}

def tool_refresh_prices_api():
    # optional: wire your existing price refresh here
    try:
        updated = refresh_rds_prices_from_aws()  # <- your function if present
        return {"status": "ok", "updated_rows": int(updated or 0)}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def tool_explain_view(name: str):
    # Plug your one-line summaries here
    summaries = {
        "rds_actions_ranked": "Ranked RDS actions (kill/merge, downsize, off-hours) with reason & confidence.",
        "ebs_actions_explain": "Ranked EBS actions (unattached, long-idle, gp2→gp3, io1 downgrade).",
        "ec2_ops_actions_ranked": "Ranked EC2 actions (schedule, rightsize, spot) de-duplicated.",
        "snapshots_archive_opportunity": "Snapshots that could be archived with estimated monthly savings.",
    }
    return {"name": name, "summary": summaries.get(name, "No summary available.")}

def tool_export(result_id: str, fmt: str = "csv"):
    if result_id not in RESULT_CACHE:
        return {"error": "Unknown result_id"}
    df = RESULT_CACHE[result_id]
    if fmt == "csv":
        return {"format": "csv", "content": df.to_csv(index=False)}
    if fmt in ("md", "markdown"):
        return {"format": "markdown", "content": df.head(200).to_markdown(index=False)}
    return {"error": "Unsupported format"}

# ---------- JSON schemas for tool calling ----------
TOOLS = [
    {"name": "list_views", "description": "List available DuckDB views", "parameters": {"type": "object","properties":{"prefix":{"type":"string"}},"required":[]}},
    {"name": "get_schema", "description": "Get column names and row count for a view", "parameters": {"type": "object","properties":{"name":{"type":"string"}},"required":["name"]}},
    {"name": "run_view", "description": "Run a curated view with safe filters", "parameters": {"type":"object","properties":{"name":{"type":"string"},"filters":{"type":"object"},"limit":{"type":"integer"}},"required":["name"]}},
    {"name": "run_sql_select", "description": "Run read-only SELECT (guarded)", "parameters": {"type":"object","properties":{"sql":{"type":"string"},"limit":{"type":"integer"}},"required":["sql"]}},
    {"name": "top_actions", "description": "Get top actions across services", "parameters": {"type":"object","properties":{"service":{"type":"string"},"limit":{"type":"integer"}},"required":[]}},
    {"name": "refresh_prices_api", "description": "Refresh RDS pricing from AWS", "parameters": {"type":"object","properties":{},"required":[]}},
    {"name": "explain_view", "description": "Explain what a view does", "parameters": {"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}},
    {"name": "export", "description": "Export last result as CSV/Markdown", "parameters": {"type":"object","properties":{"result_id":{"type":"string"},"fmt":{"type":"string"}},"required":["result_id"]}},
]

# ---------- System prompt ----------
SYSTEM_PROMPT = """You are a FinOps assistant over DuckDB.
Prefer curated views over raw SQL. Never invent columns.
When you answer: (1) short takeaway, (2) call tools to fetch data, (3) include the SQL used.
If a filter/column doesn't exist on a view, ignore it and proceed. Keep previews <= 500 rows.
"""

# ---------- Simple chat loop (OpenAI) ----------
def _call_llm(messages, tools):
    if PROVIDER == "openai":
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        return client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            tools=[{"type":"function","function":t} for t in tools],
            tool_choice="auto",
            temperature=0.2,
        )
    else:
        # Gemini alternative (pseudo; adapt to your gemini client)
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        # Use the Gemini function-calling equivalent here
        raise NotImplementedError("Gemini wiring placeholder")

# ---------- Dispatcher for tool calls ----------
TOOL_IMPL = {
    "list_views": lambda args: tool_list_views(**args),
    "get_schema": lambda args: tool_get_schema(**args),
    "run_view": lambda args: tool_run_view(**args),
    "run_sql_select": lambda args: tool_run_sql_select(**args),
    "top_actions": lambda args: tool_top_actions(**args),
    "refresh_prices_api": lambda args: tool_refresh_prices_api(),
    "explain_view": lambda args: tool_explain_view(**args),
    "export": lambda args: tool_export(**args),
}

# ---------- Streamlit UI ----------
def render_agent_tab():
    st.markdown("### Agent (Beta)")
    if "agent_msgs" not in st.session_state:
        st.session_state.agent_msgs = [{"role":"system","content":SYSTEM_PROMPT}]

    for m in st.session_state.agent_msgs[1:]:
        with st.chat_message(m["role"]):
            st.markdown(m.get("content",""))

    user_q = st.chat_input("Ask about EC2/EBS/RDS/Snapshots savings…")
    if not user_q: 
        return

    st.session_state.agent_msgs.append({"role":"user","content":user_q})
    with st.chat_message("user"):
        st.markdown(user_q)

    # one-step tool-calling loop
    reply = _call_llm(st.session_state.agent_msgs, TOOLS)
    msg = reply.choices[0].message

    # tool call?
    if msg.tool_calls:
        final_text = ""
        for tc in msg.tool_calls:
            name = tc.function.name
            args = json.loads(tc.function.arguments or "{}")
            out = TOOL_IMPL[name](args)
            # show tool output compactly
            with st.chat_message("assistant"):
                st.markdown(f"**Tool:** `{name}`\n\n```json\n{json.dumps(out, indent=2)[:2000]}\n```")
            st.session_state.agent_msgs.append({
                "role":"tool",
                "tool_call_id": tc.id,
                "name": name,
                "content": json.dumps(out)
            })

        # follow-up assistant message to summarize results
        reply2 = _call_llm(st.session_state.agent_msgs, TOOLS)
        msg2 = reply2.choices[0].message
        final_text = msg2.content or "(no content)"
        with st.chat_message("assistant"):
            st.markdown(final_text)
        st.session_state.agent_msgs.append({"role":"assistant","content":final_text})
    else:
        # plain answer
        with st.chat_message("assistant"):
            st.markdown(msg.content or "(no content)")
        st.session_state.agent_msgs.append({"role":"assistant","content":msg.content})

# call render_agent_tab() from your main app when radio== "Agent (Beta)"
