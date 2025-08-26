# --- replace the whole _call_llm with this ---
import os, json
from types import SimpleNamespace

def _call_llm(messages, tools):
    """
    Returns an OpenAI-shaped response so the rest of your code doesn't change:
      resp.choices[0].message.content (str)
      resp.choices[0].message.tool_calls (list of {"id","type":"function","function":{"name","arguments"}})
    """
    if PROVIDER.lower() == "openai":
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        # tools may already be wrapped as {"type":"function","function": {...}}
        tool_payload = [
            (t if ("type" in t and "function" in t) else {"type":"function","function":t})
            for t in (tools or [])
        ]
        return client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            tools=tool_payload,
            tool_choice="auto",
            temperature=0.2,
        )

    # -------- Gemini branch --------
    import google.generativeai as genai

    # Allow secrets or env var
    api_key = GEMINI_API_KEY or os.getenv("GEMINI_API_KEY")
    genai.configure(api_key=api_key)

    # 1) Convert OpenAI tool specs -> Gemini function_declarations
    func_decls = []
    if tools:
        for t in tools:
            if isinstance(t, dict) and "function" in t:   # {"type":"function","function":{...}}
                func_decls.append(t["function"])
            else:
                func_decls.append(t)
    gem_tools = [{"function_declarations": func_decls}] if func_decls else None

    # 2) Split system + user content (Gemini likes system_instruction)
    system_text = "\n".join(m["content"] for m in messages if m.get("role") == "system")
    # For tool responses (OpenAI style: role="tool"), we append a readable transcript
    user_chunks = []
    for m in messages:
        r = m.get("role")
        if r == "system":
            continue
        if r == "tool":
            name = m.get("name","tool")
            content = m.get("content","")
            user_chunks.append(f"[TOOL RESULT: {name}]\n{content}")
        else:
            user_chunks.append(m.get("content",""))
    user_text = "\n\n".join([x for x in user_chunks if x]).strip() or " "

    model = genai.GenerativeModel(
        model_name="gemini-1.5-pro",
        tools=gem_tools,
        system_instruction=system_text or None,
    )

    # 3) Ask Gemini with AUTO function calling
    resp = model.generate_content(
        user_text,
        tool_config={"function_calling_config": "AUTO"},
        generation_config={"temperature": 0.2},
    )

    # 4) Adapt Gemini response => OpenAI-shaped
    tool_calls = []
    text_chunks = []

    try:
        cand = resp.candidates[0]
        parts = getattr(cand.content, "parts", []) or []
        for p in parts:
            if getattr(p, "text", None):
                text_chunks.append(p.text)
            fc = getattr(p, "function_call", None)
            if fc:
                # Make it look like OpenAI's function tool call
                tool_calls.append({
                    "id": str(uuid.uuid4()),
                    "type": "function",
                    "function": {
                        "name": fc.name,
                        "arguments": json.dumps(dict(fc.args or {})),
                    }
                })
    except Exception:
        if hasattr(resp, "text") and resp.text:
            text_chunks.append(resp.text)

    content = "\n".join([t for t in text_chunks if t]).strip()
    message = SimpleNamespace(content=content, tool_calls=tool_calls)
    choice  = SimpleNamespace(index=0, message=message, finish_reason=None)
    out     = SimpleNamespace(id=None, model="gemini-1.5-pro", choices=[choice])
    return out


for tc in msg.tool_calls:
    # Support both OpenAI and our Gemini-adapted objects
    fn = getattr(tc, "function", None) or tc.get("function")
    name = getattr(fn, "name", None) or (fn.get("name") if isinstance(fn, dict) else None)
    arg_str = getattr(fn, "arguments", None) or (fn.get("arguments") if isinstance(fn, dict) else "{}")
    args = json.loads(arg_str or "{}")
    out = TOOL_IMPL[name](args)

    # Ensure we have a tool_call_id for the transcript
    tc_id = getattr(tc, "id", None) or tc.get("id") or str(uuid.uuid4())

    with st.chat_message("assistant"):
        st.markdown(f"**Tool:** `{name}`\n\n```json\n{json.dumps(out, indent=2)[:2000]}\n```")

    st.session_state.agent_msgs.append({
        "role":"tool",
        "tool_call_id": tc_id,
        "name": name,
        "content": json.dumps(out)
    })
