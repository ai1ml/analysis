def _call_llm(messages, tools):
    """
    messages: OpenAI-style [{"role":"system"/"user"/"assistant","content":"..."}]
    tools:    list of function specs (either [{"type":"function","function":{...}}] or raw {name,description,parameters})

    Returns OpenAI-shaped object:
      resp.choices[0].message.content (str)
      resp.choices[0].message.tool_calls (list of {"type":"function","function":{"name","arguments"}})
    """
    provider = os.getenv("LLM_PROVIDER", st.secrets.get("LLM_PROVIDER", "gemini")).lower()

    # ---------------- OpenAI branch ----------------
    if provider == "openai":
        from openai import OpenAI
        openai_key = os.getenv("OPENAI_API_KEY", st.secrets.get("OPENAI_API_KEY", ""))
        client = OpenAI(api_key=openai_key)

        # Accept either raw function specs or OpenAI-wrapped ones
        if tools and isinstance(tools[0], dict) and "function" in tools[0]:
            openai_tools = [{"type": "function", "function": t["function"]} for t in tools]
        else:
            openai_tools = [{"type": "function", "function": t} for t in (tools or [])]

        return client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", st.secrets.get("OPENAI_MODEL", "gpt-4o-mini")),
            messages=messages,
            tools=openai_tools,
            tool_choice="auto",
            temperature=0.2,
        )

    # ---------------- Gemini branch ----------------
    import google.generativeai as genai

    gemini_key   = os.getenv("GEMINI_API_KEY",  st.secrets.get("GEMINI_API_KEY", ""))
    gemini_model = os.getenv("GEMINI_MODEL",    st.secrets.get("GEMINI_MODEL", "gemini-1.5-pro"))
    genai.configure(api_key=gemini_key)

    # 1) Convert tools -> Gemini function_declarations
    func_decls = []
    if tools:
        for t in tools:
            func_decls.append(t["function"] if (isinstance(t, dict) and "function" in t) else t)
    gem_tools = [{"function_declarations": func_decls}] if func_decls else None

    # 2) Split system vs user (Gemini prefers system_instruction)
    system_text = "\n".join(m["content"] for m in messages if m.get("role") == "system")
    # Include tool outputs in the user transcript so Gemini sees context
    user_chunks = []
    for m in messages:
        r = m.get("role")
        if r == "system":
            continue
        if r == "tool":
            name = m.get("name", "tool")
            content = m.get("content", "")
            user_chunks.append(f"[TOOL RESULT: {name}]\n{content}")
        else:
            user_chunks.append(m.get("content", ""))
    user_text = "\n\n".join([x for x in user_chunks if x]).strip() or " "

    model = genai.GenerativeModel(
        model_name=gemini_model,
        tools=gem_tools,
        system_instruction=system_text or None,
    )

    # 3) Ask model (AUTO tool-calling)
    resp = model.generate_content(
        user_text,
        tool_config={"function_calling_config": "AUTO"},
        safety_settings=None,
        generation_config={"temperature": 0.2},
    )

    # 4) Normalize Gemini output -> OpenAI-shaped
    tool_calls, text_chunks = [], []

    if hasattr(resp, "candidates") and resp.candidates:
        cand  = resp.candidates[0]
        parts = getattr(getattr(cand, "content", None), "parts", []) or []
        for p in parts:
            fc = getattr(p, "function_call", None)
            if fc:
                tool_calls.append({
                    "id": str(uuid.uuid4()),
                    "type": "function",
                    "function": {
                        "name": fc.name,
                        "arguments": json.dumps(dict(fc.args or {})),
                    }
                })
            elif getattr(p, "text", None):
                text_chunks.append(p.text)
    elif hasattr(resp, "text") and resp.text:
        text_chunks.append(resp.text)

    content = "\n".join([t for t in text_chunks if t]).strip()

    # Build an OpenAI-like response object so downstream code stays unchanged
    message = SimpleNamespace(content=content, tool_calls=tool_calls)
    choice  = SimpleNamespace(index=0, message=message, finish_reason=None)
    return SimpleNamespace(id=None, model=gemini_model, choices=[choice])
