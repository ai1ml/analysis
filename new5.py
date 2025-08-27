def _call_llm(messages, tools):
    """
    messages: OpenAI-style [{"role":"system"/"user"/"assistant","content":"..."}]
    tools:    list of function specs (either [{"type":"function","function":{...}}] or [{name,description,parameters}])

    Returns OpenAI-shaped object:
      resp.choices[0].message.content (str)
      resp.choices[0].message.tool_calls (list of {"type":"function","function":{"name","arguments"}})
    """
    PROVIDER = os.getenv("LLM_PROVIDER", st.secrets.get("LLM_PROVIDER", "gemini")).lower()

    # ---------------- OpenAI branch ----------------
    if PROVIDER == "openai":
        from openai import OpenAI
        OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", st.secrets.get("OPENAI_API_KEY", ""))
        client = OpenAI(api_key=OPENAI_API_KEY)

        # Accept either raw function specs or OpenAI-wrapped ones
        openai_tools = []
        if tools:
            if isinstance(tools[0], dict) and "function" in tools[0]:
                openai_tools = [{"type": "function", "function": t["function"]} for t in tools]
            else:
                openai_tools = [{"type": "function", "function": t} for t in tools]

        return client.chat.completions.create(
            model="gpt-4o-mini",
            messages=messages,
            tools=openai_tools,
            tool_choice="auto",
            temperature=0.2,
        )

    # ---------------- Gemini branch ----------------
    import google.generativeai as genai
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", st.secrets.get("GEMINI_API_KEY", ""))
    genai.configure(api_key=GEMINI_API_KEY)

    # 1) Convert tools -> Gemini function_declarations
    func_decls = []
    if tools:
        for t in tools:
            func_decls.append(t["function"] if ("function" in t) else t)
    gem_tools = [{"function_declarations": func_decls}] if func_decls else None

    # 2) Split system vs user (Gemini prefers system_instruction)
    system_text = "\n".join(m["content"] for m in messages if m.get("role") == "system")
    user_text   = "\n\n".join(m["content"] for m in messages if m.get("role") != "system").strip() or " "

    model = genai.GenerativeModel(
        model_name="gemini-1.5-pro",
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
                tool_calls.append({
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

    # Build an OpenAI-like response object so the rest of your code works unchanged
    message = SimpleNamespace(content=content, tool_calls=tool_calls)
    choice  = SimpleNamespace(index=0, message=message, finish_reason=None)
    return SimpleNamespace(id=None, model="gemini-1.5-pro", choices=[choice])
