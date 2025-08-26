# one-step tool-calling loop (OpenAI + Gemini)
reply = _call_llm(st.session_state.agent_msgs, TOOLS)

# -------- normalize the model reply into (content, tool_calls) ----------
if PROVIDER == "openai":
    # OpenAI format
    msg = reply.choices[0].message
    content = msg.content or ""
    tool_calls = getattr(msg, "tool_calls", None) or []
else:
    # Gemini format
    cand = reply.candidates[0]
    content_parts, tool_calls = [], []
    for part in cand.content.parts:
        # parts can be text or function_call
        if hasattr(part, "function_call") and part.function_call:
            tool_calls.append(part.function_call)
        elif hasattr(part, "text") and part.text:
            content_parts.append(part.text)
    content = "\n".join(content_parts).strip()

# --------------------- handle tool calls (if any) -----------------------
if tool_calls:
    for tc in tool_calls:
        # OpenAI vs Gemini argument shapes
        if PROVIDER == "openai":
            name = tc.function.name
            args = json.loads(tc.function.arguments or "{}")
        else:
            name = tc.name
            # tc.args may already be a dict (preferred) or a JSON string
            if isinstance(tc.args, dict):
                args = tc.args
            else:
                try:
                    args = json.loads(tc.args or "{}")
                except Exception:
                    args = {}

        # run tool safely
        out = {}
        try:
            impl = TOOL_IMPL.get(name)
            out = impl(args) if impl else {"error": f"unknown tool '{name}'"}
        except Exception as e:
            out = {"error": f"Tool '{name}' failed."}

        # record tool result (quietly; don't dump huge JSON to user)
        st.session_state.agent_msgs.append({
            "role": "tool",
            "name": name,
            "content": json.dumps(out)
        })

        # small inline note to the user
        with st.chat_message("assistant"):
            if "error" in out:
                st.info(f"Ran tool `{name}` but hit an issue. I’ll still summarize what I found.")
            else:
                st.caption(f"Ran tool `{name}` ✓")

    # follow-up assistant message to summarize tool outputs
    reply2 = _call_llm(st.session_state.agent_msgs, TOOLS)
    if PROVIDER == "openai":
        final_text = reply2.choices[0].message.content or "(no content)"
    else:
        parts = [p.text for p in reply2.candidates[0].content.parts if hasattr(p, "text")]
        final_text = ("\n".join(parts)).strip() or "(no content)"

    with st.chat_message("assistant"):
        st.markdown(final_text)
    st.session_state.agent_msgs.append({"role": "assistant", "content": final_text})

else:
    # no tool calls: plain answer
    with st.chat_message("assistant"):
        st.markdown(content or "(no content)")
    st.session_state.agent_msgs.append({"role": "assistant", "content": content})


reply = _call_llm(st.session_state.agent_msgs, TOOLS)

if PROVIDER == "openai":
    msg = reply.choices[0].message
    tool_calls = getattr(msg, "tool_calls", None)
    content = msg.content
else:  # gemini
    cand = reply.candidates[0]
    tool_calls = []
    content_parts = []
    for part in cand.content.parts:
        if hasattr(part, "function_call"):
            tool_calls.append(part.function_call)
        elif hasattr(part, "text"):
            content_parts.append(part.text)
    content = "\n".join(content_parts).strip() if content_parts else None

# If there are tool calls
if tool_calls:
    for tc in tool_calls:
        name = tc.name
        args = {a.key: json.loads(a.value) if a.value else None for a in tc.args}
        out = TOOL_IMPL[name](args)
        with st.chat_message("assistant"):
            st.markdown(f"**Tool:** `{name}`\n\n```json\n{json.dumps(out, indent=2)[:2000]}\n```")
        st.session_state.agent_msgs.append({"role":"tool","name":name,"content":json.dumps(out)})

    # follow-up summarization
    reply2 = _call_llm(st.session_state.agent_msgs, TOOLS)
    if PROVIDER == "openai":
        final_text = reply2.choices[0].message.content
    else:
        parts = [p.text for p in reply2.candidates[0].content.parts if hasattr(p,"text")]
        final_text = "\n".join(parts).strip()
    with st.chat_message("assistant"):
        st.markdown(final_text or "(no content)")
    st.session_state.agent_msgs.append({"role":"assistant","content":final_text})
else:
    # plain answer
    with st.chat_message("assistant"):
        st.markdown(content or "(no content)")
    st.session_state.agent_msgs.append({"role":"assistant","content":content})


