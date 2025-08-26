# tool call?
if getattr(msg, "tool_calls", None):
    final_text = ""
    for tc in msg.tool_calls:
        # Support both OpenAI objects and Gemini dicts
        if hasattr(tc, "function"):
            # OpenAI-style
            name = tc.function.name
            arg_str = tc.function.arguments or "{}"
            tc_id = getattr(tc, "id", None) or str(uuid.uuid4())
        else:
            # Gemini-adapted dict
            f = tc.get("function", {}) if isinstance(tc, dict) else {}
            name = f.get("name")
            arg_str = f.get("arguments", "{}")
            tc_id = tc.get("id") if isinstance(tc, dict) else str(uuid.uuid4())

        args = json.loads(arg_str or "{}")
        out = TOOL_IMPL[name](args)

        # show tool output compactly
        with st.chat_message("assistant"):
            st.markdown(f"**Tool:** `{name}`\n\n```json\n{json.dumps(out, indent=2)[:2000]}\n```")

        # add tool result back into the conversation
        st.session_state.agent_msgs.append({
            "role": "tool",
            "tool_call_id": tc_id,
            "name": name,
            "content": json.dumps(out)
        })

    # follow-up assistant message to summarize results
    reply2 = _call_llm(st.session_state.agent_msgs, TOOLS)
    msg2 = reply2.choices[0].message
    final_text = getattr(msg2, "content", None) or "(no content)"
    with st.chat_message("assistant"):
        st.markdown(final_text)
    st.session_state.agent_msgs.append({"role": "assistant", "content": final_text})
else:
    # plain answer
    with st.chat_message("assistant"):
        st.markdown(getattr(msg, "content", None) or "(no content)")
    st.session_state.agent_msgs.append({"role": "assistant", "content": getattr(msg, "content", None)})
