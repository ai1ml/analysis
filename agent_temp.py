pip install openai>=1.40.0

LLM_PROVIDER = "openai"
OPENAI_API_KEY = "sk-your-key"
# (optional)
# LLM_PROVIDER = "gemini"
# GEMINI_API_KEY = "your-gemini-key"

from agent_tab import render_agent_tab

def set_agent_dependencies(conn, ebs_where, rds_where, ec2_where, snap_where):
    global con, ebs_where_for_view, rds_where_for_view, ec2_where_for_view, snap_where_for_view
    con = conn
    ebs_where_for_view = ebs_where
    rds_where_for_view = rds_where
    ec2_where_for_view = ec2_where
    snap_where_for_view = snap_where


from agent_tab import render_agent_tab, set_agent_dependencies
set_agent_dependencies(
    con,
    ebs_where_for_view,
    rds_where_for_view,
    ec2_where_for_view,
    snap_where_for_view,
)

ANALYSES = ["EBS", "EC2: OnDemand", "EC2: Reserved (RI)", "Snapshots", "RDS", "Agent (Beta)"]
analysis = st.radio("Choose analysis", ANALYSES, horizontal=True, key="analysis_choice_main")


elif analysis == "Agent (Beta)":
    render_agent_tab()


show_reload = analysis != "Agent (Beta)"
if show_reload:
    # draw your reload buttons here…
    ...


if analysis == "Agent (Beta)":
    st.caption("Tip: Ask things like “Top EC2 off-hours in us-east-1 > $100” or “RDS rightsizing <10% CPU in BA2”.")


