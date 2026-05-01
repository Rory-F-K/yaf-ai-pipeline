"""
Stage 2 — Rule Extraction
"""
import sys
from pathlib import Path

import streamlit as st
import pandas as pd

PROJECT_ROOT  = Path(__file__).parent.parent.parent
DASHBOARD_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(DASHBOARD_DIR))

from dashboard_utils import load_json, domain, run_stage_streaming

ALL_RULES_PATH = PROJECT_ROOT / "rules" / "extracted" / "all_rules.json"

st.set_page_config(page_title="Rule Extraction", page_icon=None, layout="wide")

# ── Load data ──────────────────────────────────────────────────────────────────

rules = load_json(ALL_RULES_PATH)

all_categories = sorted({r.get("category", "Unknown") for r in rules} if rules else [])
all_domains    = sorted({domain(r.get("source", "")) for r in rules} if rules else [])

# ── Session state ──────────────────────────────────────────────────────────────

for k, v in [("s2_out", ""), ("s2_rc", None), ("s2_run", False)]:
    if k not in st.session_state:
        st.session_state[k] = v

# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    if rules:
        st.markdown("### Filter")
        keyword  = st.text_input("Search", placeholder="Keyword in title or description…")
        sel_cats = st.multiselect("Category", all_categories, default=all_categories[:3])
        sel_doms = st.multiselect("Source",   all_domains,    default=all_domains[:3])
        st.caption(f"{len(rules)} rules total")
        st.divider()
    else:
        keyword  = ""
        sel_cats = []
        sel_doms = []

    st.markdown("### Run")
    run_btn     = st.button("Run Stage 2", type="primary", use_container_width=True,
                            disabled=st.session_state.s2_run)
    sb_status   = st.empty()
    sb_progress = st.empty()

    if st.session_state.s2_rc == 0:
        sb_status.caption("✅ Last run completed successfully.")
    elif st.session_state.s2_rc is not None:
        sb_status.caption(f"Last run failed (code {st.session_state.s2_rc}).")

# ── Live log placeholder (shown while running) ─────────────────────────────────

live_log = st.empty()

# ── Handle run ─────────────────────────────────────────────────────────────────

if run_btn:
    st.session_state.s2_run = True
    st.session_state.s2_out = ""
    final_rc = 0

    for item in run_stage_streaming("extract"):
        if len(item) == 4:
            prog, stat, output, final_rc = item
        else:
            prog, stat, output = item
        sb_progress.progress(min(prog, 1.0), text=stat)
        sb_status.caption(f"Running: {stat}")
        # Show last 60 lines of output live so the user can see what's happening
        lines = output.splitlines()
        live_log.code("\n".join(lines[-60:]), language="text")

    live_log.empty()
    st.session_state.s2_out = output
    st.session_state.s2_rc  = final_rc
    st.session_state.s2_run = False
    sb_status.caption("✅ Complete." if final_rc == 0 else f"Error (code {final_rc}).")
    st.rerun()

# ── Header ─────────────────────────────────────────────────────────────────────

st.title("2. Rule Extraction")
st.caption(
    "The AI reads every document segment and identifies specific accessibility rules, "
    "policies, and entitlements. Near-duplicate rules from different documents are "
    "automatically merged."
)
st.divider()

# ── No data state ──────────────────────────────────────────────────────────────

if rules is None:
    st.info("No rules extracted yet. Use **Run Stage 2** in the sidebar to extract rules from the processed documents.")
    st.stop()

# ── Metrics ────────────────────────────────────────────────────────────────────

n_sources = len({r.get("source") for r in rules})
n_cats    = len({r.get("category") for r in rules})

_cards = [
    {
        "value": f"{len(rules):,}",
        "label": "Rules Discovered",
        "detail": "Extracted by AI",
        "bg": "#f7f9fc", "border": "#d0dae4", "vc": "#1a1a1a", "dc": "#777",
    },
    {
        "value": str(n_sources),
        "label": "Sources Covered",
        "detail": "Unique origin websites",
        "bg": "#f7f9fc", "border": "#d0dae4", "vc": "#1a1a1a", "dc": "#777",
    },
    {
        "value": str(n_cats),
        "label": "Categories Found",
        "detail": "Rule topic areas",
        "bg": "#f7f9fc", "border": "#d0dae4", "vc": "#1a1a1a", "dc": "#777",
    },
]

cols = st.columns(3)
for col, card in zip(cols, _cards):
    col.markdown(
        f"""<div style="background:{card['bg']};border:1.5px solid {card['border']};
            border-radius:10px;padding:18px 12px;text-align:center;height:110px;
            display:flex;flex-direction:column;justify-content:center;gap:4px;">
            <div style="font-size:1.9rem;font-weight:700;color:{card['vc']};line-height:1">{card['value']}</div>
            <div style="font-weight:600;font-size:0.88rem;color:#1a1a1a;margin-top:2px">{card['label']}</div>
            <div style="font-size:0.72rem;color:{card['dc']}">{card['detail']}</div>
        </div>""",
        unsafe_allow_html=True,
    )

st.markdown("<br>", unsafe_allow_html=True)
st.divider()

# ── Filter + table ─────────────────────────────────────────────────────────────

q = keyword.lower().strip()
filtered = [
    r for r in rules
    if r.get("category") in sel_cats
    and domain(r.get("source", "")) in sel_doms
    and (not q or q in r.get("title","").lower() or q in r.get("description","").lower())
]

st.caption(f"Showing **{len(filtered)}** of {len(rules)} rules. Click a row to read the full rule.")

df = pd.DataFrame([
    {
        "ID":       r.get("rule_id"),
        "Category": r.get("category"),
        "Title":    r.get("title"),
        "Source":   domain(r.get("source", "")),
    }
    for r in filtered
])

event = st.dataframe(
    df, use_container_width=True, hide_index=True,
    selection_mode="single-row", on_select="rerun",
    column_config={"ID": st.column_config.TextColumn(width="small")},
)

# ── Rule detail ────────────────────────────────────────────────────────────────

sel = event.selection.rows if event.selection else []
if sel:
    rule = filtered[sel[0]]
    with st.container(border=True):
        h1, h2 = st.columns([4, 1])
        h1.markdown(f"### {rule.get('title')}")
        h2.markdown(f"`{rule.get('rule_id')}`")
        c1, c2 = st.columns(2)
        c1.markdown(f"**Category:** {rule.get('category')}")
        c2.markdown(f"**Source:** {domain(rule.get('source',''))}")
        st.markdown("---")
        st.write(rule.get("description"))
        with st.expander("Source URL"):
            st.code(rule.get("source",""), language=None)
else:
    st.caption("Select a row above to view the full rule.")

# ── Output log ─────────────────────────────────────────────────────────────────

if st.session_state.s2_out:
    with st.expander("Output log"):
        out = st.session_state.s2_out
        st.code(out[-3000:] if len(out) > 3000 else out, language="text")
