"""
Stage 4 — Versioning
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

CLEAN_RULES_PATH = PROJECT_ROOT / "rules" / "validated" / "clean_rules.json"
SNAPSHOT_PATH    = PROJECT_ROOT / "rules" / "snapshots"  / "snapshot.json"

st.set_page_config(page_title="Versioning", page_icon=None, layout="wide")

# ── Load data ──────────────────────────────────────────────────────────────────

clean_rules = load_json(CLEAN_RULES_PATH)
snapshot    = load_json(SNAPSHOT_PATH)

# ── Session state ──────────────────────────────────────────────────────────────

for k, v in [("s4_out", ""), ("s4_rc", None), ("s4_run", False)]:
    if k not in st.session_state:
        st.session_state[k] = v

# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### Show")
    show_new       = st.checkbox("New",       value=True)
    show_updated   = st.checkbox("Updated",   value=True)
    show_unchanged = st.checkbox("Unchanged", value=True)
    st.divider()

    st.markdown("### Run")
    run_btn     = st.button("Run Stage 4", type="primary", use_container_width=True,
                            disabled=st.session_state.s4_run)
    sb_status   = st.empty()
    sb_progress = st.empty()

    if st.session_state.s4_rc == 0:
        sb_status.caption("✅ Last run completed successfully.")
    elif st.session_state.s4_rc is not None:
        sb_status.caption(f"Last run failed (code {st.session_state.s4_rc}).")

# ── Handle run ─────────────────────────────────────────────────────────────────

if run_btn:
    st.session_state.s4_run = True
    st.session_state.s4_out = ""
    final_rc = 0

    for item in run_stage_streaming("version"):
        if len(item) == 4:
            prog, stat, output, final_rc = item
        else:
            prog, stat, output = item
        sb_progress.progress(min(prog, 1.0), text=stat)
        sb_status.caption(f"Running: {stat}")

    st.session_state.s4_out = output
    st.session_state.s4_rc  = final_rc
    st.session_state.s4_run = False
    sb_status.caption("✅ Complete." if final_rc == 0 else f"Error (code {final_rc}).")
    st.rerun()

# ── Header ─────────────────────────────────────────────────────────────────────

st.title("4. Versioning")
st.caption(
    "Compares the current rules against the previous run to track what's new, "
    "what changed, and what stayed the same. Each rule carries a version number "
    "that increments whenever its content is updated."
)
st.divider()

# ── No data state ──────────────────────────────────────────────────────────────

if clean_rules is None:
    st.info("No versioned rules yet. Use **Run Stage 4** in the sidebar.")
    st.stop()

# ── Classify rules ─────────────────────────────────────────────────────────────

prev_hashes = set((snapshot or {}).get("rules", {}).keys())

new_rules       = []
updated_rules   = []
unchanged_rules = []

for rule in clean_rules:
    h = rule.get("content_hash", "")
    v = rule.get("version", 1)
    if v > 1:
        updated_rules.append(rule)
    elif h not in prev_hashes:
        new_rules.append(rule)
    else:
        unchanged_rules.append(rule)

# ── Metrics ────────────────────────────────────────────────────────────────────

_cards = [
    {
        "value": str(len(clean_rules)),
        "label": "Total Clean Rules",
        "detail": "Passed validation",
        "bg": "#f7f9fc", "border": "#d0dae4", "vc": "#1a1a1a", "dc": "#777",
    },
    {
        "value": str(len(new_rules)),
        "label": "New",
        "detail": f"+{len(new_rules)} this run" if new_rules else "None this run",
        "bg": "#f0faf4" if new_rules else "#fafafa",
        "border": "#9fcfb0" if new_rules else "#e0e0e0",
        "vc": "#1d6a3a" if new_rules else "#aaa",
        "dc": "#4a8a60" if new_rules else "#bbb",
    },
    {
        "value": str(len(updated_rules)),
        "label": "Updated",
        "detail": "Version incremented" if updated_rules else "None changed",
        "bg": "#f0f4ff" if updated_rules else "#fafafa",
        "border": "#a0b4f0" if updated_rules else "#e0e0e0",
        "vc": "#1a3a8b" if updated_rules else "#aaa",
        "dc": "#4a6abf" if updated_rules else "#bbb",
    },
    {
        "value": str(len(unchanged_rules)),
        "label": "Unchanged",
        "detail": "Identical to last run",
        "bg": "#fafafa", "border": "#e0e0e0", "vc": "#555", "dc": "#999",
    },
]

cols = st.columns(4)
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

if snapshot:
    ts = str(snapshot.get("timestamp", ""))[:19]
    st.caption(f"Snapshot timestamp: {ts}")

st.divider()

# ── Change groups ──────────────────────────────────────────────────────────────

def rule_df(rule_list):
    return pd.DataFrame([
        {
            "Rule":     r.get("rule_id"),
            "Category": r.get("category"),
            "Title":    r.get("title"),
            "Source":   domain(r.get("source", "")),
            "Version":  r.get("version", 1),
        }
        for r in rule_list
    ])

if show_new:
    with st.expander(f"New rules ({len(new_rules)})", expanded=bool(new_rules)):
        if new_rules:
            st.caption("Rules that appear for the first time in this pipeline run.")
            st.dataframe(rule_df(new_rules), use_container_width=True, hide_index=True,
                         column_config={"Rule": st.column_config.TextColumn(width="small"),
                                        "Version": st.column_config.NumberColumn(width="small")})
        else:
            st.info("No new rules in this run.")

if show_updated:
    with st.expander(f"Updated rules ({len(updated_rules)})", expanded=bool(updated_rules)):
        if updated_rules:
            st.caption("Rules whose content has changed since a previous run. Version number has been incremented.")
            st.dataframe(rule_df(updated_rules), use_container_width=True, hide_index=True,
                         column_config={"Rule": st.column_config.TextColumn(width="small"),
                                        "Version": st.column_config.NumberColumn(width="small")})
        else:
            st.info("No updated rules in this run.")

if show_unchanged:
    with st.expander(f"Unchanged rules ({len(unchanged_rules)})"):
        if unchanged_rules:
            st.caption("Rules with identical content to the previous run.")
            st.dataframe(rule_df(unchanged_rules), use_container_width=True, hide_index=True,
                         column_config={"Rule": st.column_config.TextColumn(width="small"),
                                        "Version": st.column_config.NumberColumn(width="small")})
        else:
            st.info("No unchanged rules.")

# ── Output log ─────────────────────────────────────────────────────────────────

if st.session_state.s4_out:
    st.divider()
    with st.expander("Output log"):
        out = st.session_state.s4_out
        st.code(out[-3000:] if len(out) > 3000 else out, language="text")
