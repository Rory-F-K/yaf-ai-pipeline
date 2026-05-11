"""
Stage 3 — Validation
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

REPORT_PATH       = PROJECT_ROOT / "rules" / "validated" / "report.json"
ALL_ENTITIES_PATH = PROJECT_ROOT / "rules" / "extracted" / "all_entities.json"

st.set_page_config(page_title="Validation", page_icon=None, layout="wide")

CHECK_LABELS = {
    "entity_structure":  "Entity Structure",
    "entity_type":       "Entity Type",
    "services_present":  "Services Present",
    "service_structure": "Service Structure",
    "duplicate_types":   "Duplicate Types",
}
CHECK_DESC = {
    "entity_structure":  "Required top-level fields present and non-empty",
    "entity_type":       "entity_type is 'airline' or 'airport'",
    "services_present":  "Services array exists and is non-empty",
    "service_structure": "Each service has type, bilingual description ≥20 chars",
    "duplicate_types":   "No duplicate service types within the same entity",
}
SEV_ICON = {"error": "E", "warning": "W", "info": "I"}

# ── Load data ──────────────────────────────────────────────────────────────────

report   = load_json(REPORT_PATH)
summary  = (report or {}).get("summary", {})
issues   = (report or {}).get("issues", [])

# ── Session state ──────────────────────────────────────────────────────────────

for k, v in [("s3_out", ""), ("s3_rc", None), ("s3_run", False)]:
    if k not in st.session_state:
        st.session_state[k] = v

# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    if report:
        st.markdown("### Filter Issues")
        sev_filter   = st.multiselect("Severity", ["error", "warning", "info"],
                                      default=["error", "warning"],
                                      label_visibility="collapsed")
        check_filter = st.multiselect(
            "Check type",
            list(summary.keys()),
            default=list(summary.keys()),
            format_func=lambda k: CHECK_LABELS.get(k, k),
            label_visibility="collapsed",
        )
        failed_only = st.toggle("Show failed entities only", value=False)
        st.divider()
    else:
        sev_filter   = ["error", "warning"]
        check_filter = []
        failed_only  = False

    st.markdown("### Run")
    run_btn     = st.button("Run Stage 3", type="primary", use_container_width=True,
                            disabled=st.session_state.s3_run)
    sb_status   = st.empty()
    sb_progress = st.empty()

    if st.session_state.s3_rc == 0:
        sb_status.caption("✅ Last run completed successfully.")
    elif st.session_state.s3_rc is not None:
        sb_status.caption(f"Last run failed (code {st.session_state.s3_rc}).")

# ── Handle run ─────────────────────────────────────────────────────────────────

if run_btn:
    st.session_state.s3_run = True
    st.session_state.s3_out = ""
    final_rc = 0

    for item in run_stage_streaming("validate"):
        if len(item) == 4:
            prog, stat, output, final_rc = item
        else:
            prog, stat, output = item
        sb_progress.progress(min(prog, 1.0), text=stat)
        sb_status.caption(f"Running: {stat}")

    st.session_state.s3_out = output
    st.session_state.s3_rc  = final_rc
    st.session_state.s3_run = False
    sb_status.caption("✅ Complete." if final_rc == 0 else f"Error (code {final_rc}).")
    st.rerun()

# ── Header ─────────────────────────────────────────────────────────────────────

st.title("3. Validation")
st.caption(
    "Every extracted entity (airline or airport) passes through five independent quality checks. "
    "Entities that fail are excluded from the final output. Only clean, verified entities move forward."
)
st.divider()

# ── No data state ──────────────────────────────────────────────────────────────

if report is None:
    st.info("Validation hasn't been run yet. Use **Run Stage 3** in the sidebar.")
    st.stop()

# Build entity lookup from all_entities.json for detail inspection
all_entities  = load_json(ALL_ENTITIES_PATH) or []
entity_lookup = {e.get("entity_name") or e.get("source_id"): e for e in all_entities}

total   = report.get("total", 0)
passed  = report.get("passed", 0)
error_n = sum(1 for i in issues if i.get("severity") == "error")
warn_n  = sum(1 for i in issues if i.get("severity") == "warning")

# ── Metric cards ──────────────────────────────────────────────────────────────

excluded  = total - passed
pass_rate = f"{round(passed / total * 100)}%" if total else "N/A"

_cards = [
    {
        "value": str(total),
        "label": "Entities Checked",
        "detail": "Total entities evaluated",
        "bg": "#f7f9fc", "border": "#d0dae4", "vc": "#1a1a1a", "dc": "#777",
    },
    {
        "value": str(passed),
        "label": "Entities Accepted",
        "detail": f"{pass_rate} pass rate",
        "bg": "#f0faf4", "border": "#9fcfb0", "vc": "#1d6a3a", "dc": "#4a8a60",
    },
    {
        "value": str(excluded),
        "label": "Entities Excluded",
        "detail": "Failed quality checks",
        "bg": "#fff3f3" if excluded else "#f0faf4",
        "border": "#f5a0a0" if excluded else "#9fcfb0",
        "vc": "#8b1a1a" if excluded else "#1d6a3a",
        "dc": "#c0504d" if excluded else "#4a8a60",
    },
    {
        "value": str(len(issues)),
        "label": "Issues Found",
        "detail": f"{error_n} errors · {warn_n} warnings" if issues else "No issues",
        "bg": "#fff3f3" if error_n else ("#fffbea" if warn_n else "#f0faf4"),
        "border": "#f5a0a0" if error_n else ("#f0d080" if warn_n else "#9fcfb0"),
        "vc": "#8b1a1a" if error_n else ("#7a5c00" if warn_n else "#1d6a3a"),
        "dc": "#c0504d" if error_n else ("#b8860b" if warn_n else "#4a8a60"),
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
st.divider()

# ── Five checks overview ───────────────────────────────────────────────────────

st.markdown("#### Quality Checks")
check_cols = st.columns(5)
for col, (check, sevs) in zip(check_cols, summary.items()):
    with col:
        e = sevs.get("error", 0)
        w = sevs.get("warning", 0)
        bg = "#fff3f3" if e else ("#fffbea" if w else "#f0faf4")
        border = "#f5a0a0" if e else ("#f0d080" if w else "#9fcfb0")
        lines = "".join(
            f"<div style='font-size:0.72rem'>{SEV_ICON[s]} {s}: <b>{sevs[s]}</b></div>"
            for s in ("error", "warning", "info") if sevs.get(s, 0)
        ) or "<div style='font-size:0.72rem;color:#4caf50'>✓ No issues</div>"
        st.markdown(
            f"""<div style="background:{bg};border:1.5px solid {border};border-radius:8px;
                padding:12px;min-height:100px;">
                <div style="font-weight:700;font-size:0.85rem">{CHECK_LABELS.get(check, check)}</div>
                <div style="font-size:0.7rem;color:#777;margin-bottom:6px">{CHECK_DESC.get(check,'')}</div>
                {lines}
            </div>""",
            unsafe_allow_html=True,
        )

st.divider()

# ── Issues table ───────────────────────────────────────────────────────────────

error_ids = {i.get("entity_id") for i in issues if i.get("severity") == "error"}

if failed_only:
    failed_entities = [e for e in all_entities
                       if (e.get("entity_name") or e.get("source_id")) in error_ids]
else:
    shown_issues = [
        i for i in issues
        if i.get("severity") in sev_filter and i.get("check") in check_filter
    ]

if not failed_only:
    st.caption(f"**Issues** - {len(shown_issues)} of {len(issues)} shown. Click a row to inspect the entity.")

    if shown_issues:
        df_i = pd.DataFrame([
            {
                "":        SEV_ICON.get(i.get("severity"), ""),
                "Entity":  i.get("entity_id"),
                "Check":   CHECK_LABELS.get(i.get("check"), i.get("check")),
                "Severity":i.get("severity"),
                "Issue":   i.get("message"),
            }
            for i in shown_issues
        ])
        ev = st.dataframe(
            df_i, use_container_width=True, hide_index=True,
            selection_mode="single-row", on_select="rerun",
            column_config={
                "":        st.column_config.TextColumn(width="small"),
                "Entity":  st.column_config.TextColumn(width="small"),
                "Severity":st.column_config.TextColumn(width="small"),
                "Issue":   st.column_config.TextColumn(width="large"),
            },
        )
        sel = ev.selection.rows if ev.selection else []
        if sel:
            issue  = shown_issues[sel[0]]
            entity = entity_lookup.get(issue.get("entity_id"))
            if entity:
                with st.container(border=True):
                    h1, h2 = st.columns([4, 1])
                    h1.markdown(f"### {entity.get('entity_name', issue.get('entity_id'))}")
                    h2.markdown(f"`{entity.get('entity_type','')}`")
                    c1, c2, c3 = st.columns(3)
                    c1.markdown(f"**Type:** {entity.get('entity_type','')}")
                    c2.markdown(f"**Services:** {len(entity.get('services', []))}")
                    c3.markdown(f"**Issue:** {SEV_ICON.get(issue.get('severity'),'')} {issue.get('severity')}")
                    st.markdown("---")
                    st.caption(f"Flagged by **{CHECK_LABELS.get(issue.get('check'), issue.get('check'))}** check: {issue.get('message')}")
            else:
                st.info(f"Entity `{issue.get('entity_id')}` detail not available.")
    else:
        st.success("No issues match the current filters.")

else:
    st.caption(f"**Excluded entities** - {len(error_ids)} entities with errors")
    df_fail = pd.DataFrame([
        {
            "Entity": e.get("entity_name") or e.get("source_id"),
            "Type":   e.get("entity_type"),
            "Services": len(e.get("services", [])),
        }
        for e in failed_entities
    ])
    st.dataframe(df_fail, use_container_width=True, hide_index=True)

# ── Output log ─────────────────────────────────────────────────────────────────

if st.session_state.s3_out:
    with st.expander("Output log"):
        out = st.session_state.s3_out
        st.code(out[-3000:] if len(out) > 3000 else out, language="text")
