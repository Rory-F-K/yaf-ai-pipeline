"""
Stage 1 — Scrape & Document Processing
"""
import sys
import json
from pathlib import Path

import streamlit as st
import pandas as pd

PROJECT_ROOT  = Path(__file__).parent.parent.parent
DASHBOARD_DIR = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(DASHBOARD_DIR))

from dashboard_utils import domain, run_stage_streaming

AGENTIC_DIR = PROJECT_ROOT / "chunk_store" / "agentic"

st.set_page_config(page_title="Scrape & Chunk", page_icon=None, layout="wide")

# ── Load sources ───────────────────────────────────────────────────────────────

try:
    from config import Config
    sources = Config.SOURCES
except Exception:
    sources = []

STATUS_ICON = {"Processed": "✅", "Partial": "~", "Pending": "-"}

def get_source_status(src_id: str) -> tuple[str, int]:
    agentic = AGENTIC_DIR / f"{src_id}.json"
    if agentic.exists():
        try:
            return "Processed", len(json.loads(agentic.read_text(encoding="utf-8")))
        except Exception:
            return "Processed", 0
    if (AGENTIC_DIR / f"{src_id}_partial.json").exists():
        return "Partial", 0
    return "Pending", 0

rows = []
for src in sources:
    src_id = src.get("id", "")
    status, n_chunks = get_source_status(src_id)
    rows.append({"label": src_id.replace("_", " ").title(),
                 "website": domain(src.get("url", "")),
                 "status": status, "segments": n_chunks})

processed_n  = sum(1 for r in rows if r["status"] == "Processed")
total_chunks = sum(r["segments"] for r in rows)
pending_n    = sum(1 for r in rows if r["status"] == "Pending")

# ── Session state ──────────────────────────────────────────────────────────────

for k, v in [("s1_out", ""), ("s1_rc", None), ("s1_run", False)]:
    if k not in st.session_state:
        st.session_state[k] = v

# ── Sidebar ────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### Run")
    run_btn     = st.button("Run Stage 1", type="primary", use_container_width=True,
                            disabled=st.session_state.s1_run)
    sb_status   = st.empty()
    sb_progress = st.empty()

    if st.session_state.s1_rc == 0:
        sb_status.caption("✅ Last run completed successfully.")
    elif st.session_state.s1_rc is not None:
        sb_status.caption(f"Last run failed (code {st.session_state.s1_rc}).")

# ── Handle run ─────────────────────────────────────────────────────────────────

if run_btn:
    st.session_state.s1_run = True
    st.session_state.s1_out = ""
    final_rc = 0

    for item in run_stage_streaming("chunk"):
        if len(item) == 4:
            prog, stat, output, final_rc = item
        else:
            prog, stat, output = item
        sb_progress.progress(min(prog, 1.0), text=stat)
        sb_status.caption(f"Running: {stat}")

    st.session_state.s1_out = output
    st.session_state.s1_rc  = final_rc
    st.session_state.s1_run = False
    sb_status.caption("✅ Complete." if final_rc == 0 else f"Error (code {final_rc}).")
    st.rerun()

# ── Header ─────────────────────────────────────────────────────────────────────

st.title("1. Scrape & Document Processing")
st.caption(
    "Fetches content from aviation websites and regulatory documents, "
    "then breaks them into small segments for the AI to process."
)
st.divider()

# ── Metrics ────────────────────────────────────────────────────────────────────

all_done = processed_n == len(sources) and len(sources) > 0

_cards = [
    {
        "value": f"{processed_n} / {len(sources)}",
        "label": "Sources Processed",
        "detail": "All done" if all_done else f"{len(sources) - processed_n} remaining",
        "bg": "#f0faf4" if all_done else "#f7f9fc",
        "border": "#9fcfb0" if all_done else "#d0dae4",
        "vc": "#1d6a3a" if all_done else "#1a1a1a",
        "dc": "#4a8a60" if all_done else "#777",
    },
    {
        "value": f"{total_chunks:,}",
        "label": "Document Segments",
        "detail": "Created from all sources",
        "bg": "#f7f9fc", "border": "#d0dae4", "vc": "#1a1a1a", "dc": "#777",
    },
    {
        "value": str(pending_n),
        "label": "Sources Pending",
        "detail": f"{pending_n} to run" if pending_n else "Nothing left to run",
        "bg": "#fffbea" if pending_n else "#f0faf4",
        "border": "#f0d080" if pending_n else "#9fcfb0",
        "vc": "#7a5c00" if pending_n else "#1d6a3a",
        "dc": "#b8860b" if pending_n else "#4a8a60",
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

# ── Source table ───────────────────────────────────────────────────────────────

if rows:
    df = pd.DataFrame([
        {
            "":        STATUS_ICON[r["status"]],
            "Source":  r["label"],
            "Website": r["website"],
            "Status":  r["status"],
            "Segments": f"{r['segments']:,}" if r["status"] == "Processed" else "-",
        }
        for r in rows
    ])
    st.dataframe(
        df, use_container_width=True, hide_index=True,
        column_config={
            "":         st.column_config.TextColumn(width="small"),
            "Segments": st.column_config.TextColumn(width="small"),
        },
    )
else:
    st.info("No sources configured.")

# ── Output log ─────────────────────────────────────────────────────────────────

if st.session_state.s1_out:
    with st.expander("Output log"):
        out = st.session_state.s1_out
        st.code(out[-3000:] if len(out) > 3000 else out, language="text")
