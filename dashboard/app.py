"""
Aviation Accessibility Pipeline — Home
Run with: streamlit run dashboard/app.py   (from project root)
"""
import sys
from pathlib import Path
from collections import Counter

import streamlit as st
import pandas as pd

PROJECT_ROOT  = Path(__file__).parent.parent
DASHBOARD_DIR = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(DASHBOARD_DIR))

from dashboard_utils import load_json, count_json_files, domain

st.set_page_config(
    page_title="Aviation Accessibility Pipeline",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Load state ─────────────────────────────────────────────────────────────────

all_rules   = load_json(PROJECT_ROOT / "rules" / "extracted"  / "all_rules.json")
clean_rules = load_json(PROJECT_ROOT / "rules" / "validated"  / "clean_rules.json")
report      = load_json(PROJECT_ROOT / "rules" / "validated"  / "report.json")
snapshot    = load_json(PROJECT_ROOT / "rules" / "snapshots"  / "snapshot.json")
agentic_n   = count_json_files(PROJECT_ROOT / "chunk_store"   / "agentic")

try:
    from config import Config
    sources   = Config.SOURCES
    n_sources = len({s["id"] for s in sources})
except Exception:
    sources   = []
    n_sources = 0

# ── Header ─────────────────────────────────────────────────────────────────────

st.markdown(
    "<h1 style='margin-bottom:4px'>Aviation Accessibility Pipeline</h1>"
    "<p style='color:#555;font-size:1.08rem;margin-top:0;margin-bottom:0'>"
    "Automated extraction, validation, and versioning of accessibility rules "
    "from airlines, airports, and regulatory bodies, powered by AI</p>",
    unsafe_allow_html=True,
)
st.divider()

# ── Pipeline stage cards ───────────────────────────────────────────────────────

STEPS = [
    ("1", "Scrape & Chunk", "Fetch and segment source content", "",   agentic_n > 0,          f"{agentic_n} sources done" if agentic_n else "Not yet run"),
    ("2", "Extract",        "AI identifies accessibility rules", "",   all_rules is not None,  f"{len(all_rules):,} rules found" if all_rules else "Not yet run"),
    ("3", "Validate",       "Quality checks on every rule",     "",   report is not None,     f"{report['passed']}/{report['total']} passed" if report else "Not yet run"),
    ("4", "Version",        "Track changes since last run",     "",   snapshot is not None,   "Changes recorded" if snapshot else "Not yet run"),
    ("5", "Database",       "Rules stored in Firestore",        "🗄️", None,                   "Live in Firestore"),
]

cols = st.columns(5)
for col, (num, label, sublabel, stage_icon, done, detail) in zip(cols, STEPS):
    with col:
        if done is None:
            icon, bg, border, tc = stage_icon, "#f7f9fc", "#d0dae4", "#555"
        elif done:
            icon, bg, border, tc = "✅", "#f0faf4", "#9fcfb0", "#1d6a3a"
        else:
            icon, bg, border, tc = stage_icon, "#fafafa", "#e0e0e0", "#aaa"

        st.markdown(
            f"""<div style="background:{bg};border:1.5px solid {border};border-radius:10px;
                padding:18px 12px;text-align:center;height:135px;
                display:flex;flex-direction:column;justify-content:center;gap:3px;">
                <div style="font-size:1.5rem;line-height:1">{icon}</div>
                <div style="font-weight:700;font-size:0.95rem;color:#1a1a1a;margin-top:4px">{num}. {label}</div>
                <div style="font-size:0.72rem;color:#777">{sublabel}</div>
                <div style="font-size:0.78rem;font-weight:600;color:{tc};margin-top:4px">{detail}</div>
            </div>""",
            unsafe_allow_html=True,
        )

st.markdown("<br>", unsafe_allow_html=True)

# ── Key metrics ────────────────────────────────────────────────────────────────

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Sources Monitored",  n_sources)
m2.metric("Documents Processed", agentic_n)
m3.metric("Rules Discovered",   f"{len(all_rules):,}"  if all_rules   else "-")
m4.metric("Rules Accepted",
          f"{len(clean_rules):,}" if clean_rules else "-",
          delta_color="inverse")
m5.metric("Quality Issues",     len(report["issues"]) if report else "-")

st.divider()

# ── About ──────────────────────────────────────────────────────────────────────

left, right = st.columns([3, 2])

with left:
    st.markdown("#### How it works")
    st.markdown(
        "This pipeline automatically monitors aviation accessibility sources: airline websites, "
        "airport portals, and regulatory documents (IATA guidelines, EU regulations, US DOT rules) "
        "and turns them into structured, queryable data.\n\n"
        "- **Scrape & Chunk**: fetches live content and splits it into segments\n"
        "- **Extract**: AI reads each segment and identifies specific accessibility rules\n"
        "- **Validate**: five quality checks ensure every rule is complete and well-formed\n"
        "- **Version**: detects new, updated, and unchanged rules across runs\n"
        "- **Database**: clean rules are stored in Firestore, ready for downstream use\n\n"
        "Navigate through each stage using the sidebar."
    )

with right:
    if clean_rules:
        st.markdown("#### Validated Rules by Category")
        cat_counts = Counter(r.get("category", "Unknown") for r in clean_rules)
        df_cat = (
            pd.DataFrame(cat_counts.most_common(), columns=["Category", "Rules"])
            .head(8)
        )
        st.dataframe(df_cat, use_container_width=True, hide_index=True)

# ── Source list ────────────────────────────────────────────────────────────────

if sources:
    st.divider()
    with st.expander("Monitored Sources"):
        df_s = pd.DataFrame([
            {
                "Source": s.get("id", "").replace("_", " ").title(),
                "Website": domain(s.get("url", "")),
                "Status":  "✅ Processed" if (PROJECT_ROOT / "chunk_store" / "agentic" / f"{s.get('id')}.json").exists() else "Pending",
            }
            for s in sources
        ])
        st.dataframe(df_s, use_container_width=True, hide_index=True)
