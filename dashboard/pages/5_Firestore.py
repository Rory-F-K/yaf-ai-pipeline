"""
Stage 5 — Database (Firestore)
Live view of rules in Firebase + comparison with local pipeline output.
"""
import sys
import os
import json
from pathlib import Path
from collections import Counter

import streamlit as st
import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT     = Path(__file__).parent.parent.parent
DASHBOARD_DIR    = Path(__file__).parent.parent
CLEAN_RULES_PATH = PROJECT_ROOT / "rules" / "validated" / "clean_rules.json"

sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(DASHBOARD_DIR))
load_dotenv(PROJECT_ROOT / ".env")

from dashboard_utils import load_json, domain

st.set_page_config(page_title="Database", page_icon=None, layout="wide")

# ── Header ─────────────────────────────────────────────────────────────────────

st.title("5. Database")
st.caption(
    "Live view of rules stored in Firebase Firestore. "
    "Push is disabled in demo mode. Use the Compare tab to see what would be synced."
)
st.divider()

# ── Connection status ──────────────────────────────────────────────────────────

project_id  = os.getenv("FIRESTORE_PROJECT_ID")
creds_path  = os.getenv("FIREBASE_CREDENTIALS_PATH")
creds_ok    = Path(creds_path).exists() if creds_path else False

with st.container(border=True):
    c1, c2, c3 = st.columns(3)
    c1.markdown("**Project**")
    c1.code(project_id or "Not set", language=None)
    c2.markdown("**Credentials**")
    c2.code(("✅ Found" if creds_ok else "Not found") if creds_path else "Not set", language=None)
    c3.markdown("**Push**")
    c3.code("Disabled (demo mode)", language=None)

missing = [v for v, k in [("FIRESTORE_PROJECT_ID", project_id), ("FIREBASE_CREDENTIALS_PATH", creds_path)] if not k]
if missing or not creds_ok:
    if missing:
        st.error(f"Missing in `.env`: {', '.join(f'`{m}`' for m in missing)}")
    elif not creds_ok:
        st.error(f"Credentials file not found: `{creds_path}`")
    st.stop()

# ── Fetch from Firestore ───────────────────────────────────────────────────────

@st.cache_data(show_spinner="Connecting to Firebase…", ttl=60)
def fetch_db_rules() -> list:
    from firestore.client import FirestoreClient
    return FirestoreClient().fetch_all_rules()

r_col, s_col = st.columns([1, 4])
if r_col.button("Refresh", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

try:
    db_rules = fetch_db_rules()
except Exception as e:
    st.error(f"Could not connect to Firebase: {e}")
    st.stop()

s_col.caption(f"Connected · **{len(db_rules):,}** rules in database · auto-refreshes every 60 s")
st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab_db, tab_diff = st.tabs(["Database Rules", "Compare with Local"])

# ════════════════ TAB 1 — Database browse ═══════════════════════════════════

with tab_db:
    if not db_rules:
        st.info("No rules found in the database.")
        st.stop()

    categories = Counter(r.get("category","Unknown") for r in db_rules)
    sources_ctr = Counter(domain(r.get("source","")) for r in db_rules)

    m1, m2, m3 = st.columns(3)
    m1.metric("Rules in Database", f"{len(db_rules):,}")
    m2.metric("Categories",        len(categories))
    m3.metric("Sources",           len(sources_ctr))

    with st.sidebar:
        st.markdown("### Filter")
        keyword     = st.text_input("Search", placeholder="Title or description…")
        all_cats    = sorted(categories.keys())
        sel_cats    = st.multiselect("Category", all_cats,              default=all_cats[:3])
        all_domains = sorted(sources_ctr.keys())
        sel_domains = st.multiselect("Source",   all_domains,           default=all_domains[:3])

    q = keyword.lower().strip()
    filtered = [
        r for r in db_rules
        if r.get("category") in sel_cats
        and domain(r.get("source","")) in sel_domains
        and (not q or q in r.get("title","").lower() or q in r.get("description","").lower())
    ]

    st.caption(f"Showing **{len(filtered)}** of {len(db_rules)} rules. Click a row to view.")

    if filtered:
        df = pd.DataFrame([
            {
                "ID":        r.get("rule_id"),
                "Category":  r.get("category"),
                "Title":     r.get("title"),
                "Source":    domain(r.get("source","")),
                "Version":   r.get("version",""),
                "Pushed":    str(r.get("pushed_at",""))[:10],
            }
            for r in filtered
        ])
        ev = st.dataframe(
            df, use_container_width=True, hide_index=True,
            selection_mode="single-row", on_select="rerun",
            column_config={
                "ID":      st.column_config.TextColumn(width="small"),
                "Version": st.column_config.TextColumn(width="small"),
                "Pushed":  st.column_config.TextColumn(width="small"),
            },
        )
        sel = ev.selection.rows if ev.selection else []
        if sel:
            rule = filtered[sel[0]]
            with st.container(border=True):
                h1, h2 = st.columns([4, 1])
                h1.markdown(f"### {rule.get('title','')}")
                h2.markdown(f"`{rule.get('rule_id')}` v{rule.get('version','')}")
                c1, c2, c3 = st.columns(3)
                c1.markdown(f"**Category:** {rule.get('category','')}")
                c2.markdown(f"**Source:** {domain(rule.get('source',''))}")
                c3.markdown(f"**Pushed:** {str(rule.get('pushed_at',''))[:10]}")
                st.markdown("---")
                st.write(rule.get("description",""))
                with st.expander("Technical details"):
                    st.code(rule.get("content_hash",""), language=None)
                    st.caption("This is the document ID used in Firestore.")
        else:
            st.caption("Click a row to read the full rule.")
    else:
        st.info("No rules match the current filters.")

# ════════════════ TAB 2 — Compare with local ════════════════════════════════

with tab_diff:
    local_rules = load_json(CLEAN_RULES_PATH)

    if local_rules is None:
        st.warning("No local rules found. Run Stages 1–4 first to generate `clean_rules.json`.")
        st.stop()

    local_by_hash = {r["content_hash"]: r for r in local_rules if r.get("content_hash")}
    db_by_hash    = {r["content_hash"]: r for r in db_rules    if r.get("content_hash")}

    local_hashes  = set(local_by_hash)
    db_hashes     = set(db_by_hash)

    new_local     = [local_by_hash[h] for h in local_hashes - db_hashes]
    only_in_db    = [db_by_hash[h]    for h in db_hashes    - local_hashes]
    in_sync       = [local_by_hash[h] for h in local_hashes & db_hashes]

    st.caption(
        "Compares `rules/validated/clean_rules.json` (local pipeline output) "
        "against rules currently in Firestore, matched by content hash."
    )

    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Local Rules",     len(local_rules))
    s2.metric("In Database",     len(db_rules))
    s3.metric("Not Yet Pushed",  len(new_local),  delta=f"+{len(new_local)}"   if new_local  else None)
    s4.metric("Only in Database",len(only_in_db), delta=f"−{len(only_in_db)}" if only_in_db else None, delta_color="inverse")

    st.divider()

    with st.expander(f"Not yet in database ({len(new_local)})", expanded=bool(new_local)):
        if new_local:
            st.caption("These rules exist locally but haven't been pushed. Push is disabled in demo mode.")
            st.dataframe(pd.DataFrame([
                {"Rule": r.get("rule_id"), "Category": r.get("category"),
                 "Title": r.get("title"), "Source": domain(r.get("source",""))}
                for r in new_local
            ]), use_container_width=True, hide_index=True)
        else:
            st.success("All local rules are already in the database.")

    with st.expander(f"Only in database ({len(only_in_db)})"):
        if only_in_db:
            st.caption("In Firestore but not in the current local output. May have been changed or removed.")
            st.dataframe(pd.DataFrame([
                {"Rule": r.get("rule_id"), "Category": r.get("category"),
                 "Title": r.get("title"), "Pushed": str(r.get("pushed_at",""))[:10]}
                for r in only_in_db
            ]), use_container_width=True, hide_index=True)
        else:
            st.success("No database rules are missing from local output.")

    with st.expander(f"✅ In sync ({len(in_sync)})"):
        if in_sync:
            st.dataframe(pd.DataFrame([
                {"Rule": r.get("rule_id"), "Category": r.get("category"),
                 "Title": r.get("title"), "Source": domain(r.get("source",""))}
                for r in in_sync
            ]), use_container_width=True, hide_index=True)
        else:
            st.info("No rules are in sync yet.")
