"""
Stage 5 — Database (Firestore)
Live view of entities in Firebase + comparison with local pipeline output.
"""
import sys
import os
from pathlib import Path

import streamlit as st
import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT        = Path(__file__).parent.parent.parent
DASHBOARD_DIR       = Path(__file__).parent.parent
CLEAN_ENTITIES_PATH = PROJECT_ROOT / "rules" / "validated" / "clean_entities.json"

sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(DASHBOARD_DIR))
load_dotenv(PROJECT_ROOT / ".env")

from dashboard_utils import load_json

st.set_page_config(page_title="Database", page_icon="🗄️", layout="wide")

# ── Header ─────────────────────────────────────────────────────────────────────

st.title("🗄️ Stage 5: Database")
st.caption(
    "Live view of entities stored in Firebase Firestore (v2_airlines, v2_airports, v2_regulations). "
    "Use the Compare tab to see how the local pipeline output aligns with the database."
)
st.divider()

# ── Connection status ──────────────────────────────────────────────────────────

project_id = os.getenv("FIRESTORE_PROJECT_ID")
creds_path = os.getenv("FIREBASE_CREDENTIALS_PATH")
creds_ok   = Path(creds_path).exists() if creds_path else False

with st.container(border=True):
    c1, c2, c3 = st.columns(3)
    c1.markdown("**Project**")
    c1.code(project_id or "Not set", language=None)
    c2.markdown("**Credentials**")
    c2.code(("✅ Found" if creds_ok else "Not found") if creds_path else "Not set", language=None)
    c3.markdown("**Collections**")
    c3.code("v2_airlines · v2_airports · v2_regulations", language=None)

missing = [v for v, k in [("FIRESTORE_PROJECT_ID", project_id), ("FIREBASE_CREDENTIALS_PATH", creds_path)] if not k]
if missing or not creds_ok:
    if missing:
        st.error(f"Missing in `.env`: {', '.join(f'`{m}`' for m in missing)}")
    elif not creds_ok:
        st.error(f"Credentials file not found: `{creds_path}`")
    st.stop()

# ── Fetch from Firestore ───────────────────────────────────────────────────────

@st.cache_data(show_spinner="Connecting to Firebase…", ttl=60)
def fetch_db_entities() -> list:
    from firestore.client import FirestoreClient
    return FirestoreClient().fetch_all_entities()

r_col, s_col = st.columns([1, 4])
if r_col.button("Refresh", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

try:
    db_entities = fetch_db_entities()
except Exception as e:
    st.error(f"Could not connect to Firebase: {e}")
    st.stop()

s_col.caption(f"Connected · **{len(db_entities):,}** entities in database · auto-refreshes every 60 s")
st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab_db, tab_diff = st.tabs(["Database Entities", "Compare with Local"])

# ════════════════ TAB 1 — Database browse ═══════════════════════════════════

with tab_db:
    if not db_entities:
        st.info("No entities found in the database.")
        st.stop()

    from collections import Counter
    type_counts = Counter(e.get("entity_type") or "unknown" for e in db_entities)
    coll_counts = Counter(e.get("collection") or "" for e in db_entities)

    m1, m2, m3 = st.columns(3)
    m1.metric("Entities in Database", len(db_entities))
    m2.metric("Airlines",  type_counts.get("airline", 0))
    m3.metric("Airports",  type_counts.get("airport", 0))

    with st.sidebar:
        st.markdown("### Filter")
        all_types = sorted(type_counts.keys())
        sel_types = st.multiselect("Entity Type", all_types, default=all_types)
        keyword   = st.text_input("Search", placeholder="Entity name…")

    q = keyword.lower().strip()
    filtered = [
        e for e in db_entities
        if e.get("entity_type") in sel_types
        and (not q or q in e.get("entity_name", "").lower())
    ]

    st.caption(f"Showing **{len(filtered)}** of {len(db_entities)} entities. Click a row to view services.")

    if filtered:
        df = pd.DataFrame([
            {
                "Entity":     e.get("entity_name"),
                "Type":       e.get("entity_type"),
                "Services":   e.get("n_services", 0),
                "Collection": e.get("collection", "").replace("v2_", ""),
                "Updated":    e.get("updated_at", ""),
            }
            for e in filtered
        ])
        ev = st.dataframe(
            df, use_container_width=True, hide_index=True,
            selection_mode="single-row", on_select="rerun",
            column_config={
                "Services":   st.column_config.NumberColumn(width="small"),
                "Type":       st.column_config.TextColumn(width="small"),
                "Updated":    st.column_config.TextColumn(width="small"),
                "Collection": st.column_config.TextColumn(width="small"),
            },
        )
        sel = ev.selection.rows if ev.selection else []
        if sel:
            entity = filtered[sel[0]]
            st.caption(f"Document ID: `{entity.get('source_id')}`")
        else:
            st.caption("Click a row to view its Firestore document ID.")
    else:
        st.info("No entities match the current filters.")

# ════════════════ TAB 2 — Compare with local ════════════════════════════════

with tab_diff:
    local_entities = load_json(CLEAN_ENTITIES_PATH)

    if local_entities is None:
        st.warning(
            "No local entities found at `rules/validated/clean_entities.json`. "
            "Run Stages 1–3 first to generate validated entities."
        )
        st.stop()

    def _norm(name: str) -> str:
        return name.strip().lower()

    def _tokens(name: str) -> set:
        return set(_norm(name).split())

    def _match(local_name: str, db_names_list: list) -> str | None:
        """Return the db entity_name that best matches local_name, or None."""
        local_key = _norm(local_name)
        local_tok = _tokens(local_name)
        for db_name in db_names_list:
            db_key = _norm(db_name)
            if local_key == db_key:
                return db_name
            if local_tok & _tokens(db_name):  # any shared word
                return db_name
        return None

    db_name_list = [e.get("entity_name", "") for e in db_entities if e.get("entity_name")]
    db_by_name   = {_norm(e.get("entity_name", "")): e for e in db_entities if e.get("entity_name")}

    not_pushed = []
    in_sync    = []
    matched_db_names = set()

    for local_e in local_entities:
        local_name = local_e.get("entity_name", "")
        if not local_name:
            continue
        matched = _match(local_name, db_name_list)
        if matched:
            in_sync.append(local_e)
            matched_db_names.add(_norm(matched))
        else:
            not_pushed.append(local_e)

    only_in_db = [
        e for e in db_entities
        if e.get("entity_name") and _norm(e.get("entity_name", "")) not in matched_db_names
    ]

    st.caption(
        "Compares `rules/validated/clean_entities.json` (local pipeline output) "
        "against entities currently in Firestore, matched by entity name."
    )

    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Local Entities",    len(local_entities))
    s2.metric("In Database",       len(db_entities))
    s3.metric("Not Yet Pushed",    len(not_pushed),
              delta=f"+{len(not_pushed)}" if not_pushed else None)
    s4.metric("Only in Database",  len(only_in_db),
              delta=f"−{len(only_in_db)}" if only_in_db else None,
              delta_color="inverse")

    st.divider()

    with st.expander(f"Not yet in database ({len(not_pushed)})", expanded=bool(not_pushed)):
        if not_pushed:
            st.caption("These entities exist locally but have not yet been pushed to Firestore.")
            st.dataframe(pd.DataFrame([
                {
                    "Entity":   e.get("entity_name"),
                    "Type":     e.get("entity_type"),
                    "Services": len(e.get("services", [])),
                }
                for e in not_pushed
            ]), use_container_width=True, hide_index=True)
        else:
            st.success("All local entities are already in the database.")

    with st.expander(f"Only in database ({len(only_in_db)})"):
        if only_in_db:
            st.caption("In Firestore but not in the current local output.")
            st.dataframe(pd.DataFrame([
                {
                    "Entity":     e.get("entity_name"),
                    "Type":       e.get("entity_type"),
                    "Collection": e.get("collection", "").replace("v2_", ""),
                    "Updated":    e.get("updated_at", ""),
                }
                for e in only_in_db
            ]), use_container_width=True, hide_index=True)
        else:
            st.success("No database entities are missing from local output.")

    with st.expander(f"✅ In sync ({len(in_sync)})"):
        if in_sync:
            st.dataframe(pd.DataFrame([
                {
                    "Entity":   e.get("entity_name"),
                    "Type":     e.get("entity_type"),
                    "Services": len(e.get("services", [])),
                }
                for e in in_sync
            ]), use_container_width=True, hide_index=True)
        else:
            st.info("No entities are in sync yet.")
