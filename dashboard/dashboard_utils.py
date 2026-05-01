"""
Shared utilities for the YAF AI Pipeline dashboard.
"""
import os
import sys
import json
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
RUNNER       = PROJECT_ROOT / "dashboard" / "run_stage.py"

# ── Progress markers ──────────────────────────────────────────────────────────
# Each entry: (substring to detect, progress 0-1, human-readable status)

PROGRESS_MARKERS: dict[str, list] = {
    "all": [
        ("STAGE 1",               0.02, "Fetching sources…"),
        ("[Chunk] Local",         0.08, "Processing local files…"),
        ("[Chunk] Remote",        0.14, "Scraping websites…"),
        ("[Done] Local",          0.25, "Local files done"),
        ("[Done] All sources",    0.32, "All sources processed"),
        ("STAGE 2",               0.36, "Loading documents for AI extraction…"),
        ("re-extract everything", 0.42, "Running AI rule extraction…"),
        ("[Extractor]",           0.52, "Extracting rules…"),
        ("Dedup removed",         0.62, "Removing duplicate rules…"),
        ("STAGE 3",               0.66, "Starting quality checks…"),
        ("[Validator] Running",   0.72, "Validating rules…"),
        ("cross_source",          0.78, "Checking for cross-source conflicts…"),
        ("Report saved",          0.83, "Saving validation report…"),
        ("STAGE 4",               0.87, "Tracking changes…"),
        ("[Versioning] Saved",    0.95, "Version snapshot saved"),
        ("All stages complete",   1.00, "Pipeline complete"),
        ("[Runner]",              1.00, "Done"),
    ],
    "chunk": [
        ("STAGE 1",               0.05, "Initialising…"),
        ("[Chunk] Local",         0.20, "Processing local files…"),
        ("[Chunk] Remote",        0.40, "Scraping websites…"),
        ("[Done] Local",          0.60, "Local files done"),
        ("[Done] All sources",    0.92, "All sources processed"),
        ("[Runner]",              1.00, "Done"),
    ],
    "extract": [
        ("STAGE 2",               0.08, "Loading documents…"),
        ("[Extractor] Semantic",  0.30, "Processing standard segments…"),
        ("[Extractor] Agentic",   0.58, "Processing advanced segments…"),
        ("Dedup removed",         0.82, "Removing duplicate rules…"),
        ("All rules saved",       0.95, "Saving rules…"),
        ("[Runner]",              1.00, "Done"),
    ],
    "validate": [
        ("STAGE 3",               0.10, "Loading rules…"),
        ("[Validator] Running",   0.32, "Running quality checks…"),
        ("cross_source",          0.72, "Checking for conflicts…"),
        ("Report saved",          0.92, "Saving report…"),
        ("[Runner]",              1.00, "Done"),
    ],
    "version": [
        ("STAGE 4",               0.20, "Loading rules…"),
        ("[Versioning]",          0.82, "Recording changes…"),
        ("[Runner]",              1.00, "Done"),
    ],
}


def get_progress(output: str, stage: str) -> tuple[float, str]:
    markers  = PROGRESS_MARKERS.get(stage, PROGRESS_MARKERS["all"])
    progress = 0.0
    status   = "Starting…"
    for marker, pct, label in markers:
        if marker in output:
            progress = pct
            status   = label
    return progress, status


def run_stage_streaming(stage_key: str):
    """Stream subprocess output; yields (progress, status, output) per line, then (..., rc) at end."""
    env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"}
    process = subprocess.Popen(
        [sys.executable, "-u", str(RUNNER), stage_key],
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace",
        bufsize=1, env=env,
    )
    lines = []
    for line in process.stdout:
        lines.append(line)
        full = "".join(lines)
        progress, status = get_progress(full, stage_key)
        yield progress, status, full
    process.wait()
    full = "".join(lines)
    progress, status = get_progress(full, stage_key)
    if process.returncode != 0:
        status = f"Error: exit code {process.returncode}"
    yield progress, status, full, process.returncode


def load_json(path: Path):
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def domain(src: str) -> str:
    return src.split("/")[2] if "://" in src else src


def count_json_files(directory: Path) -> int:
    if not directory.exists():
        return 0
    return sum(1 for f in directory.glob("*.json") if not f.name.endswith("_partial.json"))


def run_ui(stage_key: str, stage_label: str, note: str, session_prefix: str):
    """
    Renders a self-contained Run Stage UI block.
    Call inside an expander or container.
    """
    import streamlit as st

    for k, v in [(f"{session_prefix}_out", ""), (f"{session_prefix}_rc", None), (f"{session_prefix}_run", False)]:
        if k not in st.session_state:
            st.session_state[k] = v

    st.caption(note)
    run_btn = st.button(f"Run {stage_label}", type="primary", key=f"btn_{session_prefix}",
                        disabled=st.session_state[f"{session_prefix}_run"])

    status_slot   = st.empty()
    progress_slot = st.empty()
    result_slot   = st.empty()

    if run_btn:
        st.session_state[f"{session_prefix}_run"] = True
        st.session_state[f"{session_prefix}_out"] = ""

        final_rc = 0
        for item in run_stage_streaming(stage_key):
            if len(item) == 4:
                prog, stat, output, final_rc = item
            else:
                prog, stat, output = item

            progress_slot.progress(min(prog, 1.0), text=stat)
            status_slot.caption(f"Running: {stat}")

        st.session_state[f"{session_prefix}_out"] = output
        st.session_state[f"{session_prefix}_rc"]  = final_rc
        st.session_state[f"{session_prefix}_run"] = False

        if final_rc == 0:
            result_slot.success("Completed successfully.")
        else:
            result_slot.error(f"Exited with code {final_rc}.")
        st.rerun()

    elif st.session_state[f"{session_prefix}_out"]:
        rc = st.session_state[f"{session_prefix}_rc"]
        if rc == 0:
            result_slot.success("Completed successfully.")
        elif rc is not None:
            result_slot.error(f"Exited with code {rc}.")

    if st.session_state[f"{session_prefix}_out"]:
        with st.expander("Output log"):
            out = st.session_state[f"{session_prefix}_out"]
            st.code(out[-3000:] if len(out) > 3000 else out, language="text")
