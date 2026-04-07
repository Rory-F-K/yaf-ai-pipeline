# main.py - Entry point for the pipeline execution
import os
from pipeline_flow_doc_process import Doc_Process_Pipeline
from pathlib import Path
from config import Config
import glob


# Main execution
if __name__ == "__main__":
    # Initialize pipeline with agentic chunking
    pipeline = Doc_Process_Pipeline(
        enable_agentic=Config.ENABLE_AGENT_CHUNKS,
        agentic_rpm=3,
        batch_size=3,
        checkpoint_every=10
    )

    # Process local files first
    local_files = (
        glob.glob("sources/*.pdf") +
        glob.glob("sources/*.txt") +
        glob.glob("sources/*.json") +
        glob.glob("sources/*.html")
    )

    for f in local_files:
        input_id = Path(f).stem

        partial = Path("chunk_store/agentic") / f"{input_id}_partial.json"

        if partial.exists():
            print(f"[Skip] Partial exists: {input_id}")
            continue

        pipeline.process(f)

    print("[Done] Local files processed")

    for src in Config.SOURCES:
        input_id = src.get("id", "remote")

        partial = Path("chunk_store/agentic") / f"{input_id}_partial.json"

        if partial.exists():
            print(f"[Skip] Partial exists: {input_id}")
            continue

        pipeline.process(src)

    print("[Done] All sources processed")