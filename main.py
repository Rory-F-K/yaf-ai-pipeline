# main.py - Entry point for the pipeline execution
from pipeline_flow import Pipeline
from pathlib import Path
from config import SOURCES
import glob

# Main execution
if __name__ == "__main__":
    # Initialize pipeline with agentic chunking
    pipeline = Pipeline(
        enable_agentic=False,   # turn on later
        agentic_rpm=3,
        batch_size=5,
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

    for src in SOURCES:
        input_id = src.get("id", "remote")

        partial = Path("chunk_store/agentic") / f"{input_id}_partial.json"

        if partial.exists():
            print(f"[Skip] Partial exists: {input_id}")
            continue

        pipeline.process(src)

    print("[Done] All sources processed")