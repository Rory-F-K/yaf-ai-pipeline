from pipeline_flow import Pipeline
from pathlib import Path

if __name__ == "__main__":
    pipeline = Pipeline()

    # Supported file extensions
    SUPPORTED_EXTENSIONS = [".pdf", ".txt", ".json", ".html"]

    # Scan sources folder
    sources_path = Path("sources")
    files_to_process = [
        f for f in sources_path.iterdir() 
        if f.suffix.lower() in SUPPORTED_EXTENSIONS
    ]

    if not files_to_process:
        print("No supported files found in sources/ folder.")
    else:
        print(f"Found {len(files_to_process)} files to process:")
        for f in files_to_process:
            print(f" - {f.name}")

    # Process each file
    for f in files_to_process:
        pipeline.process_file(str(f))

    for f in files_to_process:
        chunks = pipeline.process_file(f)
        files_to_process.extend(chunks)

    pipeline.save_chunks(files_to_process)
    print(f"Saved {len(files_to_process )} chunks to firestore/chunk_store")