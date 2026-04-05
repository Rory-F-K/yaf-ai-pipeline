# pipeline.py
from parser.ingest import ingest_file
from chunker.semantic import semantic_chunk
from chunker.agentic_gemini import GeminiChunker
from pathlib import Path
import json

class Pipeline:
    def __init__(self):
        print("Initializing pipeline...")
        self.chunker = GeminiChunker()
        print("Pipeline initialized.")

    def process_file(self, file_path: str):
        # Determine file type and read content
        print (f"Processing file: {file_path}")
        suffix = Path(file_path).suffix.lower()
        
        text = ingest_file(file_path)
        print (f"File read successfully, length: {len(text)} characters")

        # Step 1: Semantic chunking
        print("Starting semantic chunking...")
        sem_chunks = semantic_chunk(text)
        final_chunks = []
        print (f"Semantic chunking completed, {len(sem_chunks)} chunks created.")
        
        # Step 2: Agentic chunking using Gemini
        print("Starting agentic chunking with Gemini...")
        for i, chunk in enumerate(sem_chunks, start=1):  # i = chunk number
            agentic_chunks = self.chunker.chunk(chunk)
            final_chunks.extend(agentic_chunks)
            print (f"Processed semantic chunk: {i}/{len(sem_chunks)}, created {len(agentic_chunks)} agentic chunks. Total so far: {len(final_chunks)}")
        
        # Step 3: Save chunks to Firestore (or local JSON for now)
        self.save_chunks(final_chunks, file_path)
        return final_chunks

    def save_chunks(self, chunks, input_file):
        base_name = Path(input_file).stem  # filename without extension
        output_dir = Path("firestore/chunk_store")
        output_dir.mkdir(parents=True, exist_ok=True)  # create folder if missing
        output_file = output_dir / f"{base_name}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(chunks, f, indent=2)