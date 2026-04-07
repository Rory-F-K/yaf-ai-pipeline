# pipeline_flow_doc_process
from dotenv import load_dotenv
from parser.local.local_ingest import ingest_local
from parser.remote.remote_ingest import ingest_remote
from chunker.semantic import semantic_chunk
from chunker.agentic_gemini import GeminiChunker


from pathlib import Path
import json
import time
import os
import random

load_dotenv()

class Doc_Process_Pipeline:
    # Configurable parameters:
    def __init__(self, enable_agentic=True, agentic_rpm=3, batch_size=5, checkpoint_every=10):
        print("Initializing pipeline...")

        self.enable_agentic = enable_agentic
        self.chunker = GeminiChunker() if enable_agentic else None

        self.delay = 60 / agentic_rpm
        self.batch_size = batch_size
        self.checkpoint_every = checkpoint_every

        print(f"Pipeline initialized | Agentic: {self.enable_agentic} | Delay: {self.delay:.2f}s")

    # HELPER FUNCTIONS

    # Helper to yield batches of chunks
    def batch_chunks(self, chunks):
        for i in range(0, len(chunks), self.batch_size):
            yield chunks[i:i + self.batch_size]

    # Helper to flatten chunk texts for agentic processing
    def flatten_chunks(self, chunks):
        return "\n\n".join(c["text"] for c in chunks if "text" in c)

    # Helper to deduplicate chunks based on text content
    def dedupe(self, chunks):
        seen = set()
        unique = []

        for c in chunks:
            key = c.get("text", "")[:200]
            if key not in seen:
                seen.add(key)
                unique.append(c)

        return unique

    # Agentic chunking with retry logic and exponential backoff
    def agentic_with_retry(self, semantic_batch, retries=5):
        if not self.enable_agentic:
            return []

        for attempt in range(retries):
            try:
                return self.chunker.chunk(semantic_batch)

            except Exception as e:
                wait = (2 ** attempt)
                print(f"[Agentic] Retry {attempt+1}/{retries} | Error: {e}")
                time.sleep(wait)

        print("[Agentic] Failed — fallback to empty")
        return []

    # MAIN PIPELINE LOGIC
    def process(self, input_item, output_dir="chunk_store"):
        # Determine if input is remote (dict) or local (file path)
        if isinstance(input_item, dict):
            pages = ingest_remote(input_item)
            input_id = input_item.get("id", "remote")
            source_url = input_item.get("url")
            source_type = "remote"
        else:
            text = ingest_local(input_item)
            pages = [{"text": text}]
            input_id = Path(input_item).stem
            source_url = str(input_item)
            source_type = "local"


        print(f"[Ingest] {input_id} → {len(pages)} pages")

        semantic_file = Path(output_dir) / "semantic" / f"{input_id}.json"
        agentic_file = Path(output_dir) / "agentic" / f"{input_id}.json"

        # Semantic chunking with caching
        # Check for existing semantic cache and load if available
        if semantic_file.exists():
            print("[Resume] Loading semantic cache")
            with open(semantic_file, "r", encoding="utf-8") as f:
                all_semantic = json.load(f)
        else:
            all_semantic = []
            debug = ""
            for page in pages:
                raw_text = page["text"]


                sem_chunks = semantic_chunk(raw_text)

                for c in sem_chunks:
                    c["source"] = source_url
                    c["source_id"] = input_id
                    c["type"] = source_type

                all_semantic.extend(sem_chunks)

            all_semantic = self.dedupe(all_semantic)

            self.save_chunks(all_semantic, semantic_file)

        print(f"[Semantic] {len(all_semantic)} chunks")

        # Agentic chunking with batching, rate limiting, and checkpointing
        all_agentic = []

        # Only run agentic chunking if enabled
        if self.enable_agentic:

            batch_count = 0

            for batch in self.batch_chunks(all_semantic):

                agentic_out = self.agentic_with_retry(batch)

                for c in agentic_out:
                    c["source"] = source_url
                    c["source_id"] = input_id
                    c["type"] = source_type

                all_agentic.extend(agentic_out)

                batch_count += 1
                print(f"[Agentic] Batch {batch_count} → total {len(all_agentic)}")

                time.sleep(self.delay)

                if batch_count % self.checkpoint_every == 0:
                    partial = Path(output_dir) / "agentic" / f"{input_id}_partial.json"
                    self.save_chunks(all_agentic, partial)

            all_agentic = self.dedupe(all_agentic)


            self.save_chunks(all_agentic, agentic_file)

        return all_semantic, all_agentic

    @staticmethod
    def save_chunks(chunks, output_file):
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(chunks, f, indent=2)