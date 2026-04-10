from dotenv import load_dotenv
from parser.local.local_ingest import ingest_local
from parser.remote.remote_ingest import ingest_remote
from chunker.semantic import semantic_chunk
from chunker.agentic_gemini import GeminiChunker

from pathlib import Path
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()


class Doc_Process_Pipeline:

    def __init__(
        self,
        enable_agentic=True,
        agentic_rpm=10, # increased safe baseline
        batch_size=30, # increased (fewer LLM calls)
        checkpoint_every=10,
        max_workers=3 # parallel Gemini calls
    ):
        print("Initializing pipeline...")

        self.enable_agentic = enable_agentic
        self.chunker = GeminiChunker() if enable_agentic else None

        self.agentic_rpm = agentic_rpm
        self.batch_size = batch_size
        self.checkpoint_every = checkpoint_every
        self.max_workers = max_workers

        print(f"Pipeline ready | Agentic: {enable_agentic}")


    # Helper to batch chunks for agentic processing
    def batch_chunks(self, chunks):
        for i in range(0, len(chunks), self.batch_size):
            yield chunks[i:i + self.batch_size]

    def dedupe(self, chunks):
        seen = set()
        unique = []

        for c in chunks:
            key = (c.get("text", "")[:200], c.get("section", ""))
            if key not in seen:
                seen.add(key)
                unique.append(c)

        return unique


    # Agentic chunking with retry logic and fixed backoff
    def agentic_with_retry(self, batch, retries=3):
        if not self.enable_agentic:
            return []

        for attempt in range(retries):
            try:
                # pass JSON instead of huge string concat
                return self.chunker.chunk(batch)

            except Exception as e:
                print(f"[Agentic] Retry {attempt+1}/{retries}: {e}")

                # small fixed backoff (not exponential explosion)
                time.sleep(1)

        return []


    # Parallel processing of batches with ThreadPoolExecutor
    def process_batches_parallel(self, batches):
        results = []
        completed = 0

        def run(batch):
            return self.agentic_with_retry(batch)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(run, b) for b in batches]

            for f in as_completed(futures):
                try:
                    out = f.result()
                    results.extend(out)
                except Exception as e:
                    print(f"[Batch Error] {e}")

                completed += 1
                print(f"[Agentic] Completed batch {completed}/{len(batches)}")

        return results


    # Main processing function
    def process(self, input_item, output_dir="chunk_store"):
        # ingest docs
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


        # Semantic chunking (no retries, deterministic)
        if semantic_file.exists():
            print("[Resume] Loading semantic cache")
            all_semantic = json.load(open(semantic_file, "r", encoding="utf-8"))
        else:
            all_semantic = []

            for page in pages:
                sem_chunks = semantic_chunk(page["text"])

                for c in sem_chunks:
                    c["source"] = source_url
                    c["source_id"] = input_id
                    c["type"] = source_type

                all_semantic.extend(sem_chunks)

            all_semantic = self.dedupe(all_semantic)

            self.save_chunks(all_semantic, semantic_file)

        print(f"[Semantic] {len(all_semantic)} chunks")


        # Agentic chunking (LLM-based, with retries and parallelism)
        all_agentic = []

        if self.enable_agentic:

            batches = list(self.batch_chunks(all_semantic))

            print(f"[Agentic] {len(batches)} batches starting...")

            all_agentic = self.process_batches_parallel(batches)

            # attach metadata
            for c in all_agentic:
                c["source"] = source_url
                c["source_id"] = input_id
                c["type"] = source_type

            all_agentic = self.dedupe(all_agentic)

            self.save_chunks(all_agentic, agentic_file)

        return all_semantic, all_agentic


    # Save chunks to JSON with pretty formatting and ensure directory exists
    @staticmethod
    def save_chunks(chunks, output_file):
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(chunks, f, indent=2)