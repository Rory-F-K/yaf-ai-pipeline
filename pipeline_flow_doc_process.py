from dotenv import load_dotenv
from parser.local.local_ingest import ingest_local
from parser.remote.remote_ingest import ingest_remote
from chunker.semantic import semantic_chunk
from chunker.agentic_gemini import GeminiChunker
from parser.social_media.social_base import SocialMediaProvider # base type only; no platform imports

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
        max_workers=15, # parallel Gemini calls, limited by 15 per minute
        social_provider: SocialMediaProvider | None = None,  # ← pluggable slot
    ):
        print("Initializing pipeline...")

        self.enable_agentic = enable_agentic
        self.chunker = GeminiChunker() if enable_agentic else None

        self.agentic_rpm = agentic_rpm
        self.batch_size = batch_size
        self.checkpoint_every = checkpoint_every
        self.max_workers = max_workers

        self.social_provider = social_provider

        print(f"Pipeline ready | Agentic: {enable_agentic} | Social provider: {type(social_provider).__name__ if social_provider else 'None'}")


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

                entity = input_item.get("entity") if isinstance(input_item, dict) else None
                entity_type = input_item.get("entity_type") if isinstance(input_item, dict) else None

                for c in sem_chunks:
                    c["source"] = source_url
                    c["source_id"] = input_id
                    c["type"] = source_type
                    c["entity"] = entity
                    c["entity_type"] = entity_type

                all_semantic.extend(sem_chunks)

            all_semantic = self.dedupe(all_semantic)

            self.save_chunks(all_semantic, semantic_file)

        print(f"[Semantic] {len(all_semantic)} chunks")


        # Agentic chunking (LLM-based, with retries and parallelism)
        all_agentic = []

        if self.enable_agentic:

            if agentic_file.exists():
                print("[Resume] Loading agentic cache")
                all_agentic = json.load(open(agentic_file, "r", encoding="utf-8"))
                return all_semantic, all_agentic

            batches = list(self.batch_chunks(all_semantic))

            print(f"[Agentic] {len(batches)} batches starting...")

            all_agentic = self.process_batches_parallel(batches)

            # attach metadata
            for c in all_agentic:
                c["source"] = source_url
                c["source_id"] = input_id
                c["type"] = source_type
                c["entity"] = entity
                c["entity_type"] = entity_type

            all_agentic = self.dedupe(all_agentic)

            self.save_chunks(all_agentic, agentic_file)

            self.save_chunks(all_semantic, semantic_file)

        return all_semantic, all_agentic

    def process_social(
        self,
        run_id: str = "social",
        tags: list[str] | None = None,
        count_per_tag: int = 5,
        output_dir: str = "chunk_store",
    ) -> list[dict]:
        """
        Fetch posts from the configured social provider, deduplicate, and
        save them to chunk_store/social/raw/{run_id}.json.

        The saved records use the same schema as doc chunks so downstream
        stages (extraction, validation, Firestore push) need no changes.

        Parameters
        ----------
        run_id        : Filename stem for the output file.
        tags          : Query list passed to provider.fetch_all().
                        None → provider uses its own default tag list.
        count_per_tag : Posts to request per query/tag.
        output_dir    : Root of the chunk store (default: "chunk_store").

        Returns
        -------
        List of normalised social records (same shape as doc chunks).
        """
        if self.social_provider is None:
            print("[Social] No social_provider configured — skipping.")
            return []

        raw_dir = Path(output_dir) / "social"
        out_file = raw_dir / f"{run_id}.json"

        # Resume: if output already exists, load and return cached records
        if out_file.exists():
            print(f"[Social] Resume — loading cached records from {out_file}")
            with open(out_file, "r", encoding="utf-8") as f:
                return json.load(f)

        platform = self.social_provider.platform_name
        print(f"[Social] Fetching from platform: {platform}  run_id: {run_id}")

        records = self.social_provider.fetch_all(
            tags=tags,
            count_per_tag=count_per_tag,
        )

        print(f"[Social] {len(records)} records fetched from {platform}")

        # Attach pipeline-level metadata that doc chunks also carry
        for record in records:
            record.setdefault("source_id", run_id) # provider sets per-post source_id; this is a run-level fallback only
            record["entity"] = None # no single entity - posts are from many users
            record["entity_type"] = "social_media"
            # print(f"[Social] {record['source_id']} records fetched from {platform}") # debug log for source_id

        self.save_chunks(records, out_file)
        print(f"[Social] Saved → {out_file}")

        return records


    # Save chunks to JSON with pretty formatting and ensure directory exists
    @staticmethod
    def save_chunks(chunks, output_file):
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(chunks, f, indent=2)