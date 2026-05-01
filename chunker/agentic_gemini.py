import json
import re
import os
import time
from google import genai
from google.genai import Client
from google.genai.types import Content, Part

from dotenv import load_dotenv
import hashlib

def generate_id(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()

SYSTEM_PROMPT = """
You are an expert in aviation accessibility policies.

You are given PRE-CHUNKED semantic data.

Your job is to REFINE it into higher-quality chunks.

STRICT RULES:
- Each chunk MUST represent ONE clear topic
- You MAY split chunks if they contain multiple ideas
- You MAY rewrite for clarity
- DO NOT merge unrelated topics
- KEEP content factual and grounded in input
- DO NOT hallucinate new information

IMPORTANT:
- Preserve meaning, improve structure
- Prefer smaller, precise chunks
- Each output chunk MUST include the original "id" from input
- Do NOT generate new IDs
- If splitting a chunk, reuse the same "id" for all resulting chunks

INPUT FORMAT:
[
  {
    "id": "...",
    "section": "...",
    "text": "...",
    "source": "...",
    "source_id": "...",
    "type": "..."
  }
]

OUTPUT FORMAT (STRICT JSON ONLY):
[
  {
    "parent_id": "...",
    "section": "Short title",
    "text": "Clear explanation",
    "source": "...",
    "source_id": "...",
    "type": "..."
  }
]

ONLY RETURN VALID JSON.
"""
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL_NAME = os.getenv("GEMINI_MODEL_NAME")

class GeminiChunker:
    def __init__(self):
        if not GEMINI_API_KEY:
            raise ValueError("No Gemini API key found. Set GEMINI_API_KEY in your .env file.")
        self.client = genai.Client(api_key=GEMINI_API_KEY)

    # Utility to extract JSON from text using regex (fallback if response is not pure JSON)
    def extract_json(self, text):
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except:
                return []
        return []

    # Validate that chunks have required fields and reasonable content length
    def validate(self, chunks):
        valid = []

        for c in chunks:
            if not isinstance(c, dict):
                continue

            section = c.get("section", "").strip()
            text = c.get("text", "").strip()
            cid = c.get("parent_id")

            if section and text and len(text) > 30:
                valid.append({
                    "id": generate_id(text),
                    "parent_id": cid,
                    "section": section,
                    "text": text,
                    "source": c.get("source"),
                    "source_id": c.get("source_id"),
                    "type": c.get("type"),
                    "sent": False # track if this chunk has been sent to rule extraction yet
                })


        return valid

    def chunk(self, texts):
        # Only process chunks that have not been sent
        unsent = [t for t in texts if not t.get("sent", False)]

        # If all chunks have been sent, skip processing
        if not unsent:
            print("[Gemini] No new chunks to process")
            return []

        # Pass the data as JSON to avoid token issues with large concatenated strings
        chunk_text = json.dumps(unsent, indent=2)

        contents = [
            Content(parts=[Part(text=SYSTEM_PROMPT)]), # system prompt
            Content(parts=[Part(text=chunk_text)]) # user content
        ] 
        response = self.client.models.generate_content(
            model=GEMINI_MODEL_NAME,
            contents=contents, 
            config={
                "temperature": 0,
                "max_output_tokens": 16384,
            }
        )

        raw = response.text

        try:
            chunks = json.loads(raw)
        except:
            chunks = self.extract_json(raw)

        result = self.validate(chunks)

        # Mark original chunks as sent
        unsent_ids = {t["id"] for t in unsent if "id" in t}

        for t in texts:
            if t.get("id") in unsent_ids:
                t["sent"] = True

        return result