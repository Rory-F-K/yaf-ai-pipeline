import json
import re
import os
import time
from google import genai
from google.genai import Client
from google.genai.types import Content, Part

from dotenv import load_dotenv

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

            if section and text and len(text) > 30:
                valid.append({
                    "section": section,
                    "text": text,
                    "source": c.get("source"),
                    "source_id": c.get("source_id"),
                    "type": c.get("type"),
                })


        return valid

    def chunk(self, texts):
        chunk_text = "\n\n".join([t if isinstance(t, str) else str(t) for t in texts])

        contents = [
            Content(parts=[Part(text=SYSTEM_PROMPT)]),  # system prompt
            Content(parts=[Part(text=chunk_text)])      # user content
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

        return self.validate(chunks)