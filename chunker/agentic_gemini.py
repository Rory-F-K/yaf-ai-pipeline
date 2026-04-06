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

Your task is to transform raw extracted document text into structured semantic chunks.

STRICT RULES:
- Each chunk MUST represent ONE clear topic
- DO NOT mix multiple topics
- Prefer smaller, precise chunks over large ones
- Focus on rules, constraints, requirements, and procedures
- Ignore navigation text, headers, footers, and duplicates

For each chunk:
- "section": short descriptive title
- "text": clean, complete explanation

OUTPUT FORMAT (STRICT JSON ONLY):

[
  {
    "section": "Battery Rules",
    "text": "Lithium-ion batteries must not exceed 300Wh..."
  }
]

DO NOT include:
- explanations
- markdown
- comments
- extra text

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

    def extract_json(self, text):
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except:
                return []
        return []

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
                    "text": text
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