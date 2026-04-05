import json
import re
from google import genai
import os
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

class GeminiChunker:

    def __init__(self):
        load_dotenv()  # This loads the variables from the .env file
        GEMINI_API_KEY=os.getenv("GEMINI_API_KEY")
        self.client = genai.Client(api_key=GEMINI_API_KEY)

    def _extract_json(self, text):
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except:
                return []
        return []

    def _validate(self, chunks):
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

    def chunk(self, text: str):

        response = self.client.models.generate_content(
            model= os.getenv("GEMINI_MODEL_NAME"),
            contents=[
                {
                    "role": "user",
                    "parts": [
                        {"text": SYSTEM_PROMPT},
                        {"text": text[:15000]}
                    ]
                }
            ]
        )

        raw = response.text

        try:
            chunks = json.loads(raw)
        except:
            chunks = self._extract_json(raw)

        return self._validate(chunks)