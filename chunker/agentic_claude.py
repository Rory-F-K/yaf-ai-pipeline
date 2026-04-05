import json
import re
import anthropic
from chunker.semantic import semantic_chunk
import os
from dotenv import load_dotenv

# Agentic prompt for chunking aviation accessibility policies into structured JSON sections.
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

import json
import re
import anthropic
from config import CLAUDE_API_KEY

class ClaudeChunker:

    def __init__(self):
        load_dotenv()  # This loads the variables from the .env file
        CLAUDE_API_KEY=os.getenv("CLAUDE_API_KEY")
        self.client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)

    def _extract_json(self, text):
        match = re.search(r"\[.*\]", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except:
                return []
        return []

    def _validate_chunks(self, chunks):
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
        inputs = semantic_chunk(text, max_chars=4000)
        all_chunks = []

        for chunk in inputs:
            response = self.client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=2000,
                temperature=0.1,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": chunk}]
            )

            raw = response.content[0].text

            try:
                parsed = json.loads(raw)
            except:
                parsed = self._extract_json(raw)

            valid = self._validate_chunks(parsed)
            all_chunks.extend(valid)

        return all_chunks