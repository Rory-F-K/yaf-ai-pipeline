from google import genai
import os
from dotenv import load_dotenv

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found in .env")

client = genai.Client(api_key=GEMINI_API_KEY)

# List available models
models = client.models.list()
print("Available Gemini Models:\n")
for m in models:
    # Only print fields that exist
    name = getattr(m, "name", "N/A")
    display_name = getattr(m, "display_name", "N/A")
    print(f"- {name} | {display_name}")