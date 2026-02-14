import os
import re
import sys # Added sys import
from google import genai
import time

# --- Configuration ---
# Load API key from environment variable or direct assignment
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# Flag to temporarily disable Gemini transliteration for debugging/testing purposes
# Set to True to skip Gemini API calls.
DISABLE_GEMINI_TRANSLITERATION = False # <--- SET THIS TO FALSE

if not GEMINI_API_KEY and not DISABLE_GEMINI_TRANSLITERATION:
    raise ValueError("GEMINI_API_KEY not found. Please set it as an environment variable or provide it directly.")

# Initialize the Gemini client (only if not disabled)
client = None
if not DISABLE_GEMINI_TRANSLITERATION:
    client = genai.Client(api_key=GEMINI_API_KEY)

def slugify(text):
    """
    Converts text into a URL-friendly and filesystem-safe slug.
    Removes non-alphanumeric characters, converts to lowercase, and replaces spaces with hyphens.
    """
    text = re.sub(r'[^\w\s-]', '', text).strip().lower()
    text = re.sub(r'[-\s]+', '-', text)
    return text

def transliterate_kannada_to_english(kannada_text: str, model_name: str = "gemini-2.0-flash") -> str: # Reverted default to 2.0-flash
    """
    Transliterates Kannada text to English using the Gemini client library,
    then slugifies the result for use in filenames.
    """
    if not kannada_text:
        return ""

    if DISABLE_GEMINI_TRANSLITERATION:
        print(f"DEBUG: Gemini transliteration skipped for: '{kannada_text}' (DISABLED). Using fallback ASCII slugification.", file=sys.stderr)
        return slugify(kannada_text.encode('ascii', 'ignore').decode('ascii'))

    print(f"DEBUG: Attempting Gemini transliteration for: '{kannada_text}' using model '{model_name}'...")
    try:
        if client is None:
            raise RuntimeError("Gemini client not initialized. Check GEMINI_API_KEY or DISABLE_GEMINI_TRANSLITERATION flag.")

        prompt_contents = [
            {"role": "user", "parts": [
                {"text": "You are a professional linguist. Provide ONLY the Romanized English transliteration. Do not include translations, explanations, or quotes."},
                {"text": f"Transliterate the following Kannada text into Romanized English: {kannada_text}"}
            ]}
        ]

        response = client.models.generate_content(
            model=model_name,
            contents=prompt_contents,
            config={
                "system_instruction": (
                    "You are a professional linguist. "
                    "Provide ONLY the Romanized English transliteration. "
                    "Do not include translations, explanations, or quotes."
                ),
                "temperature": 0.0 # Set temperature to 0 for consistent output
            }
        )
        
        english_text = response.text.strip()
        print(f"DEBUG: Gemini transliteration successful: '{english_text}'")
        return slugify(english_text)

    except Exception as e:
        print(f"DEBUG: Error during Gemini transliteration with client library ({model_name}): {e}", file=sys.stderr)
        print(f"DEBUG: Falling back to basic ASCII conversion for '{kannada_text}'", file=sys.stderr)
        return slugify(kannada_text.encode('ascii', 'ignore').decode('ascii'))

if __name__ == "__main__":
    # Example Usage
    kannada_example_title = "ಅಧ್ಯಾತ್ಮವೆಂದರೇನು (ಪ್ರಶ್ನೋತ್ತರ)"
    kannada_example_author = "ಶ್ರೀ ಶ್ರೀಸಚ್ಚಿದಾನಂದೇಂದ್ರಸರಸ್ವತೀ ಸ್ವಾಮಿಗಳವರು"

    print(f"\nOriginal Kannada Title: {kannada_example_title}")
    transliterated_title = transliterate_kannada_to_english(kannada_example_title)
    print(f"Transliterated Title (slugified): {transliterated_title}")

    print(f"\nOriginal Kannada Author: {kannada_example_author}")
    transliterated_author = transliterate_kannada_to_english(kannada_example_author)
    print(f"Transliterated Author (slugified): {transliterated_author}")

    kannada_example_complex = "ಕನ್ನಡದಲ್ಲಿ ಸಂಕೀರ್ಣವಾದ ವಿಷಯ"
    transliterated_complex = transliterate_kannada_to_english(kannada_example_complex)
    print(f"\nOriginal Kannada Complex: {kannada_example_complex}")
    print(f"Transliterated Complex (slugified): {transliterated_complex}")
    
    print("\n--- Testing with gemini-2.0-flash (explicitly) ---")
    transliterated_title_flash = transliterate_kannada_to_english(kannada_example_title, model_name="gemini-2.0-flash")
    print(f"Transliterated Title (gemini-2.0-flash): {transliterated_title_flash}")