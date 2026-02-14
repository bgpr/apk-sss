import json
import os

state_file_path = "/data/data/com.termux/files/home/apk/processing_state_kannada_books.json"

try:
    # Read the full JSON content
    with open(state_file_path, 'r', encoding='utf-8') as f:
        state = json.load(f)

    # Counter for reset books
    reset_count = 0

    # Iterate through all books and modify entries with status "failed" or "in_progress"
    for book in state['books']:
        if book.get('status') == "failed" or book.get('status') == "in_progress":
            book['status'] = "pending"
            book['ocr_status'] = "pending"
            book['docx_conversion_status'] = "pending"
            if 'error_message' in book:
                del book['error_message']
            reset_count += 1

    # Write the modified JSON content back to the file
    with open(state_file_path, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=4, ensure_ascii=False)

    print(f"Successfully reset statuses for {reset_count} failed books in {state_file_path}")

except FileNotFoundError:
    print(f"Error: State file not found at {state_file_path}")
except json.JSONDecodeError:
    print(f"Error: Could not decode JSON from {state_file_path}. Is it a valid JSON file?")
except Exception as e:
    print(f"An unexpected error occurred: {e}")