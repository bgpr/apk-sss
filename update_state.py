import json
import os
import sys
from urllib.parse import urlparse

# Helper function to get page slug
def get_page_slug(base_url):
    """Generates a slug from the URL path part before .php"""
    parsed_url = urlparse(base_url)
    path_segments = parsed_url.path.split('/')
    for segment in path_segments:
        if '.php' in segment:
            return segment.split('.php')[0].replace('/', '_').strip('_')
    return parsed_url.path.replace('/', '_').strip('_')

def slugify(text):
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text).strip('-')
    return text

# Note: re was not imported in previous version but used in slugify
import re

def reset_statuses(target_page_slug=None):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file_path = os.path.join(script_dir, "config.json")

    if not os.path.exists(config_file_path):
        print(f"Error: config.json not found at {config_file_path}. Exiting.")
        sys.exit(1)

    try:
        with open(config_file_path, 'r', encoding='utf-8') as f:
            full_config = json.load(f)
            # Handle new config structure: {"pages": [...], "domain_handlers": {...}}
            if isinstance(full_config, dict) and "pages" in full_config:
                pages = full_config["pages"]
            else:
                pages = full_config # Fallback to old list structure
    except json.JSONDecodeError as e:
        print(f"Error: Could not decode JSON from {config_file_path}: {e}. Exiting.")
        sys.exit(1)

    processed_any = False
    for page_config in pages:
        page_url = page_config.get('url')
        page_name = page_config.get('name', 'Unknown')
        
        if page_url:
            page_slug = get_page_slug(page_url)
        else:
            page_slug = slugify(page_name)
        
        if target_page_slug and page_slug != target_page_slug:
            continue

        state_file_path = os.path.join(script_dir, f"processing_state_{page_slug}.json")

        try:
            if os.path.exists(state_file_path):
                with open(state_file_path, 'r', encoding='utf-8') as f:
                    current_state = json.load(f)
            else:
                print(f"No state file found for '{page_name}' at {state_file_path}. Skipping.")
                continue

            reset_count = 0
            for book in current_state.get('books', []):
                if book.get('status') == "failed" or book.get('status') == "in_progress":
                    book['status'] = "pending"
                    book['download_status'] = "pending"
                    book['ocr_status'] = "pending"
                    book['docx_conversion_status'] = "pending"
                    if 'error_message' in book:
                        del book['error_message']
                    reset_count += 1

            if reset_count > 0:
                with open(state_file_path, 'w', encoding='utf-8') as f:
                    json.dump(current_state, f, indent=4, ensure_ascii=False)
                print(f"Successfully reset statuses for {reset_count} failed/in-progress books in '{page_name}' ({state_file_path})")
                processed_any = True
            else:
                print(f"No failed/in-progress books to reset in '{page_name}'.")

        except Exception as e:
            print(f"An error occurred while processing '{page_name}': {e}")
    
    if not processed_any and target_page_slug:
        print(f"No matching page or state file found for target_page_slug: {target_page_slug}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Reset statuses of failed/in-progress books in state JSON files.")
    parser.add_argument("--page-slug", help="Optional: Reset statuses only for a specific page slug.")
    args = parser.parse_args()

    reset_statuses(args.page_slug)
