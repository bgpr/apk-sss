import json
import os
import sys
from urllib.parse import urlparse

# Helper function to get page slug, duplicated from scraper.py to avoid circular imports
def get_page_slug(base_url):
    """Generates a slug from the URL path part before .php"""
    parsed_url = urlparse(base_url)
    # Extract the part before .php in the path, e.g., /php/kannada_books
    path_segments = parsed_url.path.split('/')
    # Find the segment ending with .php and take the part before it
    for segment in path_segments:
        if '.php' in segment:
            return segment.split('.php')[0].replace('/', '_').strip('_') # slugify simple for this helper
    # Fallback if no .php is found, though it should be for this specific URL
    return parsed_url.path.replace('/', '_').strip('_')


def reset_statuses(target_page_slug=None):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file_path = os.path.join(script_dir, "config.json")

    if not os.path.exists(config_file_path):
        print(f"Error: config.json not found at {config_file_path}. Exiting.")
        sys.exit(1)

    try:
        with open(config_file_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error: Could not decode JSON from {config_file_path}: {e}. Exiting.")
        sys.exit(1)

    processed_any = False
    for page_config in config:
        page_url = page_config['url']
        page_slug = get_page_slug(page_url)
        
        if target_page_slug and page_slug != target_page_slug:
            continue # Skip if a specific page is targeted and this is not it

        state_file_path = os.path.join(script_dir, f"processing_state_{page_slug}.json")

        try:
            current_state = {"books": []}
            if os.path.exists(state_file_path):
                with open(state_file_path, 'r', encoding='utf-8') as f:
                    current_state = json.load(f)
            else:
                print(f"No state file found for '{page_config['name']}' at {state_file_path}. Skipping.")
                continue

            reset_count = 0
            for book in current_state['books']:
                if book.get('status') == "failed" or book.get('status') == "in_progress":
                    book['status'] = "pending"
                    book['download_status'] = "pending" # Reset download status too, in case of partial download failure
                    book['ocr_status'] = "pending"
                    book['docx_conversion_status'] = "pending"
                    if 'error_message' in book:
                        del book['error_message']
                    reset_count += 1

            if reset_count > 0:
                with open(state_file_path, 'w', encoding='utf-8') as f:
                    json.dump(current_state, f, indent=4, ensure_ascii=False)
                print(f"Successfully reset statuses for {reset_count} failed/in-progress books in '{page_config['name']}' ({state_file_path})")
                processed_any = True
            else:
                print(f"No failed/in-progress books to reset in '{page_config['name']}'.")

        except FileNotFoundError:
            print(f"Error: State file not found at {state_file_path} for '{page_config['name']}'.")
        except json.JSONDecodeError as e:
            print(f"Error: Could not decode JSON from {state_file_path} for '{page_config['name']}'. Is it a valid JSON file? Error: {e}")
        except Exception as e:
            print(f"An unexpected error occurred while processing '{page_config['name']}': {e}")
    
    if not processed_any and target_page_slug:
        print(f"No matching page found in config.json for target_page_slug: {target_page_slug}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Reset statuses of failed/in-progress books in state JSON files.")
    parser.add_argument("--page-slug", help="Optional: Reset statuses only for a specific page slug (e.g., 'kannada_books'). If omitted, resets for all configured pages.")
    args = parser.parse_args()

    reset_statuses(args.page_slug)
