import os
import json
import requests
import time
import subprocess
import hashlib 
import re 
import sys
import logging
import importlib
from urllib.parse import urlparse
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("main_processor_debug.log"),
        logging.StreamHandler(sys.stderr)
    ]
)
logger = logging.getLogger(__name__)

from scraper import load_state, save_state, scrape_books, get_page_slug, slugify
from ocr_pdf import ocr_to_markdown, process_markdown_to_docx

# Global Domain Handlers Cache
LOADED_HANDLERS = {}

# --- Configuration ---
SARVAM_AI_API_KEY = os.environ.get("SARVAM_AI_API_KEY")
if not SARVAM_AI_API_KEY:
    logger.error("SARVAM_AI_API_KEY environment variable not set. Exiting.")
    sys.exit(1)

RAW_PDF_BASE_DIR = "raw_pdf"
PROCESSED_DOCS_BASE_DIR = "processed_docs"
SDCARD_RAW_PDF_DIR = "/sdcard/raw_pdf"
SDCARD_PROCESSED_DOCS_DIR = "/sdcard/processed_docs"

os.makedirs(SDCARD_RAW_PDF_DIR, exist_ok=True)
os.makedirs(SDCARD_PROCESSED_DOCS_DIR, exist_ok=True)

def get_full_path_for_book(base_dir_root, page_slug, book_id, filename):
    page_dir = os.path.join(base_dir_root, page_slug)
    index_dir = os.path.join(page_dir, str(book_id).zfill(3))
    os.makedirs(index_dir, exist_ok=True)
    return os.path.join(index_dir, filename)

def calculate_sha256(filepath):
    sha256_hash = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        logger.error(f"Failed to calculate SHA256 for '{filepath}': {e}")
        return None

def resolve_external_url(pdf_url, domain_handlers):
    """
    Checks if the URL domain has a registered handler in config.
    If so, dynamically loads the module and resolves the direct PDF link.
    """
    parsed_url = urlparse(pdf_url)
    domain = parsed_url.netloc
    
    # Check for matches (e.g. archive.org or www.archive.org)
    handler_module_name = None
    for pattern, module in domain_handlers.items():
        if pattern in domain:
            handler_module_name = module
            break
            
    if not handler_module_name:
        return pdf_url # No handler, return original

    try:
        if handler_module_name not in LOADED_HANDLERS:
            LOADED_HANDLERS[handler_module_name] = importlib.import_module(handler_module_name)
        
        handler_module = LOADED_HANDLERS[handler_module_name]
        if hasattr(handler_module, "resolve_pdf_url"):
            resolved_url = handler_module.resolve_pdf_url(pdf_url)
            return resolved_url if resolved_url else pdf_url
        else:
            logger.warning(f"Handler '{handler_module_name}' has no resolve_pdf_url function.")
            return pdf_url
    except Exception as e:
        logger.error(f"Error resolving URL via handler '{handler_module_name}': {e}")
        return pdf_url

def download_pdf(book_info, title_display):
    pdf_url = book_info['pdf_url']
    local_path = book_info['local_pdf_path']
    book_id = book_info['id']
    expected_hash = book_info.get('pdf_sha256_hash')

    if os.path.exists(local_path):
        current_hash = calculate_sha256(local_path)
        if current_hash:
            if expected_hash and current_hash == expected_hash:
                logger.info(f"PDF for Book {book_id} ('{title_display}') verified. Skipping.")
                return True
            elif not expected_hash:
                book_info['pdf_sha256_hash'] = current_hash
                return True
        os.remove(local_path)

    logger.info(f"Downloading PDF for {book_id} ('{title_display}') from '{pdf_url}'...")
    try:
        response = requests.get(pdf_url, stream=True, timeout=60)
        response.raise_for_status()
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        new_hash = calculate_sha256(local_path)
        if new_hash:
            book_info['pdf_sha256_hash'] = new_hash
            return True
        return False
    except Exception as e:
        logger.error(f"Download failed for {book_id}: {e}")
        return False

def copy_to_phone_storage(source_path, destination_base_dir, book_id, page_slug):
    filename = os.path.basename(source_path)
    destination_dir = os.path.join(destination_base_dir, page_slug, str(book_id).zfill(3))
    os.makedirs(destination_dir, exist_ok=True)
    destination_path = os.path.join(destination_dir, filename)
    if os.path.exists(source_path):
        try:
            subprocess.run(["cp", "-f", source_path, destination_path], check=True)
            return True
        except Exception as e:
            logger.error(f"Copy failed: {e}")
    return False

def process_books_workflow(page_config, domain_handlers, sarvam_ai_api_key, rescan_books=False):
    page_name = page_config.get('name', 'Unknown Page')
    page_url = page_config.get('url')
    page_language = page_config.get('language', 'kn-IN') 
    limit_books = page_config.get('limit')
    assorted_books_data = page_config.get('books')

    page_slug = get_page_slug(page_url) if page_url else slugify(page_name)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    state_file_path = os.path.join(script_dir, f"processing_state_{page_slug}.json")
    
    current_state = load_state(state_file_path) if os.path.exists(state_file_path) else {"books": []}

    # Step 1: Source Identification
    books_to_add = []
    if page_url:
        if rescan_books or not current_state['books']:
            existing_map = {b['id']: b for b in current_state['books']}
            books_to_add = scrape_books(page_url, existing_map, page_language)
        else:
            books_to_add = current_state['books']
    elif assorted_books_data:
        for b in assorted_books_data:
            book_id = b['id']
            # Dynamic URL Resolution
            resolved_url = resolve_external_url(b['pdf_url'], domain_handlers)
            
            title_slug = b.get('title_english_slug', slugify(b['title_original']))
            author_slug = b.get('author_english_slug', slugify(b.get('author_original', '')))
            
            # Name sanitization
            combined_slug = f"{title_slug}_{author_slug}".strip('_')
            if len(combined_slug) > 50: combined_slug = combined_slug[:47] + "..."
            filename = f"{book_id}_{combined_slug}.pdf"

            books_to_add.append({
                "id": book_id,
                "title_original": b['title_original'],
                "author_original": b.get('author_original', ''),
                "pdf_url": resolved_url,
                "local_pdf_path": get_full_path_for_book(RAW_PDF_BASE_DIR, page_slug, book_id, filename),
                "local_md_path": get_full_path_for_book(PROCESSED_DOCS_BASE_DIR, page_slug, book_id, filename.replace('.pdf', '.md')),
                "local_docx_path": get_full_path_for_book(PROCESSED_DOCS_BASE_DIR, page_slug, book_id, filename.replace('.pdf', '.docx')),
                "status": "pending"
            })

    # State Merge & Sync
    existing_map = {b['id']: b for b in current_state['books']}
    for new_b in books_to_add:
        if new_b['id'] in existing_map:
            existing_map[new_b['id']].update({
                "pdf_url": new_b['pdf_url'],
                "local_pdf_path": new_b['local_pdf_path'],
                "local_md_path": new_b['local_md_path'],
                "local_docx_path": new_b['local_docx_path']
            })
        else:
            current_state['books'].append(new_b)
    
    current_state['books'].sort(key=lambda x: str(x['id']).zfill(3))
    save_state(current_state, state_file_path)

    # Filtering & Limiting
    to_process = [b for b in current_state['books'] if b.get('status') != "completed"]
    if limit_books: to_process = to_process[:limit_books]

    logger.info(f"Processing {len(to_process)} books for '{page_name}'...")

    for i, book in enumerate(to_process):
        book_id = book['id']
        title = book.get('title_original', 'Unknown')
        
        # Absolute paths for local worker
        book['local_pdf_path'] = os.path.join(script_dir, book['local_pdf_path'])
        book['local_md_path'] = os.path.join(script_dir, book['local_md_path'])
        book['local_docx_path'] = os.path.join(script_dir, book['local_docx_path'])

        logger.info(f"[{i+1}/{len(to_process)}] ID={book_id} - {title}")

        # 2. Download
        if book.get('download_status') != "completed":
            if download_pdf(book, title):
                book['download_status'] = "completed"
            else:
                book['status'] = "failed"
                save_state(current_state, state_file_path)
                continue

        # 3. OCR
        if book.get('ocr_status') != "completed":
            md_path = ocr_to_markdown(book['local_pdf_path'], sarvam_ai_api_key, book['local_md_path'], lang_code=page_language)
            if md_path:
                book['ocr_status'] = "completed"
            else:
                book['status'] = "failed"
                save_state(current_state, state_file_path)
                continue

        # 4. Conversion
        if book.get('docx_conversion_status') != "completed":
            if process_markdown_to_docx(book['local_md_path'], book['local_docx_path']):
                book['docx_conversion_status'] = "completed"

        # 5. SD Card Sync
        copy_to_phone_storage(book['local_pdf_path'], SDCARD_RAW_PDF_DIR, book_id, page_slug)
        m_ok = copy_to_phone_storage(book['local_md_path'], SDCARD_PROCESSED_DOCS_DIR, book_id, page_slug)
        d_ok = copy_to_phone_storage(book['local_docx_path'], SDCARD_PROCESSED_DOCS_DIR, book_id, page_slug)

        if m_ok and d_ok:
            book['status'] = "completed"
        
        save_state(current_state, state_file_path)
        time.sleep(1)

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "config.json")
    
    with open(config_path, 'r') as f:
        full_config = json.load(f)
    
    domain_handlers = full_config.get("domain_handlers", {})
    pages = full_config.get("pages", [])

    for page in pages:
        if page.get("process_flag"):
            process_books_workflow(page, domain_handlers, SARVAM_AI_API_KEY, rescan_books=True)
