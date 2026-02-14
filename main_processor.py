import os
import json
import requests
import time
import subprocess
import hashlib 
import re 
import sys
import logging
from dotenv import load_dotenv # Import load_dotenv
load_dotenv() # Load environment variables from .env file

# Configure logging for main_processor.py
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("main_processor_debug.log"),
        logging.StreamHandler(sys.stderr) # Still print to stderr for immediate feedback if possible
    ]
)
logger = logging.getLogger(__name__)

# Import functions from scraper and ocr_pdf
from scraper import load_state, save_state, scrape_books, get_page_slug
from ocr_pdf import ocr_to_markdown, process_markdown_to_docx # Directly import the functions

# --- Configuration ---
# Your Sarvam AI Key - retrieved from initial state_snapshot
SARVAM_AI_API_KEY = os.environ.get("SARVAM_AI_API_KEY")
if not SARVAM_AI_API_KEY:
    logger.error("SARVAM_AI_API_KEY environment variable not set. Exiting.")
    sys.exit(1)
BASE_URL = "https://adhyatmaprakasha.org/php/kannada_books.php" # Define BASE_URL here for main_processor

RAW_PDF_BASE_DIR = "raw_pdf" # Base directory for raw PDFs
PROCESSED_DOCS_BASE_DIR = "processed_docs" # Base directory for processed docs

# Phone storage directories (absolute paths)
SDCARD_RAW_PDF_DIR = "/sdcard/raw_pdf"
SDCARD_PROCESSED_DOCS_DIR = "/sdcard/processed_docs"

# Ensure phone storage directories exist
os.makedirs(SDCARD_RAW_PDF_DIR, exist_ok=True)
os.makedirs(SDCARD_PROCESSED_DOCS_DIR, exist_ok=True)

# --- Helper Functions ---

def calculate_sha256(filepath):
    """Calculates the SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except FileNotFoundError:
        logger.error(f"File not found for SHA256 calculation: '{filepath}'")
        return None
    except Exception as e:
        logger.error(f"Failed to calculate SHA256 for '{filepath}': {e}")
        return None

def download_pdf(book_info):
    """
    Downloads a PDF file from the given URL and saves it locally.
    Performs integrity check if hash exists.
    Returns True on successful download and verification, False otherwise.
    """
    pdf_url = book_info['pdf_url']
    local_path = book_info['local_pdf_path']
    book_id = book_info['id']
    title_kannada = book_info['title_kannada']
    expected_hash = book_info.get('pdf_sha256_hash') # Get existing hash from state

    # Check if file exists and verify hash if available
    if os.path.exists(local_path):
        current_hash = calculate_sha256(local_path)
        if current_hash:
            if expected_hash and current_hash == expected_hash:
                logger.info(f"PDF for Book {book_id} ('{title_kannada}') already exists and is verified at '{local_path}'. Skipping download.")
                return True
            elif expected_hash and current_hash != expected_hash:
                logger.warning(f"PDF for Book {book_id} at '{local_path}' is corrupted (hash mismatch). Removing and re-downloading...")
                os.remove(local_path) # Remove corrupted file
            elif not expected_hash:
                # If file exists but no hash was recorded (e.g., first run before this feature was added)
                # Assume it's valid for now, but record its hash
                logger.info(f"PDF for Book {book_id} already exists at '{local_path}', no hash recorded. Calculating and storing hash.")
                book_info['pdf_sha256_hash'] = current_hash
                return True
        else:
            logger.error(f"Could not calculate SHA256 for existing file '{local_path}'. Will attempt re-download.")
            if os.path.exists(local_path):
                os.remove(local_path) # Remove potentially problematic file

    logger.info(f"Downloading PDF for Book {book_id} ('{title_kannada}') from '{pdf_url}' to '{local_path}'...")
    try:
        response = requests.get(pdf_url, stream=True)
        response.raise_for_status()

        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Successfully downloaded PDF for Book {book_id}.")
        
        # Calculate and record hash after successful download
        new_hash = calculate_sha256(local_path)
        if new_hash:
            book_info['pdf_sha256_hash'] = new_hash
            logger.info(f"Recorded SHA256 hash for {book_id}: {new_hash}")
            return True
        else:
            logger.error(f"Failed to calculate SHA256 for newly downloaded PDF '{local_path}'.")
            return False

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download PDF for Book {book_id} from '{pdf_url}': {e}")
        return False

def copy_to_phone_storage(source_path, destination_base_dir, book_id, page_slug):
    """
    Copies a file to the phone's SD card storage, maintaining the page_slug/book_id subdirectory structure.
    """
    filename = os.path.basename(source_path)
    destination_dir = os.path.join(destination_base_dir, page_slug, str(book_id).zfill(3))
    os.makedirs(destination_dir, exist_ok=True)
    destination_path = os.path.join(destination_dir, filename)

    if os.path.exists(source_path):
        try:
            subprocess.run(["cp", "-f", source_path, destination_path], check=True)
            logger.info(f"Copied '{source_path}' to '{destination_path}'")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to copy '{source_path}' to '{destination_path}': {e}")
            return False
    else:
        logger.warning(f"Source file '{source_path}' not found for copying.")
        return False

# --- Main Processing Logic ---

def process_books_workflow(sarvam_ai_api_key, limit_books=None, rescan_books=False):
    """
    Orchestrates the entire book processing workflow.
    """
    logger.info("--- Starting Book Processing Workflow ---")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Determine the page slug once
    page_slug = get_page_slug(BASE_URL)
    state_file_path = os.path.join(script_dir, f"processing_state_{page_slug}.json")
    
    current_state = {"books": []}
    if os.path.exists(state_file_path):
        current_state = load_state(state_file_path) # Always load state to preserve history

    # 1. Scrape book information and update state
    if rescan_books or not current_state['books']: # Rescan if forced or if state is empty
        logger.info("STEP 1: Scraping book information...")
        
        # Initialize existing_books_map BEFORE calling scrape_books
        existing_books_map = {book['id']: book for book in current_state['books']}
        
        # Directly call scrape_books function
        scraped_books_data = scrape_books(BASE_URL, existing_books_map)
        
        logger.info("Scraper finished. Loading and updating state.")
        
        # Merge scraped data into current_state, handling existing entries
        existing_books_map = {book['id']: book for book in current_state['books']}
        for new_book in scraped_books_data:
            if new_book['id'] in existing_books_map:
                # Update existing book's details but preserve its original status unless explicitly changed
                existing_book = existing_books_map[new_book['id']]
                
                # Only update mutable fields like paths, and KANNADA titles/authors.
                # Preserve existing slugs to avoid re-transliteration unless they are missing.
                existing_book.update({
                    "title_kannada": new_book['title_kannada'],
                    "author_kannada": new_book['author_kannada'],
                    "pdf_url": new_book['pdf_url'],
                    "local_pdf_path": new_book['local_pdf_path'],
                    "local_md_path": new_book['local_md_path'],
                    "local_docx_path": new_book['local_docx_path'],
                })
                # Only update English slugs if they were previously empty or missing
                if not existing_book.get('title_english_slug'):
                    existing_book['title_english_slug'] = new_book['title_english_slug']
                if not existing_book.get('author_english_slug'):
                    existing_book['author_english_slug'] = new_book['author_english_slug']

            else:
                current_state['books'].append(new_book)
                existing_books_map[new_book['id']] = new_book # Add to map for later updates

        # Sort books by ID to ensure consistent processing order
        current_state['books'].sort(key=lambda x: str(x['id']).zfill(3)) 
        save_state(current_state, state_file_path) # Save state after initial scrape/update
    else:
        logger.info(f"STEP 1: Skipping scraping. Using existing metadata from {state_file_path}.")

    books_to_process = []
    for book in current_state['books']:
        # Only process if status is not 'completed'
        if book.get('status') != "completed":
            books_to_process.append(book)
    
    if limit_books:
        books_to_process = books_to_process[:limit_books]

    logger.info(f"Found {len(books_to_process)} books to process (status: not completed).")

    for i, book in enumerate(books_to_process):
        book_id = book['id']
        title_kannada = book['title_kannada']
        
        # Convert relative paths (stored in state) to absolute paths for file operations
        # These paths were constructed with RAW_PDF_BASE_DIR/page_slug/book_id/...
        # so now prepend script_dir
        # CRITICAL: book['local_pdf_path'] contains 'raw_pdf', book['local_md_path'] contains 'processed_docs'
        book['local_pdf_path'] = os.path.join(script_dir, book['local_pdf_path'])
        book['local_md_path'] = os.path.join(script_dir, book['local_md_path'])
        book['local_docx_path'] = os.path.join(script_dir, book['local_docx_path'])

        logger.info(f"\n--- Processing Book {i+1}/{len(books_to_process)}: ID={book_id} - '{title_kannada}' ---")
        
        # --- Stage: Download PDF ---
        if book.get('download_status') != "completed":
            book['status'] = "in_progress" # Mark general status
            book['download_status'] = "in_progress"
            save_state(current_state, state_file_path)

            max_download_retries = 3
            download_success = False
            for attempt in range(max_download_retries):
                logger.info(f"STEP 2: Downloading PDF (Attempt {attempt+1}/{max_download_retries}) for Book {book_id}...")
                
                # download_pdf modifies book directly for hash, so pass reference
                if download_pdf(book): 
                    book['download_status'] = "completed"
                    download_success = True
                    logger.info(f"PDF for Book {book_id} downloaded successfully.")
                    break
                else:
                    logger.warning(f"PDF download failed for Book {book_id} (Attempt {attempt+1}). Retrying in 5 seconds...")
                    time.sleep(5)
            
            if not download_success:
                book['download_status'] = "failed"
                book['status'] = "failed"
                book['error_message'] = "PDF download failed after multiple retries."
                save_state(current_state, state_file_path)
                logger.error(f"PDF download ultimately failed for Book {book_id}. Skipping to next book.")
                time.sleep(2) # Delay before next book
                continue # Move to next book
            save_state(current_state, state_file_path) # Save state after successful download

        # --- Stage: OCR to Markdown ---
        if book.get('ocr_status') != "completed":
            # Verify downloaded PDF exists and is intact before OCR
            # Ensure local_pdf_path is correct and points to the file saved by download_pdf
            if not os.path.exists(book['local_pdf_path']) or \
               (book.get('pdf_sha256_hash') and calculate_sha256(book['local_pdf_path']) != book['pdf_sha256_hash']):
                logger.error(f"Downloaded PDF '{book['local_pdf_path']}' not found or corrupted before OCR. Marking as failed for OCR.")
                book['ocr_status'] = "failed"
                book['status'] = "failed"
                book['error_message'] = "PDF file missing or corrupted before OCR."
                save_state(current_state, state_file_path)
                time.sleep(2)
                continue # Move to next book

            book['status'] = "in_progress" # Mark general status
            book['ocr_status'] = "in_progress"
            save_state(current_state, state_file_path)

            max_ocr_retries = 3
            ocr_success = False
            for attempt in range(max_ocr_retries):
                logger.info(f"STEP 3: Performing OCR (Attempt {attempt+1}/{max_ocr_retries}) for Book {book_id}...")
                logger.debug(f"Passing local_md_path to ocr_pdf.py: {book['local_md_path']}") # DEBUG PRINT

                try:
                    # Directly call the ocr_to_markdown function from ocr_pdf.py
                    # Pass book['local_md_path'] as the explicit output path
                    md_file_path_from_ocr = ocr_to_markdown(book['local_pdf_path'], sarvam_ai_api_key, book['local_md_path'])
                    
                    if md_file_path_from_ocr and os.path.exists(md_file_path_from_ocr):
                        book['ocr_status'] = "completed"
                        book['local_md_path'] = md_file_path_from_ocr # Update with actual path from OCR script
                        ocr_success = True
                        logger.info(f"OCR successful for Book {book_id}. MD saved to {md_file_path_from_ocr}")
                        break
                    else:
                        logger.error(f"OCR function did not return a valid MD path or file not found. Returned: {md_file_path_from_ocr}")
                        
                except Exception as e:
                    logger.error(f"An unexpected error occurred during OCR for Book {book_id}: {e}")
                
                logger.warning(f"OCR failed for Book {book_id} (Attempt {attempt+1}). Retrying in 5 seconds...")
                time.sleep(5)
            
            if not ocr_success:
                book['ocr_status'] = "failed"
                book['status'] = "failed"
                book['error_message'] = "OCR to Markdown failed after multiple retries."
                save_state(current_state, state_file_path)
                logger.error(f"OCR ultimately failed for Book {book_id}. Skipping to next book.")
                time.sleep(2)
                continue
            save_state(current_state, state_file_path) # Save state after successful OCR

        # --- Stage: Convert Markdown to DOCX ---
        if book.get('docx_conversion_status') != "completed":
            # Verify MD file exists before DOCX conversion
            # Ensure local_md_path is correct for verification
            current_md_path_to_check = book['local_md_path'] 
            
            if not os.path.exists(current_md_path_to_check):
                logger.error(f"Markdown file '{current_md_path_to_check}' not found before DOCX conversion. Marking as failed for DOCX.")
                book['docx_conversion_status'] = "failed"
                book['status'] = "failed"
                book['error_message'] = "Markdown file missing before DOCX conversion."
                save_state(current_state, state_file_path)
                time.sleep(2)
                continue # Move to next book

            book['status'] = "in_progress" # Mark general status
            book['docx_conversion_status'] = "in_progress"
            save_state(current_state, state_file_path)

            max_docx_retries = 3
            docx_success = False
            for attempt in range(max_docx_retries):
                logger.info(f"STEP 4: Converting Markdown to DOCX (Attempt {attempt+1}/{max_docx_retries}) for Book {book_id}...")
                try:
                    # Directly call the process_markdown_to_docx function from ocr_pdf.py
                    # Pass book['local_docx_path'] as the explicit output path
                    if process_markdown_to_docx(current_md_path_to_check, book['local_docx_path']):
                        if os.path.exists(book['local_docx_path']): # Check if DOCX file exists after conversion
                            book['docx_conversion_status'] = "completed"
                            docx_success = True
                            logger.info(f"DOCX conversion successful for Book {book_id}. DOCX saved to {book['local_docx_path']}")
                            break
                        else:
                            logger.error(f"DOCX conversion function reported success, but file '{book['local_docx_path']}' not found.")

                except Exception as e:
                    logger.error(f"An unexpected error occurred during DOCX conversion for Book {book_id}: {e}")
                
                logger.warning(f"DOCX conversion failed for Book {book_id} (Attempt {attempt+1}). Retrying in 5 seconds...")
                time.sleep(5)
            
            if not docx_success:
                book['docx_conversion_status'] = "failed"
                book['status'] = "failed"
                book['error_message'] = "DOCX conversion failed after multiple retries."
                save_state(current_state, state_file_path)
                logger.error(f"DOCX conversion ultimately failed for Book {book_id}. Skipping to next book.")
                time.sleep(2)
                continue
            save_state(current_state, state_file_path) # Save state after successful DOCX conversion

        # --- Stage: Copy to Phone Storage ---
        logger.info(f"STEP 5: Copying generated MD and DOCX for Book {book_id} to phone storage...")
        # Ensure raw PDF is copied if not already (e.g., if re-running from OCR step)
        raw_pdf_dest_path_on_sd = os.path.join(SDCARD_RAW_PDF_DIR, page_slug, str(book_id).zfill(3), os.path.basename(book['local_pdf_path']))
        if not os.path.exists(raw_pdf_dest_path_on_sd):
             copy_to_phone_storage(book['local_pdf_path'], SDCARD_RAW_PDF_DIR, book_id, page_slug)

        copy_md_success = copy_to_phone_storage(book['local_md_path'], SDCARD_PROCESSED_DOCS_DIR, book_id, page_slug)
        copy_docx_success = copy_to_phone_storage(book['local_docx_path'], SDCARD_PROCESSED_DOCS_DIR, book_id, page_slug)

        if copy_md_success and copy_docx_success:
            book['copy_to_sdcard_status'] = "completed"
            book['status'] = "completed" # Mark overall process as completed
            logger.info(f"All processing and copying completed successfully for Book {book_id}.")
        else:
            book['copy_to_sdcard_status'] = "failed"
            book['status'] = "failed"
            book['error_message'] = book.get('error_message', "File copying to SD card failed.")
            logger.error(f"Book {book_id} processing ended with file copying issues.")
        
        save_state(current_state, state_file_path)
        
        # Add a small delay between processing books to avoid overloading APIs
        time.sleep(2) 

    logger.info("\n--- Book Processing Workflow Finished ---")
    
    # Final summary
    current_state = load_state(state_file_path) # Reload state to get latest statuses
    processed_count = sum(1 for b in current_state['books'] if b.get('status') == "completed")
    failed_count = sum(1 for b in current_state['books'] if b.get('status') == "failed")
    pending_count = sum(1 for b in current_state['books'] if b.get('status') == "pending" or b.get('status') == "in_progress")
    logger.info(f"Summary: {processed_count} completed, {failed_count} failed, {pending_count} pending.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Automate OCR and document conversion for Kannada scanned PDFs.")
    parser.add_argument("--limit", type=int, help="Limit the number of books to process for testing.")
    parser.add_argument("--rescan-books", action="store_true", help="Force rescanning of book metadata from the website.")
    args = parser.parse_args()

    # Pass the SARVAM_AI_API_KEY from config
    process_books_workflow(SARVAM_AI_API_KEY, limit_books=args.limit, rescan_books=args.rescan_books)