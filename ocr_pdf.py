import requests
import json
import time
import os
import sys
import zipfile
import io
import subprocess # Added for pandoc
import logging # Import logging module
import traceback # Import traceback for detailed exception info
from PyPDF2 import PdfReader, PdfWriter # Import PyPDF2
from PyPDF2.errors import PdfReadError # Import specific error for PDF corruption

# Configure logging for ocr_pdf.py
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ocr_pdf_debug.log"),
        logging.StreamHandler(sys.stderr)
    ]
)
logger = logging.getLogger(__name__)

# Import SarvamAI SDK
try:
    from sarvamai import SarvamAI
    from sarvamai.core.api_error import ApiError
except ImportError:
    logger.error("SarvamAI SDK not found. Attempting to install...")
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "sarvamai"], check=True)
        from sarvamai import SarvamAI
        from sarvamai.core.api_error import ApiError
        logger.info("SarvamAI SDK installed successfully in current environment.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to install SarvamAI SDK: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred during SDK installation: {e}")
        sys.exit(1)

# Flag to temporarily disable Sarvam AI OCR for debugging/testing purposes
# Set to True to skip Sarvam AI API calls.
DISABLE_SARVAM_AI_OCR = False 

# Sarvam AI page limit for OCR
SARVAM_AI_PAGE_LIMIT = 500

def get_pdf_page_count_and_check_integrity(pdf_path):
    """
    Returns the number of pages in a PDF and checks for basic integrity.
    Returns (page_count, None) on success, (0, error_message) on failure/corruption.
    """
    if not os.path.exists(pdf_path):
        return 0, "PDF file not found."
    
    try:
        reader = PdfReader(pdf_path)
        page_count = len(reader.pages)
        # Attempt to read a page to check for corruption
        if page_count > 0:
            _ = reader.pages[0] # Try to access the first page
        return page_count, None
    except PdfReadError as e:
        return 0, f"Corrupted PDF file: {e}"
    except Exception as e:
        return 0, f"Error reading PDF file: {e}"

def split_pdf_into_chunks(original_pdf_path, max_pages_per_chunk):
    """
    Splits a large PDF into smaller PDF files (chunks).
    Returns a list of paths to the created chunk PDF files.
    """
    if not os.path.exists(original_pdf_path):
        logger.error(f"Original PDF not found for splitting: {original_pdf_path}")
        return []

    original_filename = os.path.basename(original_pdf_path)
    base_name, _ = os.path.splitext(original_filename)
    output_dir = os.path.join(os.path.dirname(original_pdf_path), f"{base_name}_chunks")
    os.makedirs(output_dir, exist_ok=True)

    chunk_paths = []
    try:
        reader = PdfReader(original_pdf_path)
        total_pages = len(reader.pages)
        
        for i in range(0, total_pages, max_pages_per_chunk):
            writer = PdfWriter()
            start_page = i
            end_page = min(i + max_pages_per_chunk, total_pages)
            
            for page_num in range(start_page, end_page):
                writer.add_page(reader.pages[page_num])
            
            chunk_filename = f"{base_name}_part_{start_page // max_pages_per_chunk + 1}.pdf"
            chunk_path = os.path.join(output_dir, chunk_filename)
            with open(chunk_path, 'wb') as output_pdf:
                writer.write(output_pdf)
            chunk_paths.append(chunk_path)
            logger.debug(f"Created PDF chunk: {chunk_path} (pages {start_page+1}-{end_page})")
            
    except Exception as e:
        logger.error(f"Error splitting PDF '{original_pdf_path}' into chunks: {e}")
        return []
    
    return chunk_paths


# Function to handle Markdown to DOCX conversion
def process_markdown_to_docx(md_input_path, docx_output_path):
    """
    Converts a Markdown file to a DOCX file using Pandoc.
    Returns True on success, False otherwise.
    """
    if not os.path.exists(md_input_path):
        logger.error(f"Markdown file not found at {md_input_path}")
        return False

    # Ensure the output directory exists
    os.makedirs(os.path.dirname(docx_output_path), exist_ok=True)

    logger.info(f"Converting Markdown '{md_input_path}' to DOCX '{docx_output_path}' (Pandoc)...")
    try:
        # Check if pandoc exists
        subprocess.run(["pandoc", "--version"], check=True, text=True, capture_output=True)
        subprocess.run(["pandoc", md_input_path, "-o", docx_output_path], check=True, text=True, capture_output=True)
        logger.info(f"Successfully converted '{md_input_path}' to '{docx_output_path}'")
        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"Error converting Markdown to DOCX with Pandoc: {e}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        return False
    except FileNotFoundError:
        logger.error("Pandoc is not installed or not in PATH. Please install pandoc.")
        return False

def _ocr_single_pdf_chunk(pdf_path, sarvam_ai_api_key, output_md_path, lang_code, output_format):
    """
    Helper function to perform OCR on a single PDF (or chunk) using Sarvam AI SDK.
    Returns the path to the extracted MD file on success, None otherwise.
    """
    if DISABLE_SARVAM_AI_OCR:
        logger.debug(f"Sarvam AI OCR skipped for '{os.path.basename(pdf_path)}' (DISABLED). Creating placeholder MD.")
        placeholder_content = f"# OCR Skipped for {os.path.basename(pdf_path)}\n\nThis is a placeholder Markdown file as Sarvam AI OCR was disabled for testing purposes."
        with open(output_md_path, 'w', encoding='utf-8') as f_out:
            f_out.write(placeholder_content)
        logger.info(f"Placeholder MD created at {output_md_path}")
        return os.path.abspath(output_md_path)

    # Initialize SarvamAI client
    try:
        client = SarvamAI(api_subscription_key=sarvam_ai_api_key)
    except Exception as e:
        logger.error(f"Failed to initialize SarvamAI SDK client for chunk '{os.path.basename(pdf_path)}': {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

    try:
        logger.info(f"Creating Document Intelligence job for chunk '{os.path.basename(pdf_path)}' (Sarvam AI SDK)...")
        job = client.document_intelligence.create_job(
            language=lang_code,
            output_format=output_format
        )
        logger.info(f"Job created successfully. Job ID: {job.job_id}")

        logger.info(f"Uploading document chunk '{os.path.basename(pdf_path)}' (Sarvam AI SDK)...")
        job.upload_file(pdf_path)
        logger.info("Document chunk uploaded.")

        logger.info("Starting document chunk processing (Sarvam AI SDK)...")
        job.start()
        logger.info("Processing started.")

        logger.info("Waiting for chunk job completion (Sarvam AI SDK)...")
        status = job.wait_until_complete()
        logger.info(f"Chunk job completed with state: {status.job_state}")
        
        if status.job_state == "Completed":
            temp_zip_path = os.path.join(os.path.dirname(output_md_path), f"sarvam_output_{job.job_id}.zip")
            logger.info(f"Downloading chunk output to '{temp_zip_path}' (Sarvam AI SDK)...")
            job.download_output(temp_zip_path)
            logger.info(f"Chunk output saved to '{temp_zip_path}'")

            with zipfile.ZipFile(temp_zip_path, 'r') as z:
                md_files = [f.filename for f in z.infolist() if f.filename.endswith(f'.{output_format}')]
                if not md_files:
                    logger.error(f"No {output_format} file found in the downloaded ZIP for chunk '{os.path.basename(pdf_path)}'.")
                    os.remove(temp_zip_path)
                    return None
                
                with open(output_md_path, 'w', encoding='utf-8') as f_out:
                    f_out.write(z.read(md_files[0]).decode('utf-8'))
            logger.info(f"Successfully extracted {output_format} from chunk to {output_md_path}")
            os.remove(temp_zip_path) # Clean up temporary zip
            return os.path.abspath(output_md_path)
        else:
            logger.error(f"Sarvam AI chunk job did not complete successfully. Final state: {status.job_state}")
            if hasattr(status, 'error'):
                logger.error(f"ERROR Details: {status.error}")
            return None
            
    except ApiError as e:
        logger.error(f"API ERROR during Sarvam AI OCR for chunk '{os.path.basename(pdf_path)}': Status {e.status_code}, Body: {e.body}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None
    except Exception as e:
        logger.error(f"AN UNEXPECTED ERROR OCCURRED during Sarvam AI OCR for chunk '{os.path.basename(pdf_path)}': {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

def ocr_to_markdown(pdf_path, sarvam_ai_api_key, output_md_path, lang_code="kn-IN", output_format="md"):
    """
    Performs OCR on a PDF using Sarvam AI SDK and extracts markdown.
    If the PDF exceeds SARVAM_AI_PAGE_LIMIT, it is split into chunks.
    Writes the extracted Markdown to output_md_path.
    Returns the path to the extracted MD file on success, None otherwise.
    """
    logger.debug(f"Received output_md_path: {output_md_path}")

    if not os.path.exists(pdf_path):
        logger.error(f"PDF file not found at {pdf_path}. Cannot proceed with OCR.")
        return None

    os.makedirs(os.path.dirname(output_md_path), exist_ok=True)

    # --- Pre-OCR PDF Check with PyPDF2 ---
    page_count, pdf_error = get_pdf_page_count_and_check_integrity(pdf_path)
    if pdf_error:
        logger.error(f"PDF pre-check failed for '{os.path.basename(pdf_path)}': {pdf_error}. Skipping OCR.")
        placeholder_content = f"# OCR Failed for {os.path.basename(pdf_path)}\n\nError: {pdf_error}\nThis is a placeholder Markdown file due to PDF pre-check failure."
        with open(output_md_path, 'w', encoding='utf-8') as f_out:
            f_out.write(placeholder_content)
        return None

    if page_count > SARVAM_AI_PAGE_LIMIT:
        logger.info(f"PDF '{os.path.basename(pdf_path)}' has {page_count} pages, exceeding the limit of {SARVAM_AI_PAGE_LIMIT}. Splitting into chunks...")
        chunk_pdf_paths = split_pdf_into_chunks(pdf_path, SARVAM_AI_PAGE_LIMIT)
        
        if not chunk_pdf_paths:
            logger.error(f"Failed to split PDF '{os.path.basename(pdf_path)}' into chunks.")
            return None

        all_chunk_md_paths = []
        overall_success = True
        for i, chunk_pdf_path in enumerate(chunk_pdf_paths):
            chunk_base_name = os.path.basename(chunk_pdf_path)
            chunk_md_output_path = os.path.join(os.path.dirname(output_md_path), f"{os.path.splitext(chunk_base_name)[0]}.md")
            
            logger.info(f"Processing chunk {i+1}/{len(chunk_pdf_paths)}: '{chunk_base_name}'")
            chunk_md_path = _ocr_single_pdf_chunk(chunk_pdf_path, sarvam_ai_api_key, chunk_md_output_path, lang_code, output_format)
            
            if chunk_md_path:
                all_chunk_md_paths.append(chunk_md_path)
            else:
                logger.error(f"OCR failed for chunk '{chunk_base_name}'. Aborting processing for '{os.path.basename(pdf_path)}'.")
                overall_success = False
                break
        
        # Clean up chunk PDF files regardless of success
        for p in chunk_pdf_paths:
            os.remove(p)
        if os.path.exists(os.path.dirname(chunk_pdf_paths[0])): # Remove chunk directory
            os.rmdir(os.path.dirname(chunk_pdf_paths[0]))

        if not overall_success:
            return None # Indicate failure for the main PDF
        
        # Merge all chunk Markdown files into the final output_md_path
        logger.info(f"Merging {len(all_chunk_md_paths)} Markdown chunks into '{output_md_path}'...")
        with open(output_md_path, 'w', encoding='utf-8') as final_md:
            for chunk_md_file in all_chunk_md_paths:
                with open(chunk_md_file, 'r', encoding='utf-8') as chunk_content:
                    final_md.write(chunk_content.read())
                    final_md.write("\n\n---\n\n") # Separator between chunks
                os.remove(chunk_md_file) # Clean up individual chunk MD
        logger.info(f"Successfully merged all chunks into {output_md_path}")
        return os.path.abspath(output_md_path)

    logger.info(f"PDF pre-check passed for '{os.path.basename(pdf_path)}'. Page count: {page_count}. Performing OCR directly.")
    # Fall through to original single PDF OCR logic if not chunked
    return _ocr_single_pdf_chunk(pdf_path, sarvam_ai_api_key, output_md_path, lang_code, output_format)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Perform OCR on PDF and convert to DOCX.")
    parser.add_argument("--pdf", help="Path to the PDF file for OCR.")
    parser.add_argument("--md-output", help="Path to the output Markdown file for OCR.")
    parser.add_argument("--md-input", help="Path to the Markdown file for DOCX conversion.")
    parser.add_argument("--docx-output", help="Path to the output DOCX file (required for --md-input).")
    parser.add_argument("--sarvam-key", help="Sarvam AI API Key.")
    parser.add_argument("--lang-code", default="kn-IN", help="Language code for Sarvam AI OCR (e.g., 'kn-IN', 'en-IN').")
    args = parser.parse_args()

    if args.md_input and args.docx_output:
        # Check if pandoc exists only when DOCX conversion is requested
        try:
            subprocess.run(["pandoc", "--version"], check=True, text=True, capture_output=True)
        except FileNotFoundError:
            logger.error("Pandoc is not installed or not in PATH. DOCX conversion will fail.")
            sys.exit(1)
        process_markdown_to_docx(args.md_input, args.docx_output)
    elif args.pdf and args.sarvam_key and args.md_output:
        md_file_path = ocr_to_markdown(args.pdf, args.sarvam_key, args.md_output, args.lang_code)
        if md_file_path:
            logger.info(md_file_path)
    else:
        logger.info("Usage for OCR: python ocr_pdf.py --pdf <path_to_pdf_file> --sarvam-key <your_sarvam_ai_api_key> --md-output <path_for_md_output> [--lang-code <language>]")
        logger.info("Usage for DOCX: python ocr_pdf.py --md-input <path_to_md_file> --docx-output <path_for_docx_output>")
        sys.exit(1)