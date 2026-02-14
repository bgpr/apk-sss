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
DISABLE_SARVAM_AI_OCR = False # <--- SET THIS TO FALSE

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

def ocr_to_markdown(pdf_path, sarvam_ai_api_key, output_md_path, lang_code="kn-IN", output_format="md"):
    """
    Performs OCR on a PDF using Sarvam AI SDK and extracts markdown.
    Writes the extracted Markdown to output_md_path.
    Returns the path to the extracted MD file on success, None otherwise.
    """
    logger.debug(f"Received output_md_path: {output_md_path}") # DEBUG PRINT

    if not os.path.exists(pdf_path):
        logger.error(f"PDF file not found at {pdf_path}")
        return None

    # Ensure the output directory exists for the MD file
    os.makedirs(os.path.dirname(output_md_path), exist_ok=True)

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
        # logger.debug("SarvamAI SDK client initialized in ocr_to_markdown.")
    except Exception as e:
        logger.error(f"Failed to initialize SarvamAI SDK client: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

    try:
        # Create a Document Intelligence job
        logger.info(f"Creating Document Intelligence job for '{os.path.basename(pdf_path)}' (Sarvam AI SDK)...")
        job = client.document_intelligence.create_job(
            language=lang_code,
            output_format=output_format
        )
        logger.info(f"Job created successfully. Job ID: {job.job_id}")

        # Upload your document
        logger.info(f"Uploading document '{os.path.basename(pdf_path)}' (Sarvam AI SDK)...")
        job.upload_file(pdf_path)
        logger.info("Document uploaded.")

        # Start processing
        logger.info("Starting document processing (Sarvam AI SDK)...")
        job.start()
        logger.info("Processing started.")

        # Wait for completion
        logger.info("Waiting for job completion (Sarvam AI SDK)...")
        status = job.wait_until_complete()
        logger.info(f"Job completed with state: {status.job_state}")
        
        if status.job_state == "Completed":
            # Create a temporary path for the downloaded ZIP
            # Place temp zip in the same directory as the MD output
            temp_zip_path = os.path.join(os.path.dirname(output_md_path), f"sarvam_output_{job.job_id}.zip")

            # Download the output (ZIP file containing the processed document)
            logger.info(f"Downloading output to '{temp_zip_path}' (Sarvam AI SDK)...")
            job.download_output(temp_zip_path)
            logger.info(f"Output saved to '{temp_zip_path}'")

            # Extract markdown from the ZIP file
            with zipfile.ZipFile(temp_zip_path, 'r') as z:
                # Find the markdown file within the zip (assuming one .md file)
                md_files = [f.filename for f in z.infolist() if f.filename.endswith(f'.{output_format}')]
                if not md_files:
                    logger.error(f"No {output_format} file found in the downloaded ZIP.")
                    os.remove(temp_zip_path)
                    return None
                
                # Extract and save the content to the specified output_md_path
                with open(output_md_path, 'w', encoding='utf-8') as f_out:
                    f_out.write(z.read(md_files[0]).decode('utf-8'))
            logger.info(f"Successfully extracted {output_format} to {output_md_path}")
            os.remove(temp_zip_path) # Clean up temporary zip
            return os.path.abspath(output_md_path) # Return the absolute path
        else:
            logger.error(f"Sarvam AI job did not complete successfully. Final state: {status.job_state}")
            if hasattr(status, 'error'):
                logger.error(f"ERROR Details: {status.error}")
            return None
            
    except ApiError as e:
        logger.error(f"API ERROR during Sarvam AI OCR: Status {e.status_code}, Body: {e.body}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None
    except Exception as e:
        logger.error(f"AN UNEXPECTED ERROR OCCURRED during Sarvam AI OCR: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Perform OCR on PDF and convert to DOCX.")
    parser.add_argument("--pdf", help="Path to the PDF file for OCR.")
    parser.add_argument("--md-output", help="Path to the output Markdown file for OCR.")
    parser.add_argument("--md-input", help="Path to the Markdown file for DOCX conversion.")
    parser.add_argument("--docx-output", help="Path to the output DOCX file (required for --md-input).")
    parser.add_argument("--sarvam-key", help="Sarvam AI API Key.")
    args = parser.parse_args()

    if args.md_input and args.docx_output:
        # Check if pandoc is installed only when DOCX conversion is requested
        try:
            subprocess.run(["pandoc", "--version"], check=True, text=True, capture_output=True)
        except FileNotFoundError:
            logger.error("Pandoc is not installed or not in PATH. DOCX conversion will fail.")
            sys.exit(1)
        process_markdown_to_docx(args.md_input, args.docx_output)
    elif args.pdf and args.sarvam_key and args.md_output:
        md_file_path = ocr_to_markdown(args.pdf, args.sarvam_key, args.md_output)
        if md_file_path:
            # When run standalone, if OCR is successful, print the md_file_path
            # main_processor.py expects this path for subsequent steps
            logger.info(md_file_path) # Changed to logger.info
    else:
        logger.info("Usage for OCR: python ocr_pdf.py --pdf <path_to_pdf_file> --sarvam-key <your_sarvam_ai_api_key> --md-output <path_for_md_output>")
        logger.info("Usage for DOCX: python ocr_pdf.py --md-input <path_to_md_file> --docx-output <path_for_docx_output>")
        sys.exit(1)
