import os
import sys
import time
import subprocess

# Ensure sarvamai SDK is installed
try:
    from sarvamai import SarvamAI
    from sarvamai.core.api_error import ApiError
except ImportError:
    print("SarvamAI SDK not found. Installing...")
    try:
        subprocess.run([sys.executable, "-m", "pip", "install", "sarvamai"], check=True)
        from sarvamai import SarvamAI
        from sarvamai.core.api_error import ApiError
        print("SarvamAI SDK installed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error installing SarvamAI SDK: {e}")
        print("Please ensure pip is available and try again.")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during SDK installation: {e}")
        sys.exit(1)

# Configuration
SARVAM_AI_API_KEY = "SK_SARVAM_AI_KEY_PURGED_FROM_HISTORY" # The new API key
TEST_PDF_PATH = "/data/data/com.termux/files/home/apk/raw_pdf/kannada_books/001/001_adhyatmavendarenu-prashnottara_shri-shri-satchidanandendra-saraswati-swamigalavaru.pdf"
OUTPUT_ZIP_PATH = "/data/data/com.termux/files/home/apk/test_sarvam_output.zip"

def test_document_intelligence_sdk():
    print("--- Starting Sarvam AI SDK Document Intelligence Test ---")
    
    if not os.path.exists(TEST_PDF_PATH):
        print(f"ERROR: Test PDF file not found at '{TEST_PDF_PATH}'. Please ensure it exists.")
        return False

    client = None
    try:
        client = SarvamAI(api_subscription_key=SARVAM_AI_API_KEY)
        print("INFO: SarvamAI client initialized.")
    except Exception as e:
        print(f"ERROR: Failed to initialize SarvamAI client: {e}")
        return False

    try:
        # Create a Document Intelligence job
        print(f"INFO: Creating Document Intelligence job for '{os.path.basename(TEST_PDF_PATH)}'...")
        job = client.document_intelligence.create_job(
            language="kn-IN",           # Kannada language code
            output_format="md"          # Output format: "md"
        )
        print(f"INFO: Job created successfully. Job ID: {job.job_id}")

        # Upload your document
        print(f"INFO: Uploading document '{os.path.basename(TEST_PDF_PATH)}'...")
        job.upload_file(TEST_PDF_PATH)
        print("INFO: Document uploaded.")

        # Start processing
        print("INFO: Starting document processing...")
        job.start()
        print("INFO: Processing started.")

        # Wait for completion
        print("INFO: Waiting for job completion...")
        status = job.wait_until_complete()
        print(f"INFO: Job completed with state: {status.job_state}")
        
        if status.job_state == "Completed":
            # Get processing metrics (optional, for verification)
            metrics = job.get_page_metrics()
            print(f"INFO: Pages processed: {metrics['pages_processed']}")

            # Download the output (ZIP file containing the processed document)
            print(f"INFO: Downloading output to '{OUTPUT_ZIP_PATH}'...")
            job.download_output(OUTPUT_ZIP_PATH)
            print(f"INFO: Output saved to '{OUTPUT_ZIP_PATH}'")
            print("--- Sarvam AI SDK Test Completed Successfully ---")
            return True
        else:
            print(f"ERROR: Job did not complete successfully. Final state: {status.job_state}")
            if hasattr(status, 'error'):
                print(f"ERROR Details: {status.error}")
            print("--- Sarvam AI SDK Test Failed ---")
            return False
            
    except ApiError as e:
        if e.status_code == 400:
            print(f"API ERROR (400 Bad Request): {e.body}")
        elif e.status_code == 403:
            print(f"API ERROR (403 Forbidden - Subscription/Auth Issue): {e.body}")
        elif e.status_code == 429:
            print(f"API ERROR (429 Rate Limit Exceeded): {e.body}")
        else:
            print(f"API ERROR ({e.status_code}): {e.body}")
        print("--- Sarvam AI SDK Test Failed ---")
        return False
    except FileNotFoundError:
        print(f"ERROR: Document file not found at '{TEST_PDF_PATH}' during SDK upload.")
        print("--- Sarvam AI SDK Test Failed ---")
        return False
    except Exception as e:
        print(f"AN UNEXPECTED ERROR OCCURRED: {e}")
        print("--- Sarvam AI SDK Test Failed ---")
        return False

if __name__ == "__main__":
    if test_document_intelligence_sdk():
        sys.exit(0)
    else:
        sys.exit(1)
