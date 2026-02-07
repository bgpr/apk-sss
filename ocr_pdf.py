import requests
import json
import time
import os
import sys
import zipfile
import io
import subprocess # Added for pandoc
from pdf_converter import convert_html_to_pdf, PDFConversionError # Imported PDF conversion module

def ocr_kannada_pdf_direct_api(pdf_path, sarvam_ai_api_key, docraptor_api_key="YOUR_DOCRAPTOR_API_KEY_HERE", lang_code="kn-IN", output_format="md"):
    if not os.path.exists(pdf_path):
        print(f"Error: PDF file not found at {pdf_path}")
        return

    base_api_url = "https://api.sarvam.ai/doc-digitization/job/v1"
    
    # Explicit Endpoints from the provided documentation
    job_creation_endpoint = base_api_url
    get_upload_urls_endpoint = f"{base_api_url}/upload-files"
    start_job_endpoint_template = f"{base_api_url}/{{job_id}}/start"
    status_endpoint_template = f"{base_api_url}/{{job_id}}/status"

    sarvam_common_headers = {
        "api-subscription-key": sarvam_ai_api_key, # This API key is for Sarvam AI
        "Accept": "application/json"
    }

    # --- Step 1: Create Job (Sarvam AI) ---
    print("--- Step 1: Creating OCR job (Sarvam AI) ---")
    create_job_payload = {
        "job_parameters": {
            "language": lang_code,
            "output_format": output_format
        }
        # "callback" can be added here if needed
    }
    try:
        response = requests.post(job_creation_endpoint, headers={**sarvam_common_headers, "Content-Type": "application/json"}, json=create_job_payload)
        response.raise_for_status()
        job_response = response.json()
        job_id = job_response.get("job_id")
        job_state = job_response.get("job_state")
        if not job_id:
            print(f"Error: job_id not found in job creation response: {job_response}")
            return
        print(f"Job created successfully. Job ID: {job_id}, Initial State: {job_state}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to create job: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response content: {e.response.text}")
        return

    # --- Step 2: Get Upload URLs (Sarvam AI) ---
    print("--- Step 2: Requesting pre-signed upload URL(s) (Sarvam AI) ---")
    file_name = os.path.basename(pdf_path)
    get_upload_payload = {
        "job_id": job_id,
        "files": [file_name]
    }
    try:
        response = requests.post(get_upload_urls_endpoint, headers={**sarvam_common_headers, "Content-Type": "application/json"}, json=get_upload_payload)
        response.raise_for_status()
        upload_urls_response = response.json()
        
        upload_urls_map = upload_urls_response.get("upload_urls")
        if not upload_urls_map or file_name not in upload_urls_map:
            print(f"Error: 'upload_urls' or '{file_name}' not found in response: {upload_urls_response}")
            return
        
        presigned_url_info = upload_urls_map.get(file_name)
        if not presigned_url_info or not presigned_url_info.get("file_url"):
            print(f"Error: 'file_url' for '{file_name}' not found in response: {upload_urls_response}")
            return
        
        presigned_url = presigned_url_info["file_url"]
        print(f"Received pre-signed URL for '{file_name}'.")

    except requests.exceptions.RequestException as e:
        print(f"Failed to get upload URLs: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response content: {e.response.text}")
        return

    # --- Step 3: Upload File to Pre-signed URL (Sarvam AI) ---
    print("--- Step 3: Uploading document to pre-signed URL (Sarvam AI) ---")
    with open(pdf_path, 'rb') as f:
        try:
            # Use PUT to the pre-signed URL, Content-Type: application/pdf
            # Add the mandatory 'x-ms-blob-type' header for Azure Blob Storage
            upload_headers = {
                "Content-Type": "application/pdf",
                "x-ms-blob-type": "BlockBlob" 
            }
            response = requests.put(presigned_url, headers=upload_headers, data=f)
            response.raise_for_status()
            print(f"Document '{file_name}' uploaded successfully to pre-signed URL.")
        except requests.exceptions.RequestException as e:
            print(f"Failed to upload document to pre-signed URL: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response content: {e.response.text}")
            return
            
    # --- Step 4: Start Job Processing (Sarvam AI) ---
    print("--- Step 4: Starting OCR job processing (Sarvam AI) ---")
    start_job_url = start_job_endpoint_template.format(job_id=job_id)
    try:
        response = requests.post(start_job_url, headers=sarvam_common_headers) # Headers include API key
        response.raise_for_status()
        print("OCR job started successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to start job: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response content: {e.response.text}")
        return

    # --- Step 5: Poll for Job Status (Sarvam AI) ---
    print("--- Step 5: Polling for OCR job completion (Sarvam AI) ---")
    status = "Running" # Initial assumption
    while status in ["Accepted", "Pending", "Running", "PartiallyCompleted"]:
        time.sleep(5) # Poll every 5 seconds
        status_url = status_endpoint_template.format(job_id=job_id)
        try:
            response = requests.get(status_url, headers=sarvam_common_headers)
            response.raise_for_status()
            status_data = response.json()
            status = status_data.get("job_state", "Unknown")
            print(f"Current job state for {job_id}: {status}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to get job status: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status: {e.response.status_code}")
                print(f"Response content: {e.response.text}")
            return
        except json.JSONDecodeError:
            print(f"Error: Could not decode JSON from status response. Content: {response.text}")
            return

    if status == "Completed":
        print("OCR job completed successfully.")
    elif status == "PartiallyCompleted":
        print("OCR job partially completed. Check job status for details.")
    elif status == "Failed":
        print(f"OCR job failed. Details: {status_data.get('error', 'No error details.')}")
        return
    else:
        print(f"OCR job finished with unexpected status: {status}")
        return

    # --- Step 6: Retrieve and Extract Results (Sarvam AI) ---
    print("--- Step 6: Retrieving and extracting OCR results (Sarvam AI) ---")
    
    # Step 6a: Get Download URLs
    print("--- Step 6a: Requesting pre-signed download URL(s) (Sarvam AI) ---")
    get_download_urls_endpoint = f"{base_api_url}/{job_id}/download-files"
    try:
        response = requests.post(get_download_urls_endpoint, headers=sarvam_common_headers)
        response.raise_for_status()
        download_urls_response = response.json()

        download_urls_map = download_urls_response.get("download_urls")
        if not download_urls_map:
            print(f"Error: 'download_urls' not found in response: {download_urls_response}")
            return
        
        # Assuming there's only one file (the output ZIP) as per previous context
        zip_file_name = next(iter(download_urls_map), None) # Get the first key from the map
        if not zip_file_name:
            print(f"Error: No ZIP file name found in download_urls_map: {download_urls_map}")
            return

        presigned_download_url_info = download_urls_map.get(zip_file_name)
        if not presigned_download_url_info or not presigned_download_url_info.get("file_url"):
            print(f"Error: 'file_url' for '{zip_file_name}' not found in response: {download_urls_response}")
            return
        
        presigned_download_url = presigned_download_url_info["file_url"]
        print(f"Received pre-signed download URL for '{zip_file_name}'.")

    except requests.exceptions.RequestException as e:
        print(f"Failed to get download URLs: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response content: {e.response.text}")
        return

    # Step 6b: Download ZIP file from pre-signed URL and extract markdown
    print(f"--- Step 6b: Downloading ZIP from pre-signed URL and extracting '{output_format}' (Sarvam AI) ---")
    try:
        # No api-subscription-key needed for the pre-signed URL itself
        response = requests.get(presigned_download_url, stream=True)
        response.raise_for_status()
        
        # Save ZIP content to an in-memory byte stream
        zip_content = io.BytesIO(response.content)

        with zipfile.ZipFile(zip_content) as z:
            md_files = [f.filename for f in z.infolist() if f.filename.endswith(f'.{output_format}')]
            if not md_files:
                print(f"Error: No {output_format} file found in the ZIP response.")
                return

            md_content = z.read(md_files[0]).decode('utf-8')
            md_output_filename = os.path.join(os.path.dirname(pdf_path), os.path.splitext(os.path.basename(pdf_path))[0] + f".{output_format}") # Save original output format

            with open(md_output_filename, 'w', encoding='utf-8') as f:
                f.write(md_content)
            print(f"Successfully extracted and saved {output_format} to {md_output_filename}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve or process results: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response status: {e.response.status_code}")
            print(f"Response content: {e.response.text}")
        return
    except zipfile.BadZipFile:
        print("Error: Received content is not a valid ZIP file.")
        print(f"Response headers: {response.headers}")
        return
    except Exception as e:
        print(f"An unexpected error occurred during result processing: {e}")
        return

    # --- Step 7: Convert Markdown to HTML (Pandoc) ---
    print("--- Step 7: Converting Markdown to HTML (Pandoc) ---")
    html_output_filename = os.path.join(os.path.dirname(pdf_path), os.path.splitext(os.path.basename(pdf_path))[0] + ".html")
    try:
        subprocess.run(["pandoc", md_output_filename, "-o", html_output_filename], check=True, text=True, capture_output=True)
        print(f"Successfully converted '{md_output_filename}' to '{html_output_filename}'")
    except subprocess.CalledProcessError as e:
        print(f"Error converting Markdown to HTML with Pandoc: {e}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        return
    except FileNotFoundError:
        print("Error: Pandoc is not installed or not in PATH. Please install pandoc to convert Markdown to HTML.")
        return

    # --- Step 8: Convert HTML to PDF (DocRaptor) ---
    print("--- Step 8: Converting HTML to PDF (DocRaptor) ---")
    pdf_output_filename = os.path.join(os.path.dirname(pdf_path), os.path.splitext(os.path.basename(pdf_path))[0] + ".pdf")
    try:
        with open(html_output_filename, 'r', encoding='utf-8') as f:
            html_content = f.read()

        converted_pdf_path = convert_html_to_pdf(html_content, pdf_output_filename, docraptor_api_key, test_mode=(docraptor_api_key == "YOUR_DOCRAPTOR_API_KEY_HERE"))
        print(f"Successfully converted HTML to PDF and saved to '{converted_pdf_path}'")

    except PDFConversionError as e:
        print(f"PDF conversion failed: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during PDF conversion: {e}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python ocr_pdf.py <path_to_pdf_file> <your_sarvam_ai_api_key> [your_docraptor_api_key]")
        sys.exit(1)

    pdf_file = sys.argv[1]
    sarvam_ai_api_key = sys.argv[2]
    docraptor_api_key = sys.argv[3] if len(sys.argv) > 3 else "YOUR_DOCRAPTOR_API_KEY_HERE"

    ocr_kannada_pdf_direct_api(pdf_file, sarvam_ai_api_key, docraptor_api_key=docraptor_api_key)