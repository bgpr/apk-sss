import re
import logging
import requests
from urllib.parse import urlparse, quote

logger = logging.getLogger(__name__)

def resolve_pdf_url(details_page_url: str) -> str | None:
    """
    Standard interface function to resolve a direct PDF link from an archive.org URL.
    Uses the Internet Archive Metadata API to find the exact PDF filename.
    """
    parsed_url = urlparse(details_page_url)
    
    # Extract item identifier: matches /details/IDENTIFIER
    match = re.search(r'/details/([^/]+)', parsed_url.path)
    
    if not match:
        logger.warning(f"Could not resolve identifier from: {details_page_url}")
        return None

    item_identifier = match.group(1)
    metadata_url = f"https://archive.org/metadata/{item_identifier}"
    
    try:
        logger.info(f"Fetching metadata from: {metadata_url}")
        response = requests.get(metadata_url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        files = data.get('files', [])
        pdf_files = [f for f in files if f.get('name', '').lower().endswith('.pdf')]
        
        if not pdf_files:
            logger.warning(f"No PDF files found in metadata for: {item_identifier}")
            return None
            
        # Strategy: Pick the first PDF that doesn't have common "metadata" or "text" suffixes
        # or just pick the largest one.
        main_pdf = None
        
        # Filter out obvious OCR/text-only PDFs if possible
        candidates = [f for f in pdf_files if '_text.pdf' not in f['name'].lower()]
        if candidates:
            # Sort by size descending to get the most likely main book
            candidates.sort(key=lambda x: int(x.get('size', 0)), reverse=True)
            main_pdf = candidates[0]['name']
        else:
            # Fallback to any PDF
            pdf_files.sort(key=lambda x: int(x.get('size', 0)), reverse=True)
            main_pdf = pdf_files[0]['name']
            
        if main_pdf:
            # Construct the download URL. Filenames on archive.org must be URL-encoded.
            pdf_download_url = f"https://archive.org/download/{item_identifier}/{quote(main_pdf)}"
            logger.info(f"Resolved archive.org URL to: {pdf_download_url}")
            return pdf_download_url
            
    except Exception as e:
        logger.error(f"Error fetching archive.org metadata for {item_identifier}: {e}")
        # Fallback to simple identifier-based URL if API fails
        return f"https://archive.org/download/{item_identifier}/{item_identifier}.pdf"
    
    return None
