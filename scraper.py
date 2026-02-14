import requests
from bs4 import BeautifulSoup
import re
import json
import os
import time
from urllib.parse import urljoin, urlparse
from transliteration_utils import transliterate_kannada_to_english, slugify 

# --- Configuration ---
# BASE_URL is now passed from main_processor.py
RAW_PDF_BASE_DIR = "raw_pdf"
PROCESSED_DOCS_BASE_DIR = "processed_docs"

# --- Helper Functions ---

def get_page_slug(base_url):
    """Generates a slug from the URL path part before .php"""
    parsed_url = urlparse(base_url)
    # Extract the part before .php in the path, e.g., /php/kannada_books
    path_segments = parsed_url.path.split('/')
    # Find the segment ending with .php and take the part before it
    for segment in path_segments:
        if '.php' in segment:
            return slugify(segment.split('.php')[0])
    # Fallback if no .php is found, though it should be for this specific URL
    return slugify(parsed_url.path.replace('/', '_').strip('_'))

def get_state_filename(base_url):
    """Generates a state filename based on the base URL."""
    page_slug = get_page_slug(base_url)
    return f"processing_state_{page_slug}.json"

def load_state(state_file_path):
    """Loads the processing state from a JSON file."""
    print(f"DEBUG: Loading state from {state_file_path}...")
    if os.path.exists(state_file_path):
        with open(state_file_path, 'r', encoding='utf-8') as f:
            state = json.load(f)
            print(f"DEBUG: State loaded. {len(state.get('books', []))} books found.")
            return state
    print(f"DEBUG: No existing state file found at {state_file_path}. Starting fresh.")
    return {"books": []}

def save_state(state, state_file_path):
    """Saves the processing state to a JSON file."""
    print(f"DEBUG: Saving state to {state_file_path}...")
    with open(state_file_path, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=4, ensure_ascii=False)
    print("DEBUG: State saved.")

def get_full_path_for_book(base_dir_root, page_slug, book_id, filename):
    """
    Constructs a full path for a file within its indexed subdirectory,
    including the page_slug. Ensures the directory exists.
    """
    # Structure: base_dir_root / page_slug / book_id / filename
    page_dir = os.path.join(base_dir_root, page_slug)
    index_dir = os.path.join(page_dir, str(book_id).zfill(3))
    os.makedirs(index_dir, exist_ok=True)
    return os.path.join(index_dir, filename)

# --- Main Scraper Logic ---

def scrape_books(url, existing_books_map=None, language="kannada"):
    """
    Scrapes the given URL for book details (title, author, download link)
    and returns a list of dictionaries.
    The 'language' parameter is used to conditionally skip Gemini transliteration.
    """
    print(f"Scraping {url} for book information...")
    try:
        response = requests.get(url)
        response.raise_for_status() # Raise an exception for HTTP errors
        print(f"DEBUG: Successfully fetched URL: {url}")
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Error fetching URL {url}: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    books_data = []
    
    books_container = soup.find('div', class_='books_from_db')
    if not books_container:
        print("ERROR: Could not find the main books container with class 'books_from_db'.")
        return []
    print("DEBUG: Found books container.")

    page_slug = get_page_slug(url) # Get page slug once per scrape operation

    all_li_elements = books_container.find_all('li', id=re.compile(r'li_id\d+'))
    print(f"DEBUG: Found {len(all_li_elements)} potential book list items.")

    for i, item in enumerate(all_li_elements):
        print(f"DEBUG: Processing item {i+1}/{len(all_li_elements)}...")
        title_original = ""
        author_original = ""
        pdf_relative_url = ""
        book_id = ""
        
        # Extract title from titlespan <a> tag
        title_a_tag = item.find('span', class_='titlespan')
        if title_a_tag and title_a_tag.a:
            title_original = title_a_tag.a.get_text(strip=True)
            toc_url = title_a_tag.a['href']
            match_id_toc = re.search(r'book_id=(\d+[A-Z]?)', toc_url)
            if match_id_toc:
                book_id = match_id_toc.group(1)
        elif title_a_tag:
            title_original = title_a_tag.get_text(strip=True)

        # Extract author from authorspan <a> tag
        author_a_tag = item.find('span', class_='authorspan')
        if author_a_tag and author_a_tag.a:
            author_original = author_a_tag.a.get_text(strip=True)
        elif author_a_tag:
            author_original = author_a_tag.get_text(strip=True).replace('—', '').strip()
            if not author_original and author_a_tag.find_next_sibling('span', class_='authorspan'):
                 author_original = author_a_tag.find_next_sibling('span', class_='authorspan').get_text(strip=True)

        # Extract PDF URL and fallback book_id if not found from title_toc
        download_tag = item.find('span', class_='downloadpdf')
        if download_tag and download_tag.a:
            pdf_relative_url = download_tag.a['href']
            if not book_id: # If book_id wasn't found from title_toc_url, try from pdf_url
                match_id_pdf = re.search(r'/(\d{3,}[A-Z]?)/index\.pdf', pdf_relative_url)
                if match_id_pdf:
                    book_id = match_id_pdf.group(1)
        
        if title_original and pdf_relative_url and book_id:
            print(f"DEBUG: Found book_id={book_id}, Title='{title_original}', Author='{author_original}'")
            transliterated_title_slug = ""
            transliterated_author_slug = ""

            # Conditional transliteration based on language
            if language == "kannada":
                # Check if transliterated slugs already exist in the map (for efficiency)
                if existing_books_map and book_id in existing_books_map:
                    existing_book = existing_books_map[book_id]
                    if existing_book.get('title_english_slug') and existing_book.get('author_english_slug'):
                        transliterated_title_slug = existing_book['title_english_slug']
                        transliterated_author_slug = existing_book['author_english_slug']
                        print(f"DEBUG: Reusing existing slugs for Book {book_id}: Title='{transliterated_title_slug}', Author='{transliterated_author_slug}'")
                    else:
                        # Existing book but missing slugs, so transliterate
                        transliterated_title_slug = transliterate_kannada_to_english(title_original)
                        transliterated_author_slug = transliterate_kannada_to_english(author_original)
                else:
                    # New book, so transliterate
                    transliterated_title_slug = transliterate_kannada_to_english(title_original)
                    transliterated_author_slug = transliterate_kannada_to_english(author_original)
            else: # For English or other languages, just slugify directly
                print(f"DEBUG: Skipping Gemini transliteration for {language} book {book_id}. Directly slugifying original title/author.")
                transliterated_title_slug = slugify(title_original)
                transliterated_author_slug = slugify(author_original)
            
            filename_parts = [transliterated_title_slug]
            if transliterated_author_slug:
                filename_parts.append(transliterated_author_slug)
            
            base_filename = f"{book_id}_{'_'.join(filter(None, filename_parts))}.pdf"
            
            books_data.append({
                "id": book_id, # Use extracted book_id as unique identifier
                "title_original": title_original, # Store original title
                "author_original": author_original, # Store original author
                "title_english_slug": transliterated_title_slug,
                "author_english_slug": transliterated_author_slug,
                "pdf_url": urljoin(url, pdf_relative_url), # Ensure absolute URL
                "local_pdf_path": get_full_path_for_book(RAW_PDF_BASE_DIR, page_slug, book_id, base_filename),
                "local_md_path": get_full_path_for_book(PROCESSED_DOCS_BASE_DIR, page_slug, book_id, base_filename.replace('.pdf', '.md')),
                "local_docx_path": get_full_path_for_book(PROCESSED_DOCS_BASE_DIR, page_slug, book_id, base_filename.replace('.pdf', '.docx')),
                "status": "pending" # Initial status
            })
            print(f"DEBUG: Book {book_id} added with filename: {base_filename}")
        else:
            print(f"WARNING: Skipping an item due to missing title, PDF URL, or book ID. Item HTML: {item}")
        
    return books_data
