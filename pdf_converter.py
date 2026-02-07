import docraptor
import os
import sys

class PDFConversionError(Exception):
    """Custom exception for PDF conversion failures."""
    pass

def convert_html_to_pdf(html_content, output_pdf_path, docraptor_api_key, test_mode=False):
    """
    Converts HTML content to a PDF file using the DocRaptor API.

    Args:
        html_content (str): The HTML content to convert.
        output_pdf_path (str): The full path including filename where the PDF should be saved.
        docraptor_api_key (str): Your DocRaptor API key.
        test_mode (bool): If True, uses DocRaptor's test mode (watermarked PDFs).

    Returns:
        str: The path to the created PDF file.

    Raises:
        PDFConversionError: If the PDF conversion fails.
    """
    doc_api = docraptor.DocApi()
    doc_api.api_client.configuration.username = docraptor_api_key

    try:
        response = doc_api.create_doc({
            'test': test_mode,
            'document_type': 'pdf',
            'document_content': html_content,
            'name': os.path.basename(output_pdf_path),
        })

        with open(output_pdf_path, 'wb') as f:
            f.write(response)
        
        return output_pdf_path

    except docraptor.rest.ApiException as error:
        error_message = f"DocRaptor API error: {error.status} - {error.reason} - {error.body}"
        raise PDFConversionError(error_message) from error
    except Exception as e:
        error_message = f"An unexpected error occurred during PDF conversion: {e}"
        raise PDFConversionError(error_message) from e

if __name__ == "__main__":
    # Example usage if run directly (for testing the module)
    if len(sys.argv) < 4:
        print("Usage: python pdf_converter.py <input_html_file> <output_pdf_file> <docraptor_api_key> [test_mode=False]")
        sys.exit(1)

    input_html_file = sys.argv[1]
    output_pdf_file = sys.argv[2]
    api_key = sys.argv[3]
    test = sys.argv[4].lower() == 'true' if len(sys.argv) > 4 else False

    if not os.path.exists(input_html_file):
        print(f"Error: Input HTML file not found at {input_html_file}")
        sys.exit(1)

    with open(input_html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    print(f"Converting '{input_html_file}' to '{output_pdf_file}' using DocRaptor...")
    try:
        converted_pdf_path = convert_html_to_pdf(html_content, output_pdf_file, api_key, test_mode=test)
        print(f"Successfully created PDF: {converted_pdf_path}")
    except PDFConversionError as e:
        print(f"PDF conversion failed: {e}")
        sys.exit(1)
