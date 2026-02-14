# Kannada PDF Processing Pipeline for Termux

## Overview
This project provides a robust, resumable pipeline designed to run within Termux on Android. It automates the process of downloading scanned Kannada PDFs from `adhyatmaprakasha.org`, performing OCR using Sarvam AI, transliterating titles/authors with the Gemini API, converting the output to DOCX, and organizing/copying files to phone storage.

## Features
-   **Resumable Workflow**: Tracks processing status for each book in a JSON state file.
-   **Smart Transliteration**: Uses Gemini API, avoiding redundant calls for already processed titles.
-   **Sarvam AI OCR Integration**: Utilizes the Sarvam AI SDK for high-quality OCR.
-   **DOCX Conversion**: Converts OCR output (Markdown) to editable DOCX files via Pandoc.
-   **Local Storage Management**: Organizes raw PDFs and processed documents in structured directories.
-   **Android Integration**: Copies final output to `/sdcard` for easy access.

## Requirements
-   Termux application on Android.
-   `proot-distro` installed in Termux, with a Debian distribution set up.
-   Python 3.9+ within the Debian `proot-distro`'s virtual environment (`/opt/venv/bin/python`).
-   `pandoc` installed in the Debian environment (`apt install pandoc`).

## Setup

1.  **Clone the Repository**:
    ```bash
    git clone <your_repo_url> /data/data/com.termux/files/home/apk
    cd /data/data/com.termux/files/home/apk
    ```
    *(Note: Assuming `/data/data/com.termux/files/home/apk` is your project root.)*

2.  **Install Python Dependencies (inside Debian `proot-distro`)**:
    Ensure `proot-distro` is set up with Python and a virtual environment at `/opt/venv`.
    ```bash
    proot-distro login debian --shared-tmp -- env TERM=xterm-256color HOME=/root bash -c "/opt/venv/bin/pip install -r /data/data/com.termux/files/home/apk/requirements.txt"
    ```
    *(Create a `requirements.txt` if you don't have one yet with `requests`, `beautifulsoup4`, `sarvamai`, `google-generativeai`, `python-dotenv`, `tqdm` if re-adding, `pypdf2` if adding)*

3.  **Configure API Keys (`.env` file)**:
    Create a `.env` file in the project root (`/data/data/com.termux/files/home/apk/.env`) with your API keys.
    ```bash
    cd /data/data/com.termux/files/home/apk/
    nano .env
    ```
    Add your keys (replace placeholders):
    ```
    SARVAM_AI_API_KEY="YOUR_SARVAM_AI_API_KEY"
    GEMINI_API_KEY="YOUR_GEMINI_API_KEY_HERE"
    ```
    Save the file (`Ctrl+X`, `Y`, `Enter`).

## Usage

The pipeline is executed via `run_pipeline.sh` for reliable execution within `proot-distro`.

1.  **Start the Pipeline (Background Mode)**:
    Run the script in the background and redirect all output to a log file.
    ```bash
    cd /data/data/com.termux/files/home/apk/ # Ensure you are in the project root
    ./run_pipeline.sh > full_run_unified.log 2>&1 &
    ```

2.  **Monitor Progress**:
    Watch the `full_run_unified.log` for real-time updates:
    ```bash
    tail -f full_run_unified.log
    ```

3.  **Command-line Arguments**:
    `run_pipeline.sh` internally calls `main_processor.py`. You can modify `run_pipeline.sh` or run `main_processor.py` directly (if comfortable with `proot-distro` context) with:
    -   `--limit <N>`: Process only the first N uncompleted books.
    -   `--rescan-books`: Force re-scraping of book metadata (transliteration will be skipped if slugs exist).

## Troubleshooting & Known Issues
-   **Sarvam AI Page Limit**: PDFs exceeding 500 pages will fail OCR. This will be addressed in future updates.
-   **Corrupted PDFs**: Some PDFs may be unreadable by Sarvam AI.
-   **Process Interruption**: The pipeline is resumable; simply re-run the `run_pipeline.sh` script to continue from where it left off.
