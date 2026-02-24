#!/bin/bash
# Execute the Python main_processor.py script within the Debian proot-distro environment.

# Construct the absolute path to the main_processor.py script
SCRIPT_PATH="/data/data/com.termux/files/home/apk/main_processor.py"
PROJECT_DIR="/data/data/com.termux/files/home/apk"

# Construct the command to run Python script inside proot-distro
# Added 'cd' to ensure we are in the correct directory for relative paths and logs
PROOT_CMD="cd ${PROJECT_DIR} && /opt/venv/bin/python ${SCRIPT_PATH} $@"

# Execute proot-distro login with the Python command
proot-distro login debian --shared-tmp -- env TERM=xterm-256color HOME=/root bash -c "${PROOT_CMD}"
