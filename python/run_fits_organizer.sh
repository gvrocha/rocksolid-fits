#!/bin/bash
# Run FITS organizer v6 with virtual environment
# Supports both interactive and CLI modes

# Activate virtual environment
source fits_env/bin/activate

# Check if arguments were provided
if [ $# -eq 0 ]; then
    # No arguments - run in interactive mode
    python fits_organizer.py
else
    # Arguments provided - run in CLI mode
    python fits_organizer.py "$@"
fi

# Deactivate when done
deactivate
