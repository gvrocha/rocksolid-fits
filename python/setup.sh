#!/bin/bash
# Setup script for FITS organizer

echo "Setting up FITS organizer virtual environment..."

# Create virtual environment
python3 -m venv fits_env

# Activate it
source fits_env/bin/activate

# Install dependencies
pip install astropy pandas Pillow

echo ""
echo "Setup complete!"
echo ""
echo "To run the organizer, use:"
echo "  source fits_env/bin/activate"
echo "  python fits_organizer.py"
echo ""
echo "Or use the run script: ./run_fits_organizer.sh"
