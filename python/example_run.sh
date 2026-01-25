#!/bin/bash
# Rocksolid-fits workflow example
# Assumes you are using a virtual environment

source fits_env/bin/activate

INDIR=~/astrophotography/data/inbox/20260119/ASIAIR
OUTDIR=~/astrophotography/data/raw/asi294mc

# Step 1: Organize FITS files and create database
echo "Step 1: Organizing FITS files..."
python3 fits_organizer.py $INDIR $OUTDIR --tz-offset -6

# Get the timestamp from the most recent organize log
LATEST_LOG=$(ls -t $OUTDIR/organize_log_*.tsv | head -1)
echo "Using log file: $LATEST_LOG"

# Step 2 & 3: Generate JPEGs and extract metadata (can run in parallel)
echo "Step 2: Generating review JPEGs..."
python3 fits_generate_review_jpgs_from_log.py $LATEST_LOG &

echo "Step 3: Extracting metadata..."
python3 fits_metadata_extractor_from_log.py $LATEST_LOG &

# Wait for both to complete
wait

echo "All processing complete!"
echo ""
echo "To review frames in web browser:"
echo "  python3 fits_frame_reviewer_server.py $OUTDIR"
echo ""
echo "Then open browser to: http://localhost:5000"
