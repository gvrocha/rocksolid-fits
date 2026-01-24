# Assumes you are using a virtual environment

source fits_env/bin/activate

INDIR=~/astrophotography/data/inbox/20260119/ASIAIR
OUTDIR=~/astrophotography/data/raw/asi294mc

# NOTE:
# 1. In example below, YYYYMMDD_HHMMSS_SSS must be replaced with the timestamp tag generated at run time during the first step
# 2. The second and third steps are independent of one another; they can be run in parallel

python3 fits_organizer.py $INDIR $OUTDIR --tz-offset -6
python3 fits_generate_review_jpgs_from_log.py $OUTDIR/organize_log_YYYYMMDD_HHMMSS_SSS.tsv
python3 fits_metadata_extractor_from_log.py $OUTDIR/organize_log_YYYYMMDD_HHMMSS_SSS.tsv 

