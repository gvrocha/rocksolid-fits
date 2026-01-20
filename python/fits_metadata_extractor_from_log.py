#!/usr/bin/env python3
"""
FITS Metadata Extractor from Organizer Log
Extracts metadata from files listed in fits_organizer TSV log
Processes only successfully copied files
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
import pandas as pd

# Import shared metadata extraction utilities
try:
    from fits_metadata_utils import extract_fits_metadata, get_stats_column_names
    UTILS_AVAILABLE = True
except ImportError:
    UTILS_AVAILABLE = False
    print("Error: fits_metadata_utils.py not found in the same directory")
    sys.exit(1)

try:
    from astropy.io import fits
    ASTROPY_AVAILABLE = True
except ImportError:
    ASTROPY_AVAILABLE = False
    print("Error: astropy is required but not installed")
    print("Install with: pip install astropy")
    sys.exit(1)


def extract_metadata_from_log(log_file, output_file=None, delete_json=False):
    """Extract metadata from files listed in organizer log"""
    
    print("=" * 60)
    print("FITS Metadata Extractor from Organizer Log")
    print("=" * 60)
    print()
    
    # Read organizer log
    print(f"Reading organizer log: {log_file}")
    try:
        log_df = pd.read_csv(log_file, sep='\t')
    except Exception as e:
        print(f"Error reading log file: {e}")
        return
    
    print(f"Log contains {len(log_df)} entries")
    
    # Filter for successfully copied files only
    copied_df = log_df[log_df['action'] == 'copied'].copy()
    
    if len(copied_df) == 0:
        print("No successfully copied files found in log (action='copied')")
        return
    
    print(f"Found {len(copied_df)} successfully copied files to process\n")
    
    # Generate output filename if not provided
    if output_file is None:
        timestamp_now = datetime.now()
        milliseconds = timestamp_now.microsecond // 1000
        tsv_timestamp = f"{timestamp_now.strftime('%Y%m%d_%H%M%S')}_{milliseconds:03d}"
        output_base = f'fits_metadata_{tsv_timestamp}'
    else:
        # Remove extension if provided
        output_base = os.path.splitext(output_file)[0]
    
    # Place output files in same directory as input log file
    log_dir = os.path.dirname(os.path.abspath(log_file))
    jsonl_file = os.path.join(log_dir, f'{output_base}.jsonl')
    tsv_file = os.path.join(log_dir, f'{output_base}.tsv')
    
    print(f"JSONL file (incremental): {jsonl_file}")
    print(f"TSV file (final): {tsv_file}")
    print("Writing incrementally to JSONL (safe for interruption)\n")
    
    # Process files and append to JSONL incrementally
    import json
    
    # Columns from organizer log to include
    log_columns = ['target', 'filter', 'exposure_sec', 'gain', 'temperature_c', 
                   'temp_folder', 'timestamp']
    
    successful = 0
    failed = 0
    start_time = datetime.now()
    
    # Calculate progress interval (1% of total files, minimum 1)
    progress_interval = max(1, len(copied_df) // 100)
    
    # Flag to show early progress for large datasets
    show_early_progress = len(copied_df) > 1000
    early_progress_shown = False
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Processing files...\n")
    
    with open(jsonl_file, 'w') as f:
        for i, (idx, row) in enumerate(copied_df.iterrows(), 1):
            filepath = row['destination_file']
            
            # Extract metadata from FITS file
            metadata = extract_fits_metadata(filepath)
            
            if metadata is not None:
                # Add log columns to metadata
                for col in log_columns:
                    metadata[col] = row.get(col, '')
                
                # Write as single-line JSON
                f.write(json.dumps(metadata) + '\n')
                f.flush()  # Ensure data is written to disk
                
                if 'extraction_error' in metadata:
                    failed += 1
                else:
                    successful += 1
            else:
                failed += 1
                print(f"Warning: Failed to extract metadata from {os.path.basename(filepath)}")
            
            # Show early progress after 10 files for large datasets
            if show_early_progress and i == 10 and not early_progress_shown:
                timestamp = datetime.now().strftime('%H:%M:%S')
                elapsed = (datetime.now() - start_time).total_seconds()
                avg_time_per_file = elapsed / i
                remaining_files = len(copied_df) - i
                eta_seconds = avg_time_per_file * remaining_files
                eta_time = datetime.now() + timedelta(seconds=eta_seconds)
                eta_str = eta_time.strftime('%Y/%m/%d %H:%M')
                percent_complete = (i / len(copied_df)) * 100
                print(f"[{timestamp}] {percent_complete:5.1f}% - Processed {i}/{len(copied_df)} files "
                      f"({successful} successful, {failed} failed) - ETA: {eta_str}")
                early_progress_shown = True
            
            # Show progress after processing at 1% intervals or last file
            if i % progress_interval == 0 or i == len(copied_df):
                timestamp = datetime.now().strftime('%H:%M:%S')
                elapsed = (datetime.now() - start_time).total_seconds()
                
                # Calculate ETA
                if i > 0:
                    avg_time_per_file = elapsed / i
                    remaining_files = len(copied_df) - i
                    eta_seconds = avg_time_per_file * remaining_files
                    eta_time = datetime.now() + timedelta(seconds=eta_seconds)
                    eta_str = eta_time.strftime('%Y/%m/%d %H:%M')
                else:
                    eta_str = "calculating..."
                
                percent_complete = (i / len(copied_df)) * 100
                print(f"[{timestamp}] {percent_complete:5.1f}% - Processed {i}/{len(copied_df)} files "
                      f"({successful} successful, {failed} failed) - ETA: {eta_str}")
    
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"\n[{timestamp}] Successfully extracted metadata from {successful} files")
    if failed > 0:
        print(f"[{timestamp}] Failed to process {failed} files")
    
    # Now read all JSONL and create TSV with complete column set
    print(f"\n[{timestamp}] Creating final TSV with complete column set...")
    
    all_metadata = []
    all_keys = set()
    
    with open(jsonl_file, 'r') as f:
        for line in f:
            metadata = json.loads(line)
            all_metadata.append(metadata)
            all_keys.update(metadata.keys())
    
    # Get stats keys for ordering
    stats_keys = get_stats_column_names()
    
    # Separate stats keys and log columns that exist, then header keys
    existing_stats_keys = [k for k in stats_keys if k in all_keys]
    existing_log_columns = [k for k in log_columns if k in all_keys]
    header_keys = sorted([k for k in all_keys if k not in stats_keys and k not in log_columns])
    
    ordered_keys = existing_stats_keys + existing_log_columns + header_keys
    
    # Write TSV
    with open(tsv_file, 'w') as f:
        # Write header
        f.write('\t'.join(ordered_keys) + '\n')
        
        # Write data rows
        for metadata in all_metadata:
            row = []
            for key in ordered_keys:
                value = metadata.get(key, '')
                # Convert to string and handle special characters
                if value is None or (isinstance(value, float) and pd.isna(value)):
                    value = ''
                else:
                    value = str(value).replace('\t', ' ').replace('\n', ' ')
                row.append(value)
            f.write('\t'.join(row) + '\n')
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] TSV file created with {len(ordered_keys)} columns")
    
    # Delete JSONL if requested
    if delete_json:
        os.remove(jsonl_file)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Deleted JSONL file")
    
    print("\nDone!")
    print("=" * 60)
    print(f"JSONL: {jsonl_file}" + (" (deleted)" if delete_json else ""))
    print(f"TSV:   {tsv_file}")


def main():
    parser = argparse.ArgumentParser(
        description='FITS Metadata Extractor from Organizer Log',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Extracts metadata from FITS files listed in a fits_organizer TSV log.
Processes only files with action='copied' (successfully organized files).

Combines:
  • FITS metadata and image statistics (from files)
  • Organizer metadata (target, filter, gain, etc. from log)
  • Validation columns (target vs target_from_path)

Designed to run immediately after fits_organizer as part of import pipeline.

Output is a TSV file with one row per successfully organized FITS file.

Examples:
  # Process organizer log
  %(prog)s organize_log_20250117_143052_234.tsv
  
  # Specify output file
  %(prog)s organize_log_20250117_143052_234.tsv --output metadata.tsv
  
  # Pipeline: organize then extract metadata
  python fits_organizer.py /raw /organized/asi294mc
  python %(prog)s organize_log_*.tsv
'''
    )
    
    parser.add_argument('log_file', help='TSV log file from fits_organizer')
    parser.add_argument('--output', '-o', help='Output file base name (default: fits_metadata_TIMESTAMP, creates .jsonl and .tsv)')
    parser.add_argument('--delete-json', action='store_true',
                       help='Delete JSONL file after TSV creation (default: keep both files)')
    
    args = parser.parse_args()
    
    # Check log file exists
    if not os.path.exists(args.log_file):
        print(f"Error: Log file '{args.log_file}' does not exist")
        sys.exit(1)
    
    # Run extraction
    extract_metadata_from_log(args.log_file, args.output, args.delete_json)


if __name__ == '__main__':
    main()
