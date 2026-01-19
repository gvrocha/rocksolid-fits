#!/usr/bin/env python3
"""
FITS Metadata Extractor v5
Extracts comprehensive metadata and image statistics from FITS files
Includes target extraction from organized filepath structure
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path

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


def find_fits_files(directory):
    """Recursively find all FITS files in directory"""
    fits_extensions = ('.fit', '.fits', '.fts')
    fits_files = []
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(fits_extensions):
                fits_files.append(os.path.join(root, file))
    
    return sorted(fits_files)


def extract_all_metadata(input_dir, output_file=None, delete_json=False):
    """Extract metadata from all FITS files in directory"""
    
    print("=" * 60)
    print("FITS Metadata Extractor")
    print("=" * 60)
    print()
    
    # Find all FITS files
    print(f"Scanning directory: {input_dir}")
    fits_files = find_fits_files(input_dir)
    
    if not fits_files:
        print(f"No FITS files found in {input_dir}")
        return
    
    print(f"Found {len(fits_files)} FITS files\n")
    
    # Generate output filename if not provided
    if output_file is None:
        timestamp_now = datetime.now()
        milliseconds = timestamp_now.microsecond // 1000
        tsv_timestamp = f"{timestamp_now.strftime('%Y%m%d_%H%M%S')}_{milliseconds:03d}"
        output_base = f'fits_metadata_{tsv_timestamp}'
    else:
        # Remove extension if provided
        output_base = os.path.splitext(output_file)[0]
    
    jsonl_file = f'{output_base}.jsonl'
    tsv_file = f'{output_base}.tsv'
    
    print(f"JSONL file (incremental): {jsonl_file}")
    print(f"TSV file (final): {tsv_file}")
    print("Writing incrementally to JSONL (safe for interruption)\n")
    
    # Process files and append to JSONL incrementally
    import json
    
    successful = 0
    failed = 0
    start_time = datetime.now()
    
    # Calculate progress interval (1% of total files, minimum 1)
    progress_interval = max(1, len(fits_files) // 100)
    
    # Flag to show early progress for large datasets
    show_early_progress = len(fits_files) > 1000
    early_progress_shown = False
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Processing files...\n")
    
    with open(jsonl_file, 'w') as f:
        for i, filepath in enumerate(fits_files, 1):
            
            metadata = extract_fits_metadata(filepath)
            
            if metadata is not None:
                # Write as single-line JSON
                f.write(json.dumps(metadata) + '\n')
                f.flush()  # Ensure data is written to disk
                
                if 'extraction_error' in metadata:
                    failed += 1
                else:
                    successful += 1
            else:
                failed += 1
            
            # Show early progress after 10 files for large datasets
            if show_early_progress and i == 10 and not early_progress_shown:
                timestamp = datetime.now().strftime('%H:%M:%S')
                elapsed = (datetime.now() - start_time).total_seconds()
                avg_time_per_file = elapsed / i
                remaining_files = len(fits_files) - i
                eta_seconds = avg_time_per_file * remaining_files
                eta_time = datetime.now() + timedelta(seconds=eta_seconds)
                eta_str = eta_time.strftime('%Y/%m/%d %H:%M')
                percent_complete = (i / len(fits_files)) * 100
                print(f"[{timestamp}] {percent_complete:5.1f}% - Processed {i}/{len(fits_files)} files "
                      f"({successful} successful, {failed} failed) - ETA: {eta_str}")
                early_progress_shown = True
            
            # Show progress after processing at 1% intervals or last file
            if i % progress_interval == 0 or i == len(fits_files):
                timestamp = datetime.now().strftime('%H:%M:%S')
                elapsed = (datetime.now() - start_time).total_seconds()
                
                # Calculate ETA
                if i > 0:
                    avg_time_per_file = elapsed / i
                    remaining_files = len(fits_files) - i
                    eta_seconds = avg_time_per_file * remaining_files
                    eta_time = datetime.now() + timedelta(seconds=eta_seconds)
                    eta_str = eta_time.strftime('%Y/%m/%d %H:%M')
                else:
                    eta_str = "calculating..."
                
                percent_complete = (i / len(fits_files)) * 100
                print(f"[{timestamp}] {percent_complete:5.1f}% - Processed {i}/{len(fits_files)} files "
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
    
    # Separate stats keys that exist and header keys
    existing_stats_keys = [k for k in stats_keys if k in all_keys]
    header_keys = sorted([k for k in all_keys if k not in stats_keys])
    
    ordered_keys = existing_stats_keys + header_keys
    
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
                if value is None:
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
        description='FITS Metadata Extractor - Extract comprehensive metadata and statistics from FITS files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Extracts all FITS header keywords plus image statistics from all FITS files
in a directory (recursively).

Image statistics include:
  • Mean, median, min, max values
  • 25th and 75th percentiles
  • Standard deviation
  • Saturation counts (low and high)
  • Total pixel count

Output is a TSV file with one row per FITS file.

Examples:
  # Extract from directory
  %(prog)s /path/to/fits/files
  
  # Specify output file
  %(prog)s /path/to/fits/files --output my_metadata.tsv
  
  # Extract from organized structure
  %(prog)s /organized/asi294mc_pro/sessions/20250114/
'''
    )
    
    parser.add_argument('input_dir', help='Directory containing FITS files (scanned recursively)')
    parser.add_argument('--output', '-o', help='Output file base name (default: fits_metadata_TIMESTAMP, creates .jsonl and .tsv)')
    parser.add_argument('--delete-json', action='store_true', 
                       help='Delete JSONL file after TSV creation (default: keep both files)')
    
    args = parser.parse_args()
    
    # Check input directory exists
    if not os.path.exists(args.input_dir):
        print(f"Error: Directory '{args.input_dir}' does not exist")
        sys.exit(1)
    
    if not os.path.isdir(args.input_dir):
        print(f"Error: '{args.input_dir}' is not a directory")
        sys.exit(1)
    
    # Run extraction
    extract_all_metadata(args.input_dir, args.output, args.delete_json)


if __name__ == '__main__':
    main()
