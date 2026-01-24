#!/usr/bin/env python3
"""
FITS Metadata Extractor from Organizer Log (with Database Support)
Extracts metadata and statistics from FITS files, outputs to TSV/JSONL and SQLite database
"""

import os
import sys
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import json

# Import shared metadata extraction utilities
try:
    from fits_metadata_utils import extract_fits_metadata, get_stats_column_names
    UTILS_AVAILABLE = True
except ImportError:
    UTILS_AVAILABLE = False
    print("Error: fits_metadata_utils.py not found in the same directory")
    sys.exit(1)

# Import database module
try:
    from fits_database import ensure_database_schema, get_database_path
    import sqlite3
    FITS_DATABASE_AVAILABLE = True
except ImportError:
    FITS_DATABASE_AVAILABLE = False
    print("Warning: fits_database.py not found. Database features will be disabled.")

try:
    from astropy.io import fits
    ASTROPY_AVAILABLE = True
except ImportError:
    ASTROPY_AVAILABLE = False
    print("Error: astropy is required but not installed")
    print("Install with: pip install astropy")
    sys.exit(1)


def import_metadata_to_database(metadata_list, log_df, db_path):
    """
    Import extracted metadata and statistics to SQLite database.
    
    Args:
        metadata_list: List of metadata dicts from extract_fits_metadata()
        log_df: DataFrame from organize log (to get fits_file_id mapping)
        db_path: Path to SQLite database
    
    Returns:
        Number of files processed
    """
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    files_processed = 0
    files_skipped = 0
    total_metadata_inserted = 0
    
    # Keywords to skip (structural FITS keywords)
    skip_keywords = {
        'SIMPLE', 'BITPIX', 'NAXIS', 'NAXIS1', 'NAXIS2', 'EXTEND',
        'COMMENT', 'HISTORY', ''
    }
    
    # Stats keys (computed, not from header)
    stats_keys = set(get_stats_column_names())
    
    print("\nImporting to database...")
    
    for metadata in metadata_list:
        fits_file = metadata.get('filepath', '')
        
        # Get fits_file_id from fits_frames table
        cursor.execute(
            "SELECT id FROM fits_frames WHERE destination_file = ?",
            (fits_file,)
        )
        
        result = cursor.fetchone()
        if not result:
            print(f"Warning: FITS file not found in fits_frames: {fits_file}")
            files_skipped += 1
            continue
        
        fits_file_id = result[0]
        
        # Check if metadata already exists for this file
        cursor.execute(
            "SELECT COUNT(*) FROM fits_metadata WHERE fits_file_id = ?",
            (fits_file_id,)
        )
        
        if cursor.fetchone()[0] > 0:
            files_skipped += 1
            continue
        
        metadata_count = 0
        
        # Insert all metadata key-value pairs
        for key, value in metadata.items():
            # Skip filepath and target_from_path (not metadata)
            if key in ['filepath', 'target_from_path', 'extraction_error']:
                continue
            
            # Skip structural FITS keywords
            if key in skip_keywords:
                continue
            
            # Determine if this is a computed stat (gets stat_ prefix)
            if key in stats_keys:
                metadata_key = f'stat_{key}'
            else:
                metadata_key = key
            
            # Determine if numeric or text
            value_numeric = None
            value_text = None
            
            if isinstance(value, (int, float)):
                if pd.notna(value):  # Skip NaN values
                    value_numeric = float(value)
            elif isinstance(value, bool):
                value_numeric = 1.0 if value else 0.0
            else:
                if value and str(value).strip():  # Skip empty strings
                    value_text = str(value)
            
            # Only insert if we have a value
            if value_numeric is not None or value_text is not None:
                try:
                    cursor.execute('''
                        INSERT INTO fits_metadata 
                        (fits_file_id, metadata_key, value_numeric, value_text)
                        VALUES (?, ?, ?, ?)
                    ''', (fits_file_id, metadata_key, value_numeric, value_text))
                    metadata_count += 1
                except sqlite3.IntegrityError:
                    # Duplicate key for this file, skip
                    pass
        
        total_metadata_inserted += metadata_count
        files_processed += 1
        
        if files_processed % 10 == 0:
            conn.commit()  # Commit periodically
    
    conn.commit()
    conn.close()
    
    print(f"Imported metadata from {files_processed} FITS files ({total_metadata_inserted} metadata entries)")
    if files_skipped > 0:
        print(f"Skipped {files_skipped} files (already processed or not found)")
    
    return files_processed


def extract_metadata_from_log(log_file, output_file=None, delete_json=False, skip_db=False):
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
        return None
    
    print(f"Log contains {len(log_df)} entries")
    
    # Process all files
    files_to_process = log_df.copy()
    
    print(f"Found {len(files_to_process)} files to process\n")
    
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
    
    # Columns from organizer log to include
    log_columns = ['target', 'filter', 'exposure_sec', 'gain', 'temperature_c', 
                   'session_date', 'timestamp']
    
    successful = 0
    failed = 0
    start_time = datetime.now()
    
    # Calculate progress interval (1% of total files, minimum 1)
    progress_interval = max(1, len(files_to_process) // 100)
    
    # Flag to show early progress for large datasets
    show_early_progress = len(files_to_process) > 1000
    early_progress_shown = False
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Processing files...\n")
    
    all_metadata = []
    
    with open(jsonl_file, 'w') as f:
        for i, (idx, row) in enumerate(files_to_process.iterrows(), 1):
            filepath = row['destination_file']
            
            # Extract metadata
            metadata = extract_fits_metadata(filepath)
            
            # Add organizer log columns
            for col in log_columns:
                if col in row:
                    metadata[col] = row[col]
            
            # Track success/failure
            if metadata.get('extraction_error'):
                failed += 1
            else:
                successful += 1
            
            # Store for later database import
            all_metadata.append(metadata)
            
            # Write to JSONL
            f.write(json.dumps(metadata) + '\n')
            f.flush()
            
            # Show early progress after 10 files for large datasets
            if show_early_progress and i == 10 and not early_progress_shown:
                timestamp = datetime.now().strftime('%H:%M:%S')
                elapsed = (datetime.now() - start_time).total_seconds()
                avg_time_per_file = elapsed / i
                remaining_files = len(files_to_process) - i
                eta_seconds = avg_time_per_file * remaining_files
                eta_time = datetime.now() + timedelta(seconds=eta_seconds)
                eta_str = eta_time.strftime('%Y/%m/%d %H:%M')
                percent_complete = (i / len(files_to_process)) * 100
                print(f"[{timestamp}] {percent_complete:5.1f}% - Processed {i}/{len(files_to_process)} files "
                      f"({successful} successful, {failed} failed) - ETA: {eta_str}")
                early_progress_shown = True
            
            # Show progress at intervals
            if i % progress_interval == 0 or i == len(files_to_process):
                timestamp = datetime.now().strftime('%H:%M:%S')
                elapsed = (datetime.now() - start_time).total_seconds()
                
                # Calculate ETA
                if i < len(files_to_process):
                    avg_time_per_file = elapsed / i
                    remaining_files = len(files_to_process) - i
                    eta_seconds = avg_time_per_file * remaining_files
                    eta_time = datetime.now() + timedelta(seconds=eta_seconds)
                    eta_str = eta_time.strftime('%Y/%m/%d %H:%M')
                else:
                    eta_str = "DONE"
                
                percent_complete = (i / len(files_to_process)) * 100
                print(f"[{timestamp}] {percent_complete:5.1f}% - Processed {i}/{len(files_to_process)} files "
                      f"({successful} successful, {failed} failed) - ETA: {eta_str}")
    
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"\n[{timestamp}] Creating final TSV with complete column set...")
    
    # Get all unique keys
    all_keys = set()
    for metadata in all_metadata:
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
    
    print("\n" + "=" * 60)
    print("File Output Complete")
    print("=" * 60)
    print(f"JSONL: {jsonl_file}" + (" (deleted)" if delete_json else ""))
    print(f"TSV:   {tsv_file}")
    print(f"Successfully extracted: {successful} files")
    print(f"Failed: {failed} files")
    
    # Import to database if enabled
    if not skip_db and FITS_DATABASE_AVAILABLE:
        print("\n" + "=" * 60)
        print("Database Import")
        print("=" * 60)
        
        # Determine database path from organize log location
        log_path = Path(log_file)
        db_path = log_path.parent / 'astrophotography.db'
        
        if not db_path.exists():
            print(f"Warning: Database not found at {db_path}")
            print("Run fits_organizer.py first to create the database")
        else:
            ensure_database_schema(db_path)
            import_metadata_to_database(all_metadata, log_df, db_path)
            print(f"Database: {db_path}")
    elif not skip_db:
        print("\nWarning: Database import skipped (fits_database.py not found)")
    
    print("\nDone!")
    return tsv_file


def main():
    parser = argparse.ArgumentParser(
        description='Extract FITS metadata and statistics, output to TSV/JSONL and database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Extracts comprehensive metadata and computed statistics from FITS files.

Outputs:
  • TSV file: Complete metadata with organized columns
  • JSONL file: Incremental output (safe for interruptions)
  • SQLite database: Searchable metadata in EAV table

Combines:
  • FITS header metadata (all keywords)
  • Image statistics (mean, median, std, percentiles)
  • Saturation analysis
  • Organizer metadata (target, filter, gain, etc.)

Examples:
  # Process and import to database (default)
  %(prog)s organize_log_20250122_*.tsv
  
  # Skip database import
  %(prog)s organize_log_20250122_*.tsv --skip-db
  
  # Specify output file
  %(prog)s organize_log_20250122_*.tsv --output metadata.tsv
  
  # Delete JSONL after TSV creation
  %(prog)s organize_log_20250122_*.tsv --delete-json
'''
    )
    
    parser.add_argument('log_file', help='TSV log file from fits_organizer')
    parser.add_argument('--output', '-o', help='Output file base name (default: fits_metadata_TIMESTAMP)')
    parser.add_argument('--delete-json', action='store_true',
                       help='Delete JSONL file after TSV creation (default: keep both)')
    parser.add_argument('--skip-db', action='store_true',
                       help='Skip database import (default: import to database)')
    
    args = parser.parse_args()
    
    # Check log file exists
    if not os.path.exists(args.log_file):
        print(f"Error: Log file '{args.log_file}' does not exist")
        sys.exit(1)
    
    # Run extraction
    extract_metadata_from_log(args.log_file, args.output, args.delete_json, args.skip_db)


if __name__ == '__main__':
    main()
