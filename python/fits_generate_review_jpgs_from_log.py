#!/usr/bin/env python3
"""
FITS Quick Review Generator (from Organizer Log)
Generates grayscale JPEG previews from files listed in fits_organizer TSV log
"""

import os
import sys
import argparse
from datetime import datetime
from pathlib import Path
import shutil
import numpy as np
import pandas as pd

try:
    from astropy.io import fits
    ASTROPY_AVAILABLE = True
except ImportError:
    ASTROPY_AVAILABLE = False
    print("Error: astropy is required but not installed")
    print("Install with: pip install astropy")
    sys.exit(1)

try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False
    print("Error: Pillow is required but not installed")
    print("Install with: pip install Pillow")
    sys.exit(1)
    
# Import database module
try:
    from fits_database import ensure_database_schema, import_fits_jpeg_review, get_database_path
    FITS_DATABASE_AVAILABLE = True
except ImportError:
    FITS_DATABASE_AVAILABLE = False
    print("Warning: fits_database.py not found. Database features will be disabled.")

try:
    import openpyxl
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False
    print("Warning: openpyxl not available. Excel export will be skipped.")
    print("Install with: pip install openpyxl")


def autostretch(data, low_percentile=0.1, high_percentile=99.9):
    """
    Apply autostretch to image data using percentile clipping
    
    Args:
        data: 2D numpy array of image data
        low_percentile: Lower percentile for clipping (default 0.1%)
        high_percentile: Upper percentile for clipping (default 99.9%)
    
    Returns:
        8-bit numpy array (0-255) suitable for JPEG
    """
    # Calculate percentile values
    low = np.percentile(data, low_percentile)
    high = np.percentile(data, high_percentile)
    
    # Clip and scale to 0-1
    if high > low:
        stretched = np.clip((data - low) / (high - low), 0, 1)
    else:
        # Handle edge case where all values are similar
        stretched = np.zeros_like(data, dtype=float)
    
    # Convert to 8-bit
    return (stretched * 255).astype(np.uint8)


def fits_to_jpeg(fits_path, output_path, max_width=1920, low_pct=0.1, high_pct=99.9):
    """
    Convert FITS file to autostretch JPEG preview
    
    Args:
        fits_path: Path to FITS file
        output_path: Full path for output JPEG
        max_width: Maximum width in pixels (maintains aspect ratio)
        low_pct: Lower percentile for autostretch
        high_pct: Upper percentile for autostretch
    
    Returns:
        True on success, False on failure
    """
    try:
        with fits.open(fits_path) as hdul:
            data = hdul[0].data
            
            if data is None:
                return False
            
            # Handle different data dimensions
            if len(data.shape) == 3:
                # Color/Bayer data - just take first channel for grayscale preview
                data = data[0]
            elif len(data.shape) != 2:
                return False
            
            # Apply autostretch
            stretched = autostretch(data, low_pct, high_pct)
            
            # Create PIL Image
            img = Image.fromarray(stretched, mode='L')
            
            # Resize if needed (maintain aspect ratio)
            if img.width > max_width:
                aspect_ratio = img.height / img.width
                new_width = max_width
                new_height = int(max_width * aspect_ratio)
                img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
            
            # Ensure output directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save as JPEG
            img.save(output_path, 'JPEG', quality=85, optimize=True)
            
            return True
            
    except Exception as e:
        print(f"Error processing {os.path.basename(fits_path)}: {e}")
        return False


def generate_previews_from_log(log_file, review_base_dir=None, max_width=1920, 
                               low_pct=0.1, high_pct=99.9, include_calibration=False):
    """Generate JPEG previews from files listed in organizer log"""
    
    print("=" * 60)
    print("FITS Quick Review Generator (from Organizer Log)")
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
    
    # Filter for successfully copied frames
    if include_calibration:
        # Include all frame types
        frames_df = log_df[log_df['action'] == 'copied'].copy()
        print(f"Processing all frame types (lights + calibration)")
    else:
        # Light frames only
        frames_df = log_df[
            (log_df['action'] == 'copied') & 
            (log_df['frame_type'].str.contains('light', case=False, na=False))
        ].copy()
        print(f"Processing light frames only (use --include-calibration for all frames)")
    
    if len(frames_df) == 0:
        if include_calibration:
            print("No successfully copied frames found in log")
        else:
            print("No successfully copied light frames found in log")
        return
    
    print(f"Found {len(frames_df)} frames to process\n")
    
    # Generate output log filename
    timestamp_now = datetime.now()
    milliseconds = timestamp_now.microsecond // 1000
    tsv_timestamp = f"{timestamp_now.strftime('%Y%m%d_%H%M%S')}_{milliseconds:03d}"
    preview_log_filename = f'preview_log_{tsv_timestamp}.tsv'
    
    # Place output log in same directory as input log file
    log_dir = os.path.dirname(os.path.abspath(log_file))
    preview_log_file = os.path.join(log_dir, preview_log_filename)
    
    # Determine review base directory
    if review_base_dir is None:
        # Use parent of first destination file
        first_dest = Path(frames_df.iloc[0]['destination_file'])
        # Go up to find organized root (look for 'sessions' or 'calibration')
        review_base_dir = first_dest
        while review_base_dir.parent != review_base_dir:
            if review_base_dir.name in ['sessions', 'calibration', 'organized']:
                review_base_dir = review_base_dir.parent
                break
            review_base_dir = review_base_dir.parent
        review_base_dir = review_base_dir / 'review'
    else:
        review_base_dir = Path(review_base_dir)
    
    print(f"Review base directory: {review_base_dir}")
    print(f"Preview log: {preview_log_file}")
    print(f"Max width: {max_width}px")
    print(f"Autostretch: {low_pct}% - {high_pct}%\n")
    
    # Process files
    successful = 0
    failed = 0
    preview_records = []
    start_time = datetime.now()
    
    # Calculate progress interval (1% of total files, minimum 1)
    progress_interval = max(1, len(frames_df) // 100)
    
    # Flag to show early progress for large datasets
    show_early_progress = len(frames_df) > 1000
    early_progress_shown = False
    
    # Check for existing JPEGs to skip
    existing_jpegs = set()
    if review_base_dir.exists():
        collection_dir = review_base_dir / 'by_target_gain_exposure'
        if collection_dir.exists():
            # Find all existing JPEGs in collection
            for jpeg_file in collection_dir.rglob('*.jpg'):
                existing_jpegs.add(jpeg_file.name)
            print(f"Found {len(existing_jpegs)} existing JPEGs in collection - will skip these\n")
    
    with open(preview_log_file, 'w') as log_f:
        # Write log header with new column order
        log_f.write('status\ttarget\tgain\texposure_sec\tcapture_time\ttemperature_c\tfits_file\tjpeg_file\tjpeg_collection_file\tprocessing_status\n')
        
        for i, (idx, row) in enumerate(frames_df.iterrows(), 1):
            fits_path = Path(row['destination_file'])
            target = row.get('target', 'unknown')
            gain = row.get('gain', 'unknown')
            exposure = row.get('exposure_sec', 'unknown')
            temperature = row.get('temperature_c', 'unknown')
            timestamp_str = row.get('timestamp', '')
            
            # Create JPEG in 'jpegs' subfolder next to FITS file
            jpeg_dir = fits_path.parent / 'jpegs'
            jpeg_path = jpeg_dir / f"{fits_path.stem}.jpg"
            
            # Keep original path for database lookups (matches organize_log)
            fits_original_path = str(fits_path)
            jpeg_original_path = str(jpeg_path)
            
            # Resolve symlinks for web-accessible collection paths only
            fits_real_path = os.path.realpath(fits_path)
            jpeg_real_path = os.path.realpath(jpeg_path)
            
            # Check if JPEG already exists
            if jpeg_path.exists():
                # JPEG already exists - record it with 'existing' processing status
                processing_status = 'existing'
                review_status = 'unverified'
                successful += 1
                
                # Store record for collection
                preview_records.append({
                    'status': review_status,
                    'target': target,
                    'gain': gain,
                    'exposure_sec': exposure,
                    'capture_time': timestamp_str,
                    'temperature_c': temperature,
                    'fits_file': fits_original_path,  # Original path for DB lookup
                    'jpeg_file': jpeg_original_path,  # Original path
                    'jpeg_file_collection': jpeg_real_path,  # Resolved path for web
                    'jpeg_path': jpeg_path,
                    'processing_status': processing_status
                })
                
                # Write to log - use original paths for DB compatibility
                log_f.write(f'{review_status}\t{target}\t{gain}\t{exposure}\t{timestamp_str}\t{temperature}\t{fits_original_path}\t{jpeg_original_path}\t\t{processing_status}\n')
                log_f.flush()
            else:
                # Convert to JPEG
                success = fits_to_jpeg(fits_path, jpeg_path, max_width, low_pct, high_pct)
                
                if success:
                    successful += 1
                    processing_status = 'generated'
                    review_status = 'unverified'
                    
                    # Store record for collection
                    preview_records.append({
                        'status': review_status,
                        'target': target,
                        'gain': gain,
                        'exposure_sec': exposure,
                        'capture_time': timestamp_str,
                        'temperature_c': temperature,
                        'fits_file': fits_original_path,  # Original path for DB lookup
                        'jpeg_file': jpeg_original_path,  # Original path
                        'jpeg_file_collection': jpeg_real_path,  # Resolved path for web
                        'jpeg_path': jpeg_path,
                        'processing_status': processing_status
                    })
                    
                    # Write to log - use original paths for DB compatibility
                    log_f.write(f'{review_status}\t{target}\t{gain}\t{exposure}\t{timestamp_str}\t{temperature}\t{fits_original_path}\t{jpeg_original_path}\t\t{processing_status}\n')
                    log_f.flush()
                else:
                    failed += 1
                    processing_status = 'failed'
                    review_status = 'discarded'
                    
                    # Still record failed conversions - use original path
                    log_f.write(f'{review_status}\t{target}\t{gain}\t{exposure}\t{timestamp_str}\t{temperature}\t{fits_original_path}\t\t\t{processing_status}\n')
                    log_f.flush()
            
            # Show early progress after 10 files for large datasets
            if show_early_progress and i == 10 and not early_progress_shown:
                timestamp = datetime.now().strftime('%H:%M:%S')
                elapsed = (datetime.now() - start_time).total_seconds()
                avg_time_per_file = elapsed / i
                remaining_files = len(frames_df) - i
                eta_seconds = avg_time_per_file * remaining_files
                from datetime import timedelta
                eta_time = datetime.now() + timedelta(seconds=eta_seconds)
                eta_str = eta_time.strftime('%Y/%m/%d %H:%M')
                percent_complete = (i / len(frames_df)) * 100
                print(f"[{timestamp}] {percent_complete:5.1f}% - Processed {i}/{len(frames_df)} files "
                      f"({successful} successful, {failed} failed) - ETA: {eta_str}")
                early_progress_shown = True
            
            # Show progress after processing at 1% intervals or last file
            if i % progress_interval == 0 or i == len(frames_df):
                timestamp = datetime.now().strftime('%H:%M:%S')
                elapsed = (datetime.now() - start_time).total_seconds()
                
                # Calculate ETA
                if i > 0:
                    avg_time_per_file = elapsed / i
                    remaining_files = len(frames_df) - i
                    eta_seconds = avg_time_per_file * remaining_files
                    from datetime import timedelta
                    eta_time = datetime.now() + timedelta(seconds=eta_seconds)
                    eta_str = eta_time.strftime('%Y/%m/%d %H:%M')
                else:
                    eta_str = "calculating..."
                
                percent_complete = (i / len(frames_df)) * 100
                print(f"[{timestamp}] {percent_complete:5.1f}% - Processed {i}/{len(frames_df)} files "
                      f"({successful} successful, {failed} failed) - ETA: {eta_str}")
    
    print("\n" + "=" * 60)
    print(f"Generated {successful} JPEGs in local 'jpegs' folders")
    if failed > 0:
        print(f"Failed: {failed} files")
    
    # Create collection organized by target/gain/exposure
    if preview_records:
        print("\nCreating organized collection...")
        collection_dir = review_base_dir / 'by_target_gain_exposure'
        collection_dir.mkdir(parents=True, exist_ok=True)
        
        # Group by target, gain, exposure
        from collections import defaultdict
        groups = defaultdict(list)
        
        for record in preview_records:
            # Sanitize names for folder structure
            target = str(record['target']).replace(' ', '_').replace('/', '_')
            gain = str(record['gain']).replace(' ', '_')
            exposure = str(record['exposure_sec']).replace(' ', '_').replace('.', 'p')
            
            group_key = (target, gain, exposure)
            groups[group_key].append(record)
        
        print(f"Creating {len(groups)} collection folders...")
        
        # Update log with collection paths
        collection_mapping = {}
        
        for (target, gain, exposure), records in groups.items():
            # Create folder: target/gain/exposure/
            folder = collection_dir / target / gain / f"{exposure}s"
            folder.mkdir(parents=True, exist_ok=True)
            
            # Copy JPEGs, sorted by capture time
            records_sorted = sorted(records, key=lambda x: x['capture_time'])
            
            for record in records_sorted:
                src_jpeg = record['jpeg_path']
                # Keep original filename (which includes timestamp for sorting)
                dst_jpeg = folder / src_jpeg.name
                shutil.copy2(src_jpeg, dst_jpeg)
                # Resolve symlink for collection path (for web access)
                dst_jpeg_real = os.path.realpath(dst_jpeg)
                # Map from original jpeg_file path to collection path
                collection_mapping[record['jpeg_file']] = dst_jpeg_real
        
        # Update log file with collection paths
        print("Updating preview log with collection paths...")
        
        # Read log back
        with open(preview_log_file, 'r') as f:
            lines = f.readlines()
        
        # Update collection column (column 9, index 8)
        with open(preview_log_file, 'w') as f:
            f.write(lines[0])  # Header
            for line in lines[1:]:
                parts = line.rstrip('\n').split('\t')
                if len(parts) >= 8:
                    jpeg_file = parts[7]  # jpeg_file is column 8 (index 7)
                    collection_file = collection_mapping.get(jpeg_file, '')
                    # Update collection path column (index 8)
                    if len(parts) == 9:
                        # Has processing_status but no collection path yet
                        parts[8] = collection_file
                    elif len(parts) == 10:
                        # Already has both columns
                        parts[8] = collection_file
                    else:
                        # Shouldn't happen, but handle gracefully
                        while len(parts) < 10:
                            parts.append('')
                        parts[8] = collection_file
                f.write('\t'.join(parts) + '\n')
        
        # Copy the preview log to the collection directory
        collection_log_path = collection_dir / preview_log_filename
        shutil.copy2(preview_log_file, collection_log_path)
        print(f"Copied preview log to collection: {collection_log_path}")
        
        # Create Excel version with separate tabs per target
        if OPENPYXL_AVAILABLE:
            print("Creating Excel workbook with tabs per target...")
            excel_filename = preview_log_filename.replace('.tsv', '.xlsx')
            excel_path = collection_dir / excel_filename
            
            try:
                # Read the updated TSV log
                log_df = pd.read_csv(preview_log_file, sep='\t')
                
                # Group by target
                targets = log_df['target'].unique()
                
                # Create Excel writer
                with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                    # Create a tab for each target
                    for target in sorted(targets):
                        target_df = log_df[log_df['target'] == target].copy()
                        
                        # Sanitize sheet name (Excel has 31 char limit, no special chars)
                        sheet_name = str(target).replace('/', '_').replace('\\', '_')[:31]
                        
                        # Write to sheet
                        target_df.to_excel(writer, sheet_name=sheet_name, index=False)
                        
                        # Get worksheet for formatting
                        worksheet = writer.sheets[sheet_name]
                        
                        # Enable autofilter on header row
                        worksheet.auto_filter.ref = worksheet.dimensions
                        
                        # Auto-adjust column widths based on content
                        for column in worksheet.columns:
                            max_length = 0
                            column_letter = column[0].column_letter
                            
                            for cell in column:
                                try:
                                    cell_value = str(cell.value) if cell.value is not None else ''
                                    if len(cell_value) > max_length:
                                        max_length = len(cell_value)
                                except:
                                    pass
                            
                            # Add padding and set width (no cap for true auto-width)
                            adjusted_width = max_length + 2
                            worksheet.column_dimensions[column_letter].width = adjusted_width
                
                print(f"Created Excel workbook: {excel_path}")
                print(f"  {len(targets)} tabs (one per target)")
            
            except Exception as e:
                print(f"Warning: Could not create Excel file: {e}")
        
        print(f"Collection created: {collection_dir}")
        print(f"  {len(groups)} folders (target/gain/exposure)")
        print(f"  JPEGs sorted by capture time within each folder")
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Generated:  {successful} JPEGs")
    if failed > 0:
        print(f"Failed:     {failed} files")
    print(f"Local JPEGs: Next to FITS files in 'jpegs' subdirectories")
    print(f"Collection:  {collection_dir}")
    if preview_records:
        print(f"TSV log:     {collection_dir / preview_log_filename}")
        if OPENPYXL_AVAILABLE:
            excel_filename = preview_log_filename.replace('.tsv', '.xlsx')
            print(f"Excel log:   {collection_dir / excel_filename} (tabs per target)")
        print(f"TSV log (original location): {preview_log_file}")
    else:
        print(f"Log file:    {preview_log_file}")
    print("=" * 60)
    
    return preview_log_file  # Return log file path for database import



def main():
    parser = argparse.ArgumentParser(
        description='FITS Quick Review Generator - Generate previews from organizer log',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Generates grayscale JPEG previews from frames listed in fits_organizer log.

By default, only light frames are processed. Use --include-calibration to also 
process calibration frames (darks, flats, bias).

Creates JPEGs in two locations:
  1. Local: In 'jpegs' subfolder next to each FITS file
  2. Collection: Organized by target/gain/exposure (sorted by capture time)

Also creates TSV and Excel logs in the collection directory. The Excel file has
separate tabs for each target, making it easy to review images by object.

Skips JPEGs that already exist in the collection to avoid duplicate work.

Designed to run after fits_organizer as part of import pipeline.

Examples:
  # Generate from organizer log (lights only)
  %(prog)s organize_log_20250117_143052_234.tsv
  
  # Include calibration frames
  %(prog)s organize_log_20250117_143052_234.tsv --include-calibration
  
  # Custom review base directory
  %(prog)s organize_log_20250117_143052_234.tsv --review-dir /path/to/review
  
  # Smaller previews
  %(prog)s organize_log_20250117_143052_234.tsv --width 1280
  
  # Pipeline: organize → metadata → previews
  python fits_organizer.py /raw /organized
  python fits_metadata_extractor_from_log.py organize_log_*.tsv
  python %(prog)s organize_log_*.tsv

Requirements:
  - Required: astropy, Pillow, pandas
  - Optional: openpyxl (for Excel export with tabs per target)
'''
    )
    
    parser.add_argument('log_file', help='TSV log file from fits_organizer')
    parser.add_argument('--review-dir', help='Base directory for review collection (default: auto-detect)')
    parser.add_argument('--include-calibration', action='store_true',
                       help='Include calibration frames (darks, flats, bias) in addition to lights (default: lights only)')
    parser.add_argument('--width', '-w', type=int, default=1920,
                       help='Maximum width in pixels (default: 1920)')
    parser.add_argument('--low', type=float, default=0.1,
                       help='Lower percentile for autostretch (default: 0.1)')
    parser.add_argument('--high', type=float, default=99.9,
                       help='Upper percentile for autostretch (default: 99.9)')
    parser.add_argument('--skip-db', action='store_true',
                       help='Skip database import (default: import to SQLite database)')
    
    args = parser.parse_args()
    
    # Check log file exists
    if not os.path.exists(args.log_file):
        print(f"Error: Log file '{args.log_file}' does not exist")
        sys.exit(1)
    
    # Validate percentiles
    if not (0 <= args.low < args.high <= 100):
        print("Error: Percentiles must satisfy 0 <= low < high <= 100")
        sys.exit(1)
    
    # Generate previews
    preview_log_file = generate_previews_from_log(args.log_file, args.review_dir, args.width, 
                               args.low, args.high, args.include_calibration)
    
    # Import to database unless skipped
    if preview_log_file and not args.skip_db and FITS_DATABASE_AVAILABLE:
        print()
        print("=" * 60)
        print("Importing to database...")
        print("=" * 60)
        
        # Determine database path from organize log location
        organize_log_path = Path(args.log_file)
        # Database should be in the output folder (parent of organize log)
        db_path = organize_log_path.parent / 'astrophotography.db'
        
        ensure_database_schema(db_path)
        import_fits_jpeg_review(preview_log_file, db_path)
        print(f"Database: {db_path}")
    elif not args.skip_db and not FITS_DATABASE_AVAILABLE:
        print("\nWarning: Database import skipped (fits_database.py not found)")
    elif args.skip_db:
        print("\nDatabase import skipped (--skip-db flag)")



if __name__ == '__main__':
    main()
