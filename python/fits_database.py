"""
Shared SQLite database module for FITS file organization and review system.

This module provides database schema creation and data import functions
for the astrophotography workflow.
"""

import sqlite3
import os.path
from pathlib import Path
from datetime import datetime


def ensure_database_schema(db_path):
    """
    Create database and tables if they don't exist.
    
    Args:
        db_path: Path to SQLite database file
    
    Returns:
        True if schema was created, False if already existed
    """
    db_path = Path(db_path)
    db_existed = db_path.exists()
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    # Enable foreign key constraints
    cursor.execute("PRAGMA foreign_keys = ON")
    
    # Check if schema exists
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='fits_frames'
    """)
    
    schema_exists = cursor.fetchone() is not None
    
    if not schema_exists:
        print(f"Creating database schema in {db_path}")
        
        # Main FITS frames table - tracks all organized FITS files
        cursor.execute('''
            CREATE TABLE fits_frames (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_date TEXT NOT NULL,
                target TEXT,
                filter TEXT,
                gain TEXT,
                exposure_sec REAL,
                temperature_c REAL,
                timestamp TEXT,
                source_file TEXT NOT NULL,
                destination_file TEXT UNIQUE NOT NULL,
                file_hash TEXT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Index for common queries
        cursor.execute('''
            CREATE INDEX idx_frames_target ON fits_frames(target)
        ''')
        cursor.execute('''
            CREATE INDEX idx_frames_session ON fits_frames(session_date)
        ''')
        cursor.execute('''
            CREATE INDEX idx_frames_dest_file ON fits_frames(destination_file)
        ''')
        
        # Long/skinny metadata table - stores all FITS header metadata
        cursor.execute('''
            CREATE TABLE fits_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fits_file_id INTEGER NOT NULL,
                metadata_key TEXT NOT NULL,
                value_numeric REAL,
                value_text TEXT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (fits_file_id) REFERENCES fits_frames(id) ON DELETE CASCADE,
                UNIQUE(fits_file_id, metadata_key)
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX idx_metadata_key ON fits_metadata(metadata_key)
        ''')
        cursor.execute('''
            CREATE INDEX idx_metadata_fits_id ON fits_metadata(fits_file_id)
        ''')
        
        # JPEG preview and review table - tracks JPEG generation AND review status
        cursor.execute('''
            CREATE TABLE fits_jpeg_review (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fits_file_id INTEGER NOT NULL,
                jpeg_file TEXT,
                jpeg_collection_file TEXT,
                processing_status TEXT,
                review_status TEXT DEFAULT 'unverified' CHECK(review_status IN ('unverified', 'selected', 'discarded', 'undecided')),
                reviewed_at TIMESTAMP,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (fits_file_id) REFERENCES fits_frames(id) ON DELETE CASCADE,
                UNIQUE(fits_file_id)
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX idx_jpeg_review_fits_id ON fits_jpeg_review(fits_file_id)
        ''')
        cursor.execute('''
            CREATE INDEX idx_jpeg_review_status ON fits_jpeg_review(review_status)
        ''')
        
        conn.commit()
        print("Database schema created successfully")
    else:
        if not db_existed:
            print(f"Created new database: {db_path}")
        else:
            print(f"Using existing database: {db_path}")
    
    conn.close()
    return not schema_exists


def import_fits_frames(tsv_file, db_path):
    """
    Import organize log TSV into database.
    
    Args:
        tsv_file: Path to organize log TSV file
        db_path: Path to SQLite database
    
    Returns:
        Number of rows imported
    """
    import pandas as pd
    
    # Read TSV
    df = pd.read_csv(tsv_file, sep='\t')
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    imported_count = 0
    skipped_count = 0
    
    for _, row in df.iterrows():
        try:
            # Check if destination_file already exists
            cursor.execute(
                "SELECT id FROM fits_frames WHERE destination_file = ?",
                (row['destination_file'],)
            )
            
            if cursor.fetchone():
                skipped_count += 1
                continue
            
            cursor.execute('''
                INSERT INTO fits_frames 
                (session_date, target, filter, gain, exposure_sec, temperature_c, 
                 timestamp, source_file, destination_file, file_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                row.get('session_date', ''),
                row.get('target', ''),
                row.get('filter', ''),
                row.get('gain', ''),
                row.get('exposure_sec', None),
                row.get('temperature_c', None),
                row.get('timestamp', ''),
                row.get('source_file', ''),
                row.get('destination_file', ''),
                row.get('file_hash', '')
            ))
            
            imported_count += 1
            
        except sqlite3.IntegrityError as e:
            print(f"Skipping duplicate entry: {row.get('destination_file', 'unknown')}")
            skipped_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"Imported {imported_count} rows into fits_frames (skipped {skipped_count} duplicates)")
    return imported_count


def import_fits_jpeg_review(tsv_file, db_path):
    """
    Import preview log TSV into database.
    
    Args:
        tsv_file: Path to preview log TSV file
        db_path: Path to SQLite database
    
    Returns:
        Number of rows imported
    """
    import pandas as pd
    
    # Read TSV
    df = pd.read_csv(tsv_file, sep='\t')
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    imported_count = 0
    skipped_count = 0
    
    for _, row in df.iterrows():
        try:
            fits_file = row.get('fits_file', '')
            
            # Get fits_file_id from fits_frames
            cursor.execute(
                "SELECT id FROM fits_frames WHERE destination_file = ?",
                (fits_file,)
            )
            
            result = cursor.fetchone()
            if not result:
                print(f"Warning: FITS file not found in fits_frames: {fits_file}")
                skipped_count += 1
                continue
            
            fits_file_id = result[0]
            
            # Check if already exists
            cursor.execute(
                "SELECT id FROM fits_jpeg_review WHERE fits_file_id = ?",
                (fits_file_id,)
            )
            
            if cursor.fetchone():
                skipped_count += 1
                continue
            
            cursor.execute('''
                INSERT INTO fits_jpeg_review 
                (fits_file_id, jpeg_file, jpeg_collection_file, processing_status, review_status)
                VALUES (?, ?, ?, ?, 'unverified')
            ''', (
                fits_file_id,
                row.get('jpeg_file', ''),
                row.get('jpeg_collection_file', ''),
                row.get('processing_status', '')
            ))
            
            imported_count += 1
            
        except sqlite3.IntegrityError as e:
            skipped_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"Imported {imported_count} rows into fits_jpeg_review (skipped {skipped_count} duplicates)")
    return imported_count


def import_review_results(tsv_file, db_path):
    """
    Import review results TSV and update review_status in fits_jpeg_review table.
    
    Args:
        tsv_file: Path to review results TSV file
        db_path: Path to SQLite database
    
    Returns:
        Number of rows updated
    """
    import pandas as pd
    
    # Read TSV
    df = pd.read_csv(tsv_file, sep='\t')
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    updated_count = 0
    skipped_count = 0
    
    for _, row in df.iterrows():
        try:
            fits_file = row.get('fits_file', '')
            review_status = row.get('review_status', 'unverified')
            
            # Get fits_file_id from fits_frames
            cursor.execute(
                "SELECT id FROM fits_frames WHERE destination_file = ?",
                (fits_file,)
            )
            
            result = cursor.fetchone()
            if not result:
                print(f"Warning: FITS file not found in fits_frames: {fits_file}")
                skipped_count += 1
                continue
            
            fits_file_id = result[0]
            
            # Update review_status in fits_jpeg_review
            cursor.execute('''
                UPDATE fits_jpeg_review 
                SET review_status = ?, reviewed_at = CURRENT_TIMESTAMP
                WHERE fits_file_id = ?
            ''', (review_status, fits_file_id))
            
            if cursor.rowcount > 0:
                updated_count += 1
            else:
                print(f"Warning: No fits_jpeg_review entry found for {fits_file}")
                skipped_count += 1
            
        except sqlite3.IntegrityError as e:
            print(f"Error updating review result: {e}")
            skipped_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"Updated {updated_count} review statuses in fits_jpeg_review (skipped {skipped_count})")
    return updated_count


def get_database_path(output_folder):
    """
    Get the standard database path for a given output folder.
    
    Args:
        output_folder: Path to organized output folder
    
    Returns:
        Path to database file
    """
    return Path(output_folder) / 'astrophotography.db'


def import_fits_headers_only(tsv_file, db_path):
    """
    Extract and import ONLY FITS header keywords (fast, no image data reading).
    
    Used by fits_organizer.py for quick metadata import during organization.
    Does NOT compute statistics - use import_fits_metadata() for that.
    
    Args:
        tsv_file: Path to organize log TSV file
        db_path: Path to SQLite database
    
    Returns:
        Number of FITS files processed
    """
    import pandas as pd
    
    # Import the header extraction function
    try:
        from fits_metadata_utils import extract_fits_headers_only
    except ImportError:
        print("Warning: fits_metadata_utils.py not found. Skipping header import.")
        return 0
    
    from astropy.io import fits as astropy_fits
    
    # Read TSV
    df = pd.read_csv(tsv_file, sep='\t')
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    files_processed = 0
    files_skipped = 0
    total_metadata_inserted = 0
    
    # Keywords to skip (handled in fits_frames table already)
    skip_keywords = {
        'SIMPLE', 'BITPIX', 'NAXIS', 'NAXIS1', 'NAXIS2', 'EXTEND',
        'COMMENT', 'HISTORY', '', 'filepath', 'target_from_path', 'extraction_error'
    }
    
    for _, row in df.iterrows():
        fits_file = row['destination_file']
        
        # Get fits_file_id from fits_frames
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
        
        # Extract FITS header (no image data)
        try:
            if not Path(fits_file).exists():
                print(f"Warning: FITS file not found on disk: {fits_file}")
                files_skipped += 1
                continue
            
            metadata = extract_fits_headers_only(fits_file)
            
            if metadata.get('extraction_error'):
                print(f"Warning: Could not extract headers from {fits_file}: {metadata['extraction_error']}")
                files_skipped += 1
                continue
            
            metadata_count = 0
            
            # Insert header keywords
            for key, value in metadata.items():
                if key in skip_keywords:
                    continue
                
                # Determine if numeric or text
                value_numeric = None
                value_text = None
                
                if isinstance(value, (int, float)):
                    value_numeric = float(value)
                elif isinstance(value, bool):
                    value_numeric = 1.0 if value else 0.0
                else:
                    value_text = str(value)
                
                # Insert metadata
                try:
                    cursor.execute('''
                        INSERT INTO fits_metadata 
                        (fits_file_id, metadata_key, value_numeric, value_text)
                        VALUES (?, ?, ?, ?)
                    ''', (fits_file_id, key, value_numeric, value_text))
                    metadata_count += 1
                except sqlite3.IntegrityError:
                    # Duplicate key for this file, skip
                    pass
            
            total_metadata_inserted += metadata_count
            files_processed += 1
            
            if files_processed % 10 == 0:
                conn.commit()  # Commit periodically
                    
        except Exception as e:
            print(f"Error reading FITS header from {fits_file}: {e}")
            files_skipped += 1
    
    conn.commit()
    conn.close()
    
    print(f"Imported headers from {files_processed} FITS files ({total_metadata_inserted} header keywords)")
    if files_skipped > 0:
        print(f"Skipped {files_skipped} files (already processed or not found)")
    return files_processed


def import_fits_metadata(tsv_file, db_path):
    """
    Extract and import full FITS header metadata AND computed statistics into the metadata table.
    
    This function imports EVERYTHING (headers + statistics). If headers were already imported
    by fits_organizer.py, this will skip files that already have ANY metadata present.
    
    Reads FITS files listed in organize log and extracts:
    1. All header keywords (skipped if already present from organizer)
    2. Computed statistics (mean, median, std, min, max, percentiles)
    3. Saturation counts
    
    Args:
        tsv_file: Path to organize log TSV file
        db_path: Path to SQLite database
    
    Returns:
        Number of FITS files processed
    """
    import pandas as pd
    import numpy as np
    from astropy.io import fits as astropy_fits
    
    # Read TSV
    df = pd.read_csv(tsv_file, sep='\t')
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    files_processed = 0
    files_skipped = 0
    total_metadata_inserted = 0
    
    # Keywords to skip (handled in fits_frames table already)
    skip_keywords = {
        'SIMPLE', 'BITPIX', 'NAXIS', 'NAXIS1', 'NAXIS2', 'EXTEND',
        'COMMENT', 'HISTORY', ''  # Skip empty keys
    }
    
    for _, row in df.iterrows():
        fits_file = row['destination_file']
        
        # Get fits_file_id from fits_frames
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
        
        # Extract FITS header
        try:
            if not Path(fits_file).exists():
                print(f"Warning: FITS file not found on disk: {fits_file}")
                files_skipped += 1
                continue
            
            with astropy_fits.open(fits_file) as hdul:
                header = hdul[0].header
                data = hdul[0].data
                
                metadata_count = 0
                
                # 1. Extract header keywords
                for key in header.keys():
                    if key in skip_keywords:
                        continue
                    
                    value = header[key]
                    
                    # Determine if numeric or text
                    value_numeric = None
                    value_text = None
                    
                    if isinstance(value, (int, float)):
                        value_numeric = float(value)
                    elif isinstance(value, bool):
                        value_numeric = 1.0 if value else 0.0
                    else:
                        value_text = str(value)
                    
                    # Insert metadata
                    try:
                        cursor.execute('''
                            INSERT INTO fits_metadata 
                            (fits_file_id, metadata_key, value_numeric, value_text)
                            VALUES (?, ?, ?, ?)
                        ''', (fits_file_id, key, value_numeric, value_text))
                        metadata_count += 1
                    except sqlite3.IntegrityError:
                        # Duplicate key for this file, skip
                        pass
                
                # 2. Compute and insert image statistics (if data exists)
                if data is not None:
                    # Basic statistics
                    stats = {
                        'stat_mean': float(np.mean(data)),
                        'stat_median': float(np.median(data)),
                        'stat_min': float(np.min(data)),
                        'stat_max': float(np.max(data)),
                        'stat_std': float(np.std(data)),
                        'stat_total_pixels': int(data.size),
                    }
                    
                    # Saturation analysis
                    maxadu = header.get('MAXADU', None)
                    if maxadu is not None:
                        saturation_threshold = float(maxadu)
                    elif data.dtype == np.uint16:
                        saturation_threshold = 65535
                    else:
                        saturation_threshold = np.iinfo(data.dtype).max if np.issubdtype(data.dtype, np.integer) else np.finfo(data.dtype).max
                    
                    stats['stat_saturation_threshold'] = saturation_threshold
                    stats['stat_pixels_saturated_low'] = int(np.sum(data == np.min(data)))
                    stats['stat_pixels_saturated_high'] = int(np.sum(data >= saturation_threshold))
                    
                    # Percentiles (5, 10, 15, ..., 95)
                    percentile_list = list(range(5, 100, 5))
                    percentile_values = np.percentile(data, percentile_list)
                    for i, p in enumerate(percentile_list):
                        stats[f'stat_percentile_{p:02d}'] = float(percentile_values[i])
                    
                    # Insert all computed statistics
                    for stat_key, stat_value in stats.items():
                        try:
                            cursor.execute('''
                                INSERT INTO fits_metadata 
                                (fits_file_id, metadata_key, value_numeric, value_text)
                                VALUES (?, ?, ?, NULL)
                            ''', (fits_file_id, stat_key, stat_value))
                            metadata_count += 1
                        except sqlite3.IntegrityError:
                            pass

                
                total_metadata_inserted += metadata_count
                files_processed += 1
                
                if files_processed % 10 == 0:
                    print(f"  Processed metadata: {files_processed}/{len(df)} files...")
                    conn.commit()  # Commit periodically
                    
        except Exception as e:
            print(f"Error reading FITS header from {fits_file}: {e}")
            files_skipped += 1
    
    conn.commit()
    conn.close()
    
    print(f"Imported metadata from {files_processed} FITS files ({total_metadata_inserted} metadata entries)")
    print(f"Skipped {files_skipped} files (already processed or not found)")
    return files_processed
