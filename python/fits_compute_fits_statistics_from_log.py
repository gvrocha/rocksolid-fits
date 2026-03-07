#!/usr/bin/env python3
"""
fits_compute_fits_statistics_from_log.py
Computes image statistics from FITS files listed in an organizer log and
inserts them into the fits_metadata table (stat_* keys only).

Skips files that already have stat_* entries. Does NOT re-insert headers.
"""

import os
import sys
import argparse
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

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
    from fits_database import ensure_database_schema
    FITS_DATABASE_AVAILABLE = True
except ImportError:
    FITS_DATABASE_AVAILABLE = False
    print("Warning: fits_database.py not found. Database import will be disabled.")


def compute_statistics(fits_path):
    """
    Compute image statistics for a single FITS file.

    Returns dict of stat_* keys, or None on failure.
    """
    try:
        with fits.open(fits_path) as hdul:
            header = hdul[0].header
            data = hdul[0].data

            if data is None:
                return None

            maxadu_value = header.get('MAXADU', None)

            if maxadu_value is not None:
                saturation_threshold = float(maxadu_value)
            elif data.dtype == np.uint16:
                saturation_threshold = 65535
            else:
                saturation_threshold = (
                    np.iinfo(data.dtype).max
                    if np.issubdtype(data.dtype, np.integer)
                    else np.finfo(data.dtype).max
                )

            percentile_list = list(range(5, 100, 5))
            percentile_values = np.percentile(data, percentile_list)

            stats = {
                'stat_mean': float(np.mean(data)),
                'stat_median': float(np.median(data)),
                'stat_min': float(np.min(data)),
                'stat_max': float(np.max(data)),
                'stat_std': float(np.std(data)),
                'stat_saturation_threshold_used': saturation_threshold,
                'stat_pixels_saturated_low': int(np.sum(data == np.min(data))),
                'stat_pixels_saturated_high': int(np.sum(data >= saturation_threshold)),
                'stat_total_pixels': int(data.size),
            }

            if maxadu_value is not None:
                stats['stat_maxadu'] = float(maxadu_value)

            for i, p in enumerate(percentile_list):
                stats[f'stat_percentile_{p:02d}'] = float(percentile_values[i])

            return stats

    except Exception as e:
        print(f"Error reading {os.path.basename(fits_path)}: {e}")
        return None


def insert_statistics(cursor, fits_file_id, stats):
    """Insert stat_* entries for a single file. Returns count inserted."""
    count = 0
    for key, value in stats.items():
        try:
            cursor.execute(
                '''
                INSERT INTO fits_metadata (fits_file_id, metadata_key, value_numeric, value_text)
                VALUES (?, ?, ?, NULL)
                ''',
                (fits_file_id, key, value),
            )
            count += 1
        except sqlite3.IntegrityError:
            pass  # Already exists
    return count


def compute_statistics_from_log(log_file, db_path=None):
    """Main processing function."""

    print("=" * 60)
    print("FITS Statistics Computer from Organizer Log")
    print("=" * 60)
    print()

    # Read organizer log
    print(f"Reading organizer log: {log_file}")
    try:
        log_df = pd.read_csv(log_file, sep='\t')
    except Exception as e:
        print(f"Error reading log file: {e}")
        sys.exit(1)

    # Only process successfully copied files
    frames_df = log_df[log_df['action'] == 'copied'].copy()
    print(f"Found {len(frames_df)} copied files to process\n")

    if len(frames_df) == 0:
        print("No copied files found in log.")
        return

    # Resolve database path
    if db_path is None:
        db_path = Path(log_file).parent / 'astrophotography.db'
    else:
        db_path = Path(db_path)

    if not db_path.exists():
        print(f"Error: Database not found at {db_path}")
        print("Run fits_organizer.py first to create the database.")
        sys.exit(1)

    if FITS_DATABASE_AVAILABLE:
        ensure_database_schema(db_path)

    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()

    processed = 0
    skipped_no_record = 0
    skipped_stats_exist = 0
    failed = 0
    total_inserted = 0

    start_time = datetime.now()
    progress_interval = max(1, len(frames_df) // 100)
    show_early_progress = len(frames_df) > 1000
    early_progress_shown = False

    for i, (_, row) in enumerate(frames_df.iterrows(), 1):
        fits_file = row['destination_file']

        # Look up fits_file_id
        cursor.execute(
            "SELECT id FROM fits_frames WHERE destination_file = ?",
            (fits_file,),
        )
        result = cursor.fetchone()
        if not result:
            skipped_no_record += 1
            continue

        fits_file_id = result[0]

        # Skip if stats already exist
        cursor.execute(
            "SELECT COUNT(*) FROM fits_metadata WHERE fits_file_id = ? AND metadata_key LIKE 'stat_%'",
            (fits_file_id,),
        )
        if cursor.fetchone()[0] > 0:
            skipped_stats_exist += 1
            continue

        # Check file exists on disk
        if not Path(fits_file).exists():
            print(f"Warning: File not found on disk: {fits_file}")
            failed += 1
            continue

        # Compute statistics
        stats = compute_statistics(fits_file)
        if stats is None:
            failed += 1
            continue

        # Insert into database
        count = insert_statistics(cursor, fits_file_id, stats)
        total_inserted += count
        processed += 1

        if processed % 10 == 0:
            conn.commit()

        # Progress reporting
        if show_early_progress and i == 10 and not early_progress_shown:
            _print_progress(i, len(frames_df), processed, failed, start_time)
            early_progress_shown = True

        if i % progress_interval == 0 or i == len(frames_df):
            _print_progress(i, len(frames_df), processed, failed, start_time)

    conn.commit()
    conn.close()

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Processed:            {processed} files ({total_inserted} stat entries inserted)")
    print(f"Skipped (stats exist):{skipped_stats_exist} files")
    print(f"Skipped (not in DB):  {skipped_no_record} files")
    if failed:
        print(f"Failed:               {failed} files")
    print(f"Database: {db_path}")
    print("Done!")


def _print_progress(i, total, processed, failed, start_time):
    ts = datetime.now().strftime('%H:%M:%S')
    elapsed = (datetime.now() - start_time).total_seconds()
    if i > 0:
        eta_seconds = (elapsed / i) * (total - i)
        eta_str = (datetime.now() + timedelta(seconds=eta_seconds)).strftime('%Y/%m/%d %H:%M')
    else:
        eta_str = "calculating..."
    pct = (i / total) * 100
    print(f"[{ts}] {pct:5.1f}% - {i}/{total} files ({processed} processed, {failed} failed) - ETA: {eta_str}")


def main():
    parser = argparse.ArgumentParser(
        description='Compute FITS image statistics and insert into database (stat_* keys only)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Reads FITS files listed in an organizer log, computes image statistics,
and inserts them into the fits_metadata table as stat_* keys.

Skips files that already have stat_* entries. Does NOT touch header entries.
Intended to be run after fits_organizer.py (which imports headers).

Statistics computed:
  stat_mean, stat_median, stat_min, stat_max, stat_std
  stat_saturation_threshold_used
  stat_pixels_saturated_low, stat_pixels_saturated_high
  stat_total_pixels, stat_maxadu (if MAXADU header present)
  stat_percentile_05 ... stat_percentile_95

Examples:
  %(prog)s organize_log_20260119_143052.tsv
  %(prog)s organize_log_20260119_143052.tsv --db /path/to/astrophotography.db
'''
    )
    parser.add_argument('log_file', help='TSV log file from fits_organizer')
    parser.add_argument('--db', help='Database path (default: auto-detect from log location)')

    args = parser.parse_args()

    if not os.path.exists(args.log_file):
        print(f"Error: Log file '{args.log_file}' does not exist")
        sys.exit(1)

    compute_statistics_from_log(args.log_file, args.db)


if __name__ == '__main__':
    main()
