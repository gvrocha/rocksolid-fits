#!/usr/bin/env python3
"""
Extract FITS Metadata to Database

Extracts full FITS header metadata from organized files into the database.
Useful for populating metadata table from existing organized collections.
"""

import os
import sys
import argparse
from pathlib import Path

try:
    from fits_database import ensure_database_schema, import_fits_metadata, get_database_path
    FITS_DATABASE_AVAILABLE = True
except ImportError:
    print("Error: fits_database.py not found")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='Extract FITS header metadata into SQLite database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
This script reads the organize log TSV and extracts ALL FITS header keywords
into the metadata table using the EAV (Entity-Attribute-Value) pattern.

Examples:
  # Extract metadata from organize log
  %(prog)s organize_log_20260121_183045.tsv
  
  # Specify database location
  %(prog)s organize_log_20260121_183045.tsv --db /path/to/astrophotography.db
  
  # Process all organize logs in a directory
  for log in organized/asi294mc/organize_log_*.tsv; do
      %(prog)s "$log"
  done

Query Examples After Import:
  # Find files with specific FOCUSPOS
  SELECT o.destination_file 
  FROM organize_log o
  JOIN fits_metadata m ON o.id = m.fits_file_id
  WHERE m.metadata_key = 'FOCUSPOS' AND m.value_numeric > 5000;
  
  # Get all metadata for a specific file
  SELECT metadata_key, value_numeric, value_text
  FROM fits_metadata
  WHERE fits_file_id = 1
  ORDER BY metadata_key;
  
  # Find all unique metadata keys
  SELECT DISTINCT metadata_key 
  FROM fits_metadata 
  ORDER BY metadata_key;
'''
    )
    
    parser.add_argument('organize_log', help='Organize log TSV file')
    parser.add_argument('--db', help='Database path (default: auto-detect from log location)')
    
    args = parser.parse_args()
    
    # Check organize log exists
    if not os.path.exists(args.organize_log):
        print(f"Error: Organize log '{args.organize_log}' does not exist")
        sys.exit(1)
    
    # Determine database path
    if args.db:
        db_path = Path(args.db)
    else:
        # Database should be in same directory as organize log
        log_path = Path(args.organize_log)
        db_path = log_path.parent / 'astrophotography.db'
    
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}")
        print("Run fits_organizer.py first to create the database")
        sys.exit(1)
    
    print("=" * 60)
    print("Extract FITS Metadata to Database")
    print("=" * 60)
    print(f"Organize log: {args.organize_log}")
    print(f"Database:     {db_path}")
    print()
    
    # Ensure schema exists (shouldn't be needed, but safe)
    ensure_database_schema(db_path)
    
    # Extract metadata
    import_fits_metadata(args.organize_log, db_path)
    
    print()
    print("Done!")


if __name__ == '__main__':
    main()
