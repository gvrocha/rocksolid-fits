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
        WHERE type='table' AND name='organize_log'
    """)
    
    schema_exists = cursor.fetchone() is not None
    
    if not schema_exists:
        print(f"Creating database schema in {db_path}")
        
        # Main organize log table - tracks all FITS files
        cursor.execute('''
            CREATE TABLE organize_log (
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
            CREATE INDEX idx_organize_target ON organize_log(target)
        ''')
        cursor.execute('''
            CREATE INDEX idx_organize_session ON organize_log(session_date)
        ''')
        cursor.execute('''
            CREATE INDEX idx_organize_dest_file ON organize_log(destination_file)
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
                FOREIGN KEY (fits_file_id) REFERENCES organize_log(id) ON DELETE CASCADE,
                UNIQUE(fits_file_id, metadata_key)
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX idx_metadata_key ON fits_metadata(metadata_key)
        ''')
        cursor.execute('''
            CREATE INDEX idx_metadata_fits_id ON fits_metadata(fits_file_id)
        ''')
        
        # Preview log table - tracks JPEG generation
        cursor.execute('''
            CREATE TABLE preview_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fits_file_id INTEGER NOT NULL,
                status TEXT,
                jpeg_file TEXT,
                jpeg_collection_file TEXT,
                processing_status TEXT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (fits_file_id) REFERENCES organize_log(id) ON DELETE CASCADE,
                UNIQUE(fits_file_id)
            )
        ''')
        
        cursor.execute('''
            CREATE INDEX idx_preview_fits_id ON preview_log(fits_file_id)
        ''')
        
        # Review results table - tracks manual frame selection
        cursor.execute('''​​​​​​​​​​​​​​​​

