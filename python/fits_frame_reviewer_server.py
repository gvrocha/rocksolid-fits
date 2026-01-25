#!/usr/bin/env python3
"""
FITS Frame Reviewer Server
Flask web server for reviewing FITS frames and updating review status in database
"""

import os
import sys
import argparse
import sqlite3
from pathlib import Path
from flask import Flask, jsonify, request, send_file, send_from_directory
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Global database path (set via command line)
DB_PATH = None
BASE_DIR = None


def get_db_connection():
    """Get SQLite database connection"""
    if not DB_PATH or not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found at {DB_PATH}")
    
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row  # Access columns by name
    return conn


@app.route('/')
def index():
    """Serve the main HTML page"""
    html_path = Path(__file__).parent / 'fits_frame_reviewer.html'
    if not html_path.exists():
        return "Error: fits_frame_reviewer.html not found in same directory as server", 404
    return send_file(str(html_path))


@app.route('/api/frames')
def get_frames():
    """
    Get all frames with their review status and metadata
    
    Returns JSON array of frames:
    [
        {
            "id": 123,
            "jpeg_file": "/path/to/image.jpg",
            "filename": "image.jpg",
            "fits_file": "/path/to/file.fits",
            "review_status": "good|bad|undecided",
            "target": "M31",
            "filter": "L",
            "exposure_sec": 180,
            "gain": 120,
            "temperature_c": -10.0,
            "session_date": "2025-01-19",
            "timestamp": "2025-01-19T23:45:30",
            "camera": "ZWO ASI294MC Pro"
        },
        ...
    ]
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Query all frames with their metadata, ordered by timestamp
        cursor.execute('''
            SELECT 
                jr.id,
                jr.jpeg_file,
                jr.review_status,
                ff.destination_file as fits_file,
                ff.target,
                ff.filter,
                ff.exposure_sec,
                ff.gain,
                ff.temperature_c,
                ff.session_date,
                ff.timestamp,
                ff.id as fits_file_id
            FROM fits_jpeg_review jr
            JOIN fits_frames ff ON jr.fits_file_id = ff.id
            ORDER BY ff.timestamp
        ''')
        
        frames = []
        for row in cursor.fetchall():
            # Extract just the filename from full path for client
            jpeg_path = Path(row['jpeg_file'])
            
            # Map database terminology to UI terminology
            db_status = row['review_status'] or 'unverified'
            status_mapping = {
                'selected': 'good',
                'discarded': 'bad',
                'undecided': 'undecided',
                'unverified': 'unverified'
            }
            ui_status = status_mapping.get(db_status, 'unverified')
            
            # Get camera info from metadata table (check INSTRUME, CAMERA, TELESCOP)
            camera = None
            for key in ['INSTRUME', 'CAMERA', 'TELESCOP']:
                cursor.execute('''
                    SELECT value_text 
                    FROM fits_metadata 
                    WHERE fits_file_id = ? AND metadata_key = ?
                ''', (row['fits_file_id'], key))
                result = cursor.fetchone()
                if result and result['value_text']:
                    camera = result['value_text']
                    break
            
            frames.append({
                'id': row['id'],
                'jpeg_file': row['jpeg_file'],
                'filename': jpeg_path.name,
                'fits_file': row['fits_file'],
                'review_status': ui_status,
                'target': row['target'],
                'filter': row['filter'],
                'exposure_sec': row['exposure_sec'],
                'gain': row['gain'],
                'temperature_c': row['temperature_c'],
                'session_date': row['session_date'],
                'timestamp': row['timestamp'],
                'camera': camera
            })
        
        conn.close()
        return jsonify(frames)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/frames/<int:frame_id>/review', methods=['POST'])
def update_review_status(frame_id):
    """
    Update review status for a frame
    
    Request body: {"status": "good|bad|undecided|unverified"}
    Maps to database: good->selected, bad->discarded
    """
    try:
        data = request.get_json()
        status = data.get('status')
        
        if status not in ['good', 'bad', 'undecided', 'unverified']:
            return jsonify({'error': 'Invalid status. Must be: good, bad, undecided, or unverified'}), 400
        
        # Map UI terminology to database terminology
        status_mapping = {
            'good': 'selected',
            'bad': 'discarded',
            'undecided': 'undecided',
            'unverified': 'unverified'
        }
        db_status = status_mapping[status]
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE fits_jpeg_review 
            SET review_status = ?
            WHERE id = ?
        ''', (db_status, frame_id))
        
        conn.commit()
        
        if cursor.rowcount == 0:
            conn.close()
            return jsonify({'error': 'Frame not found'}), 404
        
        conn.close()
        return jsonify({'success': True, 'id': frame_id, 'status': status})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/images/<path:filename>')
def serve_image(filename):
    """
    Serve JPEG images from database paths
    
    Looks up the full path in database and serves the file
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Find the full path for this filename
        cursor.execute('''
            SELECT jpeg_file 
            FROM fits_jpeg_review 
            WHERE jpeg_file LIKE ?
        ''', (f'%{filename}',))
        
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return "Image not found in database", 404
        
        jpeg_path = Path(row['jpeg_file'])
        
        if not jpeg_path.exists():
            return f"Image file not found: {jpeg_path}", 404
        
        return send_file(str(jpeg_path), mimetype='image/jpeg')
        
    except Exception as e:
        return str(e), 500


@app.route('/api/stats')
def get_stats():
    """
    Get review statistics
    
    Returns counts of good/bad/undecided frames
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                review_status,
                COUNT(*) as count
            FROM fits_jpeg_review
            GROUP BY review_status
        ''')
        
        stats = {
            'good': 0,
            'bad': 0,
            'undecided': 0,
            'total': 0
        }
        
        for row in cursor.fetchall():
            status = row['review_status'] or 'undecided'
            stats[status] = row['count']
            stats['total'] += row['count']
        
        conn.close()
        return jsonify(stats)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def main():
    global DB_PATH, BASE_DIR
    
    parser = argparse.ArgumentParser(
        description='FITS Frame Reviewer - Web server for reviewing frames',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Starts a Flask web server for reviewing FITS frames.

The server:
  • Serves the review HTML interface
  • Provides REST API for frame data and status updates
  • Serves JPEG preview images
  • Updates review_status in fits_jpeg_review table

Usage:
  # Start server on default port 5000
  %(prog)s ~/astrophotography/data/raw/asi294mc
  
  # Access from browser
  http://localhost:5000
  
  # Or from another device on network
  http://192.168.1.100:5000
  
  # Or via WireGuard VPN
  http://10.0.0.5:5000

The base directory should contain astrophotography.db created by fits_organizer.py
'''
    )
    
    parser.add_argument('base_dir', 
                       help='Base directory containing astrophotography.db')
    parser.add_argument('--port', type=int, default=5000,
                       help='Port to run server on (default: 5000)')
    parser.add_argument('--host', default='0.0.0.0',
                       help='Host to bind to (default: 0.0.0.0 for all interfaces)')
    
    args = parser.parse_args()
    
    # Validate base directory
    BASE_DIR = Path(args.base_dir).resolve()
    if not BASE_DIR.exists():
        print(f"Error: Base directory does not exist: {BASE_DIR}")
        sys.exit(1)
    
    # Find database
    DB_PATH = BASE_DIR / 'astrophotography.db'
    if not DB_PATH.exists():
        print(f"Error: Database not found at {DB_PATH}")
        print("Run fits_organizer.py first to create the database")
        sys.exit(1)
    
    print("=" * 60)
    print("FITS Frame Reviewer Server")
    print("=" * 60)
    print(f"Base directory: {BASE_DIR}")
    print(f"Database: {DB_PATH}")
    print(f"Server: http://{args.host}:{args.port}")
    print("=" * 60)
    print("\nPress Ctrl+C to stop server\n")
    
    # Run Flask server
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == '__main__':
    main()
