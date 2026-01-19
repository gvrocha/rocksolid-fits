"""
FITS Metadata Extraction Utilities
Shared functions for extracting metadata and statistics from FITS files
"""

import os
import numpy as np

try:
    from astropy.io import fits
    ASTROPY_AVAILABLE = True
except ImportError:
    ASTROPY_AVAILABLE = False


def extract_target_from_path(filepath):
    """
    Extract target name from organized filepath structure
    
    Assumes organized structure:
    sessions/<date>/<target>/<gain>/<exposure>/<filter?>/<temp>/
    
    Returns target name or empty string for calibration frames
    """
    # Normalize path separators
    normalized_path = filepath.replace('\\', '/')
    
    # Split into parts
    parts = normalized_path.split('/')
    
    # Look for 'sessions' in path
    try:
        sessions_idx = parts.index('sessions')
        # Target should be 2 positions after 'sessions': sessions/<date>/<target>
        if sessions_idx + 2 < len(parts):
            target = parts[sessions_idx + 2]
            return target
    except (ValueError, IndexError):
        pass
    
    # Not a sessions path (calibration or unorganized)
    return ''


def extract_fits_metadata(filepath):
    """
    Extract all header metadata and image statistics from a FITS file
    
    Returns dict with:
    - All FITS header keywords
    - Image statistics: mean, median, min, max, percentiles, std
    - Saturation counts
    - Target extracted from filepath
    
    On failure, returns dict with filepath and empty/NA values for other fields
    """
    if not ASTROPY_AVAILABLE:
        return _create_na_metadata(filepath, "astropy not available")
    
    try:
        with fits.open(filepath) as hdul:
            header = hdul[0].header
            data = hdul[0].data
            
            if data is None:
                return _create_na_metadata(filepath, "no image data")
            
            # Convert all header items to dict
            metadata = {}
            for key, value in header.items():
                # Handle multi-line comments and special characters
                if key == 'COMMENT' or key == 'HISTORY':
                    continue  # Skip these to avoid clutter
                metadata[key] = value
            
            # Get MAXADU explicitly (or mark as missing)
            maxadu_value = header.get('MAXADU', None)
            
            # Determine saturation threshold for counting saturated pixels
            if maxadu_value is not None:
                saturation_threshold = float(maxadu_value)
            elif data.dtype == np.uint16:
                saturation_threshold = 65535
            else:
                saturation_threshold = np.iinfo(data.dtype).max if np.issubdtype(data.dtype, np.integer) else np.finfo(data.dtype).max
            
            # Calculate percentiles every 5th (5, 10, 15, ..., 95)
            # Optimized: calculate all percentiles in one call (sorts data only once)
            percentile_list = list(range(5, 100, 5))
            percentile_values = np.percentile(data, percentile_list)
            
            percentiles = {}
            for i, p in enumerate(percentile_list):
                percentiles[f'percentile_{p:02d}'] = float(percentile_values[i])
            
            # Calculate image statistics
            stats = {
                'filepath': str(filepath),
                'target_from_path': extract_target_from_path(str(filepath)),
                'mean': float(np.mean(data)),
                'median': float(np.median(data)),
                'min': float(np.min(data)),
                'max': float(np.max(data)),
                'std': float(np.std(data)),
                'maxadu': maxadu_value if maxadu_value is not None else '',
                'saturation_threshold_used': saturation_threshold,
                'pixels_saturated_low': int(np.sum(data == np.min(data))),
                'pixels_saturated_high': int(np.sum(data >= saturation_threshold)),
                'total_pixels': int(data.size)
            }
            
            # Combine metadata, stats, and percentiles
            result = {**metadata, **stats, **percentiles}
            
            return result
            
    except Exception as e:
        return _create_na_metadata(filepath, str(e))


def _create_na_metadata(filepath, error_message=""):
    """
    Create a metadata dict with NA/empty values for failed extraction
    """
    # Basic structure with filepath and NAs
    na_metadata = {
        'filepath': str(filepath),
        'target_from_path': '',
        'mean': np.nan,
        'median': np.nan,
        'min': np.nan,
        'max': np.nan,
        'std': np.nan,
        'maxadu': '',
        'saturation_threshold_used': np.nan,
        'pixels_saturated_low': 0,
        'pixels_saturated_high': 0,
        'total_pixels': 0,
        'extraction_error': error_message
    }
    
    # Add NA percentiles
    for p in range(5, 100, 5):
        na_metadata[f'percentile_{p:02d}'] = np.nan
    
    return na_metadata


def get_stats_column_names():
    """
    Return ordered list of statistics column names
    Used for consistent column ordering across scripts
    """
    stats_keys = ['filepath', 'target_from_path', 'mean', 'median', 'min', 'max', 'std',
                  'maxadu', 'saturation_threshold_used',
                  'pixels_saturated_low', 'pixels_saturated_high', 'total_pixels']
    
    percentile_keys = [f'percentile_{p:02d}' for p in range(5, 100, 5)]
    
    return stats_keys + percentile_keys
