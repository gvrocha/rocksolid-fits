#!/usr/bin/env python3
"""
FITS File Organizer for Astrophotography v6
Organizes FITS files with proper hierarchy and timestamp-based naming
"""

import os
import shutil
from datetime import datetime, timedelta
import re
import math
import sys
from collections import defaultdict

try:
    from astropy.io import fits
    ASTROPY_AVAILABLE = True
except ImportError:
    ASTROPY_AVAILABLE = False
    print("Warning: astropy not available. Install with: pip install astropy")

# Import database module
try:
    from fits_database import ensure_database_schema, import_fits_frames, import_fits_headers_only, get_database_path
    FITS_DATABASE_AVAILABLE = True
except ImportError:
    print("Warning: fits_database.py not found. Database features will be disabled.")
    FITS_DATABASE_AVAILABLE = False

def sanitize_name(name):
    """Convert name to lowercase and replace spaces/special chars with underscores"""
    if name is None:
        return "unknown"
    name = str(name).lower()
    name = re.sub(r'[^a-z0-9._-]', '_', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    return name if name else "unknown"

def extract_timestamp(filepath, header=None):
    """Extract timestamp from FITS header or file"""
    if header:
        date_obs = header.get('DATE-OBS', header.get('DATE', None))
        if date_obs:
            try:
                # Parse ISO format timestamp
                date_obj = datetime.fromisoformat(date_obs.replace('Z', '+00:00'))
                # Format with milliseconds (first 3 digits of microseconds)
                milliseconds = date_obj.microsecond // 1000
                return f"{date_obj.strftime('%Y%m%d_%H%M%S')}_{milliseconds:03d}"
            except:
                pass
    
    # Fallback to file modification time
    try:
        mtime = os.path.getmtime(filepath)
        dt = datetime.fromtimestamp(mtime)
        milliseconds = dt.microsecond // 1000
        return f"{dt.strftime('%Y%m%d_%H%M%S')}_{milliseconds:03d}"
    except:
        dt = datetime.now()
        milliseconds = dt.microsecond // 1000
        return f"{dt.strftime('%Y%m%d_%H%M%S')}_{milliseconds:03d}"

def extract_metadata(filepath, tz_offset_hours=None):
    """
    Extract metadata from FITS file
    
    Args:
        filepath: Path to FITS file
        tz_offset_hours: Optional timezone offset in hours from UTC (e.g., -5 for EST, -6 for CST).
                        If None, will attempt to calculate from SITELON header, else assume UTC.
    """
    if not ASTROPY_AVAILABLE:
        return None
    
    try:
        with fits.open(filepath) as hdul:
            header = hdul[0].header
            
            # Frame type
            frame_type = header.get('FRAME', header.get('IMAGETYP', 'Unknown'))
            frame_type = sanitize_name(frame_type)
            
            # Exposure time
            exposure = header.get('EXPTIME', header.get('EXPOSURE', 0.0))
            try:
                exposure = float(exposure)
            except (ValueError, TypeError):
                exposure = 0.0
            
            # Gain
            gain = header.get('GAIN', 'unknown')
            gain = sanitize_name(str(gain))
            if not gain.startswith('gain'):
                gain = f'gain{gain}'
            
            # Filter
            filter_val = header.get('FILTER', None)
            if filter_val is not None:
                filter_str = sanitize_name(str(filter_val))
            else:
                filter_str = 'nofilter'  # Explicit marker for OSC camera or missing filter info
            
            # Temperature (round to nearest degree)
            temp = header.get('CCD-TEMP', header.get('SET-TEMP', None))
            temp_raw = None
            if temp is not None:
                try:
                    temp_raw = float(temp)
                    temp_rounded = round(temp_raw)
                    temp_str = f'{temp_rounded}c' if temp_rounded >= 0 else f'minus{abs(temp_rounded)}c'
                except (ValueError, TypeError):
                    temp_str = 'unknown_temp'
            else:
                temp_str = 'unknown_temp'
            
            # Target object
            target = header.get('OBJECT', header.get('OBJNAME', 'unknown'))
            # Fix Messier/NGC spacing before sanitization (M 31 -> M31, NGC 224 -> NGC224)
            target = re.sub(r'([MN])\s+(\d+)', r'\1\2', str(target), flags=re.IGNORECASE)
            target = sanitize_name(target)
            
            # Date observation for session grouping
            date_obs = header.get('DATE-OBS', header.get('DATE', None))
            if date_obs:
                try:
                    # DATE-OBS is in UTC, need to convert to local time for session grouping
                    date_obj = datetime.fromisoformat(date_obs.replace('Z', '+00:00'))
                    
                    # Determine timezone offset
                    calculated_tz_offset = None
                    
                    if tz_offset_hours is not None:
                        # Use provided timezone offset (preferred method)
                        calculated_tz_offset = tz_offset_hours
                    else:
                        # FALLBACK: Try to get longitude for astronomical timezone calculation
                        # NOTE: ASIAIR does not write SITELON/SITELONG to FITS headers,
                        # so this fallback is unlikely to work with ASIAIR files.
                        # Kept for compatibility with other software that may include location data.
                        site_lon = header.get('SITELON', header.get('SITELONG', header.get('LONG-OBS', None)))
                        
                        if site_lon is not None:
                            try:
                                lon = float(site_lon)
                                # Calculate timezone offset: floor(longitude/15)
                                # Positive longitude = East (ahead of UTC), negative = West (behind UTC)
                                calculated_tz_offset = math.floor(lon / 15.0)
                            except (ValueError, TypeError):
                                # If longitude is invalid, assume UTC (no conversion)
                                pass
                    
                    # Apply timezone offset if available
                    if calculated_tz_offset is not None:
                        date_obj = date_obj + timedelta(hours=calculated_tz_offset)
                    
                    # Midnight adjustment: if before noon local time, use previous day's session
                    if date_obj.hour < 12:
                        date_obj = date_obj - timedelta(days=1)
                    
                    session_date = date_obj.strftime('%Y%m%d')
                except:
                    session_date = 'unknown_date'
            else:
                # Try to extract from filename
                basename = os.path.basename(filepath)
                date_match = re.search(r'(\d{8})', basename)
                if date_match:
                    session_date = date_match.group(1)
                else:
                    session_date = 'unknown_date'
            
            # Get timestamp for filename
            timestamp = extract_timestamp(filepath, header)
            
            return {
                'frame_type': frame_type,
                'exposure': exposure,
                'gain': gain,
                'filter': filter_str,
                'temp': temp_str,
                'temp_raw': temp_raw,
                'target': target,
                'session_date': session_date,
                'timestamp': timestamp
            }
    except Exception as e:
        # Suppress console output - error will be counted in Pass 2
        return None

def format_exposure(exp_seconds):
    """Format exposure time for folder name"""
    if exp_seconds == 0:
        return '0s'
    elif exp_seconds < 1:
        return f'{int(exp_seconds * 1000)}ms'
    else:
        return f'{int(exp_seconds)}s'

def round_temp(temp):
    """Round temperature using floor(temp + 0.5) for consistent rounding"""
    return math.floor(temp + 0.5)

def format_temp_folder(temp_int):
    """Format a single temperature integer as folder name"""
    if temp_int >= 0:
        return f'{temp_int}c'
    else:
        return f'minus{abs(temp_int)}c'

def format_temp_range(min_temp, max_temp):
    """Format temperature range for folder name using floor/ceil"""
    min_int = math.floor(min_temp)
    max_int = math.ceil(max_temp)
    
    min_str = f'{min_int}c' if min_int >= 0 else f'minus{abs(min_int)}c'
    max_str = f'{max_int}c' if max_int >= 0 else f'minus{abs(max_int)}c'
    
    return f'{min_str}_to_{max_str}'

def determine_temp_folders(temps, is_calibration):
    """
    Determine temperature folder structure for a group of frames
    
    Returns: dict mapping each temp to its folder suffix
    
    For calibration frames: individual rounded temp folders
    For session frames: range-based folders with deviants if needed
    """
    if not temps:
        return {}
    
    # For calibration library: use individual rounded temps
    if is_calibration:
        temp_folders = {}
        for temp in temps:
            rounded = round_temp(temp)
            temp_folders[temp] = format_temp_folder(rounded)
        return temp_folders
    
    # For session frames: check range
    min_temp = min(temps)
    max_temp = max(temps)
    temp_range = max_temp - min_temp
    
    temp_folders = {}
    
    if temp_range <= 4.0:
        # All temps fit in one folder
        folder_name = format_temp_range(min_temp, max_temp)
        for temp in temps:
            temp_folders[temp] = folder_name
    else:
        # Find the 4°C window that contains the most frames
        # Try windows starting at each unique temp
        best_window_start = None
        best_count = 0
        
        for start_temp in sorted(set(temps)):
            count = sum(1 for t in temps if start_temp <= t <= start_temp + 4.0)
            if count > best_count:
                best_count = count
                best_window_start = start_temp
        
        window_end = best_window_start + 4.0
        
        # Main folder for temps in the window
        main_folder = format_temp_range(best_window_start, window_end)
        
        # Determine folder for each temp
        for temp in temps:
            if best_window_start <= temp <= window_end:
                temp_folders[temp] = main_folder
            elif temp < best_window_start:
                # Below range
                below_min = math.floor(best_window_start)
                below_str = f'{below_min}c' if below_min >= 0 else f'minus{abs(below_min)}c'
                temp_folders[temp] = os.path.join(main_folder, f'below_{below_str}')
            else:
                # Above range
                above_max = math.ceil(window_end)
                above_str = f'{above_max}c' if above_max >= 0 else f'minus{abs(above_max)}c'
                temp_folders[temp] = os.path.join(main_folder, f'above_{above_str}')
    
    return temp_folders

def get_output_path(metadata, output_base, use_calibration_library, temp_folder):
    """
    Determine output path based on frame type
    
    Calibration Library (darks/bias):
      calibration/darks/<gain>/<exposure>/<temp>/
      calibration/bias/<gain>/
    
    Session structure (lights/flats, or darks/bias if CalibLib=No):
      sessions/<date>/darks/<gain>/<exposure>/<filter?>/<temp_range>/
      sessions/<date>/bias/<gain>/<filter?>/
      sessions/<date>/flats/<gain>/<filter?>/
      sessions/<date>/<target>/<gain>/<exposure>/<filter?>/<temp_range>/
    
    Filter is optional - only added if present in metadata
    temp_folder: the temperature folder suffix (e.g., 'minus20c' or 'minus21c_to_minus18c')
    """
    frame_type = metadata['frame_type']
    filter_str = metadata.get('filter', None)
    
    is_dark = 'dark' in frame_type
    is_bias = 'bias' in frame_type
    is_flat = 'flat' in frame_type
    is_light = not (is_dark or is_bias or is_flat)
    
    if use_calibration_library and (is_dark or is_bias):
        # Calibration library: gain -> exposure -> temp (no filter)
        if is_dark:
            exp_str = format_exposure(metadata['exposure'])
            path = os.path.join(
                output_base,
                'calibration',
                'darks',
                metadata['gain'],
                exp_str,
                temp_folder
            )
        else:  # bias - no temperature folder
            path = os.path.join(
                output_base,
                'calibration',
                'bias',
                metadata['gain']
            )
    else:
        # Session-based structure - always include filter
        session_base = os.path.join(
            output_base,
            'sessions',
            metadata['session_date']
        )
        
        if is_dark:
            exp_str = format_exposure(metadata['exposure'])
            path = os.path.join(session_base, 'darks', metadata['gain'], exp_str, filter_str, temp_folder)
        elif is_bias:
            # Bias: no temperature folder
            path = os.path.join(session_base, 'bias', metadata['gain'], filter_str)
        elif is_flat:
            # Flats: no temperature folder
            path = os.path.join(session_base, 'flats', metadata['gain'], filter_str)
        else:  # lights
            exp_str = format_exposure(metadata['exposure'])
            path = os.path.join(session_base, metadata['target'], metadata['gain'], exp_str, filter_str, temp_folder)
    
    return path

def generate_filename(original_filepath, metadata, rename_files):
    """Generate output filename with timezone-adjusted timestamp suffix including milliseconds"""
    original_name = os.path.basename(original_filepath)
    name_without_ext = os.path.splitext(original_name)[0]
    ext = os.path.splitext(original_filepath)[1]
    
    # Use adjusted_timestamp if available, otherwise fall back to timestamp
    timestamp = metadata.get('adjusted_timestamp', metadata['timestamp'])
    
    # Get timezone offset for filename
    tz_offset = metadata.get('tz_offset_hours', None)
    if tz_offset is not None:
        # Format as UTC+/-X (e.g., UTC-6, UTC+0)
        tz_str = f"utc{tz_offset:+.0f}".replace('+', 'plus').replace('-', 'minus')
    else:
        tz_str = "utc"
    
    if rename_files:
        frame_type = metadata['frame_type']
        filter_str = metadata['filter']  # Always present now (either actual filter or 'nofilter')
        gain = metadata['gain']
        exp_str = format_exposure(metadata['exposure'])
        temp = metadata['temp']
        
        # Determine if this is a calibration frame
        is_dark = 'dark' in frame_type
        is_bias = 'bias' in frame_type
        is_flat = 'flat' in frame_type
        
        if is_bias:
            # Bias: frametype_timestamp_tz_gain (no filter, no temp, no exposure)
            base_name = f"{frame_type}_{timestamp}_{tz_str}_{gain}"
        elif is_flat:
            # Flats: frametype_timestamp_tz_filter_gain (no temp, exposure irrelevant)
            base_name = f"{frame_type}_{timestamp}_{tz_str}_{filter_str}_{gain}"
        elif is_dark:
            # Darks: frametype_timestamp_tz_gain_exposure_temp (no filter, no target)
            base_name = f"{frame_type}_{timestamp}_{tz_str}_{gain}_{exp_str}_{temp}"
        else:
            # Lights: frametype_timestamp_tz_target_filter_gain_exposure_temp
            target = metadata['target']
            base_name = f"{frame_type}_{timestamp}_{tz_str}_{target}_{filter_str}_{gain}_{exp_str}_{temp}"
        
        base_name = sanitize_name(base_name)
        filename = f"{base_name}{ext}"
    else:
        # Use original name (sanitized to lowercase) with timestamp and timezone suffix
        base_name = sanitize_name(name_without_ext)
        filename = f"{base_name}_{timestamp}_{tz_str}{ext}"
    
    return filename

def organize_fits_files(input_folder, output_folder, use_calibration_library=True, rename_files=False, tz_offset_hours=None):
    """
    Organize FITS files from input_folder into output_folder structure
    
    Args:
        input_folder: Source directory containing FITS files
        output_folder: Destination directory for organized files
        use_calibration_library: Create reusable calibration library structure
        rename_files: Rename files with metadata
        tz_offset_hours: Timezone offset from UTC (e.g., -5 for EST, -6 for CST).
                        If None, will try to extract from FITS headers or assume UTC.
    """
    if not ASTROPY_AVAILABLE:
        print("Cannot proceed without astropy. Please install it.")
        return
    
    # Find all FITS files (skip hidden files - dotfiles used by ASIAIR during capture)
    fits_files = []
    skipped_hidden = 0
    for root, dirs, files in os.walk(input_folder):
        # Skip hidden directories
        dirs[:] = [d for d in dirs if not d.startswith('.')]
        
        for file in files:
            # Skip hidden files (start with .)
            if file.startswith('.'):
                skipped_hidden += 1
                continue
            
            if file.lower().endswith(('.fit', '.fits', '.fts')):
                fits_files.append(os.path.join(root, file))
    
    if skipped_hidden > 0:
        print(f"\nSkipped {skipped_hidden} hidden files (temporary/incomplete captures)")
    
    if not fits_files:
        print(f"No FITS files found in {input_folder}")
        return
    
    print(f"\nFound {len(fits_files)} FITS files to organize...\n")
    print("Pass 1: Reading metadata from all files...\n")
    
    # Pass 1: Extract all metadata
    file_metadata = []
    for i, filepath in enumerate(fits_files, 1):
        if i % 50 == 0:
            print(f"  Reading file {i}/{len(fits_files)}...")
        
        metadata = extract_metadata(filepath, tz_offset_hours)
        if metadata is not None:
            file_metadata.append({
                'filepath': filepath,
                'metadata': metadata
            })
    
    print(f"Successfully read metadata from {len(file_metadata)} files\n")
    
    # Sort files by timestamp for chronological processing
    print("Sorting files by timestamp...\n")
    file_metadata.sort(key=lambda x: x['metadata']['timestamp'])
    
    # Group files by (session_date, target, frame_type, gain, exposure, filter)
    groups = defaultdict(list)
    for item in file_metadata:
        metadata = item['metadata']
        frame_type = metadata['frame_type']
        
        is_dark = 'dark' in frame_type
        is_bias = 'bias' in frame_type
        
        # Determine if this will go to calibration library
        is_calibration = use_calibration_library and (is_dark or is_bias)
        
        # Create group key
        if is_calibration:
            # Calibration library groups: frame_type, gain, exposure (no filter)
            if is_dark:
                group_key = ('calibration', frame_type, metadata['gain'], metadata['exposure'])
            else:  # bias
                group_key = ('calibration', frame_type, metadata['gain'], None)
        else:
            # Session groups: session_date, target, frame_type, gain, exposure, filter
            # Filter is always present now (either actual filter or 'nofilter')
            filter_str = metadata['filter']
            group_key = (metadata['session_date'], metadata['target'], frame_type, 
                        metadata['gain'], metadata['exposure'], filter_str)
        
        groups[group_key].append(item)
    
    print(f"Grouped files into {len(groups)} unique combinations\n")
    
    # Determine temperature folders for each group
    temp_folder_map = {}  # Maps (filepath) -> temp_folder_suffix
    
    for group_key, items in groups.items():
        # Extract temperatures (only valid ones)
        temps = [item['metadata']['temp_raw'] for item in items 
                if item['metadata']['temp_raw'] is not None]
        
        if not temps:
            # No valid temperatures, use 'unknown_temp' for all
            for item in items:
                temp_folder_map[item['filepath']] = 'unknown_temp'
            continue
        
        # Determine if this group uses calibration structure
        is_calibration = group_key[0] == 'calibration'
        
        # Get temperature folder assignments
        temp_folders = determine_temp_folders(temps, is_calibration)
        
        # Map each file to its temperature folder
        for item in items:
            temp_raw = item['metadata']['temp_raw']
            if temp_raw is not None:
                temp_folder_map[item['filepath']] = temp_folders.get(temp_raw, 'unknown_temp')
            else:
                temp_folder_map[item['filepath']] = 'unknown_temp'
    
    # Create output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # Create TSV log file
    timestamp_now = datetime.now()
    milliseconds = timestamp_now.microsecond // 1000
    tsv_timestamp = f"{timestamp_now.strftime('%Y%m%d_%H%M%S')}_{milliseconds:03d}"
    tsv_filename = f'organize_log_{tsv_timestamp}.tsv'
    tsv_path = os.path.join(output_folder, tsv_filename)
    
    print("Pass 2: Organizing files...\n")
    
    processed = 0
    skipped = 0
    errors = 0
    warnings = 0
    start_time = datetime.now()
    
    # Calculate progress interval (1% of total files, minimum 1)
    total_files = len(file_metadata)
    progress_interval = max(1, total_files // 100)
    
    # Flag to show early progress for large datasets
    show_early_progress = total_files > 1000
    early_progress_shown = False
    
    # Open TSV file for writing
    with open(tsv_path, 'w') as tsv_file:
        # Write header with metadata columns
        tsv_file.write('sequence_number\torigin_file\tdestination_file\taction\tframe_type\ttarget\tfilter\texposure_sec\tgain\ttemperature_c\ttemp_folder\ttimestamp\tsession_date\ttz_offset_hours\n')
        
        sequence_number = 0
        
        # Process files that had valid metadata
        for i, item in enumerate(file_metadata, 1):
            sequence_number += 1
            filepath = item['filepath']
            metadata = item['metadata']
            
            # Get temperature folder for this file
            temp_folder = temp_folder_map.get(filepath, 'unknown_temp')
            
            # Compute timezone-adjusted timestamp for filename
            adjusted_timestamp = metadata['timestamp']  # Default: no adjustment
            if tz_offset_hours is not None and metadata['timestamp']:
                timestamp_str = metadata['timestamp']
                if len(timestamp_str) >= 15:
                    try:
                        date_str = timestamp_str[:8]
                        time_str = timestamp_str[9:15]
                        milliseconds = timestamp_str[16:19] if len(timestamp_str) >= 19 else '000'
                        
                        dt = datetime.strptime(f"{date_str} {time_str}", "%Y%m%d %H%M%S")
                        dt = dt + timedelta(hours=tz_offset_hours)
                        
                        adjusted_timestamp = f"{dt.strftime('%Y%m%d_%H%M%S')}_{milliseconds}"
                    except:
                        pass  # Use original if computation fails
            
            # Add adjusted timestamp and tz_offset to metadata for filename generation
            metadata['adjusted_timestamp'] = adjusted_timestamp
            metadata['tz_offset_hours'] = tz_offset_hours
            
            # Determine output directory
            output_dir = get_output_path(metadata, output_folder, use_calibration_library, temp_folder)
            os.makedirs(output_dir, exist_ok=True)
            
            # Generate filename with timezone-adjusted timestamp
            new_filename = generate_filename(filepath, metadata, rename_files)
            output_path = os.path.join(output_dir, new_filename)
            
            # Prepare metadata fields for TSV
            frame_type = metadata['frame_type']
            # Target only for light frames
            target = metadata['target'] if 'light' in frame_type else ''
            filter_str = metadata['filter']  # Always present now
            exposure = metadata['exposure']
            gain = metadata['gain']
            
            # Temperature value for TSV
            temp_raw = metadata['temp_raw']
            if temp_raw is not None:
                temp_value = f'{temp_raw:.1f}'
            else:
                temp_value = ''
            
            timestamp_str = metadata['timestamp']
            
            # Compute session_date from timestamp with timezone offset
            session_date_str = ''
            if timestamp_str and len(timestamp_str) >= 15:
                try:
                    date_str = timestamp_str[:8]
                    time_str = timestamp_str[9:15]
                    dt = datetime.strptime(f"{date_str} {time_str}", "%Y%m%d %H%M%S")
                    
                    # Apply timezone offset if available
                    if tz_offset_hours is not None:
                        dt = dt + timedelta(hours=tz_offset_hours)
                    
                    # Apply noon-to-noon logic for session grouping
                    if dt.hour < 12:
                        dt = dt - timedelta(days=1)
                    
                    session_date_str = dt.strftime('%Y%m%d')
                except:
                    session_date_str = ''
            
            # Format tz_offset for TSV
            tz_offset_str = f'{tz_offset_hours}' if tz_offset_hours is not None else ''
            
            # Check if file already exists
            if os.path.exists(output_path):
                # Log the skip with destination and metadata
                tsv_file.write(f'{sequence_number}\t{filepath}\t{output_path}\tskipped_exists\t{frame_type}\t{target}\t{filter_str}\t{exposure}\t{gain}\t{temp_value}\t{temp_folder}\t{timestamp_str}\t{session_date_str}\t{tz_offset_str}\n')
                skipped += 1
                warnings += 1
                continue
            
            # Copy file
            try:
                shutil.copy2(filepath, output_path)
                
                # Log successful copy with metadata
                tsv_file.write(f'{sequence_number}\t{filepath}\t{output_path}\tcopied\t{frame_type}\t{target}\t{filter_str}\t{exposure}\t{gain}\t{temp_value}\t{temp_folder}\t{timestamp_str}\t{session_date_str}\t{tz_offset_str}\n')
                processed += 1
                
            except Exception as e:
                # Log the error with metadata (suppress console output)
                tsv_file.write(f'{sequence_number}\t{filepath}\t{output_path}\tskipped_error\t{frame_type}\t{target}\t{filter_str}\t{exposure}\t{gain}\t{temp_value}\t{temp_folder}\t{timestamp_str}\t{session_date_str}\t{tz_offset_str}\n')
                skipped += 1
                errors += 1
            
            # Show early progress after 10 files for large datasets
            if show_early_progress and i == 10 and not early_progress_shown:
                timestamp = datetime.now().strftime('%H:%M:%S')
                elapsed = (datetime.now() - start_time).total_seconds()
                avg_time_per_file = elapsed / i
                remaining_files = total_files - i
                eta_seconds = avg_time_per_file * remaining_files
                eta_time = datetime.now() + timedelta(seconds=eta_seconds)
                eta_str = eta_time.strftime('%Y/%m/%d %H:%M')
                percent_complete = (i / total_files) * 100
                print(f"[{timestamp}] {percent_complete:5.1f}% - Processed {i}/{total_files} files "
                      f"({processed} copied, {skipped} skipped) - ETA: {eta_str}")
                early_progress_shown = True
            
            # Show progress after processing at 1% intervals or last file
            if i % progress_interval == 0 or i == total_files:
                timestamp = datetime.now().strftime('%H:%M:%S')
                elapsed = (datetime.now() - start_time).total_seconds()
                
                # Calculate ETA
                if i > 0:
                    avg_time_per_file = elapsed / i
                    remaining_files = total_files - i
                    eta_seconds = avg_time_per_file * remaining_files
                    eta_time = datetime.now() + timedelta(seconds=eta_seconds)
                    eta_str = eta_time.strftime('%Y/%m/%d %H:%M')
                else:
                    eta_str = "calculating..."
                
                percent_complete = (i / total_files) * 100
                print(f"[{timestamp}] {percent_complete:5.1f}% - Processed {i}/{total_files} files "
                      f"({processed} copied, {skipped} skipped) - ETA: {eta_str}")
        
        # Process files that couldn't be read
        skipped_files = set(fits_files) - {item['filepath'] for item in file_metadata}
        for filepath in skipped_files:
            sequence_number += 1
            # Suppress console output - count as error
            tsv_file.write(f'{sequence_number}\t{filepath}\t\tskipped_unreadable\t\t\t\t\t\t\t\t\n')
            skipped += 1
            errors += 1
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total files found:     {len(fits_files)}")
    print(f"Successfully copied:   {processed} files")
    if warnings > 0:
        print(f"Warnings (skipped):    {warnings} files (already exist)")
    if errors > 0:
        print(f"Errors (failed):       {errors} files (unreadable or copy failed)")
    print(f"Total in destination:  {processed} files")
    print("=" * 60)
    print(f"Output location: {output_folder}")
    print(f"Log file: {tsv_path}")
    if errors > 0 or warnings > 0:
        print(f"\nNote: Check {tsv_filename} for details on skipped files")
    print("\nDone!")
    
    return tsv_path  # Return log file path for database import

def main_interactive():
    """Interactive mode - prompt user for inputs"""
    print("=" * 60)
    print("FITS File Organizer for Astrophotography v6")
    print("=" * 60)
    print()
    
    if not ASTROPY_AVAILABLE:
        print("ERROR: astropy is required but not installed")
        print("Install it with: pip install astropy")
        return
    
    try:
        print("Note: GUI not available, using text input\n")
        
        input_folder = input("Enter input folder path (where FITS files are): ").strip()
        output_folder = input("Enter output folder path (where to organize files): ").strip()
        
        if not os.path.exists(input_folder):
            print(f"Error: Input folder '{input_folder}' does not exist")
            return
        
        print(f"\nInput:  {input_folder}")
        print(f"Output: {output_folder}\n")
        
        print("Use CalibrationLibrary structure for darks/bias?")
        print("  Yes: Darks and Bias go to reusable calibration/ folder")
        print("  No:  All files stay within session-specific folders")
        use_calib = input("Use CalibrationLibrary? [Y/n]: ").strip().lower()
        use_calibration_library = use_calib != 'n'
        print()
        
        print("Rename files to standardized format?")
        print("  Yes: Files renamed with relevant metadata for each type:")
        print("       Lights: frametype_timestamp_target_filter_gain_exposure_temp.fit")
        print("       Darks:  frametype_timestamp_gain_exposure_temp.fit")
        print("       Flats:  frametype_timestamp_filter_gain.fit")
        print("       Bias:   frametype_timestamp_gain.fit")
        print("  No:  Keep original names (lowercase) with _timestamp.fit suffix")
        rename = input("Rename files? [Y/n]: ").strip().lower()
        rename_files = rename != 'n'
        print()
        
        print("Timezone offset from UTC for session grouping:")
        print("  (e.g., -5 for US Eastern, -6 for Central, -7 for Mountain, -8 for Pacific)")
        print("  Leave empty to use UTC (no conversion)")
        tz_input = input("UTC offset in hours [blank for UTC]: ").strip()
        tz_offset = None
        if tz_input:
            try:
                tz_offset = float(tz_input)
                print(f"Using UTC{tz_offset:+.0f} for session grouping")
            except ValueError:
                print("Invalid offset, using UTC")
        else:
            print("Using UTC for session grouping")
        print()
        
        print("Starting organization...\n")
        
        organize_fits_files(input_folder, output_folder, use_calibration_library, rename_files, tz_offset)
        
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
    except Exception as e:
        print(f"\nError: {e}")

def main_cli():
    """CLI mode - parse command-line arguments"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='FITS File Organizer for Astrophotography',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Organizes astrophotography FITS files into a structured hierarchy based on 
metadata extracted from file headers. Designed for deep-sky imaging workflows 
with temperature-controlled cameras.

Session Grouping and Timezones:
  ASIAIR writes DATE-OBS in UTC/GMT. For proper session grouping (noon-to-noon),
  use --tz-offset to specify your local timezone. For example:
    US Eastern:   --tz-offset -5  (EST, no DST)
    US Central:   --tz-offset -6  (CST, no DST)
    US Mountain:  --tz-offset -7  (MST, no DST)
    US Pacific:   --tz-offset -8  (PST, no DST)
  
  Astronomical timezones: Use floor(longitude/15) to avoid DST complications.
  For Fishers, IN (longitude -86.0°): floor(-86/15) = -6 hours (CST)

Key Features:
  • Smart temperature grouping: Frames within 4°C grouped together, outliers 
    separated into deviant subfolders
  • Filter support: Organizes by filter (Ha, OIII, SII, LRGB, etc.) when present
  • Calibration library: Reusable darks/bias separate from session-specific frames
  • Collision-proof: Millisecond-precision timestamps prevent filename conflicts
  • Audit trail: TSV log tracks all file operations with full metadata

Directory Structure:
  Sessions: sessions/<date>/<target>/<gain>/<exposure>/<filter?>/<temp_range>/
  Calibration: calibration/darks/<gain>/<exposure>/<temp_rounded>/
  
  Temperature folders use floor/ceil ranges (e.g., minus21c_to_minus18c) for 
  session frames. Outliers beyond 4°C tolerance go to above_*/below_* subfolders.
  
  Gain prioritized over exposure in hierarchy to facilitate matching lights with 
  flats (which require gain-matching, not exposure-matching). Filter included 
  when present in FITS headers.
  
  Note: For multiple cameras, organize at a higher level (e.g., create separate 
  output folders like /organized/asi294mc_askar/ and /organized/asi294mc_c9/). 
  Calibration frames are camera-specific and should not be mixed between cameras.

Examples:
  Interactive mode:
    %(prog)s
  
  CLI mode - basic organization (US Central timezone, files renamed by default):
    %(prog)s /raw/data /organized/asi294mc_pro --tz-offset -6
  
  CLI mode - keep original filenames:
    %(prog)s /raw/data /organized/asi294mc_pro --tz-offset -6 --no-rename
  
  CLI mode - session-only, no calibration library:
    %(prog)s /raw/data /organized/asi294mc_pro --tz-offset -6 --no-calib-library
  
  CLI mode - for Brazil imaging trip (São Paulo/Brasília):
    %(prog)s /raw/data /organized/asi294mc_pro --tz-offset -3
'''
    )
    parser.add_argument('input_folder', help='Input folder path (where FITS files are)')
    parser.add_argument('output_folder', help='Output folder path (where to organize files)')
    parser.add_argument('--tz-offset', type=float, required=True,
                       help='Timezone offset from UTC in hours for session grouping (e.g., -6 for US Central). REQUIRED.')
    parser.add_argument('--no-calib-library', action='store_true',
                       help='Do not use calibration library structure (default: use calibration library)')
    parser.add_argument('--no-rename', action='store_true',
                       help='Keep original filenames (default: rename to standardized format)')
    parser.add_argument('--skip-db', action='store_true',
                       help='Skip database import (default: import to SQLite database)')
    
    args = parser.parse_args()
    
    if not ASTROPY_AVAILABLE:
        print("ERROR: astropy is required but not installed")
        print("Install it with: pip install astropy")
        sys.exit(1)
    
    if not os.path.exists(args.input_folder):
        print(f"Error: Input folder '{args.input_folder}' does not exist")
        sys.exit(1)
    
    print("=" * 60)
    print("FITS File Organizer for Astrophotography v6")
    print("=" * 60)
    print()
    print(f"Input:  {args.input_folder}")
    print(f"Output: {args.output_folder}")
    print(f"Calibration Library: {not args.no_calib_library}")
    print(f"Rename Files: {not args.no_rename}")
    print(f"Timezone Offset: UTC{args.tz_offset:+.0f}")
    print(f"Database: {'Disabled' if args.skip_db else 'Enabled'}")
    print()
    
    log_file = organize_fits_files(
        args.input_folder,
        args.output_folder,
        use_calibration_library=not args.no_calib_library,
        rename_files=not args.no_rename,
        tz_offset_hours=args.tz_offset
    )
    
    # Import to database unless skipped
    if not args.skip_db and FITS_DATABASE_AVAILABLE and log_file:
        print()
        print("=" * 60)
        print("Importing to database...")
        print("=" * 60)
        db_path = get_database_path(args.output_folder)
        ensure_database_schema(db_path)
        import_fits_frames(log_file, db_path, tz_offset_hours=args.tz_offset)
        print()
        print("Importing FITS headers...")
        import_fits_headers_only(log_file, db_path)
        print(f"Database: {db_path}")
        print("\nNote: Run fits_metadata_extractor_from_log.py to compute image statistics")
    elif not args.skip_db and not FITS_DATABASE_AVAILABLE:
        print("\nWarning: Database import skipped (fits_database.py not found)")
    elif args.skip_db:
        print("\nDatabase import skipped (--skip-db flag)")


if __name__ == '__main__':
    # Detect mode based on arguments
    if len(sys.argv) > 1:
        main_cli()
    else:
        main_interactive()
