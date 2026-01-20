# RockSolid FITS Organizer

A Python tool for organizing astrophotography FITS files into an intelligent, structured hierarchy based on metadata extracted from file headers.

## Features

- **Smart Temperature Grouping**: Automatically groups frames within 4°C tolerance, separating temperature outliers into dedicated subfolders
- **Filter Support**: Organizes by optical filter (Ha, OIII, SII, LRGB, etc.) when present in FITS headers
- **Calibration Library**: Separates reusable calibration frames (darks/bias) from session-specific data
- **Collision-Proof Timestamps**: Millisecond-precision timestamps prevent filename conflicts during rapid exposures (flats, bias)
- **Chronological Processing**: Files are organized in capture time order
- **Comprehensive Logging**: TSV log tracks all operations with full metadata for audit trails

## Directory Structure

The organizer creates a hierarchical structure optimized for astrophotography workflows:
```
organized/
├── calibration/
│   ├── darks/
│   │   └── <gain>/
│   │       └── <exposure>/
│   │           └── <temp_rounded>/
│   └── bias/
│       └── <gain>/
└── sessions/
    └── <date>/
        ├── <target>/
        │   └── <gain>/
        │       └── <exposure>/
        │           └── <filter?>/
        │               └── <temp_range>/
        ├── flats/
        │   └── <gain>/
        │       └── <filter?>/
        ├── darks/ (if not using calibration library)
        └── bias/ (if not using calibration library)
```

### Design Principles

- **Gain before exposure**: Facilitates matching light frames with flats (which require gain-matching, not exposure-matching)
- **Temperature ranges**: Session frames use floor/ceil ranges (e.g., `minus21c_to_minus18c`). Frames beyond 4°C tolerance are placed in `above_<temp>/` or `below_<temp>/` subfolders
- **Calibration library**: Darks use individual rounded temperature folders for maximum reusability. Bias frames omit temperature (read noise is temperature-independent)
- **Flats without temperature**: Flat frames don't include temperature folders since sensor response to uniform illumination is temperature-independent

### Multiple Cameras

For setups with multiple cameras, create separate output folders per camera:
```
organized/
├── asi294mc_askar/
│   ├── calibration/
│   └── sessions/
├── asi294mc_c9/
│   ├── calibration/
│   └── sessions/
└── canon_r6/
    └── sessions/
```

Calibration frames are camera-specific and should never be mixed between different camera models or instances.

## Installation

### Requirements

- Python 3.7 or higher
- astropy

### Setup
```bash
# Clone the repository
git clone https://github.com/yourusername/rocksolid-fits.git
cd rocksolid-fits

# Create virtual environment (recommended)
python3 -m venv fits_env
source fits_env/bin/activate  # On Windows: fits_env\Scripts\activate

# Install dependencies
pip install astropy
```

## Usage

### Interactive Mode

Run without arguments for interactive prompts:
```bash
python fits_organizer.py
```

### Command-Line Mode
```bash
# Basic organization
python fits_organizer.py /path/to/raw /path/to/organized/camera_name

# With file renaming
python fits_organizer.py /path/to/raw /path/to/organized/camera_name --rename

# Session-only (no calibration library)
python fits_organizer.py /path/to/raw /path/to/organized/camera_name --no-calib-library

# Combine options
python fits_organizer.py /path/to/raw /path/to/organized/camera_name --rename --no-calib-library
```

### Shell Script (Linux/macOS)

For convenience with virtual environments:
```bash
chmod +x run_fits_organizer.sh
./run_fits_organizer.sh /path/to/raw /path/to/organized/camera_name
```

### Options

- `--rename`: Rename files to standardized format: `frametype_target_filter_exposure_gain_temp_timestamp_ms.fit`
- `--no-calib-library`: Keep all frames in session-specific folders (don't create reusable calibration library)

## Output

### Organized Files

Files are copied (not moved) to the organized structure with timestamp suffixes to prevent collisions:
```
light_20250114_153045_234.fit
bias_20250114_153046_456.fit
flat_20250114_153047_789.fit
```

When using `--rename`, filenames include relevant metadata for each frame type:
```
light_20250114_153045_234_m31_ha_gain120_180s_minus20c.fit
dark_20250114_153046_456_gain120_180s_minus20c.fit
flat_20250114_153047_789_ha_gain120.fit
bias_20250114_153048_012_gain120.fit
```

### TSV Log

Each run generates a timestamped TSV log in the output folder:
```
organize_log_20250114_153045_123.tsv
```

The log includes:
- Sequence number
- Origin filepath
- Destination filepath
- Action taken (copied, skipped_exists, skipped_error, skipped_unreadable)
- Frame type, target, filter, exposure, gain, temperature
- Temperature folder assignment
- Timestamp

## Tested Configurations

- ZWO ASI294MC Pro with ASIAIR Plus

Should work with any camera that writes standard FITS headers with DATE-OBS, GAIN, CCD-TEMP, OBJECT, and IMAGETYP/FRAME keywords.

## License

GNU General Public License v3.0 (GPL-3.0)

This software is free and open source. Derivative works must also be open source under GPL-3.0. For alternative licensing arrangements (e.g., commercial use, proprietary derivatives), please contact the author.

## Author

Built by Guilherme Veiga da Rocha for the astrophotography community.

## Acknowledgments

Developed through collaborative design discussions focusing on practical workflow optimization for amateur deep-sky imaging with temperature-controlled cameras.
