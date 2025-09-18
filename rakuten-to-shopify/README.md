# Rakuten to Shopify CSV Converter

A comprehensive pipeline that transforms Rakuten product exports into Shopify-ready CSV files with all production fixes applied in a single processing run.

## Quick Start

### Installation with uv (Recommended)

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install the project and dependencies
uv sync

# Run the converter
uv run python convert.py data/rakuten_item.csv
```

### Alternative Installation with pip

```bash
pip install -e .
# or
pip install pandas beautifulsoup4 lxml pydantic tqdm click
```

### Basic Usage

```bash
# Auto-detect file in data/input/ (recommended)
uv run python convert.py

# Clean output mode - CSV in → CSV out (no verbose logs)
uv run python convert.py --quiet

# Specify input file explicitly
uv run python convert.py data/rakuten_item.csv

# With make commands
make run-auto                              # Auto-detect
make run-quiet                             # Clean output mode
make run INPUT_FILE=data/rakuten_item.csv  # Specify file
```

### Step 1: Prepare Your Data
```bash
# Place your Rakuten file in data/input/ (auto-detection will find it)
cp your_rakuten_item.csv data/input/

# Or place it anywhere and specify the path
cp your_rakuten_item.csv data/

# Optional: Custom mapping configuration
cp your_mapping_meta.json data/
```

### Auto-Detection Feature

The converter automatically looks for input files in `data/input/` directory:

1. **rakuten_item.csv** (exact match)
2. ***rakuten_item*.csv** (pattern match)
3. ***item*.csv** (broader pattern)
4. ****.csv** (any CSV file, newest first)

```bash
# Just run without specifying a file!
uv run python convert.py
# Looking for input files in: /path/to/project/data/input
# Auto-detected input file: /path/to/project/data/input/rakuten_item.csv
```

### Output Modes

**Clean Mode (Recommended):**
```bash
uv run python convert.py --quiet
```
- Shows only essential transformation steps
- Clean "CSV in → CSV out" view
- Perfect for production use
- All detailed logs still saved to file

**Verbose Mode (Default):**
```bash
uv run python convert.py
```
- Shows detailed step-by-step processing
- Full logging to console and file
- Useful for debugging and development

### Step 3: Check Results
```bash
# Review conversion report
cat output/reports/conversion_summary.json

# Check for any issues
cat output/logs/conversion.log
```

## ⚠️ IMPORTANT: Description Images

**The generated CSV references Shopify CDN URLs for description images that don't exist yet!**

### Option A: Download & Upload Images (Recommended)
```bash
# Download description images
cd extras/
python download_description_images.py \
  --manifest ../output/reports/description_images.json

# Then manually upload to Shopify Admin → Settings → Files
# See extras/README.md for detailed instructions
```

### Option B: Keep Rakuten URLs (Testing Only)
```bash
# Use this flag to keep original Rakuten image URLs
python src/main.py --keep-rakuten-images ...
```

## What This Converter Does

### ✅ Included in Main Process
- **Data Conversion**: Rakuten CSV → Shopify format with proper encoding
- **HTML Processing**:
  - Fix tables for mobile responsiveness
  - Scope CSS to prevent theme conflicts
  - Remove Rakuten EC-UP marketing content
  - Normalize font weights in tables
- **Image Processing**: Fix gold URL patterns in descriptions
- **Tax Compliance**: Japanese 8%/10% tax rate classification
- **Metafield Mapping**: Complete attribute mapping (50+ fields)
- **Variant Handling**: SKU grouping, sorting, and positioning
- **Product Types**: Intelligent type assignment
- **Quality Validation**: Comprehensive checks and reports

### ⚠️ Separate Process Required
- **Description Images**: Download from Rakuten + Upload to Shopify
- See `extras/` folder for image handling tools

## Directory Structure
```
rakuten-to-shopify/
├── src/                    # Main source code
├── data/                   # Input files and configurations
├── output/                 # Generated files and reports
├── extras/                 # Description image tools
├── images/                 # Downloaded images (git-ignored)
├── scripts/                # Utility scripts
└── docs/                   # Documentation
```

## Key Features

### Production-Tested Fixes
This converter includes ALL fixes discovered during production deployment:
- Mobile-responsive HTML tables
- CSS scoping to prevent theme conflicts
- Rakuten marketing content removal
- Gold image URL pattern corrections
- Japanese tax rate compliance
- Font weight normalizations

### Intelligent Classification
- **Tax Rates**: Keyword-based 8%/10% classification
- **Product Types**: Category-based type assignment
- **Tags**: Smart tag generation from attributes

### Complete Validation
- Input file structure validation
- Output format compliance checks
- Quality assurance reporting
- Missing data identification

## Configuration

Main settings in `config.json`:
```json
{
  "encoding": {
    "input": "shift-jis",
    "output": "utf-8"
  },
  "cdn_base_url": "https://cdn.shopify.com/s/files/1/0637/6059/7127/files/",
  "scope_class": "shopify-product-description",
  "default_tax_rate": "10%"
}
```

## Documentation
- `REQUIREMENTS.md` - Complete technical requirements
- `docs/TROUBLESHOOTING.md` - Common issues and solutions
- `docs/API_FIXES_HISTORY.md` - History of production fixes
- `extras/README.md` - Description image handling

## Support
For issues or questions, check the documentation in the `docs/` folder first. This converter is based on real production experience and handles all known edge cases.