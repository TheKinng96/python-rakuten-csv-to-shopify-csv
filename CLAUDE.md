# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Rakuten-to-Shopify CSV conversion system that transforms large Rakuten product exports (≈500MB) into Shopify-ready product CSV files. The project handles product data migration, image processing, metafield mapping, and category management.

## Architecture

### Core Components

- **`manual/final/main.py`** - Main conversion script that processes Rakuten CSV exports and generates Shopify-compatible CSV files
- **`manual/constants/`** - Shared constants and mappings for Rakuten and Shopify field definitions
- **`manual/csv_utils/`** - Utility scripts for CSV manipulation (splitting, merging, column removal, row counting)
- **`manual/analysis/`** - Analysis scripts for metafields, SKU validation, and data quality checks
- **`manual/app/`** - GUI application for processing with PySimpleGUI interface

### Data Flow

1. **Input**: Rakuten CSV exports (`rakuten_item.csv`, `rakuten_collection.csv`)
2. **Processing**: SKU grouping, variant handling, image URL processing, metafield mapping
3. **Output**: Shopify CSV files with cleaned HTML, mapped metafields, and proper product structure

### Key Business Logic

- **SKU Handling**: Main products have no suffix, variants have suffixes (-3s, -6s, -t)
- **Variant Grouping**: Products grouped by SKU management number with common name merging
- **Image Processing**: Up to 20 images per product with automatic URL construction
- **Metafield Mapping**: Configurable attribute mapping via `mapping_meta.json`
- **Category Filtering**: Exclusion list for unwanted category classifications

## Development Commands

### Setup Environment
```bash
# Navigate to the main processing directory
cd manual/final

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install pandas beautifulsoup4 lxml
```

### Running the Main Conversion
```bash
# From manual/final directory
python main.py
```

### Running Utility Scripts
```bash
# CSV utilities (from manual/csv_utils/)
python split_csv_by_size.py
python merge_rakuten_files.py
python count_csv_rows.py

# Analysis scripts (from manual/analysis/)
python analyze_metafields.py
python check_mismatched_items.py
```

### GUI Application
```bash
# From manual/app directory
pip install -r requirements.txt
python app.py
```

## File Structure Conventions

### Input Data Structure
```
manual/final/data/
├── rakuten_item.csv           # Main product data
├── rakuten_collection.csv     # Category/collection data
└── mapping_meta.json          # Attribute mapping configuration
```

### Output Structure
```
manual/final/output/
├── shopify_products.csv                    # Main cleaned output
├── shopify_products_original_html.csv      # Output with original HTML
├── body_html_comparison.csv                # HTML cleaning comparison log
└── rejected_rows.csv                       # Filtered/rejected data log
```

## Configuration Files

- **`mapping_meta.json`** - Maps Rakuten attribute keys to Shopify metafield columns
- **`SPECIAL_TAGS`** - Handles special tag mappings for parallel imports and defective items
- **`CATEGORY_EXCLUSION_LIST`** - Categories excluded from product type assignment

## Encoding and Data Handling

- **Input Encoding**: Shift-JIS (`cp932`) for Rakuten CSV files
- **Output Encoding**: UTF-8 for Shopify compatibility
- **Memory Management**: Chunked processing for large files (500MB+)
- **HTML Cleaning**: BeautifulSoup-based cleaning with custom business rules

## Testing Approach

No formal test framework is configured. Testing is done by:
1. Running conversion on sample data
2. Validating output CSV structure
3. Checking Shopify import preview
4. Verifying metafield mappings and category assignments