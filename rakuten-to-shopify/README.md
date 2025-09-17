# Rakuten to Shopify CSV Converter

A comprehensive solution that converts Rakuten product data to Shopify-ready CSV format, incorporating all production fixes and requirements discovered during real-world deployment.

## Quick Start

### Step 1: Prepare Your Data
```bash
# Place your Rakuten files in data/input/
cp your_rakuten_item.csv data/input/
cp your_rakuten_collection.csv data/input/
cp your_mapping_meta.json data/mappings/

# Place tax master files in data/tax_master/
cp 商品マスタ_20250912.csv data/tax_master/
```

### Step 2: Run Main Conversion
```bash
# Install dependencies
pip install -r requirements.txt

# Run the main converter
python src/main.py \
  --input data/input/rakuten_item.csv \
  --collection data/input/rakuten_collection.csv \
  --output output/csv/shopify_products.csv
```

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