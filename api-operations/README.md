# API Operations

This directory contains Shopify API operations for live product data manipulation, including table content editing and bulk operations.

## Structure

- `node/` - Node.js API scripts for direct Shopify operations
  - `src/` - Main operation scripts (image management, HTML table fixes, font-weight removal, etc.)
  - `queries/` - GraphQL queries
  - `config/` - Configuration files
- `python/` - Python analysis and audit scripts
  - `src/` - Analysis scripts for data validation
  - `utils/` - Shared utility functions
- `data/` - Shopify export files
- `reports/` - Generated operation reports
- `shared/` - Shared data between tools

## Available Operations

### 🎨 Table Font-Weight Normalization (NEW)
Add `font-weight: normal` to table content elements (td, th) to override inherited bold styling and ensure consistent table text appearance.

```bash
# Analysis phase
cd python/src
python 07_analyze_table_font_normal.py

# Processing phase
cd ../../node
node src/07_add_table_font_normal.js --dry-run
node src/07_add_table_font_normal.js  # Live updates
```

### 🔧 HTML Table Fixes
Fix malformed HTML table structures and clean up table elements.

```bash
cd python/src
python 02_analyze_html_tables.py
cd ../../node
node src/02_fix_html_tables.js --dry-run
```

### 📝 Product Description Updates
Update product descriptions with cleaned content or specific pattern removal.

```bash
cd node
node src/06_update_products_description.js --dry-run
node src/06_update_products_description.js --html-table-fix
```

### 🖼️ Image Management
Remove, insert, or manage product images via Shopify API.

```bash
cd node
node src/01_remove_ss_images.js
node src/04_insert_images.js
```

## General Usage

### Environment Setup
```bash
# Copy environment configuration
cp ../../.env.example ../../.env
# Edit .env with your Shopify credentials

# Install Node.js dependencies
cd node
npm install
```

### Common Options
- `--dry-run` - Preview changes without making updates
- `--test-handle <handle>` - Test with a single product
- `--resume-from <number>` - Resume processing from a specific index

### Safety Features
- **Dry-run mode**: Preview all changes before applying
- **Rate limiting**: Respects Shopify API limits
- **Progress tracking**: Detailed logging and resumable operations
- **Error handling**: Comprehensive error reporting and recovery
- **Validation**: Pre and post-processing validation checks

## Reports and Monitoring

All operations generate detailed reports in the `reports/` directory:
- `07_table_font_normal_addition_results.json` - Font-weight normalization results
- `02_html_table_fixes_report.json` - Table structure fix results
- `06_description_update_results.json` - Description update results

See the main CLAUDE.md for detailed setup and usage instructions.

# Rakuten to shopify 

## Rules

- remove -ss
- merge all -*s into one handle
- tax matching [document](data/with-tax.csv), if doesnt have the record, to simple split by food or non-food, exceptional alcohols are 10% tax
- image rule: cabinet and gold have different image path pattern