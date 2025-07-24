# Shopify Product Management System

API-based system for managing and cleaning Shopify product data, specifically designed to handle issues from Rakuten-to-Shopify migration.

**Complete workflow**: Import data → Clean images → Fix HTML → Remove Rakuten content → Audit missing images

## Quick Start

### Setup Environment

1. **Install UV** (if not already installed):
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. **Initialize project**:
```bash
cd api
uv sync
```

3. **Configure environment**:
```bash
cp .env.example .env
# Edit .env with your Shopify credentials
```

### Environment Variables

Edit `.env` file with your credentials:

```bash
# Shopify API Configuration
SHOPIFY_TEST_STORE_URL=your-test-store.myshopify.com
SHOPIFY_TEST_ACCESS_TOKEN=your-test-store-access-token

# Optional: Production store (use with caution)
SHOPIFY_PROD_STORE_URL=your-prod-store.myshopify.com
SHOPIFY_PROD_ACCESS_TOKEN=your-prod-store-access-token

# Processing Configuration
DRY_RUN=true                    # Set to false for actual processing
MAX_REQUESTS_PER_SECOND=40
BATCH_SIZE=250
CHUNK_SIZE=1000
```

## Processing Scripts

### Step 0: Data Import
Upload all production CSV data to test Shopify store for safe processing.

**Script**: `scripts/00_import_to_test.py`
**Output**: Console progress logging only (no CSV reports)

### Issue 1: SS Images (863 products)
Images ending with `-XXss.jpg` pattern that need removal.

**Script**: `scripts/01_remove_ss_images.py`
**Reports**: 
- `reports/01_ss_images_analysis.csv` - Analysis of affected products
- `reports/01_ss_images_removed.csv` - Processing results

### Issue 2: HTML Table Problems
Nested table structures causing layout overlaps.

**Script**: `scripts/02_fix_html_tables.py` (coming next)
**Reports**:
- `reports/02_html_tables_analysis.csv` - Table structure analysis
- `reports/02_html_tables_fixed.csv` - Fix results

### Issue 3: Rakuten Content (~9,000+ products)
EC-UP comment blocks and associated content that need removal.

**Script**: `scripts/03_clean_rakuten.py` (coming next)
**Reports**:
- `reports/03_rakuten_patterns_found.csv` - All EC-UP patterns discovered
- `reports/03_rakuten_content_removed.csv` - Cleanup results

### Issue 4: Missing Images
Products/variants without images needing manual attention.

**Script**: `scripts/04_audit_images.py` (coming next)
**Reports**:
- `reports/04_missing_images_audit.csv` - Complete missing images inventory

## Usage

### 0. Import Data to Test Store

```bash
# Import all CSV data to test store (with progress logging)
uv run scripts/00_import_to_test.py
```

This will:
- Load all 5 CSV files from `data/` folder
- Convert CSV format to Shopify API format
- Upload ~1M rows as products to test store
- Show console progress: "xxx/xxx products processed"

### 1. Remove SS Images

```bash
# Analysis only (safe to run)
uv run scripts/01_remove_ss_images.py

# If you want to actually remove images, set DRY_RUN=false in .env
```

This will:
- Scan all CSV files in `data/` folder
- Identify products with `-XXss.jpg` images
- Generate analysis report
- Optionally remove images via Shopify API

### 2. Fix HTML Tables

```bash
uv run scripts/02_fix_html_tables.py
```

### 3. Clean Rakuten Content

```bash
uv run scripts/03_clean_rakuten.py
```

### 4. Audit Missing Images

```bash
uv run scripts/04_audit_images.py
```

## Project Structure

```
api/
├── data/                          # Your CSV files (products_export_*.csv)
├── reports/                       # Generated analysis and processing reports
├── scripts/                       # Processing scripts
│   ├── 00_import_to_test.py      # Import CSV data to test store
│   ├── 01_remove_ss_images.py    # Remove -XXss.jpg images
│   ├── 02_fix_html_tables.py     # Fix nested table issues  
│   ├── 03_clean_rakuten.py       # Remove EC-UP content
│   └── 04_audit_images.py        # Generate missing images report
├── src/shopify_manager/           # Core modules
│   ├── client.py                 # Shopify API client with rate limiting
│   ├── config.py                 # Configuration management
│   ├── models.py                 # Data models
│   └── logger.py                 # Logging utilities
├── .env                          # Environment configuration
└── pyproject.toml                # Python project configuration
```

## Safety Features

- **Test Store First**: All operations default to test store
- **Dry Run Mode**: Preview changes before applying (DRY_RUN=true)
- **Rate Limiting**: Respects Shopify API limits (40 calls/second)
- **Comprehensive Logging**: All changes tracked for audit and rollback
- **Error Handling**: Graceful failure with detailed error reports
- **Progress Tracking**: Resume processing from interruption points

## Generated Reports

Each script generates detailed CSV reports:

### Analysis Reports (Before Processing)
- Show exactly what issues were found
- Safe to generate anytime
- Used for decision making

### Processing Reports (After Changes)
- Record every change made
- Include timestamps and status
- Enable rollback if needed

## Current Status

- ✅ **Project Setup**: UV environment, core modules
- ✅ **Data Import Script**: Upload CSV data to test store
- ✅ **SS Images Script**: Analysis and removal functionality
- ✅ **HTML Tables Script**: Intelligent table structure fixes
- ✅ **Rakuten Cleanup Script**: Comprehensive EC-UP pattern discovery and removal
- ✅ **Missing Images Audit**: Complete image availability audit

## Development

To extend or modify:

1. Core functionality: `src/shopify_manager/`
2. New scripts: `scripts/` (follow naming convention)
3. Add dependencies: `uv add package-name`
4. Run tests: `uv run pytest` (when tests are added)

## Notes

- Always test on test store first
- Keep `DRY_RUN=true` until you're confident
- Monitor API rate limits in logs
- All CSV files are processed in memory-efficient chunks
- Reports include full traceability for compliance