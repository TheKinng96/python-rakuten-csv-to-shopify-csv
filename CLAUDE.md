# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a comprehensive Rakuten-to-Shopify migration system with two main workflows:

1. **CSV Conversion**: Transforms large Rakuten product exports (≈500MB) into Shopify-ready product CSV files
2. **API Operations**: Handles live Shopify API interactions for product data manipulation, including table content editing and bulk operations

## Architecture

### Core Components

#### CSV Conversion (`csv-conversion/`)
- **`src/main.py`** - Main conversion script that processes Rakuten CSV exports and generates Shopify-compatible CSV files
- **`src/constants/`** - Shared constants and mappings for Rakuten and Shopify field definitions
- **`src/utils/`** - Utility scripts for CSV manipulation, analysis, SKU validation, and data quality checks
- **`src/gui/`** - GUI application for processing with PySimpleGUI interface

#### API Operations (`api-operations/`)
- **`node/src/`** - Node.js scripts for direct Shopify GraphQL API operations (table content editing, image management, bulk updates)
- **`python/src/`** - Python analysis and audit scripts for data validation and reporting
- **`node/queries/`** - GraphQL query definitions for Shopify API
- **`shared/`** - Shared data files between Node.js and Python tools

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

### CSV Conversion Setup
```bash
# Navigate to CSV conversion directory
cd csv-conversion/src

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install pandas beautifulsoup4 lxml
```

### Running CSV Conversion
```bash
# From csv-conversion/src directory
python main.py
```

### CSV Utility Scripts
```bash
# From csv-conversion/src/utils directory
python split_csv_by_size.py
python merge_rakuten_files.py
python count_csv_rows.py
python analyze_metafields.py
python check_mismatched_items.py
```

### GUI Application
```bash
# From csv-conversion/src/gui directory
pip install -r requirements.txt
python app.py
```

### API Operations Setup
```bash
# Navigate to API operations directory
cd api-operations/node

# Install Node.js dependencies
npm install

# Copy environment configuration
cp ../../.env.example ../../.env
# Edit .env with your Shopify credentials
```

### Running API Operations
```bash
# Node.js operations (from api-operations/node directory)
node src/06_update_products_description.js
node src/02_fix_html_tables.js
node src/01_remove_ss_images.js

# Python analysis (from api-operations/python/src directory)
python 01_analyze_shopify_products.py
python 02_analyze_html_tables.py
```

### Test Order Creation
```bash
# Create test orders with Japanese payment methods (from api-operations/node directory)
node src/create_test_orders.js
```

## File Structure Conventions

### CSV Conversion Data Structure
```
csv-conversion/
├── data/
│   ├── rakuten_item.csv           # Main product data
│   ├── rakuten_collection.csv     # Category/collection data
│   └── mapping_meta.json          # Attribute mapping configuration
└── output/
    ├── shopify_products.csv                    # Main cleaned output
    ├── shopify_products_original_html.csv      # Output with original HTML
    ├── body_html_comparison.csv                # HTML cleaning comparison log
    └── rejected_rows.csv                       # Filtered/rejected data log
```

### API Operations Data Structure
```
api-operations/
├── data/
│   ├── products_export_1.csv     # Shopify product exports
│   ├── products_export_2.csv     # (split by size for processing)
│   └── ...
├── reports/
│   ├── html_table_fixes_report.json        # Table fixing operation results
│   ├── 06_description_update_results.json  # Description update results
│   └── variant_images_check_report.json    # Image validation reports
└── shared/
    └── html_table_fixes_to_update.json     # Shared data between operations
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

### Test Order Creation (Japanese Payment Methods)

The system includes functionality to create test orders with various Japanese payment methods for testing POS integrations and payment workflows:

#### Available Payment Methods
- **Suica** - IC card contactless payment with balance tracking
- **Credit Card** - Visa/Mastercard with authorization codes
- **PayPay** - QR code digital wallet payment
- **Cash** - Manual cash transactions with change calculation
- **Mixed Payment** - Combination payments (e.g., Suica + Cash)
- **Rakuten Pay** - Rakuten ecosystem payment with points

#### Test Order Structure
Each test order includes:
- **Line Items**: Product variants with quantities
- **Transactions**: Payment transaction details with amounts
- **Custom Attributes**: Payment-specific metadata (card IDs, transaction IDs, balances)
- **Tags**: Payment method categorization for filtering
- **Financial Status**: Automatically set to PAID for completed transactions

#### GraphQL Implementation
Uses Shopify Admin API 2025-07 with the `orderCreate` mutation:
- Input type: `OrderCreateOrderInput`
- Transaction amounts use `amountSet` with `shopMoney` objects
- All amounts in Japanese Yen (JPY)
- Custom attributes store detailed payment method information

#### Usage
1. Get product variant IDs using the `getProductVariants` query
2. Run `node src/create_test_orders.js` to generate test orders
3. Verify orders in Shopify Admin with payment method details
4. Use for testing POS system integrations and payment processing workflows