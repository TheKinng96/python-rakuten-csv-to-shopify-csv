# Rakuten to Shopify Migration System

A comprehensive system for migrating product data from Rakuten to Shopify with two main workflows:

## 🔄 CSV Conversion
Transform large Rakuten product exports into Shopify-ready CSV files for initial migration.

- **Location**: `csv-conversion/`
- **Purpose**: Bulk data conversion from Rakuten CSV exports to Shopify import format
- **Key Features**: SKU grouping, variant handling, HTML cleaning, metafield mapping

## 🔌 API Operations  
Live Shopify API operations for post-migration data manipulation and content editing.

- **Location**: `api-operations/`
- **Purpose**: Direct Shopify store operations via GraphQL API
- **Key Features**: Table content editing, image management, bulk updates, data validation

## Quick Start

### CSV Conversion
```bash
cd csv-conversion/src
python -m venv venv
source venv/bin/activate
pip install pandas beautifulsoup4 lxml
python main.py
```

### API Operations
```bash
cd api-operations/node
npm install
cp ../../.env.example ../../.env
# Edit .env with Shopify credentials
node src/06_update_products_description.js
```

## Documentation

- [`CLAUDE.md`](CLAUDE.md) - Complete development guide and commands
- [`csv-conversion/README.md`](csv-conversion/README.md) - CSV conversion workflow
- [`api-operations/README.md`](api-operations/README.md) - API operations workflow
- [`docs/`](docs/) - Additional documentation and guides

## Architecture

```
rakuten-shopify/
├── csv-conversion/     # Rakuten CSV → Shopify CSV conversion
├── api-operations/     # Live Shopify API operations  
├── docs/              # Documentation
├── .env               # Environment configuration
└── README.md          # This file
```

## Key Business Logic

- **SKU Management**: Main products (no suffix) + variants (-3s, -6s, -t)
- **Product Grouping**: By SKU management number with name merging
- **Image Processing**: Up to 20 images per product with URL construction
- **HTML Table Editing**: API-based content manipulation for live stores
- **Metafield Mapping**: Configurable attribute mapping via JSON config