# API Operations

This directory contains Shopify API operations for live product data manipulation, including table content editing and bulk operations.

## Structure

- `node/` - Node.js API scripts for direct Shopify operations
  - `src/` - Main operation scripts (image management, HTML table fixes, etc.)
  - `queries/` - GraphQL queries
  - `config/` - Configuration files
- `python/` - Python analysis and audit scripts
  - `src/` - Analysis scripts for data validation
  - `utils/` - Shared utility functions
- `data/` - Shopify export files
- `reports/` - Generated operation reports
- `shared/` - Shared data between tools

## Usage

### Node.js Operations
```bash
cd api-operations/node
npm install
node src/06_update_products_description.js
```

### Python Analysis
```bash
cd api-operations/python
python src/01_analyze_shopify_products.py
```

See the main CLAUDE.md for detailed setup and usage instructions.