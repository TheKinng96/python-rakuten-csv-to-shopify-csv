# HTML Table Fixer for Shopify Products

This tool fixes HTML table formatting issues in Shopify product CSV files, addressing problems like:
- Fixed width tables causing overflow on mobile devices
- Poor responsiveness of nested table structures  
- Images and text overflowing containers
- Complex CSS that breaks on different screen sizes

## Requirements

- Python 3.7+
- Node.js 14+
- uv (Python package manager)

## Installation

```bash
# Install Node.js dependencies
cd node
npm install
cd ..

# Install Python dependencies (using uv)
uv add beautifulsoup4 lxml
```

## Usage

### Step 1: Fix HTML Tables in CSV Files

Process all CSV files in the data folder automatically:

```bash
# Process all CSV files in the data folder
uv run python scripts/fix_html_tables.py

# Or specify custom directories
uv run python scripts/fix_html_tables.py --data-dir data --output-dir fixed
```

The script will:
- Automatically find all CSV files in the data directory
- Remove fixed width constraints from tables and cells
- Add responsive CSS styling
- Fix word wrapping and text overflow issues
- Clean up nested table structures
- Generate reports in the `reports/` folder
- Create a JSON file in the `shared/` folder for GraphQL updates

### Step 2: Update Products in Shopify via GraphQL

Update product descriptions directly using the GraphQL API:

```bash
# Navigate to the node directory
cd node

# Update products with HTML table fixes
node src/06_update_products_description.js --html-table-fix

# For dry run (preview changes without updating)
DRY_RUN=true node src/06_update_products_description.js --html-table-fix
```

The script will:
- Read from `shared/html_table_fixes_to_update.json`
- Update product descriptions via GraphQL
- Handle rate limiting automatically
- Generate a report in `reports/06_description_update_results.json`

## What Gets Fixed

### Before (Problematic HTML):
```html
<table border="0" cellpadding="0" cellspacing="0" width="840">
  <tr>
    <td style="padding: 0px 20px 0px 0px;">
      <table width="400">
        <td bgcolor="#F4F4F4" width="130">特定名称</td>
        <td bgcolor="#FFFFFF" colspan="3" width="270">純米大吟醸</td>
      </table>
    </td>
  </tr>
</table>
```

### After (Responsive HTML):
```html
<div style="width: 100%; overflow-x: auto; -webkit-overflow-scrolling: touch;">
  <table style="width: 100%; max-width: 100%; table-layout: auto; border-collapse: collapse;">
    <tr>
      <td style="padding: 8px; word-wrap: break-word; overflow-wrap: break-word;">
        <table style="width: 100%; max-width: 100%; table-layout: auto; border-collapse: collapse;">
          <td bgcolor="#F4F4F4" style="padding: 8px; word-wrap: break-word; overflow-wrap: break-word;">特定名称</td>
          <td bgcolor="#FFFFFF" style="padding: 8px; word-wrap: break-word; overflow-wrap: break-word;">純米大吟醸</td>
        </table>
      </td>
    </tr>
  </table>
</div>
```

## Results

When processing all 5 CSV files:
- **23,942 rows processed**
- **10,223 HTML fixes applied** (42.7% of products had table issues)
- Fixed files saved to `fixed/` directory
- Report saved to `reports/html_table_fixes_report.json`
- Update data saved to `shared/html_table_fixes_to_update.json`

## Files Structure

```
api/
├── scripts/
│   └── fix_html_tables.py     # Python script to fix HTML tables
├── node/
│   └── src/
│       └── 06_update_products_description.js  # GraphQL update script
├── data/                      # Original CSV export files
│   ├── products_export_1.csv
│   ├── products_export_2.csv
│   └── ...
├── fixed/                     # Fixed CSV files (created by script)
│   └── products_export_*_fixed.csv
├── reports/                   # Analysis and update reports
│   ├── html_table_fixes_report.json
│   └── 06_description_update_results.json
└── shared/                    # Shared data between scripts
    └── html_table_fixes_to_update.json
```

## Complete Workflow Example

```bash
# 1. Fix HTML tables in all CSV files
uv run python scripts/fix_html_tables.py

# 2. Review the report
cat reports/html_table_fixes_report.json | jq '.summary'

# 3. Update products in Shopify (dry run first)
cd node
DRY_RUN=true node src/06_update_products_description.js --html-table-fix

# 4. Perform actual update
DRY_RUN=false node src/06_update_products_description.js --html-table-fix
```

## Alternative: Using Fixed CSV Files

If you prefer to re-import the CSV files instead of using GraphQL:
1. The fixed CSV files are in the `fixed/` directory
2. Import them back into Shopify via the admin interface
3. This will overwrite all product data, not just the HTML

The fixes ensure your product descriptions will display properly on all device sizes and won't cause overflow issues in Shopify's responsive themes.