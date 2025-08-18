# CSV Conversion

This directory contains the Rakuten-to-Shopify CSV conversion system that transforms large Rakuten product exports into Shopify-ready product CSV files.

## Structure

- `src/main.py` - Main conversion script
- `src/constants/` - Rakuten and Shopify field mappings
- `src/utils/` - CSV utilities and analysis scripts
- `src/gui/` - PySimpleGUI application
- `data/` - Input data files (rakuten_item.csv, etc.)
- `output/` - Generated Shopify CSV files
- `config/` - Configuration files (mapping_meta.json)

## Usage

```bash
cd csv-conversion/src
python main.py
```

See the main CLAUDE.md for detailed setup and usage instructions.