# Rakuten-Shopify CSV Utilities & Analysis

## Overview
This project provides a collection of Python scripts for processing, cleaning, transforming, and analyzing CSV files exported from Rakuten for migration to Shopify. It includes tools for splitting/merging CSVs, removing unwanted columns, analyzing metafields, and more.

## Directory Structure

```
rakuten-shopify/
├── csv_utils/                # Scripts for CSV manipulation (split, merge, remove columns, count rows)
│   ├── split_csv_by_size.py
│   ├── merge_rakuten_files.py
│   ├── remove_description_columns.py
│   ├── remove_content_columns.py
│   ├── count_csv_rows.py
│   ├── extract_skus.py
│   └── convert_rakuten_to_shopify.py
├── analysis/                 # Scripts for analysis and reporting
│   ├── analyze_metafields.py
│   ├── log_unique_sku.py
│   └── check_mismatched_items.py
├── constants/                # Constants/shared config
│   ├── rakuten_constants.py
│   └── shopify_constants.py
├── sample/                   # Sample and output CSVs (as before)
│   └── ...
├── output/                   # Output files (as before)
│   └── ...
├── split_output/             # Split output files (as before)
│   └── ...
├── migration-planning.md
├── todo.md
├── rakuten-header.txt
├── shopify-header.txt
└── .gitignore
```

## Script Usage

- **csv_utils/**: Tools for manipulating CSVs (splitting, merging, column removal, etc.)
- **analysis/**: Scripts for analyzing CSVs and generating reports.
- **constants/**: Shared constants for Rakuten and Shopify formats.

Each script can be run directly with Python 3. Example:
```sh
python csv_utils/split_csv_by_size.py
```

## How to Add New Utilities
- Place new CSV processing scripts in `csv_utils/`.
- Place new analysis/reporting scripts in `analysis/`.
- Place shared constants/config in `constants/`.

---
For more details, see inline comments in each script or contact the project maintainer.
