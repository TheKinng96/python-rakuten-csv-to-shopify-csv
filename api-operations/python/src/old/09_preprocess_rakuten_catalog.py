#!/usr/bin/env python3
"""
Preprocess Rakuten CSV to extract SKU to Catalog ID mappings.

The Rakuten CSV has a multi-row structure:
- First row: Contains main product info including カタログID (column 250)
- Second row: Contains SKU info including SKU管理番号 (column 214)

Usage:
    python 09_preprocess_rakuten_catalog.py --test    # Process only first product group
    python 09_preprocess_rakuten_catalog.py           # Process all products
"""

import argparse
import csv
import json
import os
from collections import defaultdict
from pathlib import Path

# File paths
RAKUTEN_CSV_PATH = "/Users/gen/corekara/rakuten-shopify/manual/sample/dl-normal-item_no_desc.csv"
OUTPUT_DIR = Path("/Users/gen/corekara/rakuten-shopify/api-operations/data")
OUTPUT_FILE = OUTPUT_DIR / "rakuten_sku_catalog_mapping.json"

# Column indices (0-based)
PRODUCT_ID_COL = 0      # 商品管理番号（商品URL）
SKU_COL = 213           # SKU管理番号 (column 214 in 1-based)
CATALOG_ID_COL = 249    # カタログID (column 250 in 1-based)

def read_rakuten_csv(test_mode=False):
    """Read Rakuten CSV and extract product groups."""
    product_groups = defaultdict(list)
    
    print(f"Reading Rakuten CSV: {RAKUTEN_CSV_PATH}")
    
    # Try different encodings for the Rakuten CSV
    encodings = ['utf-8', 'shift_jis', 'cp932', 'euc-jp']
    f = None
    for encoding in encodings:
        try:
            f = open(RAKUTEN_CSV_PATH, 'r', encoding=encoding)
            f.readline()  # Test read
            f.seek(0)     # Reset to beginning
            print(f"Successfully opened file with encoding: {encoding}")
            break
        except UnicodeDecodeError:
            if f:
                f.close()
            continue
    
    if not f:
        raise ValueError("Could not decode the CSV file with any of the tried encodings")
    
    with f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header
        
        for row_num, row in enumerate(reader, start=2):
            if len(row) <= max(PRODUCT_ID_COL, SKU_COL, CATALOG_ID_COL):
                continue
                
            product_id = row[PRODUCT_ID_COL].strip()
            if not product_id:
                continue
                
            product_groups[product_id].append(row)
            
            # Test mode: stop after first product group is complete (need at least 2 rows)
            if test_mode and len(product_groups) >= 1:
                first_product_id = next(iter(product_groups.keys()))
                if len(product_groups[first_product_id]) >= 2:
                    print(f"Test mode: Processing only first product group '{first_product_id}'")
                    break
                
            if row_num % 10000 == 0:
                print(f"Processed {row_num} rows, found {len(product_groups)} product groups")
    
    print(f"Total product groups found: {len(product_groups)}")
    return product_groups

def extract_sku_catalog_mapping(product_groups):
    """Extract SKU to Catalog ID mapping from product groups."""
    mappings = {}
    stats = {
        "total_products": 0,
        "products_with_catalog": 0,
        "products_without_catalog": 0,
        "products_without_sku": 0
    }
    
    for product_id, rows in product_groups.items():
        stats["total_products"] += 1
        
        if len(rows) == 0:
            continue
            
        # Get catalog ID from second row if available, otherwise first row
        catalog_row = rows[1] if len(rows) > 1 else rows[0]
        catalog_id = catalog_row[CATALOG_ID_COL].strip() if len(catalog_row) > CATALOG_ID_COL else ""
        
        # Remove .0 suffix if it exists (catalog IDs are stored as floats)
        if catalog_id.endswith('.0'):
            catalog_id = catalog_id[:-2]
        
        if not catalog_id:
            stats["products_without_catalog"] += 1
            print(f"No catalog ID for product: {product_id}")
            continue
            
        stats["products_with_catalog"] += 1
        
        # Get SKU from second row, fallback to first row if no second row
        sku_row = rows[1] if len(rows) > 1 else rows[0]
        sku = sku_row[SKU_COL].strip() if len(sku_row) > SKU_COL else ""
        
        # If no SKU in designated column, use product_id as fallback
        if not sku:
            sku = product_id
            stats["products_without_sku"] += 1
            print(f"Using product ID as SKU for: {product_id}")
        
        mappings[sku] = catalog_id
        
        # Debug output for test mode
        if len(product_groups) == 1:
            print(f"Test mapping - Product ID: {product_id}")
            print(f"Test mapping - SKU: {sku}")
            print(f"Test mapping - Catalog ID: {catalog_id}")
            print(f"Catalog row catalog col [{CATALOG_ID_COL}]: {catalog_row[CATALOG_ID_COL] if len(catalog_row) > CATALOG_ID_COL else 'N/A'}")
            print(f"SKU row SKU col [{SKU_COL}]: {sku_row[SKU_COL] if len(sku_row) > SKU_COL else 'N/A'}")
    
    return mappings, stats

def save_mapping(mappings, stats, output_file):
    """Save SKU to Catalog ID mapping to JSON file."""
    output_data = {
        "mappings": mappings,
        "stats": stats,
        "metadata": {
            "source_file": RAKUTEN_CSV_PATH,
            "total_mappings": len(mappings)
        }
    }
    
    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"Saved mapping to: {output_file}")
    print(f"Total mappings: {len(mappings)}")
    print("Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")

def main():
    parser = argparse.ArgumentParser(description='Extract SKU to Catalog ID mappings from Rakuten CSV')
    parser.add_argument('--test', action='store_true', help='Test mode: process only first product group')
    args = parser.parse_args()
    
    try:
        # Read Rakuten CSV
        product_groups = read_rakuten_csv(test_mode=args.test)
        
        if not product_groups:
            print("No product groups found!")
            return
        
        # Extract mappings
        mappings, stats = extract_sku_catalog_mapping(product_groups)
        
        # Save results
        output_file = OUTPUT_FILE
        if args.test:
            output_file = OUTPUT_DIR / "rakuten_sku_catalog_mapping_test.json"
        
        save_mapping(mappings, stats, output_file)
        
        print("Processing completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()