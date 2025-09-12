#!/usr/bin/env python3
"""
Map Rakuten Catalog IDs to Shopify products by SKU.

Reads the preprocessed SKU to Catalog ID mapping and matches it with
Shopify product export CSVs to generate updates for the API.

Usage:
    python 10_map_catalog_to_shopify.py --test --handle "test-product-handle"   # Test mode
    python 10_map_catalog_to_shopify.py                                         # Process all
"""

import argparse
import csv
import json
import os
from pathlib import Path
import glob

# File paths
SHOPIFY_DATA_DIR = Path("/Users/gen/corekara/rakuten-shopify/api-operations/data")
RAKUTEN_MAPPING_FILE = SHOPIFY_DATA_DIR / "rakuten_sku_catalog_mapping.json"
RAKUTEN_MAPPING_TEST_FILE = SHOPIFY_DATA_DIR / "rakuten_sku_catalog_mapping_test.json"
OUTPUT_FILE = SHOPIFY_DATA_DIR / "catalog_id_updates.json"
OUTPUT_TEST_FILE = SHOPIFY_DATA_DIR / "catalog_id_updates_test.json"
MISSING_CATALOG_CSV = SHOPIFY_DATA_DIR / "products_without_catalog_ids.csv"

# Shopify CSV column indices (0-based)
HANDLE_COL = 0          # Handle
VARIANT_SKU_COL = 17    # Variant SKU (column 18 in 1-based)
VARIANT_BARCODE_COL = 27 # Variant Barcode (column 28 in 1-based)

def load_rakuten_mapping(test_mode=False, specific_handle=None):
    """Load SKU to Catalog ID mapping from preprocessed file."""
    # If testing a specific handle, always use the full mapping file
    mapping_file = RAKUTEN_MAPPING_FILE if specific_handle else (RAKUTEN_MAPPING_TEST_FILE if test_mode else RAKUTEN_MAPPING_FILE)
    
    if not mapping_file.exists():
        raise FileNotFoundError(f"Mapping file not found: {mapping_file}")
    
    with open(mapping_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    mappings = data.get("mappings", {})
    stats = data.get("stats", {})
    
    print(f"Loaded {len(mappings)} SKU to Catalog ID mappings")
    print(f"Rakuten stats: {stats}")
    
    return mappings

def get_shopify_csv_files():
    """Get all Shopify product export CSV files."""
    pattern = str(SHOPIFY_DATA_DIR / "products_export_*.csv")
    csv_files = glob.glob(pattern)
    csv_files.sort()
    
    if not csv_files:
        raise FileNotFoundError(f"No Shopify CSV files found in: {SHOPIFY_DATA_DIR}")
    
    print(f"Found {len(csv_files)} Shopify export files:")
    for f in csv_files:
        print(f"  {os.path.basename(f)}")
    
    return csv_files

def process_shopify_csvs(sku_catalog_mapping, test_mode=False, test_handle=None):
    """Process Shopify CSVs to find products that need barcode updates."""
    updates = []
    missing_skus = set()
    stats = {
        "total_shopify_variants": 0,
        "variants_with_sku": 0,
        "variants_with_existing_barcode": 0,
        "mapped_count": 0,
        "unmapped_count": 0,
        "files_processed": 0
    }
    
    # For tracking products without catalog IDs
    products_without_catalog = []
    
    csv_files = get_shopify_csv_files()
    
    for csv_file in csv_files:
        print(f"Processing: {os.path.basename(csv_file)}")
        stats["files_processed"] += 1
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Skip header
            
            for row_num, row in enumerate(reader, start=2):
                if len(row) <= max(HANDLE_COL, VARIANT_SKU_COL, VARIANT_BARCODE_COL):
                    continue
                
                stats["total_shopify_variants"] += 1
                
                handle = row[HANDLE_COL].strip()
                variant_sku = row[VARIANT_SKU_COL].strip()
                current_barcode = row[VARIANT_BARCODE_COL].strip()
                
                # Test mode: only process specified handle
                if test_mode and test_handle and handle != test_handle:
                    continue
                
                if not variant_sku:
                    continue
                
                stats["variants_with_sku"] += 1
                
                if current_barcode:
                    stats["variants_with_existing_barcode"] += 1
                    # Skip products that already have barcodes
                    continue
                
                # Check if we have a catalog ID for this SKU
                if variant_sku in sku_catalog_mapping:
                    catalog_id = sku_catalog_mapping[variant_sku]
                    updates.append({
                        "handle": handle,
                        "variant_sku": variant_sku,
                        "catalog_id": catalog_id,
                        "current_barcode": current_barcode
                    })
                    stats["mapped_count"] += 1
                else:
                    missing_skus.add(variant_sku)
                    stats["unmapped_count"] += 1
                    
                    # Add to products without catalog IDs report
                    products_without_catalog.append({
                        "handle": handle,
                        "variant_sku": variant_sku,
                        "current_barcode": current_barcode
                    })
                
                # Test mode: stop after finding the test handle
                if test_mode and test_handle and handle == test_handle and updates:
                    print(f"Test mode: Found and processed handle '{test_handle}'")
                    break
                
                if row_num % 50000 == 0:
                    print(f"  Processed {row_num} rows, found {len(updates)} updates")
        
        # Test mode: stop after first file if we found our test handle
        if test_mode and test_handle and updates:
            break
    
    # Save products without catalog IDs to CSV
    if not test_mode and products_without_catalog:
        save_missing_catalog_report(products_without_catalog)
    
    return updates, list(missing_skus), stats

def save_missing_catalog_report(products_without_catalog):
    """Save products without catalog IDs to CSV file."""
    with open(MISSING_CATALOG_CSV, 'w', newline='', encoding='utf-8') as f:
        if products_without_catalog:
            fieldnames = products_without_catalog[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(products_without_catalog)
    
    print(f"Saved {len(products_without_catalog)} products without catalog IDs to: {MISSING_CATALOG_CSV}")

def save_updates(updates, missing_skus, stats, test_mode=False):
    """Save catalog ID updates to JSON file."""
    output_data = {
        "updates": updates,
        "missing_skus": missing_skus[:100],  # Limit to first 100 for readability
        "summary": {
            **stats,
            "total_missing_skus": len(missing_skus)
        },
        "metadata": {
            "total_updates": len(updates)
        }
    }
    
    output_file = OUTPUT_TEST_FILE if test_mode else OUTPUT_FILE
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"Saved {len(updates)} updates to: {output_file}")
    print("Processing Summary:")
    for key, value in stats.items():
        print(f"  {key}: {value}")
    print(f"  total_missing_skus: {len(missing_skus)}")

def main():
    parser = argparse.ArgumentParser(description='Map Rakuten Catalog IDs to Shopify products')
    parser.add_argument('--test', action='store_true', help='Test mode: process limited data')
    parser.add_argument('--handle', type=str, help='Specific handle to process in test mode')
    args = parser.parse_args()
    
    try:
        # Load Rakuten SKU to Catalog ID mapping
        sku_catalog_mapping = load_rakuten_mapping(test_mode=args.test, specific_handle=args.handle)
        
        if not sku_catalog_mapping:
            print("No SKU to Catalog ID mappings found!")
            return
        
        # Process Shopify CSVs
        updates, missing_skus, stats = process_shopify_csvs(
            sku_catalog_mapping, 
            test_mode=args.test, 
            test_handle=args.handle
        )
        
        # Save results
        save_updates(updates, missing_skus, stats, test_mode=args.test)
        
        if args.test:
            print(f"Test mode completed! Found {len(updates)} updates for handle: {args.handle}")
            if updates:
                print("Sample update:")
                print(json.dumps(updates[0], indent=2))
        else:
            print("Processing completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()