#!/usr/bin/env python3
"""
Simple catalog ID lookup for missing products.

Reads products_without_catalog_ids.csv and searches for カタログID 
in dl-normal-item_no_desc.csv for each handle (checking all rows for same handle).

Usage:
    python catalog_id_lookup.py
"""

import csv
import json
import re
from pathlib import Path

# File paths
DATA_DIR = Path("/Users/gen/corekara/rakuten-shopify/api-operations/data")
MANUAL_DIR = Path("/Users/gen/corekara/rakuten-shopify/manual/sample")

INPUT_CSV = DATA_DIR / "products_without_catalog_ids.csv"
RAKUTEN_CSV = MANUAL_DIR / "dl-normal-item_no_desc.csv"
OUTPUT_JSON = DATA_DIR / "catalog_id_updates_found.json"

def load_missing_products():
    """Load products without catalog IDs."""
    products = []
    
    with open(INPUT_CSV, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            products.append({
                'handle': row['handle'],
                'variant_sku': row['variant_sku'],
                'current_barcode': row.get('current_barcode', '')
            })
    
    print(f"Loaded {len(products)} products without catalog IDs")
    return products

def find_catalog_ids(missing_products):
    """Find catalog IDs in Rakuten CSV for missing products."""
    found_updates = []
    
    # Create lookup by handle
    handle_lookup = {}
    for product in missing_products:
        handle = product['handle']
        if handle not in handle_lookup:
            handle_lookup[handle] = []
        handle_lookup[handle].append(product)
    
    print(f"Searching for catalog IDs in: {RAKUTEN_CSV}")
    
    with open(RAKUTEN_CSV, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f)
        
        for row_num, row in enumerate(reader, 1):
            if len(row) < 2:
                continue
                
            # Check if this row contains a handle we're looking for
            first_cell = row[0].strip()
            if first_cell in handle_lookup:
                # Found a matching handle, now look for カタログID in this row
                catalog_id = None
                
                # Search all cells in this row for catalog ID pattern
                for cell in row:
                    cell_str = str(cell).strip()
                    # Look for 13-digit catalog ID pattern
                    catalog_matches = re.findall(r'\b(\d{13})\b', cell_str)
                    if catalog_matches:
                        catalog_id = catalog_matches[0]
                        break
                
                if catalog_id:
                    # Add updates for all products with this handle
                    for product in handle_lookup[first_cell]:
                        update = {
                            "handle": product['handle'],
                            "variant_sku": product['variant_sku'],
                            "catalog_id": catalog_id,
                            "current_barcode": product['current_barcode']
                        }
                        found_updates.append(update)
                        print(f"Found: {product['variant_sku']} -> {catalog_id}")
    
    return found_updates

def create_output_json(updates):
    """Create output JSON in the same format as catalog_id_updates.json."""
    output_data = {
        "updates": updates,
        "missing_skus": [],
        "summary": {
            "total_found": len(updates),
            "search_method": "Handle-based lookup in Rakuten CSV"
        },
        "metadata": {
            "total_updates": len(updates),
            "search_date": "2025-08-29",
            "source": "dl-normal-item_no_desc.csv"
        }
    }
    
    # Save to JSON file
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"\nSaved {len(updates)} catalog ID updates to: {OUTPUT_JSON}")
    return output_data

def main():
    print("=" * 60)
    print("🔍 CATALOG ID LOOKUP")
    print("=" * 60)
    
    # Check input files exist
    if not INPUT_CSV.exists():
        print(f"Input file not found: {INPUT_CSV}")
        return
    
    if not RAKUTEN_CSV.exists():
        print(f"Rakuten file not found: {RAKUTEN_CSV}")
        return
    
    # Load missing products
    missing_products = load_missing_products()
    
    # Find catalog IDs
    updates = find_catalog_ids(missing_products)
    
    if not updates:
        print("No catalog IDs found!")
        return
    
    # Create output JSON
    output_data = create_output_json(updates)
    
    # Print summary
    print("\n" + "=" * 60)
    print("📊 LOOKUP COMPLETE")
    print("=" * 60)
    print(f"Total products searched: {len(missing_products)}")
    print(f"Catalog IDs found: {len(updates)}")
    print(f"Success rate: {len(updates) / len(missing_products) * 100:.1f}%")
    print(f"\n📁 Output file: {OUTPUT_JSON}")

if __name__ == "__main__":
    main()