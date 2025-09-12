#!/usr/bin/env python3
"""
Convert notfound.csv with updated catalog IDs to JSON format matching catalog_id_updates_found.json
"""

import pandas as pd
import json
import os
from datetime import datetime

def convert_notfound_to_json(notfound_csv, output_file=None):
    """
    Convert notfound.csv to JSON format for barcode updates.
    
    Args:
        notfound_csv: Path to notfound.csv file
        output_file: Output JSON file path (optional)
    
    Returns:
        str: Path to generated JSON file
    """
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_dir = os.path.dirname(notfound_csv)
        output_file = os.path.join(base_dir, f"notfound_updates_{timestamp}.json")
    
    try:
        # Load the CSV file
        df = pd.read_csv(notfound_csv, encoding='utf-8')
        
        # The last column should contain the updated catalog IDs
        # Check if there's an unnamed column or get the last column
        columns = df.columns.tolist()
        if len(columns) >= 6:
            catalog_col = columns[-1]  # Last column
        else:
            print("Expected at least 6 columns in the CSV file")
            return None
        
        print(f"Using column '{catalog_col}' for catalog IDs")
        
        # Filter rows that have valid catalog IDs (not empty, not #N/A, not NaN)
        valid_catalog_mask = (
            df[catalog_col].notna() & 
            (df[catalog_col] != '') & 
            (df[catalog_col] != '#N/A') &
            (df[catalog_col].astype(str).str.strip() != '') &
            (df[catalog_col].astype(str).str.strip() != '#N/A')
        )
        
        valid_df = df[valid_catalog_mask].copy()
        
        if len(valid_df) == 0:
            print("No valid catalog IDs found in the CSV file")
            return None
        
        print(f"Found {len(valid_df)} products with valid catalog IDs")
        
        # Create updates list following the catalog_id_updates_found.json format
        updates = []
        
        for _, row in valid_df.iterrows():
            catalog_id = str(row[catalog_col]).strip()
            
            update_entry = {
                "handle": str(row['Handle']).strip() if pd.notna(row['Handle']) else "",
                "variant_sku": str(row['SKU']).strip() if pd.notna(row['SKU']) else "",
                "catalog_id": catalog_id,
                "current_barcode": ""  # Empty as these products currently have no barcode
            }
            updates.append(update_entry)
        
        # Create the JSON structure
        json_data = {
            "updates": updates
        }
        
        # Save to JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        print(f"Generated catalog updates JSON: {output_file}")
        print(f"Total updates: {len(updates)}")
        
        # Show summary by handle
        handle_counts = valid_df.groupby('Handle').size().sort_values(ascending=False)
        print(f"\nTop products by variant count:")
        for handle, count in handle_counts.head(10).items():
            print(f"  {handle}: {count} variants")
        
        return output_file
    
    except Exception as e:
        print(f"Error converting notfound CSV to JSON: {str(e)}")
        return None

def main():
    """Main function to convert notfound.csv to JSON."""
    notfound_file = "/Users/gen/corekara/rakuten-shopify/api-operations/data/notfound.csv"
    
    if not os.path.exists(notfound_file):
        print(f"File not found: {notfound_file}")
        return
    
    print(f"Converting: {os.path.basename(notfound_file)}")
    
    # Convert to JSON
    json_file = convert_notfound_to_json(notfound_file)
    
    if json_file:
        print(f"\nJSON updates ready: {os.path.basename(json_file)}")
        print("This file can be used for updating product barcodes in Shopify.")
    else:
        print("Failed to convert CSV to JSON")

if __name__ == "__main__":
    main()