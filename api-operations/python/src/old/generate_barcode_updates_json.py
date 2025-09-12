#!/usr/bin/env python3
"""
Generate JSON update file for products with found barcodes, following catalog_id_updates_found.json format
"""

import pandas as pd
import json
import os
from datetime import datetime

def generate_barcode_updates_json(summary_file, output_file=None):
    """
    Generate JSON update file for products with found barcodes.
    
    Args:
        summary_file: Path to barcode_summary CSV file
        output_file: Output JSON file path (optional)
    
    Returns:
        str: Path to generated JSON file
    """
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_dir = os.path.dirname(summary_file)
        output_file = os.path.join(base_dir, f"barcode_updates_found_{timestamp}.json")
    
    try:
        # Load the summary file
        df = pd.read_csv(summary_file, encoding='utf-8')
        
        # Filter only products with found barcodes
        found_df = df[df['Barcode_Match_Status'] == 'Found'].copy()
        
        if len(found_df) == 0:
            print("No products with found barcodes to process")
            return None
        
        # Create updates list following the catalog_id_updates_found.json format
        updates = []
        
        for _, row in found_df.iterrows():
            update_entry = {
                "handle": str(row['Handle']) if pd.notna(row['Handle']) else "",
                "variant_sku": str(row['SKU']) if pd.notna(row['SKU']) else "",
                "catalog_id": str(row['Barcode']) if pd.notna(row['Barcode']) else "",
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
        
        print(f"Generated barcode updates JSON: {output_file}")
        print(f"Total updates: {len(updates)}")
        
        # Show summary by handle
        handle_counts = found_df.groupby('Handle').size().sort_values(ascending=False)
        print(f"\nTop products by variant count:")
        for handle, count in handle_counts.head(10).items():
            print(f"  {handle}: {count} variants")
        
        return output_file
    
    except Exception as e:
        print(f"Error generating barcode updates JSON: {str(e)}")
        return None

def main():
    """Main function to generate JSON updates from latest summary."""
    data_dir = "/Users/gen/corekara/rakuten-shopify/api-operations/data"
    
    # Find the latest barcode summary file
    import glob
    summary_pattern = os.path.join(data_dir, "barcode_summary_*.csv")
    summary_files = glob.glob(summary_pattern)
    
    if not summary_files:
        print("No barcode summary found. Run export_barcode_summary.py first.")
        return
    
    # Get the most recent summary
    latest_summary = max(summary_files, key=os.path.getmtime)
    print(f"Using summary: {os.path.basename(latest_summary)}")
    
    # Generate JSON updates
    json_file = generate_barcode_updates_json(latest_summary)
    
    if json_file:
        print(f"\nJSON updates ready: {os.path.basename(json_file)}")
        print("This file can be used for updating product barcodes in Shopify.")
    else:
        print("Failed to generate JSON updates")

if __name__ == "__main__":
    main()