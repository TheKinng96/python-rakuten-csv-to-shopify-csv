#!/usr/bin/env python3
"""
Filter products that have SKU but no barcode from products_export_* CSV files.
Updates the CSV files in place to only include products with SKU but missing barcodes.
"""

import pandas as pd
import os
import glob

def filter_missing_barcodes(data_dir):
    """Filter products with SKU but no barcode from all products_export_* files."""
    
    # Find all products_export_*.csv files
    csv_pattern = os.path.join(data_dir, "products_export_*.csv")
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        print(f"No products_export_*.csv files found in {data_dir}")
        return
    
    total_filtered = 0
    
    for csv_file in sorted(csv_files):
        print(f"\nProcessing {os.path.basename(csv_file)}...")
        
        try:
            # Read CSV file
            df = pd.read_csv(csv_file, encoding='utf-8')
            original_count = len(df)
            
            # Filter for rows that have SKU but no barcode
            # SKU column: 'Variant SKU', Barcode column: 'Variant Barcode'
            filtered_df = df[
                (df['Variant SKU'].notna()) &  # Has SKU
                (df['Variant SKU'] != '') &     # SKU is not empty string
                (
                    (df['Variant Barcode'].isna()) |  # No barcode (NaN)
                    (df['Variant Barcode'] == '') |   # Empty string barcode
                    (df['Variant Barcode'] == '""')   # Empty quoted string
                )
            ]
            
            filtered_count = len(filtered_df)
            
            if filtered_count > 0:
                # Save the filtered results back to the same file
                filtered_df.to_csv(csv_file, index=False, encoding='utf-8')
                print(f"  Original rows: {original_count}")
                print(f"  Filtered rows: {filtered_count}")
                print(f"  Removed: {original_count - filtered_count}")
                total_filtered += filtered_count
            else:
                print(f"  No products with SKU but missing barcode found")
                # Create empty file if no matches
                pd.DataFrame(columns=df.columns).to_csv(csv_file, index=False, encoding='utf-8')
        
        except Exception as e:
            print(f"Error processing {csv_file}: {str(e)}")
    
    print(f"\nTotal products with SKU but missing barcode: {total_filtered}")

if __name__ == "__main__":
    data_dir = "/Users/gen/corekara/rakuten-shopify/api-operations/data"
    filter_missing_barcodes(data_dir)