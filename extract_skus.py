#!/usr/bin/env python3
"""
Script to extract and sort SKUs (handles) and their variant SKUs from shopify_products.csv.
The script will:
1. Read the shopify_products.csv file
2. Extract handles and their variant SKUs
3. Sort them in ascending order
4. Save them to a new CSV file
"""

import pandas as pd
import os
from typing import List, Dict
from collections import defaultdict
from pathlib import Path

def extract_and_sort_skus(input_file: str = 'shopify_products.csv', 
                         output_file: str = 'sorted_skus.csv') -> None:
    """
    Extract and sort SKUs from the input CSV file.
    
    Args:
        input_file: Path to the input CSV file (default: 'shopify_products.csv')
        output_file: Path to save the sorted SKUs (default: 'sorted_skus.csv')
    """
    # Convert to Path objects
    input_path = Path(input_file)
    output_path = Path(output_file)
    
    # Check if input file exists
    if not input_path.exists():
        print(f"Error: Input file '{input_path}' does not exist.")
        print(f"Current working directory: {os.getcwd()}")
        print(f"Absolute path: {input_path.absolute()}")
        return
    
    try:
        # Read the CSV file
        print(f"Reading {input_path}...")
        df = pd.read_csv(input_path, encoding='utf-8')
        
        # Group variant SKUs by handle
        print("Grouping variant SKUs by handle...")
        handle_variants: Dict[str, List[str]] = defaultdict(list)
        
        for _, row in df.iterrows():
            handle = row['Handle']
            variant_sku = row['Variant SKU']
            if pd.notna(handle) and pd.notna(variant_sku):
                handle_variants[handle].append(variant_sku)
        
        # Sort handles and their variant SKUs
        print("Sorting handles and variant SKUs...")
        sorted_handles = sorted(handle_variants.keys())
        
        # Create a list of dictionaries for the DataFrame
        data = []
        for handle in sorted_handles:
            variants = sorted(handle_variants[handle])
            data.append({
                'Handle': handle,
                'Variant SKUs': ', '.join(variants),
                'Number of Variants': len(variants)
            })
        
        # Create DataFrame and save to CSV
        result_df = pd.DataFrame(data)
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        print(f"Saving sorted SKUs to {output_path}...")
        result_df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"Successfully saved {len(result_df)} handles to {output_path}")
        
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")

if __name__ == "__main__":
    import sys
    import traceback
    
    if len(sys.argv) > 2:
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        extract_and_sort_skus(input_file, output_file)
    else:
        print("Usage: python extract_skus.py <input_file> <output_file>")
        print("Example: python extract_skus.py ./output/shopify_products.csv ./output/sorted_skus.csv") 