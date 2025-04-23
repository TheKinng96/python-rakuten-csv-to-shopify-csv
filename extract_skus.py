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

def extract_and_sort_skus(input_file: str = 'shopify_products.csv', 
                         output_file: str = 'sorted_skus.csv') -> None:
    """
    Extract and sort SKUs from the input CSV file.
    
    Args:
        input_file: Path to the input CSV file (default: 'shopify_products.csv')
        output_file: Path to save the sorted SKUs (default: 'sorted_skus.csv')
    """
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' does not exist.")
        return
    
    try:
        # Read the CSV file
        print(f"Reading {input_file}...")
        df = pd.read_csv(input_file, encoding='utf-8')
        
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
            # Sort variant SKUs for each handle
            sorted_variants = sorted(handle_variants[handle])
            for variant in sorted_variants:
                data.append({
                    'Handle': handle,
                    'Variant SKU': variant
                })
        
        # Create DataFrame
        result_df = pd.DataFrame(data)
        
        # Save to CSV
        print(f"Saving sorted handles and variant SKUs to {output_file}...")
        result_df.to_csv(output_file, index=False, encoding='utf-8')
        
        print(f"Successfully saved {len(sorted_handles)} unique handles with their variant SKUs to {output_file}")
        
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        return

if __name__ == "__main__":
    extract_and_sort_skus() 