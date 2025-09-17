#!/usr/bin/env python3
"""
Simple approach: Let pandas parse the CSV directly with error handling,
then filter out any rows that don't have valid product IDs in the first column.
"""

import pandas as pd
from pathlib import Path
import sys
import re

def find_new_products_simple(new_file_path: str, old_file_path: str, output_file_path: str) -> None:
    """
    Simple approach: Parse CSV directly, then filter by valid product IDs.
    """
    new_path = Path(new_file_path)
    old_path = Path(old_file_path)
    output_path = Path(output_file_path)

    try:
        print("Step 1: Loading old file...")
        old_df = pd.read_csv(old_path, encoding='utf-8', low_memory=False)
        old_product_ids = set(old_df.iloc[:, 0].astype(str))
        print(f"Found {len(old_product_ids):,} products in old file")

        print("Step 2: Loading new file with pandas (may take a while)...")
        # Let pandas parse what it can, skipping bad lines
        new_df = pd.read_csv(new_path, encoding='cp932', low_memory=False,
                           on_bad_lines='skip', skiprows=0)
        print(f"Pandas loaded {len(new_df):,} rows from new file")

        print("Step 3: Filtering for valid product IDs...")
        # Filter for rows with valid product IDs (alphanumeric with dashes)
        first_col = new_df.iloc[:, 0].astype(str)
        valid_mask = first_col.str.match(r'^[a-zA-Z0-9\-]+$', na=False)
        valid_new_df = new_df[valid_mask]
        print(f"Found {len(valid_new_df):,} rows with valid product IDs")

        print("Step 4: Finding new products...")
        valid_product_ids = valid_new_df.iloc[:, 0].astype(str)
        new_mask = ~valid_product_ids.isin(old_product_ids)
        new_products_df = valid_new_df[new_mask]

        print("Step 5: Saving results...")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        new_products_df.to_csv(output_path, index=False, encoding='utf-8')

        print(f"\n=== SIMPLE METHOD STATISTICS ===")
        print(f"Total products in old file: {len(old_product_ids):,}")
        print(f"Total valid products in new file: {len(valid_new_df):,}")
        print(f"New products found: {len(new_products_df):,}")
        print(f"Existing products (duplicates): {len(valid_new_df) - len(new_products_df):,}")
        print(f"New products saved to: {output_path}")

        # Verification
        print(f"\n=== VERIFICATION ===")
        sample_ids = new_products_df.iloc[:5, 0].tolist()
        print(f"Sample new product IDs: {sample_ids}")

        # Validate: Check that the product IDs in both files match expected format
        print(f"\n=== VALIDATION ===")
        print(f"Old file first 3 product IDs: {list(old_df.iloc[:3, 0])}")
        print(f"New file valid product IDs sample: {list(valid_new_df.iloc[:3, 0])}")

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        print(f"Stack trace: {traceback.format_exc()}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python find_new_products_simple.py <new_file> <old_file> <output_file>")
        sys.exit(1)

    new_file = sys.argv[1]
    old_file = sys.argv[2]
    output_file = sys.argv[3]

    find_new_products_simple(new_file, old_file, output_file)