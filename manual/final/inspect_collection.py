"""
Inspect Collection/Category data for specific SKUs.

This tool helps debug category-related issues by extracting all rows from
'data/rakuten_collection.csv' for one or more SKUs.

Usage:
  1. Edit the SKUS_TO_INSPECT list below.
  2. Run the script: python scripts/inspect_collections.py
"""

from pathlib import Path
import pandas as pd

# --- Configuration ---

# 1. EDIT THIS LIST with the SKUs you want to find.
SKUS_TO_INSPECT = [
    "5t-o0xo-mmot",
    "5t-o0xo-mmot-3s",
    "1o-dg74-rfa9"
    # Add any other SKUs you want to check here
]

# File paths and encoding settings
INPUT_FILE = Path("data/rakuten_collection.csv")
OUTPUT_FILE = Path("output/collection_inspection_results.csv")
RAKUTEN_ENCODING = "cp932"  # Must match the encoding of your source file


def inspect_collection_data():
    """Finds all rows for specific SKUs in the collection file and saves them."""
    if not SKUS_TO_INSPECT:
        print("The SKUS_TO_INSPECT list is empty. Please add a SKU to inspect.")
        return

    if not INPUT_FILE.exists():
        print(f"Error: Input file not found at '{INPUT_FILE}'")
        return

    print(f"Searching for {len(SKUS_TO_INSPECT)} SKU(s) in '{INPUT_FILE}'...")
    print("SKUs to find:", ", ".join(SKUS_TO_INSPECT))

    try:
        # Load the source collection CSV into memory.
        df = pd.read_csv(INPUT_FILE, encoding=RAKUTEN_ENCODING, dtype=str)

        # The column name we are searching in
        sku_column = '商品管理番号（商品URL）'

        if sku_column not in df.columns:
            print(f"Error: The required column '{sku_column}' was not found in the input file.")
            return

        # Filter the DataFrame to find rows where the SKU column is in our list.
        result_df = df[df[sku_column].str.strip().isin(SKUS_TO_INSPECT)]

        if result_df.empty:
            print("\nResult: No matching collection data found for the specified SKUs.")
        else:
            print(f"\nResult: Found {len(result_df)} matching row(s).")
            print(f"Saving results to '{OUTPUT_FILE}'...")
            # Save the found rows to a new CSV file for easy viewing.
            result_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
            print("Done. Collection inspection file created successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    inspect_collection_data()