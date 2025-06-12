"""
Inspect specific SKUs from the original Rakuten CSV.

This tool helps debug issues by extracting all raw rows for one or more
SKUs before any processing or merging occurs. This is useful for seeing
how a product is represented in the source file.

Usage:
  1. Edit the SKUS_TO_FIND list below.
  2. Run the script: python scripts/inspect_rakuten.py
"""

from pathlib import Path
import pandas as pd

# --- Configuration ---

# 1. EDIT THIS LIST with the SKUs you want to find.
SKUS_TO_FIND = [
    "img58072212",
    # Add any other SKUs you want to check here
]

# File paths and encoding settings
INPUT_FILE = Path("data/rakuten_item.csv")
OUTPUT_FILE = Path("output/inspection_results.csv")
RAKUTEN_ENCODING = "cp932"  # Must match the encoding of your source file


def inspect_sku():
    """Finds all rows for specific SKUs and saves them to a new CSV."""
    if not SKUS_TO_FIND:
        print("The SKUS_TO_FIND list is empty. Please add a SKU to inspect.")
        return

    if not INPUT_FILE.exists():
        print(f"Error: Input file not found at '{INPUT_FILE}'")
        return

    print(f"Searching for {len(SKUS_TO_FIND)} SKU(s) in '{INPUT_FILE}'...")
    print("SKUs to find:", ", ".join(SKUS_TO_FIND))

    try:
        # Load the entire source CSV into memory. This is the easiest way to search.
        # Use dtype=str to prevent pandas from misinterpreting any columns as numbers.
        df = pd.read_csv(INPUT_FILE, encoding=RAKUTEN_ENCODING, dtype=str)

        # The column name we are searching in
        sku_column = '商品管理番号（商品URL）'

        if sku_column not in df.columns:
            print(f"Error: The required column '{sku_column}' was not found in the input file.")
            return

        # Filter the DataFrame to find rows where the SKU column is in our list.
        # .str.strip() is important to remove any accidental leading/trailing whitespace.
        result_df = df[df[sku_column].str.strip().isin(SKUS_TO_FIND)]

        if result_df.empty:
            print("\nResult: No matching rows found for the specified SKUs.")
        else:
            print(f"\nResult: Found {len(result_df)} matching row(s).")
            print(f"Saving results to '{OUTPUT_FILE}'...")
            # Save the found rows to a new CSV file for easy viewing.
            result_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
            print("Done. Inspection file created successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    inspect_sku()