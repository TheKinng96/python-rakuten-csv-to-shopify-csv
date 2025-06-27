"""
Inspect specific Handles from the final Shopify CSV output.

This tool helps verify the final output by extracting all rows for one or more
Handles. This is useful for checking the final structure of variants, images,
and product-level data for a specific product.

Usage:
  1. Edit the HANDLES_TO_FIND list below.
  2. Run the script: python scripts/inspect_shopify.py
"""

from pathlib import Path
import pandas as pd
import csv

# --- Configuration ---

# 1. EDIT THIS LIST with the Handles you want to find.
HANDLES_TO_FIND = [
    "1o-dg74-rfa9",
    # Add any other Handles you want to check here
]

# File paths
INPUT_FILE = Path("output/shopify_products_with_types.csv")
OUTPUT_FILE = Path("output/shopify_inspection_results.csv")


def inspect_handle():
    """Finds all rows for specific Handles and saves them to a new CSV."""
    if not HANDLES_TO_FIND:
        print("The HANDLES_TO_FIND list is empty. Please add a Handle to inspect.")
        return

    if not INPUT_FILE.exists():
        print(f"Error: Input file not found at '{INPUT_FILE}'")
        print("Please run the main conversion script first to generate this file.")
        return

    print(f"Searching for {len(HANDLES_TO_FIND)} Handle(s) in '{INPUT_FILE}'...")
    print("Handles to find:", ", ".join(HANDLES_TO_FIND))

    try:
        # Load the entire Shopify CSV. Use dtype=str to keep all data as text.
        df = pd.read_csv(INPUT_FILE, dtype=str)

        handle_column = 'Handle'

        if handle_column not in df.columns:
            print(f"Error: The required column '{handle_column}' was not found in the input file.")
            return

        # Filter the DataFrame to find rows where the Handle column is in our list.
        # .isin() is perfect for checking against a list of values.
        result_df = df[df[handle_column].isin(HANDLES_TO_FIND)]

        if result_df.empty:
            print("\nResult: No matching rows found for the specified Handles.")
        else:
            print(f"\nResult: Found {len(result_df)} matching row(s).")
            print(f"Saving results to '{OUTPUT_FILE}'...")
            
            # Save the found rows to a new CSV file.
            # Use QUOTE_NONNUMERIC to handle complex fields like Body (HTML) robustly.
            result_df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8', quoting=csv.QUOTE_NONNUMERIC)
            print("Done. Inspection file created successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    inspect_handle()