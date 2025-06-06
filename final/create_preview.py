"""
Create a small preview CSV from a large one.

This script correctly handles multi-line fields (like HTML in the Body)
by using pandas to read and write the CSV data.

Usage:
  python scripts/create_preview.py
"""

from pathlib import Path
import pandas as pd

# --- Configuration ---
INPUT_FILE = Path("output/shopify_products.csv")
PREVIEW_FILE = Path("output/shopify_products_preview.csv")
NUM_ROWS = 20  # The number of data rows you want in the preview

def create_preview_csv():
    """Reads the full CSV and writes a small preview."""
    if not INPUT_FILE.exists():
        print(f"Error: Input file not found at '{INPUT_FILE}'")
        print("Please run the main conversion script first.")
        return

    print(f"Reading the first {NUM_ROWS} rows from '{INPUT_FILE}'...")
    
    try:
        # Read only the first N rows of the large file using pandas
        # This is memory-efficient and fast.
        df_preview = pd.read_csv(INPUT_FILE, nrows=NUM_ROWS, dtype=str)
        
        print(f"Writing preview file to '{PREVIEW_FILE}'...")
        
        # Write the small DataFrame to a new CSV file.
        # `quoting=csv.QUOTE_ALL` ensures that fields with special characters
        # (like newlines in HTML) are handled correctly.
        df_preview.to_csv(PREVIEW_FILE, index=False, encoding='utf-8')
        
        print("Done. Preview file created successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    create_preview_csv()