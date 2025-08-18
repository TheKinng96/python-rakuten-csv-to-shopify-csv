"""
Create a small preview CSV from a large one.

This script correctly reads the full CSV and creates a preview containing
the first N complete products, including all their multi-line variant and
image rows.

Usage:
  python create_preview.py
"""

from pathlib import Path
import pandas as pd

# --- Configuration ---
INPUT_FILE = Path("output/shopify_products.csv")
PREVIEW_FILE = Path("output/shopify_products_preview.csv")
NUM_PRODUCTS = 200  # The number of unique products (Handles) to include

def create_preview_csv():
    """Reads the full CSV and writes a verbatim preview of the first N products."""
    if not INPUT_FILE.exists():
        print(f"Error: Input file not found at '{INPUT_FILE}'")
        print("Please run the main conversion script first.")
        return

    try:
        # STEP 1: Use pandas to efficiently find the target handles.
        print(f"Identifying the first {NUM_PRODUCTS} products from '{INPUT_FILE}'...")
        # Read the 'Handle' column and drop any empty/NA values before finding unique ones
        df_handles = pd.read_csv(INPUT_FILE, usecols=['Handle'], dtype=str).dropna()
        # Get unique handles in the order they appear
        unique_handles_ordered = df_handles['Handle'].unique()
        # Create a set of the first N handles for fast lookups
        target_handles = set(unique_handles_ordered[:NUM_PRODUCTS])

        if not target_handles:
            print("Warning: No product handles found in the input file.")
            # Ensure the preview file is created but empty
            with open(PREVIEW_FILE, 'w', encoding='utf-8') as fout:
                # Get the header from the original file
                with open(INPUT_FILE, 'r', encoding='utf-8') as fin:
                    fout.write(fin.readline())
            return

        # STEP 2: Manually parse the file to copy full, multi-line records.
        print(f"Copying all rows for {len(target_handles)} products to '{PREVIEW_FILE}'...")
        
        lines_written = 0
        with open(INPUT_FILE, 'r', encoding='utf-8') as fin, \
             open(PREVIEW_FILE, 'w', encoding='utf-8') as fout:

            # Always write the header line.
            header = fin.readline()
            fout.write(header)
            lines_written += 1

            record_buffer = []
            is_copying_current_product = False # Flag to track if we're in a target product block

            for line in fin:
                record_buffer.append(line)

                # A valid CSV record is complete when it has an even number of quotes.
                full_record_text = "".join(record_buffer)
                
                if full_record_text.count('"') % 2 == 0:
                    # We have a complete record. Now, check its handle.
                    first_line_of_record = record_buffer[0]
                    
                    # Split safely, handling potential empty strings
                    parts = first_line_of_record.split(',', 1)
                    handle = parts[0].strip('"') if parts else ""

                    if handle:
                        # This is a new product's main row. Decide if we should start copying.
                        if handle in target_handles:
                            is_copying_current_product = True
                        else:
                            # We've hit a product that is NOT a target, so stop copying.
                            # This is especially important to stop after the Nth product.
                            is_copying_current_product = False
                    
                    # If the flag is set, it means we are in a block that should be copied.
                    # This applies to the main row of a target product AND all its
                    # subsequent variant/image rows (which have blank handles).
                    if is_copying_current_product:
                        fout.write(full_record_text)
                        lines_written += len(record_buffer)

                    # Reset the buffer for the next record.
                    record_buffer = []

        print(f"Done. Wrote {lines_written} total lines to the preview file successfully.")

    except KeyError:
        print("Error: The input CSV does not contain a 'Handle' column.")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    create_preview_csv()