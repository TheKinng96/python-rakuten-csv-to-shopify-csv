"""
Create a small preview CSV from a large one.

This script correctly reads the full CSV and creates a preview containing
the first N complete products, including all their multi-line variant and
image rows.

It achieves this by:
1. Using pandas to efficiently identify the first N product handles.
2. Manually parsing the source file record-by-record, correctly handling
   multi-line fields by counting quotes.
3. Writing the raw, unmodified text for each matching record, perfectly
   preserving all original formatting like `""`.

Usage:
  python scripts/create_preview.py
"""

from pathlib import Path
import pandas as pd

# --- Configuration ---
INPUT_FILE = Path("output/shopify_products_original_html.csv")
PREVIEW_FILE = Path("output/shopify_products_preview.csv")
NUM_PRODUCTS = 20  # The number of unique products (Handles) to include

def create_preview_csv():
    """Reads the full CSV and writes a verbatim preview of the first N products."""
    if not INPUT_FILE.exists():
        print(f"Error: Input file not found at '{INPUT_FILE}'")
        print("Please run the main conversion script first.")
        return

    try:
        # STEP 1: Use pandas to efficiently find the target handles.
        # This is fast as it only loads the 'Handle' column into memory.
        print(f"Identifying the first {NUM_PRODUCTS} products from '{INPUT_FILE}'...")
        df_handles = pd.read_csv(INPUT_FILE, usecols=['Handle'], dtype=str)
        unique_handles = df_handles['Handle'].unique()
        target_handles = set(unique_handles[:NUM_PRODUCTS]) # Use a Set for fast lookups

        if not target_handles:
            print("Warning: No product handles found in the input file.")
            PREVIEW_FILE.write_text("")
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
            for line in fin:
                record_buffer.append(line)

                # A valid CSV record is complete when it has an even number of quotes.
                # An odd number indicates a field is open and contains a newline.
                # We join the buffer to correctly count quotes across line breaks.
                full_record_text = "".join(record_buffer)
                
                if full_record_text.count('"') % 2 == 0:
                    # We have a complete record. Now, check its handle.
                    # The handle is always in the first line of the record.
                    handle = record_buffer[0].split(',', 1)[0]
                    
                    if handle in target_handles:
                        # Write the verbatim record and count the lines it contained.
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