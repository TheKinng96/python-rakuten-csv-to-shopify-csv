"""
Utilities for manipulating large Shopify CSV exports.

This script contains functions to:
1.  create_preview_csv: Create a preview of the first N products.
2.  split_csv_by_rows: Split a large CSV into smaller files with a specified
    number of rows, while correctly handling multi-line records.

The key method for both is manually parsing the source file record-by-record,
correctly handling multi-line fields by counting quotes to determine the
end of a full record.
"""
import sys
from pathlib import Path
import pandas as pd

# --- Configuration for the NEW function ---
# You can change these values to test the new function
INPUT_FILE = Path("output/shopify_products.csv")
OUTPUT_DIR = Path("output/split_files_processed/")
ROWS_PER_FILE = 10000  # The target number of rows per output file

# --- Original function (for reference) ---
def create_preview_csv_original():
    """Reads the full CSV and writes a verbatim preview of the first 20 products."""
    PREVIEW_FILE = Path("output/shopify_products_preview.csv")
    NUM_PRODUCTS = 1000

    if not INPUT_FILE.exists():
        print(f"Error: Input file not found at '{INPUT_FILE}'")
        return

    try:
        print(f"Identifying the first {NUM_PRODUCTS} products from '{INPUT_FILE}'...")
        df_handles = pd.read_csv(INPUT_FILE, usecols=['Handle'], dtype=str)
        unique_handles = df_handles['Handle'].unique()
        target_handles = set(unique_handles[:NUM_PRODUCTS])

        if not target_handles:
            print("Warning: No product handles found in the input file.")
            PREVIEW_FILE.write_text("")
            return

        print(f"Copying all rows for {len(target_handles)} products to '{PREVIEW_FILE}'...")
        lines_written = 0
        with open(INPUT_FILE, 'r', encoding='utf-8') as fin, \
             open(PREVIEW_FILE, 'w', encoding='utf-8') as fout:
            header = fin.readline()
            fout.write(header)
            lines_written += 1
            record_buffer = []
            for line in fin:
                record_buffer.append(line)
                full_record_text = "".join(record_buffer)
                if full_record_text.count('"') % 2 == 0:
                    handle = record_buffer[0].split(',', 1)[0]
                    if handle in target_handles:
                        fout.write(full_record_text)
                        lines_written += len(record_buffer)
                    record_buffer = []
        print(f"Done. Wrote {lines_written} total lines to '{PREVIEW_FILE}'.")
    except Exception as e:
        print(f"An error occurred: {e}")

# --- UPDATED FUNCTION ---
def split_csv_by_rows(input_path: Path, output_dir: Path, rows_per_file: int, output_prefix: str = "part"):
    """
    Splits a large CSV file into smaller files based on a specified number of rows.

    This function correctly handles multi-line CSV fields, ensuring that a single
    record (like a product with its variants) is never split across two files.

    Args:
        input_path (Path): The path to the large source CSV file.
        output_dir (Path): The directory where the split files will be saved.
        rows_per_file (int): The target number of data rows for each split file.
        output_prefix (str): The prefix for the output filenames (e.g., "part").
    """
    if not input_path.exists():
        print(f"Error: Input file not found at '{input_path}'")
        return

    # Create the output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Splitting '{input_path}' into files with ~{rows_per_file} rows each.")
    print(f"Output will be saved in: '{output_dir}'")

    fout = None  # Holds the current output file handle
    try:
        with open(input_path, 'r', encoding='utf-8') as fin:
            header = fin.readline()
            if not header:
                print("Error: Input file is empty.")
                return

            file_count = 0
            rows_in_current_file = 0
            total_lines_processed = 1 # Start with 1 for the header
            record_buffer = []

            for line in fin:
                total_lines_processed += 1
                record_buffer.append(line)

                # A valid CSV record is complete when it has an even number of quotes.
                full_record_text = "".join(record_buffer)
                if full_record_text.count('"') % 2 == 0:
                    # We have a complete record. Now decide where to write it.

                    # If we need to start a new file (it's the first record, or the current file is full)
                    if fout is None or rows_in_current_file >= rows_per_file:
                        if fout:
                            fout.close()
                            print(f"  -> Finished writing part_{file_count}.csv with {rows_in_current_file} rows.")

                        file_count += 1
                        output_path = output_dir / f"{output_prefix}_{file_count}.csv"
                        fout = open(output_path, 'w', encoding='utf-8')
                        fout.write(header)
                        rows_in_current_file = 0

                    # Write the complete record to the current output file
                    fout.write(full_record_text)
                    rows_in_current_file += len(record_buffer)

                    # Reset the buffer for the next record
                    record_buffer = []

            if fout:
                print(f"  -> Finished writing part_{file_count}.csv with {rows_in_current_file} rows.")

        print(f"\nDone. Processed {total_lines_processed} total lines from the input file.")
        print(f"Created {file_count} split file(s) in '{output_dir}'.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
    finally:
        # Ensure the last file handle is always closed
        if fout and not fout.closed:
            fout.close()

if __name__ == "__main__":
    # This block demonstrates how to use the new function.
    # To run it, save the code as a Python file (e.g., `process_csv.py`)
    # and execute `python process_csv.py` from your terminal.

    # Make sure INPUT_FILE exists and OUTPUT_DIR is where you want the files.
    if not INPUT_FILE.exists():
        print(f"Error: The input file '{INPUT_FILE}' was not found.")
        print("Please create a dummy file or point it to your actual CSV.")
    else:
        split_csv_by_rows(
            input_path=INPUT_FILE,
            output_dir=OUTPUT_DIR,
            rows_per_file=ROWS_PER_FILE
        )