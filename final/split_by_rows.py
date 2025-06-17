# file: split_csv.py
"""
Splits a large Shopify CSV into multiple smaller files based on product count.

This script efficiently reads a large CSV only once, splitting it into multiple
output files. Each output file contains a specific number of complete products,
including all their multi-line variant and image rows, ensuring data integrity.

This version correctly handles product groups, ensuring that a product's main
row and all its child rows (variants, images) are kept together and never
split across two files.

Usage:
  python split_csv.py
"""

from pathlib import Path
import sys

# --- Configuration ---
INPUT_FILE = Path("output/4_final_sorted_products.csv")
OUTPUT_DIR = Path("output/split_by_product/")
PRODUCTS_PER_FILE = 1000  # The number of unique products to include in each split file

def split_csv_by_products_robustly(input_path: Path, output_dir: Path, products_per_file: int):
    """
    Reads a large Shopify CSV in a single pass and splits it into multiple
    files, ensuring that all rows for a given product stay together.
    """
    if not input_path.exists():
        print(f"Error: Input file not found at '{input_path}'", file=sys.stderr)
        return

    if products_per_file <= 0:
        print(f"Error: PRODUCTS_PER_FILE must be a positive integer.", file=sys.stderr)
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        with open(input_path, 'r', encoding='utf-8') as fin:
            header = fin.readline()
            if not header.strip():
                print("Error: Input file is empty or has no header.", file=sys.stderr)
                return

            # --- State for the streaming split process ---
            output_file = None
            file_count = 0
            products_in_current_file = 0
            last_seen_handle = None
            total_records_written = 0

            # Buffer to hold lines for a single, potentially multi-line, record
            record_buffer = []

            print(f"Starting robust split of '{input_path.name}'...")
            print(f"Each file will contain up to {products_per_file} products.")

            # --- Main loop to process the file record by record ---
            for line in fin:
                record_buffer.append(line)

                # A complete CSV record/row will have an even number of quotation marks.
                # This correctly groups multi-line fields (like HTML descriptions).
                if "".join(record_buffer).count('"') % 2 == 0:
                    # --- We have a complete record ---
                    full_record_text = "".join(record_buffer)
                    record_buffer = [] # Clear buffer for the next record

                    # Get handle from the first column. It's empty for variant/image rows.
                    current_handle = full_record_text.split(',', 1)[0].strip('"')
                    is_new_product = bool(current_handle and current_handle != last_seen_handle)

                    # *** CRUCIAL LOGIC BLOCK ***
                    # We decide whether to switch files *before* processing the current record.
                    # This check happens ONLY when we detect a brand-new product.
                    if is_new_product:
                        # If the current file is full, close it and prepare to open a new one.
                        if products_in_current_file >= products_per_file:
                            if output_file:
                                output_file.close()
                                print(f"  -> Completed '{output_filename.name}' with {products_in_current_file} products.")
                            output_file = None  # Mark that a new file is needed
                            products_in_current_file = 0

                        # If no file is open (either it's the very start or we just closed one),
                        # create the new file now.
                        if output_file is None:
                            file_count += 1
                            output_filename = output_dir / f"part_{file_count}.csv"
                            print(f"\nCreating new file: '{output_filename.name}'")
                            output_file = open(output_filename, 'w', encoding='utf-8')
                            output_file.write(header)

                        # A new product has been identified, so we update our state.
                        products_in_current_file += 1
                        last_seen_handle = current_handle

                    # --- Write the record to the currently open file ---
                    # This happens for EVERY record (main product, variant, or image).
                    if output_file:
                        output_file.write(full_record_text)
                        total_records_written += 1
                    elif not file_count:
                        # This case handles files with no valid product rows after the header
                        print("Warning: Skipping rows found before the first valid product Handle.", file=sys.stderr)

            # --- After the loop, close the last open file ---
            if output_file and not output_file.closed:
                output_file.close()
                print(f"  -> Completed '{output_filename.name}' with {products_in_current_file} products.")

            if total_records_written == 0:
                print("\nWarning: No data rows were found or written after the header.")
            else:
                print(f"\nDone. Split the input into {file_count} file(s) in '{output_dir.resolve()}'.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)

if __name__ == "__main__":
    split_csv_by_products_robustly(
        input_path=INPUT_FILE,
        output_dir=OUTPUT_DIR,
        products_per_file=PRODUCTS_PER_FILE
    )