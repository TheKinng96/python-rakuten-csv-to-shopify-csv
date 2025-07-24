import csv
import os
from pathlib import Path

# --- Configuration ---
# Use pathlib for clean, cross-platform path handling.
INPUT_FILE = Path("output/products_with_updated_types.csv")
OUTPUT_FILE = Path("output/new_products_with_types.csv")

# The exact name of the column we will check.
COLUMN_TO_CHECK = 'Type'


def filter_products_with_type():
    """
    Reads a single CSV file, filters for rows where the 'Type' column HAS a value,
    and writes these rows to a new CSV file with proper quoting.
    """
    # 1. --- Input Validation ---
    if not INPUT_FILE.is_file():
        print(f"FATAL ERROR: The input file was not found at '{INPUT_FILE}'")
        print("Please make sure the file exists and the path is correct.")
        return

    # 2. --- Prepare Output Directory ---
    # This ensures the 'output' folder exists before we try to write to it.
    try:
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"FATAL ERROR: Could not create the output directory '{OUTPUT_FILE.parent}'.")
        print(f"Error details: {e}")
        return

    print(f"Processing input file: '{INPUT_FILE}'")
    print(f"Writing output to:   '{OUTPUT_FILE}'\n")

    filtered_row_count = 0
    total_row_count = 0

    try:
        # 3. --- Process Files (Streamlined) ---
        with open(INPUT_FILE, mode='r', newline='', encoding='utf-8-sig') as infile, \
             open(OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as outfile:

            reader = csv.reader(infile)
            writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)

            # --- Handle Header ---
            header = next(reader)
            writer.writerow(header)

            try:
                type_column_index = header.index(COLUMN_TO_CHECK)
                print(f"Found '{COLUMN_TO_CHECK}' column at index {type_column_index}.")
            except ValueError:
                print(f"FATAL ERROR: Column '{COLUMN_TO_CHECK}' not found in the header.")
                print("Please check the column name in the script and your CSV.")
                return

            # --- Process Rows ---
            for row in reader:
                total_row_count += 1
                # Check if the row has enough columns to avoid an IndexError
                if len(row) > type_column_index:
                    
                    # --- KEY CHANGE IS HERE ---
                    # An empty string is 'falsy' and a non-empty string is 'truthy'.
                    # This condition now checks if the 'Type' value is NOT empty.
                    if row[type_column_index].strip():
                        writer.writerow(row)
                        filtered_row_count += 1
                # We intentionally skip rows that are too short, as they don't have a Type.

    except FileNotFoundError:
        print(f"FATAL ERROR: File '{INPUT_FILE}' could not be found.")
        return
    except Exception as e:
        print(f"An unexpected error occurred during processing: {e}")
        return

    # 4. --- Final Report ---
    print("\n--------------------")
    print("Processing Complete.")
    print(f"Processed {total_row_count} data rows.")
    # Updated the print statement to be accurate
    print(f"Found and wrote {filtered_row_count} records WITH a '{COLUMN_TO_CHECK}'.")
    print(f"Results saved to '{OUTPUT_FILE}'.")
    print("--------------------")


# This makes the script runnable from the command line
if __name__ == "__main__":
    filter_products_with_type()