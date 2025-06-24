"""
Reformat a CSV to ensure specific empty fields are quoted.

Reads a CSV file (e.g., one downloaded from Google Sheets) and writes a new
version where empty values in designated columns are formatted as '""'
(an empty quoted string) instead of being completely empty.

Usage:
  1. Set the INPUT_FILE path to your downloaded CSV.
  2. Run the script: python scripts/reformat_csv.py
"""

import csv
from pathlib import Path

# --- Configuration ---

# The file you downloaded from Google Sheets or another source
INPUT_FILE = Path("output/ok-products.csv") 

# The name of the final, correctly formatted output file
OUTPUT_FILE = Path("output/shopify_products_reformatted.csv")

# The list of column headers that require '""' when empty
COLUMNS_TO_QUOTE_IF_EMPTY = {
    'Type', 
    'Tags', 
    'Variant Barcode'
}

def reformat_csv():
    """Reads a CSV and reformats specific empty fields to be '""'."""
    if not INPUT_FILE.exists():
        print(f"Error: Input file not found at '{INPUT_FILE}'")
        return

    print(f"Reading '{INPUT_FILE}' to reformat...")

    # Open both the input and output files
    with open(INPUT_FILE, 'r', newline='', encoding='utf-8') as fin, \
         open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as fout:
        
        # Use Python's built-in CSV reader
        reader = csv.reader(fin)
        
        # Get the header row to identify column indexes
        try:
            header = next(reader)
        except StopIteration:
            print("Error: Input file is empty.")
            return

        # Find the integer index for each column we need to check
        # This is more robust than assuming a fixed order
        indexes_to_check = {
            i for i, h in enumerate(header) if h in COLUMNS_TO_QUOTE_IF_EMPTY
        }

        # Write the header to the new file
        fout.write(",".join(header) + "\n")

        # Process each data row
        for row in reader:
            reformatted_row = []
            for i, cell in enumerate(row):
                # Check if the current column index is one we need to special-case
                if i in indexes_to_check and cell.strip() == '':
                    # If it is, and the cell is empty, use '""'
                    reformatted_row.append('""')
                else:
                    # Otherwise, handle the cell normally
                    # This logic ensures that values with commas or quotes are handled correctly
                    if '"' in cell or ',' in cell or '\n' in cell:
                        # Re-quote the value if it contains special characters
                        reformatted_row.append(f'"{cell.replace("\"", "\"\"")}"')
                    else:
                        reformatted_row.append(cell)
            
            # Write the newly constructed line to the output file
            fout.write(",".join(reformatted_row) + "\n")

    print(f"Done. Reformatted file saved to '{OUTPUT_FILE}'")


if __name__ == "__main__":
    reformat_csv()