import csv
from pathlib import Path

# --- Configuration ---

# The source file you are reading from.
INPUT_FILE = Path("output/products_without_type.csv")

# The new file that will be created with the updated types.
OUTPUT_FILE = Path("output/products_with_updated_types.csv")

# --- Column Name Definitions ---
# Defining these as constants makes the code cleaner and less prone to typos.
TYPE_COLUMN = 'Type'
ATTRIBUTES_COLUMN = '商品カテゴリー (product.metafields.custom.attributes)'

# --- Rule Definitions ---
# This structure makes it easy to add more rules in the future.
# Format: { 'keywords_to_find': 'new_type_value', ... }
# The script checks keywords in the order they appear here.
TYPE_UPDATE_RULES = {
    # If '飲料＆ドリンク' or 'ドリンク' is found, set Type to '飲料・ドリンク'
    ('飲料＆ドリンク', 'ドリンク'): '飲料・ドリンク',
    
    # If 'ワイン' is found, set Type to 'お酒・ワイン'
    ('ワイン', '日本酒', '焼酎','ウイスキー','日本酒・焼酎','ビール'): 'お酒・ワイン',
    
    # Add more rules here if needed, e.g.:
    # ('コーヒー', '珈琲'): 'コーヒー',
}


def update_product_types():
    """
    Reads a product CSV, updates the 'Type' column based on rules applied
    to the 'attributes' column, and writes to a new file.
    """
    # 1. --- Input Validation ---
    if not INPUT_FILE.is_file():
        print(f"FATAL ERROR: The input file was not found at '{INPUT_FILE}'")
        return

    # 2. --- Prepare Output Directory ---
    # Ensures the 'output' folder exists before we try to write to it.
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    print(f"Starting type update process...")
    print(f"  Input file:  '{INPUT_FILE}'")
    print(f"  Output file: '{OUTPUT_FILE}'\n")

    updated_row_count = 0
    total_row_count = 0

    try:
        # 3. --- Process Files (Streamlined Read/Write) ---
        with open(INPUT_FILE, mode='r', newline='', encoding='utf-8-sig') as infile, \
             open(OUTPUT_FILE, mode='w', newline='', encoding='utf-8') as outfile:

            reader = csv.reader(infile)
            # Use quoting=csv.QUOTE_ALL to ensure empty strings are written as "", e.g., `,"",`
            writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)

            # --- Handle Header ---
            header = next(reader)
            writer.writerow(header)

            # Find the indices of the columns we need to work with.
            try:
                type_col_idx = header.index(TYPE_COLUMN)
                attributes_col_idx = header.index(ATTRIBUTES_COLUMN)
                print(f"Found '{TYPE_COLUMN}' at index {type_col_idx}.")
                print(f"Found '{ATTRIBUTES_COLUMN}' at index {attributes_col_idx}.\n")
            except ValueError as e:
                print(f"FATAL ERROR: A required column is missing from the header: {e}")
                print("Please check the column names in the script and your CSV file.")
                return

            # --- Process Each Data Row ---
            for row in reader:
                total_row_count += 1
                # Ensure the row has enough columns to avoid an error
                if len(row) > max(type_col_idx, attributes_col_idx):
                    
                    attributes_text = row[attributes_col_idx]
                    original_type = row[type_col_idx]
                    
                    # Apply the rules in the defined order
                    for keywords, new_type in TYPE_UPDATE_RULES.items():
                        # Check if any of the keywords in the tuple are present
                        if any(keyword in attributes_text for keyword in keywords):
                            row[type_col_idx] = new_type
                            break # Stop checking once a match is found
                    
                    # Check if the type was actually changed to count it
                    if original_type != row[type_col_idx]:
                        updated_row_count += 1

                # Write the row to the output file, whether it was modified or not
                writer.writerow(row)

    except Exception as e:
        print(f"An unexpected error occurred during processing: {e}")
        return

    # 4. --- Final Report ---
    print("--------------------")
    print("Processing Complete.")
    print(f"Processed {total_row_count} data rows.")
    print(f"Updated the 'Type' for {updated_row_count} rows.")
    print(f"Results saved to '{OUTPUT_FILE}'.")
    print("--------------------")


# This makes the script runnable from the command line
if __name__ == "__main__":
    update_product_types()