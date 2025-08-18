import csv
from pathlib import Path

# --- Configuration ---

# 1. Path to your main Shopify product export CSV.
#    *** YOU MUST UPDATE THIS FILENAME. ***
main_csv_path = Path("output/4_final_sorted_products.csv")

# 2. Path to the CSV containing the product types (generated from the previous step).
types_csv_path = Path("output/final_consolidated_products_for_shopify.csv")

# 3. Name of the final, merged output file. This file will be created by the script.
output_csv_path = Path("output/shopify_products_with_types.csv")

# --- Main Logic ---

# --- Step 1: Load the product types into a lookup map for fast access ---
product_type_map = {}
print(f"Loading product types from '{types_csv_path}'...")
try:
    with types_csv_path.open(mode='r', encoding='utf-8-sig', newline='') as types_file:
        reader = csv.DictReader(types_file)
        for row in reader:
            # Assumes the headers are 'handler' and 'product_type'
            product_type_map[row['handler']] = row['product_type']
    print(f"-> Found {len(product_type_map)} product types to apply.")
except FileNotFoundError:
    print(f"❌ Error: The types file was not found at '{types_csv_path}'. Please ensure the file exists.")
    exit()

# --- Step 2: Read the main CSV, update the 'Type' column, and write to a new file ---
updated_row_count = 0
total_row_count = 0

print(f"\nProcessing main product file '{main_csv_path}'...")
try:
    with main_csv_path.open(mode='r', encoding='utf-8-sig', newline='') as infile, \
         output_csv_path.open(mode='w', encoding='utf-8-sig', newline='') as outfile:
        
        reader = csv.DictReader(infile)
        
        # Get the headers from the original file to use in the output file
        headers = reader.fieldnames
        if 'Handle' not in headers or 'Type' not in headers:
            print(f"❌ Error: The main CSV '{main_csv_path}' is missing 'Handle' or 'Type' columns.")
            exit()
            
        writer = csv.DictWriter(outfile, fieldnames=headers)
        writer.writeheader()
        
        # Process each row
        for row in reader:
            total_row_count += 1
            handle = row['Handle']
            
            # Check if this handle has a new type in our map
            if handle in product_type_map:
                # If the current type is different from the new type, update it
                if row['Type'] != product_type_map[handle]:
                    print(f"  - Updating Handle '{handle}': Type set to '{product_type_map[handle]}'")
                    row['Type'] = product_type_map[handle]
                    updated_row_count += 1
            
            # Write the row (either updated or original) to the new file
            writer.writerow(row)

    print("\n--- Summary ---")
    print(f"Total rows processed: {total_row_count}")
    print(f"Product types updated: {updated_row_count}")
    print(f"✅ Success! Merged data has been saved to '{output_csv_path}'.")
    print("\nYou can now import this new file into Shopify.")

except FileNotFoundError:
    print(f"❌ Error: The main product file was not found at '{main_csv_path}'. Please check the filename.")
except Exception as e:
    print(f"❌ An unexpected error occurred: {e}")