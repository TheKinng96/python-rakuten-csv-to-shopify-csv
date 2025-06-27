import csv
import re
from pathlib import Path

# --- Configuration ---

# 1. Input file: The result from the first script.
input_file_path = Path("output/product_types_for_shopify.csv")

# 2. Output file: The final, clean, and consolidated list.
output_file_path = Path("output/final_consolidated_products_for_shopify.csv")

# 3. Regular expression to find and remove numeric set notations like -2s, -12s, etc.
numeric_set_pattern = re.compile(r'-\d+s$')


def get_base_handler(handler: str) -> str:
    """
    Takes a product handler and returns its base version by stripping set notations.
    
    Examples:
        'zuisen-usa720-2s'  -> 'zuisen-usa720'
        'another-item-ss'   -> 'another-item'
        'base-product-123'  -> 'base-product-123' (no change)
    """
    # First, try to remove the numeric set pattern (e.g., -2s, -10s)
    base_handler = numeric_set_pattern.sub('', handler)
    
    # If that didn't change the string, check for '-ss' at the end.
    if base_handler == handler and handler.endswith('-ss'):
        base_handler = handler[:-3]
        
    return base_handler


# --- Main Logic ---

# We use a dictionary to store the results. This automatically handles duplicates,
# ensuring we only have one entry per unique base handler.
# Format: {base_handler: product_type}
base_product_map = {}
original_row_count = 0

print("--- Starting Product Consolidation ---")

# --- Step 1: Read the CSV and consolidate products ---
try:
    with input_file_path.open(mode='r', encoding='utf-8-sig', newline='') as infile:
        reader = csv.DictReader(infile)
        
        if 'handler' not in reader.fieldnames:
            print(f"❌ Error: Input file '{input_file_path}' is missing the 'handler' column.")
            exit()
            
        print(f"Reading from '{input_file_path}' and finding base products...")
        
        for row in reader:
            original_row_count += 1
            original_handler = row['handler']
            product_type = row['product_type']
            
            # Get the base version of the handler.
            base_handler = get_base_handler(original_handler)
            
            # If the base handler is different, show the transformation.
            if base_handler != original_handler:
                print(f"  - Consolidating '{original_handler}' -> '{base_handler}'")
            
            # Store the base handler and its type in our map.
            # If the base handler already exists, this will just overwrite it with the
            # same product type, effectively de-duplicating our list.
            base_product_map[base_handler] = product_type

except FileNotFoundError:
    print(f"❌ Error: The input file '{input_file_path}' was not found.")
    print("Please make sure you have run the previous script first.")
    exit()
except Exception as e:
    print(f"❌ An unexpected error occurred: {e}")
    exit()

# --- Step 2: Write the consolidated data to a new CSV file ---
if not base_product_map:
    print("\nNo products were processed. The output file will not be created.")
else:
    print(f"\nWriting {len(base_product_map)} unique base products to '{output_file_path}'...")
    try:
        with output_file_path.open(mode='w', encoding='utf-8-sig', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=['handler', 'product_type'])
            writer.writeheader()
            
            for handler, product_type in base_product_map.items():
                writer.writerow({'handler': handler, 'product_type': product_type})

        print("\n--- Summary ---")
        print(f"Original product entries processed: {original_row_count}")
        print(f"Final unique base products: {len(base_product_map)}")
        print(f"✅ Success! Consolidated data has been saved to '{output_file_path}'.")

    except Exception as e:
        print(f"❌ An error occurred while writing the output file: {e}")