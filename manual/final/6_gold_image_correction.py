import csv
import sys
from pathlib import Path

# --- CONFIGURATION ---
OUTPUT_DIR = Path("output")

# 1. The name of your input CSV file with the incorrect URLs.
input_csv_filename = OUTPUT_DIR / 'output_problematic_products.csv' 

# 2. The name for the new, corrected output file.
output_csv_filename = OUTPUT_DIR / 'output_problematic_products_fixed.csv'

# 3. Define the string patterns for the find-and-replace operation.
#    This is the part of the URL that is wrong.
incorrect_pattern = 'tsutsu-uraura/gold/'
#    This is what it should be replaced with.
correct_pattern = 'gold/tsutsu-uraura/'


# --- SCRIPT LOGIC ---

columns_to_correct = [
    'Image Src',
    'Variant Image'
]

def fix_image_urls_in_csv(input_file, output_file, find_str, replace_str, column_names):
    """
    Reads a CSV, fixes incorrect URL patterns in multiple specified columns, 
    and writes to a new file.
    """
    print("Starting URL correction process...")
    print(f"Input file:  '{input_file}'")
    print(f"Output file: '{output_file}'")
    print("-" * 20)
    print(f"Finding all instances of: '{find_str}'")
    print(f"Replacing with:          '{replace_str}'")
    print(f"Checking in columns:     {column_names}")
    print("-" * 20)
    
    try:
        with open(input_file, mode='r', encoding='utf-8-sig') as infile, \
             open(output_file, mode='w', encoding='utf-8-sig', newline='') as outfile:

            reader = csv.DictReader(infile)
            
            # --- Header Validation ---
            # Verify that all specified columns actually exist in the CSV file
            for col_name in column_names:
                if col_name not in reader.fieldnames:
                    print(f"Error: The CSV file is missing a required column: '{col_name}'.")
                    print("Please check the 'columns_to_correct' list in the script.")
                    sys.exit(1)

            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            writer.writeheader()

            total_replacements = 0
            total_rows_processed = 0

            # --- Row Processing ---
            for row in reader:
                # Loop through each column we need to check for this row
                for col in column_names:
                    original_url = row.get(col, '')
                    
                    # Check if the URL is not empty and contains the incorrect pattern
                    if original_url and find_str in original_url:
                        # Replace the pattern and update the row's data
                        row[col] = original_url.replace(find_str, replace_str)
                        total_replacements += 1
                
                # Write the row to the new file (it's either modified or original)
                writer.writerow(row)
                total_rows_processed += 1
        
        print("\n--- Process Complete ---")
        print(f"Total rows processed: {total_rows_processed}")
        print(f"Total URL replacements made: {total_replacements}")
        print(f"Corrected data saved to '{output_file}'.")

    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found.")
        print("Please make sure the script is in the same directory as your CSV file and the filename is correct.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# --- Run the main function ---
if __name__ == "__main__":
    fix_image_urls_in_csv(input_csv_filename, output_csv_filename, incorrect_pattern, correct_pattern, columns_to_correct)