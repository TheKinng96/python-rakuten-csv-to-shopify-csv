import csv
import sys
from pathlib import Path
from problematic_images import PROBLEM_IMAGE_SRCS as problem_image_srcs

# --- CONFIGURATION ---
OUT_DIR = Path("output")

# 1. The name of your input CSV file.
input_csv_filename = OUT_DIR / '4_final_sorted_products.csv' 

# 2. The name of the file where the results will be saved.
output_csv_filename = OUT_DIR / 'output_problematic_products.csv'

# --- SCRIPT LOGIC (No need to edit below this line) ---

def filter_csv_by_problematic_images(input_file, output_file, problem_images):
    """
    Filters a CSV to extract all rows that share a 'Handle' with rows 
    containing a problematic 'Image Src'.
    """
    # Use a set for efficient lookup of problem images and handles
    problem_images_set = set(problem_images)
    problematic_handles = set()

    print(f"Step 1: Identifying handles linked to {len(problem_images_set)} problematic images...")

    try:
        # First pass: Find all unique handles associated with the problem images
        with open(input_file, mode='r', encoding='utf-8-sig') as infile:
            # DictReader is great because we can access columns by name
            reader = csv.DictReader(infile)
            headers = reader.fieldnames # Save the headers for later
            
            # Ensure required columns exist
            if 'Handle' not in headers or 'Image Src' not in headers:
                print(f"Error: CSV must contain 'Handle' and 'Image Src' columns.")
                sys.exit(1)

            for row in reader:
                if row.get('Image Src') in problem_images_set:
                    handle = row.get('Handle')
                    if handle: # Make sure handle is not empty
                        problematic_handles.add(handle)

        if not problematic_handles:
            print("No matching handles found for the provided image URLs. No output file will be created.")
            return

        print(f"Found {len(problematic_handles)} unique handles to extract.")
        print("-" * 20)
        print(f"Step 2: Extracting all rows for these handles and writing to '{output_file}'...")

        # Second pass: Read the input file again and write all rows with matching handles
        rows_to_write = []
        with open(input_file, mode='r', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                if row.get('Handle') in problematic_handles:
                    rows_to_write.append(row)
        
        # Write the collected rows to the output file
        with open(output_file, mode='w', encoding='utf-8-sig', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows_to_write)
        
        print(f"\nSuccess! Wrote {len(rows_to_write)} rows to '{output_file}'.")

    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found.")
        print("Please make sure the script is in the same directory as your CSV file and the filename is correct.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# --- Run the main function ---
if __name__ == "__main__":
    filter_csv_by_problematic_images(input_csv_filename, output_csv_filename, problem_image_srcs)