#!/usr/bin/env python3
"""
Script to remove content columns from Rakuten CSV file.
This script removes the following columns:
- キャッチコピー (Catch copy)
- PC用商品説明文 (PC product description)
- スマートフォン用商品説明文 (Smartphone product description)
- PC用販売説明文 (PC sales description)
"""

import csv
import os
import sys
from typing import List, Dict, Any

# Columns to remove (in Japanese)
COLUMNS_TO_REMOVE = [
    "キャッチコピー",
    "PC用商品説明文",
    "スマートフォン用商品説明文",
    "PC用販売説明文"
]

def process_csv(input_file: str, output_file: str) -> None:
    """
    Process the CSV file to remove specified columns.
    
    Args:
        input_file: Path to the input CSV file
        output_file: Path to the output CSV file
    """
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' does not exist.")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    try:
        # Open the input file with Shift-JIS encoding
        with open(input_file, 'r', encoding='shift_jis', errors='replace') as infile:
            # Read the CSV file
            reader = csv.reader(infile)
            headers = next(reader)  # Get the header row
            
            # Find indices of columns to remove
            indices_to_remove = []
            for i, header in enumerate(headers):
                if header in COLUMNS_TO_REMOVE:
                    indices_to_remove.append(i)
            
            # Create new headers without the removed columns
            new_headers = [header for i, header in enumerate(headers) if i not in indices_to_remove]
            
            # Open the output file
            with open(output_file, 'w', encoding='shift_jis', newline='') as outfile:
                writer = csv.writer(outfile)
                
                # Write the new headers
                writer.writerow(new_headers)
                
                # Process each row
                row_count = 0
                for row in reader:
                    # Create new row without the removed columns
                    new_row = [value for i, value in enumerate(row) if i not in indices_to_remove]
                    writer.writerow(new_row)
                    row_count += 1
                    
                    # Print progress every 1000 rows
                    if row_count % 1000 == 0:
                        print(f"Processed {row_count} rows...")
                
                print(f"Completed! Processed {row_count} rows in total.")
                print(f"Removed {len(indices_to_remove)} columns: {', '.join(COLUMNS_TO_REMOVE)}")
                print(f"Output written to: {output_file}")
    
    except Exception as e:
        print(f"Error processing CSV file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Default input and output files
    input_file = "rakuten-sample.csv"
    output_file = "rakuten-sample-no-content.csv"
    
    # Allow command line arguments to override defaults
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    print(f"Processing {input_file}...")
    process_csv(input_file, output_file) 