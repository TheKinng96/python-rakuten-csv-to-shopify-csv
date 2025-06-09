#!/usr/bin/env python3
"""
Script to remove the Body (HTML) column from Shopify CSV files.
This is useful when the HTML content is too large for Shopify's import limits.
"""

import csv
import os
import sys
from typing import List, Dict, Any

def remove_body_html(input_file: str, output_file: str = None) -> str:
    """
    Process the Shopify CSV file to remove the Body (HTML) column.
    
    Args:
        input_file: Path to the input CSV file
        output_file: Path to the output CSV file (optional)
        
    Returns:
        str: Path to the output file
    """
    # Set default output filename if not provided
    if output_file is None:
        base, ext = os.path.splitext(input_file)
        output_file = f"{base}_no_body{ext}"
    
    # Check if input file exists
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file '{input_file}' does not exist.")
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    try:
        # Open the input and output files
        with open(input_file, 'r', encoding='utf-8', newline='') as infile, \
             open(output_file, 'w', encoding='utf-8', newline='') as outfile:
            
            reader = csv.DictReader(infile)
            
            # Get the fieldnames and remove 'Body (HTML)' if it exists
            fieldnames = [field for field in reader.fieldnames if field != 'Body (HTML)']
            
            # Write the output file with the updated headers
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Process each row
            for row in reader:
                # Remove the 'Body (HTML)' field if it exists
                if 'Body (HTML)' in row:
                    del row['Body (HTML)']
                writer.writerow(row)
        
        print(f"Successfully removed 'Body (HTML)' column. Output saved to: {output_file}")
        return output_file
        
    except Exception as e:
        raise Exception(f"Error processing CSV file: {str(e)}")

if __name__ == "__main__":
    # Check if input file is provided as command line argument
    if len(sys.argv) < 2:
        print("Usage: python remove_body_html.py <input_file> [output_file]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        remove_body_html(input_file, output_file)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
