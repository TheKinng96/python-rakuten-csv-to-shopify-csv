#!/usr/bin/env python3
"""
Script to remove specific columns from CSV files.
This version is specially designed to preserve the `""` format for specific
empty fields, which is often lost by standard CSV writers.

Commonly used to remove large or unnecessary columns like 'Body (HTML)' or 'カタログID (rakuten)'
before importing to Shopify.
"""

import csv
import os
import sys
from typing import List, Optional

# --- Configuration for Special Formatting ---
# Define which columns, when empty, should be written as `""` instead of `,,`
SPECIAL_QUOTED_EMPTY_FIELDS = {'Type', 'Tags', 'Variant Barcode'}

# --- Helper Function to Recreate Special Formatting ---
def format_csv_value(value: Optional[str], header_name: str) -> str:
    """
    Formats a single value for CSV output, applying special rules.
    - If the header is in SPECIAL_QUOTED_EMPTY_FIELDS and the value is empty, return '""'.
    - If any other value is empty, return ''.
    - If a value contains a comma or quote, wrap it in quotes and escape internal quotes.
    """
    # Rule 1: Handle our special empty fields that MUST be quoted.
    if header_name in SPECIAL_QUOTED_EMPTY_FIELDS and (value is None or value == ''):
        return '""'

    # Rule 2: Handle all other None/empty values (output as true empty).
    if value is None or value == '':
        return ''

    # Rule 3: Handle values that need standard CSV quoting.
    s_value = str(value)
    if '"' in s_value or ',' in s_value or '\n' in s_value:
        # Escape double quotes and wrap the whole thing in double quotes.
        return f'"{s_value.replace("\"", "\"\"")}"'

    # Rule 4: Value is simple and needs no special formatting.
    return s_value

def modify_columns(
    input_file: str,
    clear_columns: Optional[List[str]] = None,
    remove_columns: Optional[List[str]] = None,
    output_file: Optional[str] = None,
    encoding: str = 'utf-8'
) -> str:
    """
    Process a CSV file to modify columns, preserving special empty-field formatting.

    Args:
        input_file: Path to the input CSV file.
        clear_columns: List of column names to clear (keeps column but empties content).
        remove_columns: List of column names to remove (removes entire column).
        output_file: Path to the output CSV file (optional).
        encoding: File encoding (default: 'utf--8').

    Returns:
        str: Path to the output file.
    """
    if not clear_columns and not remove_columns:
        raise ValueError("At least one column must be specified in either clear_columns or remove_columns")

    clear_columns = set(clear_columns or [])
    remove_columns = set(remove_columns or [])

    # --- Set default output filename if not provided ---
    if output_file is None:
        base, ext = os.path.splitext(input_file)
        action_parts = []
        if clear_columns:
            action_parts.append("cleared")
        if remove_columns:
            action_parts.append("removed")
        action_str = "_".join(action_parts) + "_cols"
        output_file = f"{base}_{action_str}{ext}"

    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file '{input_file}' does not exist.")
    
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    try:
        with open(input_file, 'r', encoding=encoding, newline='', errors='replace') as infile, \
             open(output_file, 'w', encoding=encoding, newline='') as outfile:

            reader = csv.reader(infile)
            
            # --- Header Processing ---
            original_header = next(reader)
            
            # Identify indices of columns to clear/remove
            clear_indices = {i for i, h in enumerate(original_header) if h in clear_columns}
            remove_indices = {i for i, h in enumerate(original_header) if h in remove_columns}
            
            # Create the final output header
            output_header = [h for i, h in enumerate(original_header) if i not in remove_indices]
            
            # Write the new header
            outfile.write(",".join(output_header) + "\n")
            
            # --- Row Processing ---
            for row in reader:
                # Build the output row based on modification rules
                output_row_values = []
                for i, value in enumerate(row):
                    if i in remove_indices:
                        continue  # Skip removed columns
                    if i in clear_indices:
                        output_row_values.append('')  # Add an empty value for cleared columns
                    else:
                        output_row_values.append(value) # Keep original value
                
                # Format each value in the output row using our special rules
                formatted_values = [
                    format_csv_value(val, header)
                    for val, header in zip(output_row_values, output_header)
                ]
                
                # Write the manually formatted line
                outfile.write(",".join(formatted_values) + "\n")

        # --- Summary Message ---
        existing_clear = clear_columns & set(original_header)
        existing_remove = remove_columns & set(original_header)
        messages = []
        if existing_clear:
            messages.append(f"Cleared content of: {', '.join(sorted(list(existing_clear)))}")
        if existing_remove:
            messages.append(f"Removed columns: {', '.join(sorted(list(existing_remove)))}")
        
        print("\n".join(messages))
        print(f"\nOutput saved to: {output_file}")
        return output_file

    except Exception as e:
        # Provide more context on error
        import traceback
        traceback.print_exc()
        raise Exception(f"Error processing CSV file: {str(e)}")


def main():
    """Handle command line interface."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Modify CSV files by clearing or removing columns while preserving special "" formatting.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''Examples:
  # Clear content of specific columns (keeps the columns but empties them)
  python modify_columns.py input.csv --clear "Body (HTML)" "カタログID (rakuten)"
  
  # Remove specific columns entirely
  python modify_columns.py input.csv --remove "Unused Column" "Temporary Data"
  
  # Both clear and remove columns in one command
  python modify_columns.py input.csv --clear "Description" --remove "Internal ID"
  
  # Specify output file and encoding
  python modify_columns.py input.csv --output output.csv --encoding shift_jis --clear "Column1" --remove "Column2"
'''
    )
    parser.add_argument('input_file', help='Input CSV file path')
    parser.add_argument('--output', '-o', help='Output CSV file path (optional)')
    parser.add_argument('--encoding', '-e', default='utf-8', 
                       help='File encoding (default: utf-8)')
    parser.add_argument('--clear', nargs='+', default=[],
                       help='List of column names to clear (keeps columns but empties content)')
    parser.add_argument('--remove', nargs='+', default=[],
                       help='List of column names to remove (removes entire columns)')
    
    args = parser.parse_args()
    
    try:
        if not args.clear and not args.remove:
            parser.error('At least one of --clear or --remove must be specified')
            
        modify_columns(
            input_file=args.input_file,
            clear_columns=args.clear,
            remove_columns=args.remove,
            output_file=args.output,
            encoding=args.encoding
        )
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()