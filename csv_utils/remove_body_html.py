#!/usr/bin/env python3
"""
Script to remove specific columns from CSV files.
Commonly used to remove large or unnecessary columns like 'Body (HTML)' or 'カタログID (rakuten)'
before importing to Shopify.
"""

import csv
import os
import sys
from typing import List, Dict, Any, Optional

def modify_columns(
    input_file: str, 
    clear_columns: List[str] = None,
    remove_columns: List[str] = None,
    output_file: Optional[str] = None,
    encoding: str = 'utf-8'
) -> str:
    """
    Process a CSV file to modify columns by either clearing their content or removing them entirely.
    
    Args:
        input_file: Path to the input CSV file
        clear_columns: List of column names to clear (keeps column but empties content)
        remove_columns: List of column names to remove (removes entire column)
        output_file: Path to the output CSV file (optional)
        encoding: File encoding (default: 'utf-8')
        
    Returns:
        str: Path to the output file
    """
    if not clear_columns and not remove_columns:
        raise ValueError("At least one column must be specified in either clear_columns or remove_columns")
    
    clear_columns = clear_columns or []
    remove_columns = remove_columns or []
    
    # Set default output filename if not provided
    if output_file is None:
        base, ext = os.path.splitext(input_file)
        action_parts = []
        if clear_columns:
            action_parts.append("cleared_" + "_".join([col.split()[0] for col in clear_columns[:2]]))
        if remove_columns:
            action_parts.append("removed_" + "_".join([col.split()[0] for col in remove_columns[:2]]))
        
        action_str = "_".join(action_parts)
        if len(clear_columns) + len(remove_columns) > 2:
            action_str = action_str.split('_')[0] + "_columns"
            
        output_file = f"{base}_{action_str}{ext}"
    
    # Check if input file exists
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file '{input_file}' does not exist.")
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    try:
        # Open the input and output files
        with open(input_file, 'r', encoding=encoding, newline='', errors='replace') as infile, \
             open(output_file, 'w', encoding=encoding, newline='') as outfile:
            
            reader = csv.DictReader(infile)
            
            # Get the fieldnames and remove columns that should be removed
            fieldnames = [field for field in reader.fieldnames if field not in remove_columns]
            
            # Check which columns were actually found
            existing_clear_columns = set(clear_columns) & set(reader.fieldnames)
            existing_remove_columns = set(remove_columns) & set(reader.fieldnames)
            
            # Print warnings for columns that weren't found
            if clear_columns and not existing_clear_columns:
                print("Warning: None of the columns to clear were found in the CSV file.")
            if remove_columns and not existing_remove_columns:
                print("Warning: None of the columns to remove were found in the CSV file.")
            
            # Write the output file with the updated headers
            writer = csv.DictWriter(outfile, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            
            # Process each row
            for row in reader:
                # Clear the content of columns that should be cleared
                for col in existing_clear_columns:
                    if col in row:
                        row[col] = ''
                
                # Remove columns that should be removed
                for col in existing_remove_columns:
                    if col in row:
                        del row[col]
                
                writer.writerow(row)
        
        # Prepare summary message
        messages = []
        if existing_clear_columns:
            cleared_list = ", ".join(f"'{col}'" for col in sorted(existing_clear_columns))
            messages.append(f"Cleared content of: {cleared_list}")
        if existing_remove_columns:
            removed_list = ", ".join(f"'{col}'" for col in sorted(existing_remove_columns))
            messages.append(f"Removed columns: {removed_list}")
        
        print("\n".join(messages))
        print(f"\nOutput saved to: {output_file}")
        return output_file
        
    except Exception as e:
        raise Exception(f"Error processing CSV file: {str(e)}")

def main():
    """Handle command line interface."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Modify CSV files by clearing or removing columns.',
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
