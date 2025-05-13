#!/usr/bin/env python3
"""
Script to merge two dl-normal-item files from the sample folder.
The script will:
1. Check if both files have the same headers
2. If headers match, merge the files
3. Save the merged data to output/rakuten-sample.csv
"""

import pandas as pd
import os
from typing import List, Tuple
from pathlib import Path

def check_headers(file1: str, file2: str) -> Tuple[bool, List[str], List[str]]:
    """
    Check if two CSV files have the same headers.
    
    Args:
        file1: Path to the first CSV file
        file2: Path to the second CSV file
        
    Returns:
        Tuple containing:
        - Boolean indicating if headers match
        - List of headers from first file
        - List of headers from second file
    """
    try:
        # Read just the headers from both files
        df1 = pd.read_csv(file1, encoding='shift-jis', nrows=0)
        df2 = pd.read_csv(file2, encoding='shift-jis', nrows=0)
        
        headers1 = list(df1.columns)
        headers2 = list(df2.columns)
        
        # Check if headers match
        headers_match = set(headers1) == set(headers2)
        
        return headers_match, headers1, headers2
        
    except Exception as e:
        print(f"Error reading files: {str(e)}")
        return False, [], []

def merge_files(file1: str, file2: str, output_file: str) -> None:
    """
    Merge two CSV files and save to output file.
    
    Args:
        file1: Path to the first CSV file
        file2: Path to the second CSV file
        output_file: Path to save the merged data
    """
    try:
        # Read both files
        print(f"Reading {file1}...")
        df1 = pd.read_csv(file1, encoding='shift-jis')
        print(f"Found {len(df1)} rows in {file1}")
        
        print(f"Reading {file2}...")
        df2 = pd.read_csv(file2, encoding='shift-jis')
        print(f"Found {len(df2)} rows in {file2}")
        
        # Merge the DataFrames
        print("Merging files...")
        merged_df = pd.concat([df1, df2], ignore_index=True)
        print(f"Total rows after merging: {len(merged_df)}")
        
        # Ensure output directory exists
        output_dir = Path(output_file).parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save to CSV
        print(f"Saving merged data to {output_file}...")
        merged_df.to_csv(output_file, index=False, encoding='utf-8')
        
        print(f"Successfully saved merged data to {output_file}")
        
    except Exception as e:
        print(f"Error merging files: {str(e)}")
        return

def main():
    # Define file paths
    sample_dir = Path('sample')
    file1 = sample_dir / 'dl-normal-item_20250422165116-1.csv'
    file2 = sample_dir / 'dl-normal-item_20250422170300-1.csv'
    output_file = Path('output') / 'rakuten-sample.csv'
    
    # Check if input files exist
    if not file1.exists():
        print(f"Error: File {file1} does not exist.")
        return
    if not file2.exists():
        print(f"Error: File {file2} does not exist.")
        return
    
    # Check headers
    print("Checking file headers...")
    headers_match, headers1, headers2 = check_headers(file1, file2)
    
    if not headers_match:
        print("\nHeaders do not match!")
        print("\nHeaders in first file:")
        for header in headers1:
            print(f"- {header}")
        print("\nHeaders in second file:")
        for header in headers2:
            print(f"- {header}")
        print("\nDifference in first file:")
        for header in set(headers1) - set(headers2):
            print(f"- {header}")
        print("\nDifference in second file:")
        for header in set(headers2) - set(headers1):
            print(f"- {header}")
        return
    
    print("\nHeaders match! Proceeding with merge...")
    
    # Merge files
    merge_files(file1, file2, output_file)

if __name__ == "__main__":
    main() 