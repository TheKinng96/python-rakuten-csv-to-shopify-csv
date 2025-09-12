#!/usr/bin/env python3
"""
Extract tax-related data from Rakuten CSV file.

This script extracts only the following columns from the Rakuten CSV:
- 商品管理番号（商品URL） (Product Management Number/URL)
- 商品番号 (Product Number)
- 商品名 (Product Name)
- SKU管理番号 (SKU Management Number)
- 消費税 (Consumption Tax)
- 消費税率 (Tax Rate)

Usage:
    python extract_tax_data.py [input_file] [output_file]

If no arguments provided, uses default paths.
"""

import pandas as pd
import sys
from pathlib import Path
import argparse


def extract_tax_data(input_file: str, output_file: str):
    """
    Extract tax data from Rakuten CSV file.
    
    Args:
        input_file: Path to input Rakuten CSV file
        output_file: Path to output CSV file
    """
    print(f"Processing: {input_file}")
    print(f"Output to: {output_file}")
    
    try:
        # Read the CSV file with proper encoding
        print("Reading CSV file...")
        df = pd.read_csv(input_file, encoding='shift_jis', low_memory=False)
        print(f"Loaded {len(df)} rows")
        
        # Display column information
        print(f"Total columns: {len(df.columns)}")
        print("First 10 columns:")
        for i, col in enumerate(df.columns[:10]):
            print(f"  {i}: {col}")
        
        # Extract the required columns
        # Based on the structure: columns 0, 1, 2, 5, 6, 216
        required_columns = [
            df.columns[0],    # 商品管理番号（商品URL）
            df.columns[1],    # 商品番号
            df.columns[2],    # 商品名
            df.columns[216],  # SKU管理番号
            df.columns[5],    # 消費税
            df.columns[6]     # 消費税率
        ]
        
        print(f"\nExtracting columns:")
        for i, col in enumerate(required_columns):
            print(f"  {i}: {col}")
        
        # Create new dataframe with only required columns
        df_extracted = df[required_columns].copy()
        
        # Rename columns for clarity
        df_extracted.columns = [
            '商品管理番号',
            '商品番号',
            '商品名',
            'SKU管理番号',
            '消費税',
            '消費税率'
        ]
        
        # Display statistics
        print(f"\nData statistics:")
        print(f"  Total products: {len(df_extracted)}")
        
        # Count non-null tax information
        tax_flag_count = df_extracted['消費税'].notna().sum()
        tax_rate_count = df_extracted['消費税率'].notna().sum()
        
        print(f"  Products with tax flag: {tax_flag_count}")
        print(f"  Products with tax rate: {tax_rate_count}")
        
        # Show tax rate distribution
        if tax_rate_count > 0:
            tax_rates = df_extracted['消費税率'].value_counts().sort_index()
            print(f"  Tax rate distribution:")
            for rate, count in tax_rates.items():
                if pd.notna(rate):
                    percentage = (rate * 100) if rate <= 1 else rate
                    print(f"    {percentage}%: {count} products")
        
        # Show sample data
        print(f"\nSample data (first 5 rows):")
        print(df_extracted.head().to_string(index=False, max_colwidth=50))
        
        # Save to CSV
        print(f"\nSaving to: {output_file}")
        df_extracted.to_csv(output_file, index=False, encoding='utf-8')
        
        print("✅ Export completed successfully!")
        
        # Show summary
        print(f"\nSummary:")
        print(f"  Input file: {input_file}")
        print(f"  Output file: {output_file}")
        print(f"  Total records: {len(df_extracted)}")
        print(f"  Records with tax info: {max(tax_flag_count, tax_rate_count)}")
        
    except FileNotFoundError:
        print(f"❌ Error: Input file not found: {input_file}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error processing file: {e}")
        sys.exit(1)


def main():
    """Main function with command line argument parsing."""
    
    parser = argparse.ArgumentParser(
        description="Extract tax data from Rakuten CSV file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python extract_tax_data.py
  python extract_tax_data.py input.csv output.csv
  python extract_tax_data.py --input rakuten_item.csv --output tax_data.csv
        """
    )
    
    parser.add_argument(
        'input_file',
        nargs='?',
        help='Input Rakuten CSV file path'
    )
    
    parser.add_argument(
        'output_file', 
        nargs='?',
        help='Output CSV file path'
    )
    
    parser.add_argument(
        '--input', '-i',
        dest='input_override',
        help='Input file path (alternative to positional arg)'
    )
    
    parser.add_argument(
        '--output', '-o',
        dest='output_override', 
        help='Output file path (alternative to positional arg)'
    )
    
    args = parser.parse_args()
    
    # Determine input and output files
    script_dir = Path(__file__).parent
    
    # Default paths
    default_input = script_dir.parent.parent.parent / "csv-conversion" / "data" / "rakuten_item.csv"
    default_output = script_dir.parent / "output" / "rakuten_tax_data.csv"
    
    # Use command line arguments or defaults
    input_file = (
        args.input_override or 
        args.input_file or 
        str(default_input)
    )
    
    output_file = (
        args.output_override or 
        args.output_file or 
        str(default_output)
    )
    
    # Create output directory if it doesn't exist
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print("Rakuten Tax Data Extractor")
    print("=" * 40)
    
    # Run extraction
    extract_tax_data(input_file, output_file)


if __name__ == "__main__":
    main()