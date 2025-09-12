#!/usr/bin/env python3
"""
Merge rows with same 商品管理番号 in tax data CSV.

This script:
1. Analyzes the tax data CSV for duplicate 商品管理番号
2. Merges rows that have exactly 2 entries by combining non-null values
3. Reports products with 3+ rows for manual review
4. Creates a cleaned output CSV

Usage:
    python merge_tax_data.py [input_file] [output_file]
"""

import pandas as pd
import sys
from pathlib import Path
import argparse
from collections import Counter


def analyze_duplicates(df: pd.DataFrame):
    """Analyze duplicate 商品管理番号 patterns."""
    
    print("Analyzing duplicate patterns...")
    
    # Count occurrences of each 商品管理番号
    mgmt_counts = df['商品管理番号'].value_counts()
    
    # Separate by count
    singles = mgmt_counts[mgmt_counts == 1]
    doubles = mgmt_counts[mgmt_counts == 2]
    multiples = mgmt_counts[mgmt_counts >= 3]
    
    print(f"  Products with 1 row: {len(singles)}")
    print(f"  Products with 2 rows: {len(doubles)}")
    print(f"  Products with 3+ rows: {len(multiples)}")
    
    if len(multiples) > 0:
        print(f"\n⚠️  Products with 3+ rows (manual review needed):")
        for mgmt_num, count in multiples.head(20).items():
            print(f"    {mgmt_num}: {count} rows")
        
        if len(multiples) > 20:
            print(f"    ... and {len(multiples) - 20} more")
    
    return singles, doubles, multiples


def merge_duplicate_rows(df: pd.DataFrame):
    """Merge rows with same 商品管理番号 by combining non-null values."""
    
    print("\nMerging duplicate rows...")
    
    # Group by 商品管理番号
    grouped = df.groupby('商品管理番号')
    
    merged_rows = []
    problematic_products = []
    
    for mgmt_num, group in grouped:
        if len(group) == 1:
            # Single row - keep as is
            merged_rows.append(group.iloc[0])
            
        elif len(group) == 2:
            # Two rows - merge by taking non-null values
            merged_row = merge_two_rows(group)
            merged_rows.append(merged_row)
            
        else:
            # 3+ rows - keep original and flag for review
            for _, row in group.iterrows():
                merged_rows.append(row)
            problematic_products.append(mgmt_num)
    
    # Create new dataframe
    merged_df = pd.DataFrame(merged_rows).reset_index(drop=True)
    
    print(f"  Merged {len(df)} rows into {len(merged_df)} rows")
    print(f"  Reduction: {len(df) - len(merged_df)} rows")
    
    return merged_df, problematic_products


def merge_two_rows(group: pd.DataFrame) -> pd.Series:
    """Merge exactly two rows by combining non-null values."""
    
    row1 = group.iloc[0]
    row2 = group.iloc[1]
    
    merged = {}
    
    for col in group.columns:
        val1 = row1[col] if pd.notna(row1[col]) else None
        val2 = row2[col] if pd.notna(row2[col]) else None
        
        # Combine logic
        if val1 is not None and val2 is not None:
            # Both have values - prefer the first non-empty string or non-zero number
            if col in ['商品管理番号']:
                # For management number, they should be the same
                merged[col] = val1
            elif isinstance(val1, str) and isinstance(val2, str):
                # For strings, prefer the longer/more complete one
                merged[col] = val1 if len(str(val1)) >= len(str(val2)) else val2
            else:
                # For numbers, prefer non-zero values
                merged[col] = val1 if val1 != 0 else val2
        elif val1 is not None:
            merged[col] = val1
        elif val2 is not None:
            merged[col] = val2
        else:
            merged[col] = None
    
    return pd.Series(merged)


def process_tax_data(input_file: str, output_file: str):
    """Process the tax data CSV to merge duplicates."""
    
    print(f"Processing: {input_file}")
    print(f"Output to: {output_file}")
    
    try:
        # Read the CSV file
        print("\nReading CSV file...")
        df = pd.read_csv(input_file)
        print(f"Loaded {len(df)} rows")
        
        # Show original statistics
        print(f"Original columns: {list(df.columns)}")
        print(f"Unique 商品管理番号: {df['商品管理番号'].nunique()}")
        
        # Analyze duplicates
        singles, doubles, multiples = analyze_duplicates(df)
        
        # Merge duplicate rows
        merged_df, problematic = merge_duplicate_rows(df)
        
        # Show sample of merged data
        print(f"\nSample merged data (first 5 rows):")
        print(merged_df.head().to_string(index=False, max_colwidth=30))
        
        # Show statistics of merged data
        print(f"\nMerged data statistics:")
        print(f"  Total rows: {len(merged_df)}")
        print(f"  Unique 商品管理番号: {merged_df['商品管理番号'].nunique()}")
        
        # Count products with tax info in merged data
        tax_flag_count = merged_df['消費税'].notna().sum()
        tax_rate_count = merged_df['消費税率'].notna().sum()
        print(f"  Products with tax flag: {tax_flag_count}")
        print(f"  Products with tax rate: {tax_rate_count}")
        
        # Show tax rate distribution
        if tax_rate_count > 0:
            tax_rates = merged_df['消費税率'].value_counts().sort_index()
            print(f"  Tax rate distribution:")
            for rate, count in tax_rates.items():
                if pd.notna(rate):
                    percentage = (rate * 100) if rate <= 1 else rate
                    print(f"    {percentage}%: {count} products")
        
        # Save merged data
        print(f"\nSaving merged data to: {output_file}")
        merged_df.to_csv(output_file, index=False, encoding='utf-8')
        
        # Report problematic products
        if problematic:
            problematic_file = output_file.replace('.csv', '_problematic.csv')
            problematic_df = df[df['商品管理番号'].isin(problematic)]
            problematic_df.to_csv(problematic_file, index=False, encoding='utf-8')
            
            print(f"\n⚠️  Problematic products (3+ rows) saved to: {problematic_file}")
            print(f"   Please review these {len(problematic)} products manually:")
            for product in problematic[:10]:
                count = (df['商品管理番号'] == product).sum()
                print(f"     {product}: {count} rows")
            if len(problematic) > 10:
                print(f"     ... and {len(problematic) - 10} more")
        
        print("\n✅ Processing completed successfully!")
        
        # Final summary
        print(f"\nSummary:")
        print(f"  Original rows: {len(df)}")
        print(f"  Merged rows: {len(merged_df)}")
        print(f"  Rows eliminated: {len(df) - len(merged_df)}")
        print(f"  Products needing manual review: {len(problematic)}")
        
    except FileNotFoundError:
        print(f"❌ Error: Input file not found: {input_file}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error processing file: {e}")
        sys.exit(1)


def main():
    """Main function with command line argument parsing."""
    
    parser = argparse.ArgumentParser(
        description="Merge duplicate rows in tax data CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python merge_tax_data.py
  python merge_tax_data.py input.csv output.csv
  python merge_tax_data.py --input tax_data.csv --output merged_tax_data.csv
        """
    )
    
    parser.add_argument(
        'input_file',
        nargs='?',
        help='Input tax data CSV file path'
    )
    
    parser.add_argument(
        'output_file', 
        nargs='?',
        help='Output merged CSV file path'
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
    default_input = script_dir.parent / "output" / "rakuten_tax_data.csv"
    default_output = script_dir.parent / "output" / "rakuten_tax_data_merged.csv"
    
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
    
    print("Rakuten Tax Data Merger")
    print("=" * 40)
    
    # Run processing
    process_tax_data(input_file, output_file)


if __name__ == "__main__":
    main()