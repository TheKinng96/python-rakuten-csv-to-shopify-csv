#!/usr/bin/env python3
"""
Smart merge rows with same 商品管理番号 in tax data CSV.

This script:
1. Analyzes the tax data CSV for duplicate 商品管理番号
2. Merges ALL rows (including 3+ rows) by combining non-null values intelligently
3. For multiple SKUs, picks the first available non-empty SKU
4. Creates a fully cleaned output CSV with one row per product

Usage:
    python merge_tax_data_smart.py [input_file] [output_file]
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
    triples_plus = mgmt_counts[mgmt_counts >= 3]
    
    print(f"  Products with 1 row: {len(singles)}")
    print(f"  Products with 2 rows: {len(doubles)}")
    print(f"  Products with 3+ rows: {len(triples_plus)}")
    
    if len(triples_plus) > 0:
        print(f"\n📋 Products with 3+ rows (will be smart-merged):")
        for mgmt_num, count in triples_plus.head(20).items():
            print(f"    {mgmt_num}: {count} rows")
        
        if len(triples_plus) > 20:
            print(f"    ... and {len(triples_plus) - 20} more")
    
    return singles, doubles, triples_plus


def smart_merge_rows(group: pd.DataFrame) -> pd.Series:
    """Smart merge multiple rows by combining non-null values intelligently."""
    
    merged = {}
    
    # Get all non-null values for each column
    for col in group.columns:
        non_null_values = group[col].dropna()
        
        if len(non_null_values) == 0:
            merged[col] = None
        elif col == '商品管理番号':
            # Management number should be the same for all rows
            merged[col] = non_null_values.iloc[0]
        elif col == 'SKU管理番号':
            # For SKU, pick the first non-empty value that's not just the management number
            sku_candidates = []
            mgmt_num = group['商品管理番号'].iloc[0]
            
            for sku in non_null_values:
                # Convert to string for comparison
                sku_str = str(sku).strip()
                if sku_str and sku_str != str(mgmt_num):
                    sku_candidates.append(sku)
            
            if sku_candidates:
                merged[col] = sku_candidates[0]  # Pick first available unique SKU
            elif len(non_null_values) > 0:
                merged[col] = non_null_values.iloc[0]  # Fallback to first non-null
            else:
                merged[col] = None
        elif col in ['商品番号', '商品名']:
            # For product number and name, prefer the longest/most complete value
            if len(non_null_values) == 1:
                merged[col] = non_null_values.iloc[0]
            else:
                # Pick the longest non-empty string
                best_value = None
                max_length = 0
                for val in non_null_values:
                    val_str = str(val).strip()
                    if len(val_str) > max_length:
                        max_length = len(val_str)
                        best_value = val
                merged[col] = best_value if best_value is not None else non_null_values.iloc[0]
        elif col in ['消費税', '消費税率']:
            # For tax info, prefer non-zero/non-null values
            tax_values = [v for v in non_null_values if pd.notna(v) and v != 0]
            if tax_values:
                merged[col] = tax_values[0]  # Pick first valid tax value
            elif len(non_null_values) > 0:
                merged[col] = non_null_values.iloc[0]
            else:
                merged[col] = None
        else:
            # For other columns, just pick the first non-null value
            merged[col] = non_null_values.iloc[0]
    
    return pd.Series(merged)


def smart_merge_all_duplicates(df: pd.DataFrame):
    """Smart merge ALL rows with same 商品管理番号."""
    
    print("\nSmart merging ALL duplicate rows...")
    
    # Group by 商品管理番号
    grouped = df.groupby('商品管理番号')
    
    merged_rows = []
    merge_stats = {
        'single': 0,
        'double': 0,
        'triple_plus': 0,
        'sku_selections': []
    }
    
    for mgmt_num, group in grouped:
        row_count = len(group)
        
        if row_count == 1:
            # Single row - keep as is
            merged_rows.append(group.iloc[0])
            merge_stats['single'] += 1
            
        elif row_count == 2:
            # Two rows - merge
            merged_row = smart_merge_rows(group)
            merged_rows.append(merged_row)
            merge_stats['double'] += 1
            
        else:
            # 3+ rows - smart merge
            merged_row = smart_merge_rows(group)
            merged_rows.append(merged_row)
            merge_stats['triple_plus'] += 1
            
            # Track SKU selection for reporting
            skus = group['SKU管理番号'].dropna().tolist()
            if len(skus) > 1:
                selected_sku = merged_row['SKU管理番号']
                merge_stats['sku_selections'].append({
                    'mgmt_num': mgmt_num,
                    'available_skus': skus,
                    'selected_sku': selected_sku,
                    'row_count': row_count
                })
    
    # Create new dataframe
    merged_df = pd.DataFrame(merged_rows).reset_index(drop=True)
    
    print(f"  Merge statistics:")
    print(f"    Single rows (no merge needed): {merge_stats['single']}")
    print(f"    Double rows merged: {merge_stats['double']}")
    print(f"    Triple+ rows smart-merged: {merge_stats['triple_plus']}")
    print(f"  Total merged rows: {len(merged_df)}")
    print(f"  Reduction: {len(df) - len(merged_df)} rows")
    
    # Report SKU selections for complex products
    if merge_stats['sku_selections']:
        print(f"\n📋 SKU selections for products with multiple SKUs:")
        for item in merge_stats['sku_selections'][:10]:
            available = ', '.join([str(s) for s in item['available_skus']])
            selected = item['selected_sku']
            print(f"    {item['mgmt_num']} ({item['row_count']} rows):")
            print(f"      Available SKUs: {available}")
            print(f"      Selected SKU: {selected}")
        
        if len(merge_stats['sku_selections']) > 10:
            print(f"    ... and {len(merge_stats['sku_selections']) - 10} more")
    
    return merged_df, merge_stats


def process_tax_data(input_file: str, output_file: str):
    """Process the tax data CSV to smart merge all duplicates."""
    
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
        singles, doubles, triples_plus = analyze_duplicates(df)
        
        # Smart merge ALL duplicate rows
        merged_df, merge_stats = smart_merge_all_duplicates(df)
        
        # Show sample of merged data
        print(f"\nSample merged data (first 5 rows):")
        print(merged_df.head().to_string(index=False, max_colwidth=30))
        
        # Show statistics of merged data
        print(f"\nMerged data statistics:")
        print(f"  Total rows: {len(merged_df)}")
        print(f"  Unique 商品管理番号: {merged_df['商品管理番号'].nunique()}")
        
        # Verify one-to-one mapping
        unique_mgmt = merged_df['商品管理番号'].nunique()
        total_rows = len(merged_df)
        if unique_mgmt == total_rows:
            print("  ✅ Perfect 1:1 mapping - one row per product!")
        else:
            print(f"  ⚠️  Still has duplicates: {total_rows - unique_mgmt}")
        
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
        
        # Count SKU availability
        sku_count = merged_df['SKU管理番号'].notna().sum()
        print(f"  Products with SKU: {sku_count}")
        
        # Save merged data
        print(f"\nSaving smart-merged data to: {output_file}")
        merged_df.to_csv(output_file, index=False, encoding='utf-8')
        
        print("\n✅ Smart merging completed successfully!")
        
        # Final summary
        print(f"\nSummary:")
        print(f"  Original rows: {len(df)}")
        print(f"  Final merged rows: {len(merged_df)}")
        print(f"  Total elimination: {len(df) - len(merged_df)} rows")
        print(f"  Products with multiple SKUs handled: {len(merge_stats['sku_selections'])}")
        
    except FileNotFoundError:
        print(f"❌ Error: Input file not found: {input_file}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error processing file: {e}")
        sys.exit(1)


def main():
    """Main function with command line argument parsing."""
    
    parser = argparse.ArgumentParser(
        description="Smart merge ALL duplicate rows in tax data CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python merge_tax_data_smart.py
  python merge_tax_data_smart.py input.csv output.csv
  python merge_tax_data_smart.py --input tax_data.csv --output smart_merged.csv
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
        help='Output smart-merged CSV file path'
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
    default_output = script_dir.parent / "output" / "rakuten_tax_data_smart_merged.csv"
    
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
    
    print("Rakuten Tax Data Smart Merger")
    print("=" * 40)
    
    # Run processing
    process_tax_data(input_file, output_file)


if __name__ == "__main__":
    main()