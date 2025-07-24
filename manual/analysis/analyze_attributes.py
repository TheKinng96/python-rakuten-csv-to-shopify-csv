#!/usr/bin/env python3
"""
Script to analyze product attributes in Rakuten CSV files.
Analyzes 商品属性（項目）, 商品属性（値）, and 商品属性（単位）columns.
"""

import pandas as pd
import argparse
import csv
from typing import Dict, List, Tuple, Set, Any
from collections import defaultdict
import sys
import json
from pathlib import Path

def detect_encoding(file_path: str) -> str:
    """Detect the encoding of the input file."""
    encodings = ['utf-8-sig', 'cp932', 'shift_jis', 'euc-jp', 'latin1']
    for enc in encodings:
        try:
            with open(file_path, 'r', encoding=enc) as f:
                f.read(1024)  # Read a small portion to test encoding
                return enc
        except UnicodeDecodeError:
            continue
    return 'utf-8'  # Default fallback

def get_attribute_columns(df: pd.DataFrame) -> Tuple[List[str], List[str], List[str]]:
    """Extract attribute name, value, and unit columns from the dataframe."""
    name_cols = [col for col in df.columns if col.startswith('商品属性（項目）')]
    value_cols = [col for col in df.columns if col.startswith('商品属性（値）')]
    unit_cols = [col for col in df.columns if col.startswith('商品属性（単位）')]
    return name_cols, value_cols, unit_cols

def analyze_attributes(csv_path: str) -> Dict:
    """Analyze product attributes in the given CSV file."""
    # Detect encoding and read CSV
    encoding = detect_encoding(csv_path)
    try:
        df = pd.read_csv(csv_path, encoding=encoding, dtype=str, on_bad_lines='warn')
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        sys.exit(1)
    
    # Get attribute columns
    name_cols, value_cols, unit_cols = get_attribute_columns(df)
    
    # Initialize results
    results = {
        'total_products': len(df),
        'attribute_columns_found': len(name_cols),
        'attributes': {},
        'duplicate_attributes': [],
        'empty_columns': [],
        'attribute_usage': {}
    }
    
    # Track all attribute names, their positions, and row counts
    attribute_data = defaultdict(lambda: {'positions': [], 'row_counts': 0})
    empty_columns = []
    
    # Analyze each attribute column
    for idx, (name_col, value_col, unit_col) in enumerate(zip(name_cols, value_cols, unit_cols)):
        col_idx = idx + 1  # 1-based index
        
        # Check if column is completely empty
        if df[name_col].isna().all():
            empty_columns.append(col_idx)
            continue
            
        # Get non-empty rows for this column
        non_empty_rows = df[~df[name_col].isna() & (df[name_col] != '')]
        
        # Group by attribute name and count occurrences
        attr_counts = non_empty_rows[name_col].value_counts()
        
        for attr, count in attr_counts.items():
            attr = str(attr).strip()
            if attr:  # Only process non-empty attribute names
                if attr not in attribute_data:
                    attribute_data[attr] = {'positions': [], 'row_counts': 0}
                attribute_data[attr]['positions'].append(col_idx)
                attribute_data[attr]['row_counts'] += count
    
    # Process the attribute data
    for attr, data in attribute_data.items():
        positions = data['positions']
        row_count = data['row_counts']
        
        if len(positions) > 1:
            results['duplicate_attributes'].append({
                'attribute': attr,
                'positions': positions,
                'total_rows': row_count,
                'rows_per_column': row_count / len(positions)
            })
    
    # Track empty columns
    results['empty_columns'] = empty_columns
    results['empty_attributes'] = len(empty_columns)
    
    # Calculate attribute usage statistics
    total_rows = len(df)
    usage_stats = []
    
    for attr, data in attribute_data.items():
        usage_pct = (data['row_counts'] / total_rows) * 100
        usage_stats.append({
            'attribute': attr,
            'total_rows': data['row_counts'],
            'usage_percentage': round(usage_pct, 2),
            'columns': len(data['positions']),
            'columns_list': data['positions']
        })
    
    # Sort by usage (highest first)
    results['attribute_usage'] = sorted(usage_stats, key=lambda x: x['total_rows'], reverse=True)
    
    # Get summary of attributes and their values
    for attr, data in attribute_data.items():
        # Get all values for this attribute across all positions
        all_values = []
        all_units = set()
        
        for pos in data['positions']:
            name_col = f'商品属性（項目）{pos}'
            value_col = f'商品属性（値）{pos}'
            unit_col = f'商品属性（単位）{pos}'
            
            # Get rows where this attribute appears
            mask = (df[name_col] == attr) & ~df[value_col].isna() & (df[value_col] != '')
            values = df.loc[mask, value_col].unique().tolist()
            units = df.loc[mask, unit_col].dropna().unique().tolist()
            
            all_values.extend(values)
            all_units.update(units)
        
        # Get usage stats for this attribute
        usage_stats = next((x for x in results['attribute_usage'] if x['attribute'] == attr), None)
        
        # Count unique values and get sample values
        unique_values = list(set(all_values))  # deduplicate
        sample_values = unique_values[:5]  # Get first 5 unique values
        
        results['attributes'][attr] = {
            'positions': data['positions'],
            'total_rows': data['row_counts'],
            'usage_percentage': usage_stats['usage_percentage'] if usage_stats else 0,
            'unique_values_count': len(unique_values),
            'sample_values': sample_values,
            'sample_units': list(all_units)
        }
    
    return results

def save_results_to_csv(results: Dict, output_path: str):
    """Save analysis results to a CSV file with detailed attribute information."""
    rows = []
    
    # Add summary section
    rows.append(['SECTION', 'METRIC', 'VALUE'])
    rows.append(['SUMMARY', 'Total Products', results['total_products']])
    rows.append(['SUMMARY', 'Attribute Columns Found', results['attribute_columns_found']])
    rows.append(['SUMMARY', 'Empty Attribute Columns', results['empty_attributes']])
    rows.append(['SUMMARY', 'Empty Column Indices', ', '.join(map(str, results['empty_columns']))])
    rows.append(['SUMMARY', 'Unique Attributes', len(results['attributes'])])
    
    # Add duplicate attributes section with row counts
    if results['duplicate_attributes']:
        rows.append([])
        rows.append(['DUPLICATE_ATTRIBUTES', 'Attribute', 'Column Positions', 'Total Rows', 'Rows Per Column'])
        for dup in sorted(results['duplicate_attributes'], 
                         key=lambda x: x['total_rows'], 
                         reverse=True):
            rows.append([
                'DUPLICATE_ATTRIBUTES',
                dup['attribute'],
                ', '.join(map(str, dup['positions'])),
                dup['total_rows'],
                round(dup['rows_per_column'], 1)
            ])
    
    # Add attribute usage statistics
    rows.append([])
    rows.append(['ATTRIBUTE_USAGE', 'Attribute', 'Total Rows', 'Usage %', 'Columns', 'Column Indices'])
    for stat in results['attribute_usage'][:100]:  # Top 100 most used attributes
        rows.append([
            'ATTRIBUTE_USAGE',
            stat['attribute'],
            stat['total_rows'],
            f"{stat['usage_percentage']:.1f}%",
            stat['columns'],
            ', '.join(map(str, stat['columns_list']))
        ])
    
    # Add attribute details section with sample values
    rows.append([])
    rows.append(['ATTRIBUTE_DETAILS', 'Attribute', 'Total Rows', 'Usage %', 'Unique Values', 'Sample Values', 'Sample Units'])
    for attr, data in sorted(results['attributes'].items(), 
                           key=lambda x: x[1]['total_rows'], 
                           reverse=True):
        rows.append([
            'ATTRIBUTE_DETAILS',
            attr,
            data['total_rows'],
            f"{data['usage_percentage']:.1f}%",
            data['unique_values_count'],
            ' | '.join(map(str, data['sample_values'][:5])),
            ' | '.join(map(str, data['sample_units'][:5]))
        ])
    
    # Write to CSV with proper formatting
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    
    print(f"CSV results saved to {output_path}")
    print(f"Total attributes analyzed: {len(results['attributes'])}")
    print(f"Most used attribute: {results['attribute_usage'][0]['attribute']} "
          f"({results['attribute_usage'][0]['total_rows']} rows, "
          f"{results['attribute_usage'][0]['usage_percentage']:.1f}% of products)")

def print_summary(results: Dict):
    """Print a summary of the analysis."""
    print(f"\n{'='*50}")
    print(f"Product Attributes Analysis Summary")
    print(f"{'='*50}")
    print(f"Total products: {results['total_products']}")
    print(f"Attribute columns found: {results['attribute_columns_found']}")
    print(f"Empty attribute columns: {results['empty_attributes']}")
    print(f"Unique attributes found: {len(results['attributes'])}")
    
    if results['duplicate_attributes']:
        print("\nDuplicate attributes found in multiple columns:")
        for dup in results['duplicate_attributes']:
            print(f"  - '{dup['attribute']}' appears in columns: {dup['positions']}")
    
    print("\nAttribute details:")
    for attr, data in results['attributes'].items():
        print(f"\n- {attr} (in columns {data['positions']})")
        print(f"  Unique values: {data['unique_values_count']}")
        print(f"  Sample values: {', '.join(str(v) for v in data['sample_values'][:3])}" + 
              ("..." if len(data['sample_values']) > 3 else ""))
        if data['sample_units']:
            print(f"  Units: {', '.join(data['sample_units'])}")

def main():
    parser = argparse.ArgumentParser(description='Analyze product attributes in Rakuten CSV files.')
    parser.add_argument('csv_file', help='Path to the Rakuten CSV file')
    parser.add_argument('--output', '-o', help='Output file for results (CSV or JSON based on extension)')
    parser.add_argument('--format', '-f', choices=['csv', 'json', 'both'], default='csv',
                       help='Output format: csv, json, or both (default: csv)')
    
    args = parser.parse_args()
    
    print(f"Analyzing {args.csv_file}...")
    results = analyze_attributes(args.csv_file)
    
    # Print summary to console
    print_summary(results)
    
    # Determine output file names
    if args.output:
        output_path = Path(args.output)
        base_path = output_path.parent / output_path.stem
        
        # Save in requested format(s)
        if args.format in ['csv', 'both']:
            csv_path = f"{base_path}.csv" if args.format == 'both' else args.output
            save_results_to_csv(results, csv_path)
            
        if args.format in ['json', 'both']:
            json_path = f"{base_path}.json" if args.format == 'both' else args.output
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print(f"JSON results saved to {json_path}")
    else:
        # If no output file specified but format is 'both', use default names
        if args.format == 'both':
            save_results_to_csv(results, "attribute_analysis.csv")
            with open("attribute_analysis.json", 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            print("CSV results saved to attribute_analysis.csv")
            print("JSON results saved to attribute_analysis.json")

if __name__ == "__main__":
    main()
