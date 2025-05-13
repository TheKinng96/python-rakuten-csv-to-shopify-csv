import pandas as pd
import glob
import os
from collections import defaultdict

def analyze_metafields():
    # Initialize storage for metafield information
    metafields = defaultdict(lambda: {
        'count': 0,
        'unique_values': set(),
        'units': set(),
        'files_found_in': set(),
        'item_names': set()  # Add storage for item names
    })
    
    # Process each CSV file
    csv_files = glob.glob('split_output/data_part_*.csv')
    for file in csv_files:
        print(f"Processing {file}...")
        
        # Read CSV with Shift-JIS encoding
        df = pd.read_csv(file, encoding='shift-jis', low_memory=False)
        
        # Find columns related to metafields
        metafield_cols = [col for col in df.columns if '商品属性' in col]
        
        # Group columns by metafield number
        for i in range(1, 101):  # Assuming up to 100 metafields
            item_col = f'商品属性（項目）{i}' if i > 1 else '商品属性（項目）'
            value_col = f'商品属性（値）{i}' if i > 1 else '商品属性（値）'
            unit_col = f'商品属性（単位）{i}' if i > 1 else '商品属性（単位）'
            
            # Skip if item column doesn't exist
            if item_col not in df.columns:
                continue
                
            # Process each row
            for idx, row in df.iterrows():
                if pd.notna(row[item_col]):
                    item_name = str(row[item_col]).strip()
                    
                    # Update value information
                    if value_col in df.columns and pd.notna(row[value_col]):
                        value = str(row[value_col]).strip()
                        metafields[item_name]['count'] += 1
                        metafields[item_name]['unique_values'].add(value)
                        metafields[item_name]['files_found_in'].add(file)
                        
                        # Add item name if available
                        if '商品名' in df.columns and pd.notna(row['商品名']):
                            metafields[item_name]['item_names'].add(str(row['商品名']).strip())
                            print(f"Added item name: {str(row['商品名']).strip()} for {item_name}")
                    
                    # Update unit information
                    if unit_col in df.columns and pd.notna(row[unit_col]):
                        unit = str(row[unit_col]).strip()
                        if unit:
                            metafields[item_name]['units'].add(unit)

    # Generate report
    print("\n=== Metafield Analysis Report ===\n")
    print(f"Total unique metafields found: {len(metafields)}\n")
    print("Metafields Sorted by Usage Frequency:")
    print("-" * 50)
    
    # Sort metafields by count
    sorted_metafields = sorted(
        metafields.items(),
        key=lambda x: x[1]['count'],
        reverse=True
    )
    
    # Prepare data for CSV export
    csv_data = []
    
    for name, info in sorted_metafields:
        print(f"\nMetafield: {name}")
        print(f"Total occurrences: {info['count']}")
        print(f"Found in {len(info['files_found_in'])} files")
        print(f"Unique values: {len(info['unique_values'])}")
        print("Sample values:", list(info['unique_values'])[:5])
        if info['units']:
            print("Units used:", sorted(info['units']))
        if info['item_names']:
            print("Sample items:", list(info['item_names'])[:3])
        print("-" * 50)
        
        # Add data for CSV
        csv_data.append({
            'Metafield Name': name,
            'Total Occurrences': info['count'],
            'Files Found In': len(info['files_found_in']),
            'Unique Values Count': len(info['unique_values']),
            'Sample Values': '|'.join(list(info['unique_values'])[:5]),
            'Units Used': '|'.join(sorted(info['units'])) if info['units'] else '',
            'Sample Items': '|'.join(list(info['item_names'])[:3]) if info['item_names'] else ''
        })
    
    # Export to CSV
    df_report = pd.DataFrame(csv_data)
    df_report.to_csv('metafield_analysis_report.csv', index=False, encoding='utf-8-sig')
    print("\nReport exported to metafield_analysis_report.csv")

if __name__ == '__main__':
    analyze_metafields() 