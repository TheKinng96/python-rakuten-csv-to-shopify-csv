#!/usr/bin/env python3
"""
Quick check of CSV structure for size metafield migration
"""

import pandas as pd
import sys
from pathlib import Path

def check_csv_structure():
    """Check CSV structure for size metafields"""
    data_dir = Path(__file__).parent.parent.parent / "data"
    csv_file = data_dir / "products_export_1.csv"
    
    try:
        print(f"📂 Loading CSV: {csv_file}")
        df = pd.read_csv(csv_file, encoding='utf-8')
        print(f"✅ Loaded {len(df)} rows with {len(df.columns)} columns")
        
        # Find search_size column
        search_size_col = None
        for col in df.columns:
            if 'search_size' in col:
                search_size_col = col
                print(f"\n🔍 Found search_size column: {col}")
                break
        
        if search_size_col:
            # Check for data in search_size
            non_null = df[search_size_col].notna()
            # Convert to string first, then check for non-empty
            str_values = df[search_size_col].astype(str)
            non_empty = str_values.str.strip().str.len() > 0
            # Exclude 'nan' string values
            not_nan_str = str_values.str.lower() != 'nan'
            has_data = non_null & non_empty & not_nan_str
            
            print(f"\n📊 Search Size Data:")
            print(f"   Total rows: {len(df)}")
            print(f"   Non-null values: {non_null.sum()}")
            print(f"   Non-empty values: {has_data.sum()}")
            print(f"   Empty/null values: {(~has_data).sum()}")
            
            # Show some examples
            if has_data.sum() > 0:
                print(f"\n📝 Sample search_size values:")
                sample_data = df[has_data][['Handle', search_size_col]].head(5)
                for idx, row in sample_data.iterrows():
                    print(f"   - {row['Handle']}: \"{row[search_size_col]}\"")
            
            # Show products without search_size
            if (~has_data).sum() > 0:
                print(f"\n❌ Sample products WITHOUT search_size:")
                missing_data = df[~has_data][['Handle', 'Title']].head(5)
                for idx, row in missing_data.iterrows():
                    title = str(row['Title'])[:50]
                    print(f"   - {row['Handle']}: {title}...")
        
        # Check for any variant-related size columns
        variant_cols = [col for col in df.columns if 'variant' in col.lower() and 'size' in col.lower()]
        if variant_cols:
            print(f"\n🔍 Found variant size columns: {variant_cols}")
        else:
            print(f"\n❌ No variant size columns found in CSV")
        
        return {
            'total_products': len(df),
            'has_search_size': has_data.sum() if search_size_col else 0,
            'missing_search_size': (~has_data).sum() if search_size_col else len(df)
        }
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

if __name__ == "__main__":
    stats = check_csv_structure()
    if stats:
        print(f"\n📊 Summary:")
        print(f"   Migration candidates: {stats['missing_search_size']} products")
        print(f"   Already have search_size: {stats['has_search_size']} products")