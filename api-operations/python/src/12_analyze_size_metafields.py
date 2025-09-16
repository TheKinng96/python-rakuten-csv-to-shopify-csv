#!/usr/bin/env python3
"""
Analyze CSV data to understand variant custom.size and product custom.search_size metafields
"""

import pandas as pd
import sys
import os
from pathlib import Path

def analyze_csv_metafields(csv_path):
    """Analyze size metafields in CSV export"""
    print(f"📂 Loading CSV data from: {csv_path}")
    
    try:
        # Read CSV with proper encoding
        df = pd.read_csv(csv_path, encoding='utf-8')
        print(f"✅ Loaded {len(df)} rows with {len(df.columns)} columns")
        
        # Find relevant columns
        search_size_col = None
        variant_size_cols = []
        
        for col in df.columns:
            if 'search_size' in col.lower():
                search_size_col = col
                print(f"🔍 Found search_size column: {col}")
            elif 'variant' in col.lower() and 'size' in col.lower():
                variant_size_cols.append(col)
                print(f"🔍 Found variant size column: {col}")
            elif 'custom.size' in col:
                variant_size_cols.append(col)
                print(f"🔍 Found custom.size column: {col}")
        
        if search_size_col:
            # Analyze search_size data
            search_size_data = df[search_size_col].dropna()
            unique_values = search_size_data.unique()
            
            print(f"\n📊 Search Size Analysis:")
            print(f"   Total non-null values: {len(search_size_data)}")
            print(f"   Unique values: {len(unique_values)}")
            
            # Show sample values
            print(f"\n📝 Sample search_size values:")
            for i, value in enumerate(unique_values[:10]):
                if pd.notna(value) and str(value).strip():
                    print(f"   {i+1}: \"{str(value)[:100]}{'...' if len(str(value)) > 100 else ''}\"")
        
        if variant_size_cols:
            print(f"\n📊 Found {len(variant_size_cols)} variant size columns:")
            for col in variant_size_cols:
                variant_data = df[col].dropna()
                print(f"   {col}: {len(variant_data)} non-null values")
        else:
            print(f"\n❌ No variant size columns found")
        
        # Check for products that have neither or both
        if search_size_col:
            has_search_size = df[search_size_col].notna()
            
            print(f"\n📈 Distribution:")
            print(f"   Products with search_size: {has_search_size.sum()}")
            print(f"   Products without search_size: {(~has_search_size).sum()}")
            
            # Show products without search_size
            products_without = df[~has_search_size][['Handle', 'Title']].head(5)
            if not products_without.empty:
                print(f"\n📋 Sample products WITHOUT search_size:")
                for idx, row in products_without.iterrows():
                    print(f"   - {row['Handle']}: {row['Title'][:60]}...")
            
            # Show products with search_size
            products_with = df[has_search_size][['Handle', 'Title', search_size_col]].head(5)
            if not products_with.empty:
                print(f"\n📋 Sample products WITH search_size:")
                for idx, row in products_with.iterrows():
                    size_value = str(row[search_size_col])[:50]
                    print(f"   - {row['Handle']}: \"{size_value}{'...' if len(str(row[search_size_col])) > 50 else ''}\"")
        
        return {
            'total_rows': len(df),
            'search_size_col': search_size_col,
            'variant_size_cols': variant_size_cols,
            'has_search_size': has_search_size.sum() if search_size_col else 0,
            'missing_search_size': (~has_search_size).sum() if search_size_col else len(df)
        }
        
    except Exception as e:
        print(f"❌ Error analyzing CSV: {e}")
        return None

def main():
    """Main function"""
    print("=" * 70)
    print("🔍 SIZE METAFIELD ANALYSIS")
    print("=" * 70)
    
    data_dir = Path(__file__).parent.parent.parent / "data"
    csv_files = list(data_dir.glob("products_export_*.csv"))
    
    if not csv_files:
        print(f"❌ No CSV files found in {data_dir}")
        return
    
    print(f"📁 Found {len(csv_files)} CSV files")
    
    total_stats = {
        'total_products': 0,
        'total_with_search_size': 0,
        'total_without_search_size': 0
    }
    
    # Analyze each CSV file
    for csv_file in sorted(csv_files):
        print(f"\n" + "-" * 50)
        print(f"📂 Analyzing: {csv_file.name}")
        print("-" * 50)
        
        stats = analyze_csv_metafields(csv_file)
        if stats:
            total_stats['total_products'] += stats['total_rows']
            total_stats['total_with_search_size'] += stats['has_search_size']
            total_stats['total_without_search_size'] += stats['missing_search_size']
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 OVERALL SUMMARY")
    print("=" * 70)
    print(f"Total products analyzed: {total_stats['total_products']}")
    print(f"Products with search_size: {total_stats['total_with_search_size']}")
    print(f"Products without search_size: {total_stats['total_without_search_size']}")
    
    if total_stats['total_products'] > 0:
        percentage = (total_stats['total_with_search_size'] / total_stats['total_products']) * 100
        print(f"Coverage: {percentage:.1f}%")
        
        if total_stats['total_without_search_size'] > 0:
            print(f"\n💡 Migration needed for {total_stats['total_without_search_size']} products")
            print("   These products may have variant custom.size that needs to be migrated")
        else:
            print(f"\n✅ All products already have search_size metafield")

if __name__ == "__main__":
    main()