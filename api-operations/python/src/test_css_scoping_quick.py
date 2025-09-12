#!/usr/bin/env python3
"""
Quick test to process a few products with CSS scoping
"""
import sys
import json
from pathlib import Path
import pandas as pd

# Import the CSS scoper using importlib due to numeric filename
import importlib.util
spec = importlib.util.spec_from_file_location("scope_css", str(Path(__file__).parent / "11_scope_css_styles.py"))
scope_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scope_module)
CSSStyleScoper = scope_module.CSSStyleScoper


def test_quick():
    print("=" * 70)
    print("🚀 QUICK CSS SCOPING TEST")
    print("=" * 70)
    
    # Get first CSV file
    data_dir = Path(__file__).parent.parent.parent / "data"
    csv_files = sorted(data_dir.glob("products_export_*.csv"))
    
    if not csv_files:
        print("No CSV files found!")
        return
    
    csv_file = csv_files[0]
    print(f"\n📄 Reading {csv_file.name}...")
    
    # Read only first 500 rows
    df = pd.read_csv(csv_file, encoding='utf-8', low_memory=False, nrows=500)
    print(f"   Loaded {len(df)} rows")
    
    scoper = CSSStyleScoper()
    products_with_styles = []
    
    for idx, row in df.iterrows():
        handle = row.get('Handle', '')
        body_html = row.get('Body (HTML)', '')
        
        if pd.notna(body_html) and pd.notna(handle) and '<style' in str(body_html).lower():
            print(f"\n🔍 Found product with styles: {handle}")
            
            result = scoper.process_html(str(body_html), handle)
            
            if result.get('processed') and result.get('changes_made'):
                products_with_styles.append({
                    'handle': handle,
                    'changes': len(result['changes_made']),
                    'bytes_changed': result['bytes_changed']
                })
                
                # Show first few changes
                print(f"   ✅ Processed with {len(result['changes_made'])} changes:")
                for change in result['changes_made'][:3]:
                    if 'original' in change and 'scoped' in change:
                        print(f"      - '{change['original'][:30]}...' → '{change['scoped'][:50]}...'")
                    else:
                        print(f"      - {change['type']}")
                
                # Stop after finding 5 products
                if len(products_with_styles) >= 5:
                    break
    
    print(f"\n📊 Summary:")
    print(f"   Products with styles found: {len(products_with_styles)}")
    
    if products_with_styles:
        print(f"\n   Products processed:")
        for product in products_with_styles:
            print(f"      - {product['handle']}: {product['changes']} changes")
        
        # Save a sample JSON
        output_file = Path(__file__).parent.parent / "shared" / "css_scoping_sample.json"
        output_file.parent.mkdir(exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump({
                'test': True,
                'products': products_with_styles
            }, f, indent=2)
        
        print(f"\n   Sample saved to: {output_file}")
    else:
        print("   No products with style tags found in first 100 rows")


if __name__ == "__main__":
    test_quick()