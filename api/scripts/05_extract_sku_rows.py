#!/usr/bin/env python3
"""
Script to extract rows from shopify_products.csv based on Variant SKUs 
from the missing images audit report.

This script:
1. Reads the list of Variant SKUs from 04_sku_no_variant_image.csv
2. Extracts matching rows from manual/final/output/shopify_products.csv
3. Outputs the extracted rows to a new CSV file
"""
import csv
import sys
from datetime import datetime
from pathlib import Path
from typing import Set, List, Dict, Any

import pandas as pd
from tqdm import tqdm


def load_target_skus(audit_csv_path: Path) -> Set[str]:
    """Load the list of Variant SKUs that need to be extracted"""
    print(f"📂 Loading target SKUs from {audit_csv_path}")
    
    target_skus = set()
    
    try:
        df = pd.read_csv(audit_csv_path, encoding='utf-8', low_memory=False)
        
        # Get Variant SKU column
        if 'Variant SKU' in df.columns:
            skus = df['Variant SKU'].dropna()
            target_skus = set(str(sku).strip() for sku in skus if pd.notna(sku))
            print(f"   ✅ Loaded {len(target_skus)} unique Variant SKUs")
        else:
            print(f"   ❌ 'Variant SKU' column not found in {audit_csv_path}")
            return set()
            
    except Exception as e:
        print(f"   ❌ Error reading {audit_csv_path}: {e}")
        return set()
    
    return target_skus


def extract_matching_rows(shopify_csv_path: Path, target_skus: Set[str]) -> List[Dict[str, Any]]:
    """Extract rows from shopify_products.csv that match the target SKUs"""
    print(f"📂 Extracting matching rows from {shopify_csv_path}")
    
    if not shopify_csv_path.exists():
        print(f"   ❌ File not found: {shopify_csv_path}")
        return []
    
    matching_rows = []
    total_rows = 0
    
    try:
        # Read the CSV file
        df = pd.read_csv(shopify_csv_path, encoding='utf-8', low_memory=False)
        total_rows = len(df)
        
        print(f"   📊 Processing {total_rows:,} rows...")
        
        # Check if Variant SKU column exists
        if 'Variant SKU' not in df.columns:
            print(f"   ❌ 'Variant SKU' column not found in {shopify_csv_path}")
            return []
        
        # Filter rows where Variant SKU matches our target list
        for _, row in tqdm(df.iterrows(), total=total_rows, desc="Processing rows"):
            variant_sku = row.get('Variant SKU', '')
            
            if pd.notna(variant_sku):
                variant_sku_str = str(variant_sku).strip()
                if variant_sku_str in target_skus:
                    matching_rows.append(row.to_dict())
        
        print(f"   ✅ Found {len(matching_rows)} matching rows")
        
    except Exception as e:
        print(f"   ❌ Error reading {shopify_csv_path}: {e}")
        return []
    
    return matching_rows


def save_extracted_rows(extracted_rows: List[Dict[str, Any]], output_path: Path) -> bool:
    """Save the extracted rows to a new CSV file"""
    print(f"💾 Saving extracted rows to {output_path}")
    
    if not extracted_rows:
        print("   ⚠️  No rows to save")
        # Create empty file with message
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['message'])
            writer.writerow(['No matching rows found'])
        return True
    
    try:
        # Get column names from the first row
        fieldnames = list(extracted_rows[0].keys())
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for row in extracted_rows:
                writer.writerow(row)
        
        print(f"   ✅ Successfully saved {len(extracted_rows)} rows")
        return True
        
    except Exception as e:
        print(f"   ❌ Error saving to {output_path}: {e}")
        return False


def main():
    """Main execution function"""
    print("=" * 70)
    print("🔍 EXTRACT ROWS BY VARIANT SKU")
    print("=" * 70)
    
    try:
        # Define file paths
        api_dir = Path(__file__).parent.parent
        manual_dir = api_dir.parent / "manual"
        
        audit_csv_path = api_dir / "reports" / "04_sku_no_variant_image.csv"
        shopify_csv_path = manual_dir / "final" / "output" / "shopify_products.csv"
        output_path = api_dir / "reports" / "05_extracted_sku_rows.csv"
        
        # Ensure reports directory exists
        output_path.parent.mkdir(exist_ok=True)
        
        print(f"📋 Input files:")
        print(f"   📄 Target SKUs: {audit_csv_path}")
        print(f"   📄 Source data: {shopify_csv_path}")
        print(f"   📄 Output: {output_path}")
        print()
        
        # Phase 1: Load target SKUs
        target_skus = load_target_skus(audit_csv_path)
        
        if not target_skus:
            print("❌ No target SKUs found. Exiting.")
            return 1
        
        print(f"📊 Target SKUs to extract: {len(target_skus)}")
        print()
        
        # Phase 2: Extract matching rows
        extracted_rows = extract_matching_rows(shopify_csv_path, target_skus)
        
        # Phase 3: Save results
        success = save_extracted_rows(extracted_rows, output_path)
        
        if success:
            print(f"\n🎉 Extraction completed successfully!")
            print(f"   📄 Output saved to: {output_path}")
            print(f"   📊 Rows extracted: {len(extracted_rows)}")
            
            if extracted_rows:
                # Show some sample SKUs
                sample_skus = [row.get('Variant SKU', '') for row in extracted_rows[:5]]
                print(f"   📝 Sample extracted SKUs: {', '.join(sample_skus)}")
            
            print(f"\n💡 Next steps:")
            print(f"   1. Review {output_path.name} to see the extracted product data")
            print(f"   2. Use this data to identify products needing Variant Images")
            print(f"   3. Update the products with appropriate images")
            
            return 0
        else:
            print(f"\n❌ Extraction failed")
            return 1
        
    except KeyboardInterrupt:
        print("\n⏹️  Extraction interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Extraction failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())