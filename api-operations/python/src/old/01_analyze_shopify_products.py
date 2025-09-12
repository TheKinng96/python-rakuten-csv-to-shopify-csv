#!/usr/bin/env python3
"""
Script to analyze shopify_products.csv for various image patterns and issues

This script:
1. Finds products with -XXss.jpg Image Src patterns
2. Analyzes image distribution across products
3. Generates reports for data quality assessment
4. Outputs: reports/01_*.csv files
"""
import csv
import sys
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
from tqdm import tqdm


def find_products_with_ss_images() -> Path:
    """
    Find products with Image Src URLs that have -XXss.jpg pattern
    from manual/final/output/shopify_products.csv
    """
    reports_dir = Path(__file__).parent.parent / "reports"
    reports_dir.mkdir(exist_ok=True)
    
    shopify_csv_path = Path(__file__).parent.parent.parent / "manual" / "final" / "output" / "shopify_products.csv"
    output_csv_path = reports_dir / "01_products_with_ss_images.csv"
    
    if not shopify_csv_path.exists():
        print(f"⚠️  Shopify products CSV not found at {shopify_csv_path}")
        return output_csv_path
    
    print(f"\n🔍 Finding products with -XXss.jpg Image Src pattern...")
    
    try:
        df = pd.read_csv(shopify_csv_path, encoding='utf-8', low_memory=False)
        
        # Find rows with Image Src matching -XXss.jpg pattern
        ss_image_rows = []
        
        for _, row in df.iterrows():
            handle = row.get('Handle', '')
            image_src = row.get('Image Src', '')
            title = row.get('Title', '')
            
            if image_src and not pd.isna(image_src):
                image_str = str(image_src).strip()
                
                # Check for -XXss.jpg pattern (where XX is any characters)
                if image_str.lower().endswith('ss.jpg') and '-' in image_str:
                    # Extract the part before ss.jpg to check if it follows -XXss pattern
                    base_url = image_str[:-6]  # Remove 'ss.jpg'
                    if base_url.endswith('s') and '-' in base_url:
                        # This looks like -XXss pattern
                        ss_image_rows.append({
                            'Handle': handle,
                            'Title': title if not pd.isna(title) else '',
                            'Image_Src': image_str,
                            'Pattern_Match': 'ss.jpg'
                        })
        
        print(f"   📊 Found {len(ss_image_rows)} rows with -XXss.jpg pattern")
        
        if ss_image_rows:
            # Group by Handle to show unique products
            products_with_ss = {}
            for row in ss_image_rows:
                handle = row['Handle']
                if handle not in products_with_ss:
                    products_with_ss[handle] = {
                        'Handle': handle,
                        'Title': row['Title'],
                        'SS_Image_Count': 0,
                        'SS_Image_URLs': []
                    }
                products_with_ss[handle]['SS_Image_Count'] += 1
                products_with_ss[handle]['SS_Image_URLs'].append(row['Image_Src'])
            
            # Save results
            with open(output_csv_path, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['Handle', 'Title', 'SS_Image_Count', 'SS_Image_URLs']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for product_data in products_with_ss.values():
                    # Join URLs with semicolon for CSV
                    product_data['SS_Image_URLs'] = '; '.join(product_data['SS_Image_URLs'])
                    writer.writerow(product_data)
            
            unique_products = len(products_with_ss)
            print(f"   ✅ Found {unique_products} unique products with -XXss.jpg images")
            print(f"   📄 Report saved to: {output_csv_path}")
            
            # Show some examples
            print(f"\n📋 Sample products with -XXss.jpg images:")
            for i, (handle, data) in enumerate(list(products_with_ss.items())[:5]):
                print(f"   {i+1}. {handle} ({data['SS_Image_Count']} ss images)")
                for url in data['SS_Image_URLs'].split('; ')[:2]:  # Show first 2 URLs
                    print(f"      - {url}")
                if data['SS_Image_Count'] > 2:
                    print(f"      - ... and {data['SS_Image_Count']-2} more")
        else:
            # Create empty file with message
            with open(output_csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['message'])
                writer.writerow(['No products found with -XXss.jpg Image Src pattern'])
            
            print(f"   ✅ No products with -XXss.jpg pattern found")
        
    except Exception as e:
        print(f"   ❌ Error analyzing shopify_products.csv: {e}")
    
    return output_csv_path


def analyze_image_distribution() -> Path:
    """
    Analyze image distribution across products in shopify_products.csv
    """
    reports_dir = Path(__file__).parent.parent / "reports"
    reports_dir.mkdir(exist_ok=True)
    
    shopify_csv_path = Path(__file__).parent.parent.parent / "manual" / "final" / "output" / "shopify_products.csv"
    output_csv_path = reports_dir / "01_image_distribution.csv"
    
    if not shopify_csv_path.exists():
        print(f"⚠️  Shopify products CSV not found at {shopify_csv_path}")
        return output_csv_path
    
    print(f"\n📊 Analyzing image distribution across products...")
    
    try:
        df = pd.read_csv(shopify_csv_path, encoding='utf-8', low_memory=False)
        
        # Group by Handle and count images
        product_images = {}
        
        for _, row in df.iterrows():
            handle = row.get('Handle', '')
            image_src = row.get('Image Src', '')
            title = row.get('Title', '')
            
            if handle and not pd.isna(handle):
                handle = str(handle).strip()
                
                if handle not in product_images:
                    product_images[handle] = {
                        'Handle': handle,
                        'Title': title if not pd.isna(title) else '',
                        'Total_Images': 0,
                        'Valid_Images': 0,
                        'SS_Images': 0,
                        'Empty_Images': 0
                    }
                
                if image_src and not pd.isna(image_src):
                    image_str = str(image_src).strip()
                    if image_str:
                        product_images[handle]['Total_Images'] += 1
                        
                        # Check if it's an ss.jpg image
                        if image_str.lower().endswith('ss.jpg'):
                            product_images[handle]['SS_Images'] += 1
                        else:
                            product_images[handle]['Valid_Images'] += 1
                    else:
                        product_images[handle]['Empty_Images'] += 1
                else:
                    product_images[handle]['Empty_Images'] += 1
        
        # Save results
        with open(output_csv_path, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['Handle', 'Title', 'Total_Images', 'Valid_Images', 'SS_Images', 'Empty_Images']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            # Sort by total images descending
            sorted_products = sorted(product_images.values(), key=lambda x: x['Total_Images'], reverse=True)
            
            for product_data in sorted_products:
                writer.writerow(product_data)
        
        # Generate statistics
        total_products = len(product_images)
        products_with_ss = sum(1 for p in product_images.values() if p['SS_Images'] > 0)
        products_no_images = sum(1 for p in product_images.values() if p['Total_Images'] == 0)
        
        print(f"   📊 Analysis complete:")
        print(f"      Total products: {total_products}")
        print(f"      Products with -XXss.jpg images: {products_with_ss}")
        print(f"      Products with no images: {products_no_images}")
        print(f"   📄 Distribution report saved to: {output_csv_path}")
        
    except Exception as e:
        print(f"   ❌ Error analyzing image distribution: {e}")
    
    return output_csv_path


def main():
    """Main execution function"""
    print("=" * 70)
    print("📊 SHOPIFY PRODUCTS ANALYSIS")
    print("=" * 70)
    
    try:
        # Phase 1: Find products with -XXss.jpg patterns
        ss_images_path = find_products_with_ss_images()
        
        # Phase 2: Analyze image distribution
        distribution_path = analyze_image_distribution()
        
        print(f"\n🎉 Analysis completed successfully!")
        print(f"   📄 Products with -XXss.jpg Images: {ss_images_path}")
        print(f"   📄 Image Distribution Report: {distribution_path}")
        
        print(f"\n💡 Next steps:")
        print(f"   1. Review {ss_images_path.name} for products using invalid -XXss.jpg images")
        print(f"   2. Review {distribution_path.name} for image distribution insights")
        print(f"   3. Consider replacing -XXss.jpg images with proper product images")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⏹️  Analysis interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())