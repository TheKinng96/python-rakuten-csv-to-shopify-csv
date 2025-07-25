#!/usr/bin/env python3
"""
Script to audit products and variants for missing images and generate CSV list

This script:
1. Scans all CSV files to identify products and variants  
2. Analyzes image availability from CSV data
3. Generates CSV list for manual image attachment
4. Outputs: reports/04_missing_images_list.csv
"""
import csv
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Set

import pandas as pd
from tqdm import tqdm


class ImageAuditor:
    """Audits product and variant images for completeness from CSV data"""
    
    def __init__(self):
        self.priority_keywords = {
            'high': ['限定', '特別', 'プレミアム', '人気', 'おすすめ', '新商品'],
            'medium': ['セット', 'お得', 'まとめ買い', 'スーパーSALE'],
            'low': ['訳あり', 'アウトレット', 'B級品']
        }
    
    def determine_priority(self, product_title: str, product_tags: str = "", variant_count: int = 1) -> str:
        """
        Determine priority level for missing image based on product characteristics
        """
        # Handle NaN values that come as float from pandas
        if pd.isna(product_title):
            product_title = ""
        if pd.isna(product_tags):
            product_tags = ""
            
        title_lower = str(product_title).lower()
        tags_lower = str(product_tags).lower() if product_tags else ""
        combined_text = f"{title_lower} {tags_lower}"
        
        # High priority criteria
        if any(keyword in combined_text for keyword in self.priority_keywords['high']):
            return 'high'
        
        # Products with multiple variants are generally more important
        if variant_count > 3:
            return 'high'
        
        # Low priority criteria
        if any(keyword in combined_text for keyword in self.priority_keywords['low']):
            return 'low'
        
        # Medium priority criteria or default
        if any(keyword in combined_text for keyword in self.priority_keywords['medium']):
            return 'medium'
        
        return 'medium'  # Default priority
    
    def audit_product_from_csv_data(self, product_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """
        Audit a single product's images from CSV data
        Returns list of audit records for products/variants with missing images
        """
        audit_records = []
        
        if not product_data:
            return audit_records
        
        handle, rows = product_data
        
        # Get product-level info from first row
        main_row = rows[0]
        product_title = main_row.get('Title', '')
        product_tags = main_row.get('Tags', '')
        
        # Handle NaN values
        if pd.isna(product_title):
            product_title = ''
        else:
            product_title = str(product_title)
            
        if pd.isna(product_tags):
            product_tags = ''
        else:
            product_tags = str(product_tags)
        
        # Count unique images across all rows
        unique_images = set()
        for row in rows:
            image_src = row.get('Image Src', '')
            if image_src and not pd.isna(image_src):
                unique_images.add(str(image_src))
        
        product_image_count = len(unique_images)
        
        # Group variants by SKU
        variants = {}
        for row in rows:
            variant_sku = row.get('Variant SKU', '')
            if variant_sku and not pd.isna(variant_sku):
                variant_sku = str(variant_sku)
                if variant_sku not in variants:
                    variants[variant_sku] = {
                        'sku': variant_sku,
                        'title': row.get('Option1 Value', '') or variant_sku,
                        'has_image': False
                    }
                
                # Check if this variant has an associated image (either Image Src or Variant Image)
                image_src = row.get('Image Src', '')
                variant_image = row.get('Variant Image', '')
                
                if (image_src and not pd.isna(image_src)) or (variant_image and not pd.isna(variant_image)):
                    variants[variant_sku]['has_image'] = True
        
        variant_count = len(variants)
        
        # Check each variant for issues
        for variant_sku, variant_info in variants.items():
            needs_attention = False
            issues = []
            
            # No product images at all
            if product_image_count == 0:
                needs_attention = True
                issues.append("no_product_images")
            
            # Check if this variant has no specific variant image
            if not variant_info['has_image']:
                needs_attention = True
                if variant_count > 1:
                    issues.append("no_variant_image_multi_variant")
                else:
                    issues.append("no_variant_image_single_variant")
            
            if needs_attention:
                priority = self.determine_priority(product_title, product_tags, variant_count)
                
                audit_records.append({
                    'productHandle': handle,
                    'productId': None,  # Will be filled by Node.js
                    'variantSku': variant_sku,
                    'productTitle': product_title,
                    'productImageCount': product_image_count,
                    'variantCount': variant_count,
                    'variantHasImage': variant_info['has_image'],
                    'priorityLevel': priority,
                    'issues': issues,
                    'productTags': product_tags,
                    'needsAttention': True
                })
        
        return audit_records


def get_csv_files() -> List[Path]:
    """Get all CSV files from data directory"""
    data_dir = Path(__file__).parent.parent / "data"
    return list(data_dir.glob("products_export_*.csv"))

def load_and_group_csv_data() -> Dict[str, List[Dict[str, Any]]]:
    """
    Load all CSV files and group rows by product handle
    Returns dict of handle -> list of rows
    """
    print("📂 Loading CSV data files...")
    
    csv_files = get_csv_files()
    all_products = {}
    total_rows = 0
    
    for csv_file in csv_files:
        print(f"   📄 Reading {csv_file.name}...")
        
        try:
            # Read entire CSV file
            df = pd.read_csv(csv_file, encoding='utf-8', low_memory=False)
            rows_in_file = len(df)
            total_rows += rows_in_file
            
            print(f"      ✅ {rows_in_file:,} rows loaded")
            
            # Group by Handle
            for _, row in df.iterrows():
                handle = row.get('Handle', '')
                if handle and not pd.isna(handle):
                    handle = str(handle).strip()
                    if handle not in all_products:
                        all_products[handle] = []
                    all_products[handle].append(row.to_dict())
            
        except Exception as e:
            print(f"      ❌ Error reading {csv_file.name}: {e}")
            continue
    
    unique_products = len(all_products)
    print(f"📊 Summary: {total_rows:,} total rows → {unique_products:,} unique products")
    
    return all_products


def analyze_csv_files_for_missing_images() -> List[Dict[str, Any]]:
    """
    Analyze CSV files to find products with missing images
    Returns list of records for CSV output
    """
    print("🔍 Analyzing CSV files for missing images...")
    
    # Load and group CSV data
    grouped_products = load_and_group_csv_data()
    
    if not grouped_products:
        print("❌ No products found in CSV files")
        return []
    
    auditor = ImageAuditor()
    missing_image_records = []
    total_products = len(grouped_products)
    processed_count = 0
    issues_found = 0
    
    print(f"🔄 Auditing {total_products:,} products for image issues...")
    
    for handle, product_rows in tqdm(grouped_products.items(), desc="Auditing products"):
        try:
            # Audit this product's images
            audit_results = auditor.audit_product_from_csv_data((handle, product_rows))
            
            if audit_results:
                missing_image_records.extend(audit_results)
                issues_found += len(audit_results)
            
            processed_count += 1
            
            # Progress logging every 1000 products
            if processed_count % 1000 == 0:
                print(f"   📈 Processed {processed_count:,}/{total_products:,} products "
                      f"({issues_found} issues found)")
                
        except Exception as e:
            print(f"   ❌ Error auditing product {handle}: {e}")
            continue
    
    print(f"\n📊 Analysis Summary:")
    print(f"   📄 Products processed: {processed_count:,}")
    print(f"   🎯 Products with image issues: {len(set(r['productHandle'] for r in missing_image_records))}")
    print(f"   🔍 Total issues found: {issues_found}")
    
    # Show priority distribution
    priority_counts = {}
    for record in missing_image_records:
        priority = record['priorityLevel']
        priority_counts[priority] = priority_counts.get(priority, 0) + 1
    
    print(f"   📈 Priority distribution:")
    for priority in ['high', 'medium', 'low']:
        count = priority_counts.get(priority, 0)
        print(f"      - {priority}: {count}")
    
    return missing_image_records

def save_missing_images_csv(missing_image_records: List[Dict[str, Any]]) -> Path:
    """Save missing images list to CSV file"""
    reports_dir = Path(__file__).parent.parent / "reports"
    reports_dir.mkdir(exist_ok=True)
    
    csv_path = reports_dir / "04_missing_images_list.csv"
    
    print(f"\n💾 Saving CSV list to {csv_path}")
    
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'product_handle', 'product_title', 'variant_sku', 'priority_level',
            'product_image_count', 'variant_count', 'variant_has_image',
            'issues', 'product_tags', 'suggested_images_needed', 'notes'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        # Sort by priority and then by product title
        priority_order = {'high': 0, 'medium': 1, 'low': 2}
        sorted_records = sorted(
            missing_image_records,
            key=lambda r: (priority_order.get(r['priorityLevel'], 1), r['productTitle'])
        )
        
        for record in sorted_records:
            # Determine suggested number of images needed
            issues_list = record['issues']
            if 'no_product_images' in issues_list:
                suggested_images = max(1, record['variantCount'])  # At least 1, ideally 1 per variant
            elif 'no_variant_image_multi_variant' in issues_list:
                suggested_images = 1  # Just need variant-specific image
            elif 'no_variant_image_single_variant' in issues_list:
                suggested_images = 1  # Just need variant-specific image
            else:
                suggested_images = 1
            
            # Create helpful notes
            notes = []
            if 'no_product_images' in issues_list:
                notes.append("No product images at all - urgent")
            if 'no_variant_image_multi_variant' in issues_list:
                notes.append("Missing variant-specific image (multi-variant product)")
            if 'no_variant_image_single_variant' in issues_list:
                notes.append("Missing variant-specific image (single variant)")
            if record['variantCount'] > 3:
                notes.append("Multi-variant product - consider color/style variations")
            
            writer.writerow({
                'product_handle': record['productHandle'],
                'product_title': record['productTitle'],
                'variant_sku': record['variantSku'],
                'priority_level': record['priorityLevel'],
                'product_image_count': record['productImageCount'],
                'variant_count': record['variantCount'],
                'variant_has_image': record['variantHasImage'],
                'issues': ', '.join(issues_list),
                'product_tags': record['productTags'],
                'suggested_images_needed': suggested_images,
                'notes': '; '.join(notes)
            })
    
    return csv_path


def main():
    """Main execution function"""
    print("=" * 70)
    print("🔍 MISSING IMAGES AUDIT (CSV LIST OUTPUT)")
    print("=" * 70)
    
    try:
        # Phase 1: Analyze CSV files
        missing_image_records = analyze_csv_files_for_missing_images()
        
        if not missing_image_records:
            print("\n✅ All products have adequate images!")
            # Create empty CSV file for consistency
            reports_dir = Path(__file__).parent.parent / "reports"
            reports_dir.mkdir(exist_ok=True)
            csv_path = reports_dir / "04_missing_images_list.csv"
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['message'])
                writer.writerow(['No missing image issues found in CSV data'])
            return 0
        
        # Phase 2: Save CSV list
        csv_path = save_missing_images_csv(missing_image_records)
        
        # Phase 3: Generate summary list
        print(f"\n📋 Generating summary list...")
        
        summary_list = []
        handles_seen = set()
        
        for record in missing_image_records:
            handle = record['productHandle']
            if handle not in handles_seen:
                handles_seen.add(handle)
                # Count issues for this product
                product_issues = [r for r in missing_image_records if r['productHandle'] == handle]
                
                summary_list.append({
                    'handle': handle,
                    'title': record['productTitle'],
                    'image_count': record['productImageCount'],
                    'variant_count': record['variantCount'],
                    'issues_count': len(product_issues),
                    'priority': record['priorityLevel'],
                    'issues': list(set(issue for r in product_issues for issue in r['issues']))
                })
        
        # Sort by priority (high first) then by issues count
        priority_order = {'high': 0, 'medium': 1, 'low': 2}
        summary_list.sort(key=lambda x: (priority_order.get(x['priority'], 1), -x['issues_count']))
        
        # Print summary list to console  
        print(f"\n📝 Summary List - Products with Missing Images:")
        print(f"{'Handle':<30} {'Title':<40} {'Images':<8} {'Variants':<10} {'Priority':<10} {'Issues':<30}")
        print("-" * 130)
        
        for item in summary_list[:20]:  # Show first 20
            title_truncated = item['title'][:37] + "..." if len(item['title']) > 40 else item['title']
            issues_str = ', '.join(item['issues'])
            if len(issues_str) > 27:
                issues_str = issues_str[:27] + "..."
            
            print(f"{item['handle']:<30} {title_truncated:<40} {item['image_count']:<8} "
                  f"{item['variant_count']:<10} {item['priority']:<10} {issues_str:<30}")
        
        if len(summary_list) > 20:
            print(f"... and {len(summary_list)-20} more products")
        
        print(f"\n🎉 Analysis completed successfully!")
        print(f"   📄 CSV list saved to: {csv_path}")
        print(f"   📝 Use this CSV to search for correct images and attach them")
        print(f"\n💡 Next steps:")
        print(f"   1. Open the CSV file to see products needing images")
        print(f"   2. Search for and attach appropriate images")
        print(f"   3. Use Node.js GraphQL to upload images to Shopify")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⏹️  Analysis interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())