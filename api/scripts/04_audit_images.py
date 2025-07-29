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
import requests
from urllib.parse import urlparse

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
                
                # Check if variant has valid image (not ending with ss.jpg)
                has_image_src = image_src and not pd.isna(image_src)
                has_valid_variant_image = (variant_image and not pd.isna(variant_image) and 
                                         str(variant_image).strip() and 
                                         not str(variant_image).strip().lower().endswith('ss.jpg'))
                
                if has_image_src or has_valid_variant_image:
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

def check_url_exists(url: str, timeout: int = 10) -> bool:
    """
    Check if a URL exists and is accessible
    Returns True if URL returns 200 status, False otherwise
    """
    if not url or pd.isna(url):
        return False
    
    try:
        response = requests.head(str(url).strip(), timeout=timeout, allow_redirects=True)
        return response.status_code == 200
    except (requests.RequestException, Exception):
        return False

def transform_image_url(url: str) -> str:
    """
    Transform image URL based on domain patterns
    
    For URLs like: https://tshop.r10s.jp/tsutsu-uraura/gold/item-image-sp/4970941520836.jpg
    Transform to: https://tshop.r10s.jp/gold/tsutsu-uraura/item-image-sp/4970941520836.jpg
    
    For other URLs (cabinet URLs), return as-is
    """
    if not url or pd.isna(url):
        return url
    
    url_str = str(url).strip()
    
    # Check if it's a tsutsu-uraura gold URL that needs transformation
    if 'tshop.r10s.jp/tsutsu-uraura/gold/' in url_str:
        # Extract the parts after the domain
        parts = url_str.split('/')
        if len(parts) >= 6:  # https, '', tshop.r10s.jp, tsutsu-uraura, gold, item-image-sp, filename
            domain_part = '/'.join(parts[:3])  # https://tshop.r10s.jp
            shop_name = parts[3]  # tsutsu-uraura
            folder_name = parts[4]  # gold
            remaining_path = '/'.join(parts[5:])  # item-image-sp/filename.jpg
            
            # Rearrange: domain/folder/shop/remaining
            transformed_url = f"{domain_part}/{folder_name}/{shop_name}/{remaining_path}"
            return transformed_url
    
    # For cabinet URLs and others, return as-is
    return url_str

def load_variant_image_lookup() -> Dict[str, str]:
    """
    Load variant image lookup from manual/final/output/shopify_products.csv
    Returns dict mapping Variant SKU -> Variant Image URL
    """
    lookup = {}
    shopify_csv_path = Path(__file__).parent.parent.parent / "manual" / "final" / "output" / "shopify_products.csv"
    
    if not shopify_csv_path.exists():
        print(f"⚠️  Warning: Shopify products CSV not found at {shopify_csv_path}")
        return lookup
    
    print(f"📂 Loading Variant Image lookup from {shopify_csv_path}")
    
    try:
        df = pd.read_csv(shopify_csv_path, encoding='utf-8', low_memory=False)
        
        for _, row in df.iterrows():
            variant_sku = row.get('Variant SKU', '')
            variant_image = row.get('Variant Image', '')
            
            # Only add if both SKU and Image exist and image is valid (not ending with ss.jpg)
            if (variant_sku and not pd.isna(variant_sku) and 
                variant_image and not pd.isna(variant_image)):
                
                sku_str = str(variant_sku).strip()
                image_str = str(variant_image).strip()
                
                if (sku_str and image_str and 
                    not image_str.lower().endswith('ss.jpg')):
                    # Transform the URL if needed
                    transformed_url = transform_image_url(image_str)
                    lookup[sku_str] = transformed_url
        
        print(f"   ✅ Loaded {len(lookup)} SKU -> Variant Image mappings")
        
    except Exception as e:
        print(f"   ❌ Error loading Shopify products CSV: {e}")
    
    return lookup

def save_sku_no_variant_image_csv(grouped_products: Dict[str, List[Dict[str, Any]]]) -> Path:
    """Save CSV of rows that have SKU but no Variant Image, with enhanced image URLs"""
    reports_dir = Path(__file__).parent.parent / "reports"
    reports_dir.mkdir(exist_ok=True)
    
    csv_path = reports_dir / "04_sku_no_variant_image.csv"
    
    print(f"\n💾 Saving rows with SKU but no Variant Image to {csv_path}")
    
    # Load the variant image lookup from shopify_products.csv
    variant_image_lookup = load_variant_image_lookup()
    
    matching_rows = []
    enhanced_count = 0
    
    # Process all products to find rows with SKU but no Variant Image
    for handle, product_rows in grouped_products.items():
        for row in product_rows:
            variant_sku = row.get('Variant SKU', '')
            variant_image = row.get('Variant Image', '')
            
            # Check if has SKU but no Variant Image
            has_sku = variant_sku and not pd.isna(variant_sku) and str(variant_sku).strip()
            # Check if variant image exists and is valid (not ending with ss.jpg)
            has_variant_image = (variant_image and not pd.isna(variant_image) and 
                               str(variant_image).strip() and 
                               not str(variant_image).strip().lower().endswith('ss.jpg'))
            
            if has_sku and not has_variant_image:
                # Create a copy of the row to modify
                enhanced_row = row.copy()
                sku_str = str(variant_sku).strip()
                
                # Try to find the variant image URL from the lookup
                if sku_str in variant_image_lookup:
                    enhanced_row['Variant Image'] = variant_image_lookup[sku_str]
                    enhanced_count += 1
                    print(f"   🔗 Enhanced {sku_str} with URL: {variant_image_lookup[sku_str]}")
                
                matching_rows.append(enhanced_row)
    
    print(f"   📊 Found {len(matching_rows)} rows with SKU but no Variant Image")
    print(f"   ✨ Enhanced {enhanced_count} rows with missing Variant Image URLs")
    
    if matching_rows:
        # Get all column names from the first row
        fieldnames = list(matching_rows[0].keys())
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for row in matching_rows:
                writer.writerow(row)
    else:
        # Create empty file with message
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['message'])
            writer.writerow(['No rows found with SKU but missing Variant Image'])
    
    return csv_path

def check_images_to_insert_urls() -> tuple[Path, list]:
    """
    Check URLs from images_to_insert.csv for accessibility
    Returns (report_path, broken_urls_list)
    """
    reports_dir = Path(__file__).parent.parent / "reports"
    reports_dir.mkdir(exist_ok=True)
    
    images_csv_path = reports_dir / "images_to_insert.csv"
    invalid_urls_path = reports_dir / "04_invalid_image_urls.csv"
    
    if not images_csv_path.exists():
        print(f"⚠️  images_to_insert.csv not found at {images_csv_path}")
        return invalid_urls_path, []
    
    print(f"\n🔍 Checking URLs from images_to_insert.csv for accessibility...")
    
    broken_urls = []
    checked_urls = set()  # Avoid checking same URL multiple times
    
    # Read images_to_insert.csv and check each URL
    try:
        df = pd.read_csv(images_csv_path, encoding='utf-8')
        
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Checking URLs"):
            url = row.get('image_url', '')
            handle = row.get('product_handle', '')
            variant_sku = row.get('variant_sku', '')
            
            if url and not pd.isna(url):
                url = str(url).strip()
                if url and url not in checked_urls:
                    checked_urls.add(url)
                    if not check_url_exists(url):
                        broken_urls.append({
                            'product_handle': handle,
                            'variant_sku': variant_sku,
                            'image_url': url,
                            'status': 'inaccessible'
                        })
        
        print(f"   📊 Found {len(broken_urls)} broken URLs out of {len(checked_urls)} checked")
        
        # Save results
        if broken_urls:
            with open(invalid_urls_path, 'w', newline='', encoding='utf-8') as f:
                fieldnames = ['product_handle', 'variant_sku', 'image_url', 'status']
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for record in broken_urls:
                    writer.writerow(record)
            
            print(f"   ✅ Saved broken URL report to {invalid_urls_path}")
        else:
            # Create empty file with message
            with open(invalid_urls_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['message'])
                writer.writerow(['All URLs from images_to_insert.csv are accessible'])
            
            print(f"   ✅ All URLs are accessible - created empty report")
        
    except Exception as e:
        print(f"   ❌ Error checking URLs: {e}")
    
    return invalid_urls_path, broken_urls

def generate_filtered_images_to_insert(broken_urls: list) -> Path:
    """
    Generate a filtered images_to_insert.csv excluding broken URLs
    """
    reports_dir = Path(__file__).parent.parent / "reports"
    reports_dir.mkdir(exist_ok=True)
    
    original_csv_path = reports_dir / "images_to_insert.csv"
    filtered_csv_path = reports_dir / "images_to_insert_filtered.csv"
    
    if not original_csv_path.exists():
        print(f"⚠️  Original images_to_insert.csv not found")
        return filtered_csv_path
    
    # Create set of broken URLs for fast lookup
    broken_url_set = {item['image_url'] for item in broken_urls}
    
    try:
        df = pd.read_csv(original_csv_path, encoding='utf-8')
        
        # Filter out rows with broken URLs
        filtered_df = df[~df['image_url'].isin(broken_url_set)]
        
        # Save filtered CSV
        filtered_df.to_csv(filtered_csv_path, index=False, encoding='utf-8')
        
        original_count = len(df)
        filtered_count = len(filtered_df)
        removed_count = original_count - filtered_count
        
        print(f"   📄 Generated filtered CSV: {filtered_csv_path}")
        print(f"   📊 Original: {original_count} rows → Filtered: {filtered_count} rows ({removed_count} broken URLs removed)")
        
    except Exception as e:
        print(f"   ❌ Error generating filtered CSV: {e}")
    
    return filtered_csv_path


def extract_variant_suffix(sku: str) -> str:
    """
    Extract variant suffix from SKU (e.g., 'product-3s' -> '3', 'product-2.0s' -> '2')
    If no suffix, returns empty string (represents first variant)
    """
    if not sku:
        return ""
    
    sku_str = str(sku).strip()
    if sku_str.endswith('s') and '-' in sku_str:
        # Extract the part between last '-' and 's'
        parts = sku_str.rsplit('-', 1)
        if len(parts) == 2:
            suffix = parts[1]
            if suffix.endswith('s') and len(suffix) > 1:
                variant_part = suffix[:-1]  # Remove the 's'
                
                # Handle decimal cases: "2.0" -> "2"
                if '.' in variant_part and variant_part.replace('.', '').isdigit():
                    # Convert to float then int to remove .0
                    try:
                        float_val = float(variant_part)
                        if float_val == int(float_val):
                            return str(int(float_val))
                    except ValueError:
                        pass
                
                return variant_part
    
    return ""  # No suffix means first variant

def save_images_to_insert_csv(grouped_products: Dict[str, List[Dict[str, Any]]]) -> Path:
    """
    Generate CSV file for variant image insertion with format: 
    product_handle,variant_sku,image_url,image_alt,variant_title_match
    This file is used by 04_insert_images.js
    """
    reports_dir = Path(__file__).parent.parent / "reports"
    reports_dir.mkdir(exist_ok=True)
    
    csv_path = reports_dir / "images_to_insert.csv"
    
    print(f"\n💾 Generating images_to_insert.csv for Node.js script")
    
    # Load the variant image lookup from shopify_products.csv
    variant_image_lookup = load_variant_image_lookup()
    
    insert_records = []
    
    # Process all products to find rows with SKU but no Variant Image
    for handle, product_rows in grouped_products.items():
        for row in product_rows:
            variant_sku = row.get('Variant SKU', '')
            variant_image = row.get('Variant Image', '')
            product_title = row.get('Title', '')
            image_alt = row.get('Image Alt Text', '')
            
            # Check if has SKU but no Variant Image
            has_sku = variant_sku and not pd.isna(variant_sku) and str(variant_sku).strip()
            # Check if variant image exists and is valid (not ending with ss.jpg)
            has_variant_image = (variant_image and not pd.isna(variant_image) and 
                               str(variant_image).strip() and 
                               not str(variant_image).strip().lower().endswith('ss.jpg'))
            
            if has_sku and not has_variant_image:
                sku_str = str(variant_sku).strip()
                
                # Try to find the variant image URL from the lookup
                if sku_str in variant_image_lookup:
                    image_url = variant_image_lookup[sku_str]
                    
                    # Extract variant title for matching (e.g., '3' from 'product-3s')
                    variant_title_match = extract_variant_suffix(sku_str)
                    
                    # Generate appropriate alt text
                    if image_alt and not pd.isna(image_alt):
                        alt_text = str(image_alt).strip()
                    elif product_title and not pd.isna(product_title):
                        alt_text = f"{product_title} - {sku_str}"
                    else:
                        alt_text = sku_str
                    
                    insert_records.append({
                        'product_handle': handle,
                        'variant_sku': sku_str,
                        'image_url': image_url,
                        'image_alt': alt_text,
                        'variant_title_match': str(variant_title_match)  # Ensure it's stored as string
                    })
    
    print(f"   📊 Found {len(insert_records)} images to insert")
    
    if insert_records:
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['product_handle', 'variant_sku', 'image_url', 'image_alt', 'variant_title_match']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for record in insert_records:
                writer.writerow(record)
        
        print(f"   ✅ Saved {len(insert_records)} image insertion records to {csv_path}")
    else:
        # Create empty file with header
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['product_handle', 'variant_sku', 'image_url', 'image_alt', 'variant_title_match']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
        
        print(f"   ℹ️  No images to insert - created empty CSV file")
    
    return csv_path

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
        # Phase 1: Load CSV data
        grouped_products = load_and_group_csv_data()
        
        if not grouped_products:
            print("❌ No products found in CSV files")
            return 1
        
        # Phase 2: Save rows with SKU but no Variant Image
        sku_csv_path = save_sku_no_variant_image_csv(grouped_products)
        
        # Phase 2.5: Generate images_to_insert.csv for Node.js script
        insert_csv_path = save_images_to_insert_csv(grouped_products)
        
        # Phase 2.6: Check URL accessibility and generate filtered CSV
        invalid_urls_path, broken_urls = check_images_to_insert_urls()
        
        # Phase 2.7: Generate filtered CSV excluding broken URLs
        filtered_csv_path = generate_filtered_images_to_insert(broken_urls)
        
        
        # Phase 3: Analyze CSV files for missing images
        print("🔍 Analyzing CSV files for missing images...")
        
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
        else:
            # Phase 4: Save CSV list
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
        print(f"   📄 SKU without Variant Image CSV: {sku_csv_path}")
        print(f"   📄 Images to Insert CSV: {insert_csv_path}")
        print(f"   📄 Broken URLs Report: {invalid_urls_path}")
        print(f"   📄 Filtered Images CSV: {filtered_csv_path}")
        if not missing_image_records:
            print(f"   📄 Missing images CSV: (empty - no issues found)")
        else:
            print(f"   📄 Missing images CSV: {csv_path}")
        print(f"\n💡 Next steps:")
        print(f"   1. Check {sku_csv_path.name} for rows with SKU but no Variant Image")
        print(f"   2. Review {invalid_urls_path.name} for any broken image URLs")
        print(f"   3. Use {filtered_csv_path.name} (excludes broken URLs) for image insertion")
        print(f"   4. Run 'node api/node/src/04_insert_images.js' to associate images with variants")
        print(f"   5. Run 'python api/scripts/01_analyze_shopify_products.py' to find -XXss.jpg patterns")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⏹️  Analysis interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())