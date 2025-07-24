#!/usr/bin/env python3
"""
Script to import production CSV data to test Shopify store

This script:
1. Reads all 5 CSV files from data/ folder  
2. Converts CSV data to Shopify API format
3. Uploads products to test store with progress logging
4. Provides console feedback on progress (xxx over xxx)
5. No detailed CSV reports - just console logging for upload progress
"""
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
import time

import pandas as pd

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from shopify_manager.client import ShopifyClient
from shopify_manager.config import shopify_config, path_config
from shopify_manager.logger import get_script_logger

logger = get_script_logger("00_import_to_test")


class CSVToShopifyConverter:
    """Converts CSV data to Shopify API format"""
    
    def __init__(self):
        # Mapping of CSV columns to Shopify product fields
        self.column_mapping = {
            'Handle': 'handle',
            'Title': 'title', 
            'Body (HTML)': 'body_html',
            'Vendor': 'vendor',
            'Type': 'product_type',
            'Tags': 'tags',
            'Published': 'published',
            'SEO Title': 'seo_title',
            'SEO Description': 'seo_description'
        }
        
        # Image related columns
        self.image_columns = ['Image Src', 'Image Position', 'Image Alt Text']
        
        # Variant related columns  
        self.variant_columns = [
            'Variant SKU', 'Variant Price', 'Variant Compare At Price',
            'Variant Inventory Qty', 'Variant Requires Shipping',
            'Variant Taxable', 'Option1 Name', 'Option1 Value',
            'Option2 Name', 'Option2 Value', 'Option3 Name', 'Option3 Value'  
        ]
    
    def csv_row_to_shopify_product(self, product_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Convert grouped CSV rows (for same handle) to Shopify product format
        """
        if not product_rows:
            return None
        
        # Use first row for product-level data
        main_row = product_rows[0]
        
        # Basic product data
        product_data = {
            'handle': main_row.get('Handle', ''),
            'title': main_row.get('Title', ''),
            'body_html': main_row.get('Body (HTML)', ''),
            'vendor': main_row.get('Vendor', ''),
            'product_type': main_row.get('Type', ''),
            'tags': main_row.get('Tags', ''),
            'published': str(main_row.get('Published', 'true')).lower() == 'true',
            'seo_title': main_row.get('SEO Title', ''),
            'seo_description': main_row.get('SEO Description', '')
        }
        
        # Process variants
        variants = []
        for row in product_rows:
            variant_sku = row.get('Variant SKU', '')
            if variant_sku:  # Only create variant if SKU exists
                variant = {
                    'sku': variant_sku,
                    'price': self._safe_float(row.get('Variant Price', '0')),
                    'compare_at_price': self._safe_float(row.get('Variant Compare At Price')),
                    'inventory_quantity': self._safe_int(row.get('Variant Inventory Qty', '0')),
                    'requires_shipping': str(row.get('Variant Requires Shipping', 'true')).lower() == 'true',
                    'taxable': str(row.get('Variant Taxable', 'true')).lower() == 'true',
                    'inventory_management': 'shopify',
                    'inventory_policy': 'deny'
                }
                
                # Add options if they exist
                option1_name = row.get('Option1 Name', '')
                option1_value = row.get('Option1 Value', '')
                if option1_name and option1_value:
                    variant['option1'] = option1_value
                    if 'options' not in product_data:
                        product_data['options'] = []
                    if option1_name not in [opt.get('name') for opt in product_data['options']]:
                        product_data['options'].append({'name': option1_name})
                
                variants.append(variant)
        
        if not variants:
            # Create default variant if none exist
            variants.append({
                'sku': main_row.get('Handle', ''),
                'price': '0.00',
                'inventory_quantity': 0,
                'requires_shipping': True,
                'taxable': True,
                'inventory_management': 'shopify',
                'inventory_policy': 'deny'
            })
        
        product_data['variants'] = variants
        
        # Process images
        images = []
        for row in product_rows:
            image_src = row.get('Image Src', '')
            if image_src and not pd.isna(image_src):
                image = {
                    'src': image_src,
                    'position': self._safe_int(row.get('Image Position', '1')),
                    'alt': row.get('Image Alt Text', '')
                }
                
                # Avoid duplicate images
                if not any(img['src'] == image['src'] for img in images):
                    images.append(image)
        
        if images:
            product_data['images'] = images
        
        return product_data
    
    def _safe_float(self, value) -> Optional[float]:
        """Safely convert value to float"""
        if pd.isna(value) or value == '':
            return None
        try:
            return float(str(value))
        except (ValueError, TypeError):
            return None
    
    def _safe_int(self, value) -> int:
        """Safely convert value to int"""
        if pd.isna(value) or value == '':
            return 0
        try:
            return int(float(str(value)))
        except (ValueError, TypeError):
            return 0


def load_and_group_csv_data() -> Dict[str, List[Dict[str, Any]]]:
    """
    Load all CSV files and group rows by product handle
    Returns dict of handle -> list of rows
    """
    print("📂 Loading CSV data files...")
    
    csv_files = path_config.get_csv_files()
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


def upload_products_to_shopify(grouped_products: Dict[str, List[Dict[str, Any]]]) -> None:
    """
    Upload products to Shopify test store with progress logging
    """
    print(f"🚀 Starting upload to Shopify test store...")
    print(f"   📦 {len(grouped_products):,} products to upload")
    
    # Initialize converter and client
    converter = CSVToShopifyConverter()
    client = ShopifyClient(shopify_config, use_test_store=True)
    
    # Track progress
    total_products = len(grouped_products)
    uploaded_count = 0
    error_count = 0
    start_time = time.time()
    
    # Process each product
    for current_num, (handle, product_rows) in enumerate(grouped_products.items(), 1):
        try:
            # Convert to Shopify format
            product_data = converter.csv_row_to_shopify_product(product_rows)
            
            if not product_data:
                print(f"   ⚠️  Skipping empty product: {handle}")
                continue
            
            # Upload to Shopify
            if shopify_config.dry_run:
                # Simulate upload in dry run mode
                time.sleep(0.01)  # Small delay to simulate API call
                uploaded_count += 1
            else:
                response = client.create_product(product_data)
                if response.get('product', {}).get('id'):
                    uploaded_count += 1
                else:
                    error_count += 1
                    logger.warning(f"Failed to upload product {handle}")
            
            # Progress logging every 50 products or at milestones
            if current_num % 50 == 0 or current_num in [1, 10, 100, 500] or current_num == total_products:
                elapsed = time.time() - start_time
                rate = current_num / elapsed if elapsed > 0 else 0
                eta_seconds = (total_products - current_num) / rate if rate > 0 else 0
                eta_minutes = eta_seconds / 60
                
                status = "DRY RUN" if shopify_config.dry_run else "UPLOADING"
                print(f"   📈 [{status}] {current_num:,}/{total_products:,} products processed "
                      f"({uploaded_count:,} success, {error_count:,} errors) "
                      f"| Rate: {rate:.1f}/sec | ETA: {eta_minutes:.1f}min")
        
        except KeyboardInterrupt:
            print(f"\n⏹️  Upload interrupted by user")
            print(f"   📊 Progress: {current_num:,}/{total_products:,} processed")
            return
        except Exception as e:
            error_count += 1
            logger.error(f"Error uploading product {handle}: {e}")
            
            # Continue with next product
            continue
    
    # Final summary
    elapsed_total = time.time() - start_time
    print(f"\n🎉 Upload completed!")
    print(f"   📊 Total: {total_products:,} products")
    print(f"   ✅ Successful: {uploaded_count:,}")
    print(f"   ❌ Errors: {error_count:,}")
    print(f"   ⏱️  Time taken: {elapsed_total/60:.1f} minutes")
    
    if shopify_config.dry_run:
        print(f"   🔍 This was a DRY RUN - no actual uploads performed")
        print(f"   💡 Set DRY_RUN=false in .env to perform actual upload")


def verify_test_store_connection() -> bool:
    """Verify connection to test store before upload"""
    print("🔗 Verifying connection to test store...")
    
    try:
        client = ShopifyClient(shopify_config, use_test_store=True)
        
        # Try to get a small number of products to test connection
        response = client.get_products(limit=1)
        
        if 'products' in response:
            print(f"   ✅ Successfully connected to test store")
            return True
        else:
            print(f"   ❌ Unexpected response from test store")
            return False
            
    except Exception as e:
        print(f"   ❌ Failed to connect to test store: {e}")
        print(f"   💡 Please check your .env configuration")
        return False


def main():
    """Main execution function"""
    print("=" * 70)
    print("🏪 SHOPIFY TEST STORE DATA IMPORT")
    print("=" * 70)
    
    try:
        # Phase 1: Verify connection
        if not verify_test_store_connection():
            print("\n❌ Cannot proceed without valid test store connection")
            return 1
        
        # Phase 2: Load CSV data
        grouped_products = load_and_group_csv_data()
        
        if not grouped_products:
            print("❌ No products found in CSV files")
            return 1
        
        # Phase 3: Confirm upload
        if not shopify_config.dry_run:
            print(f"\n⚠️  LIVE UPLOAD MODE - This will upload {len(grouped_products):,} products to your test store!")
            response = input("Continue? (y/N): ")
            if response.lower() != 'y':
                print("Upload cancelled by user")
                return 0
        else:
            print(f"\n🔍 DRY RUN MODE - Will simulate upload of {len(grouped_products):,} products")
        
        # Phase 4: Upload products
        upload_products_to_shopify(grouped_products)
        
        print(f"\n✨ Import process completed successfully!")
        
    except KeyboardInterrupt:
        print("\n⏹️  Import interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Import failed with error: {e}")
        logger.error(f"Import script failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())