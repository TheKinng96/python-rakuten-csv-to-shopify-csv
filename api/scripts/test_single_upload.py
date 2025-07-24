#!/usr/bin/env python3
"""
Test script to upload a single product from CSV to debug issues

This script:
1. Picks one product handle from CSV files
2. Shows the raw CSV data for that product
3. Shows the converted Shopify API format
4. Attempts to upload and shows any errors
5. Helps debug JSON/float/data issues before full import
"""
import csv
import json
import math
import sys
import time
from pathlib import Path
from typing import List, Dict, Any, Optional

import pandas as pd

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from shopify_manager.client import ShopifyClient
from shopify_manager.config import shopify_config, path_config
from shopify_manager.logger import get_script_logger

logger = get_script_logger("test_single_upload")


def clean_numeric_value(value) -> Optional[float]:
    """Clean numeric values for JSON compliance"""
    if pd.isna(value) or value == '' or value is None:
        return None
    
    try:
        num_value = float(str(value))
        
        # Check for NaN, infinity, or other non-JSON compliant values
        if math.isnan(num_value) or math.isinf(num_value):
            return None
            
        return num_value
    except (ValueError, TypeError):
        return None


def clean_string_value(value) -> str:
    """Clean string values for JSON compliance"""
    if pd.isna(value) or value is None:
        return ""
    
    return str(value).strip()


def safe_bool_value(value) -> bool:
    """Safely convert value to boolean"""
    if pd.isna(value) or value == '':
        return True  # Default to True for most Shopify boolean fields
    
    return str(value).lower() in ['true', 'yes', '1', 'on']


class SafeCSVToShopifyConverter:
    """Converts CSV data to Shopify API format with proper data cleaning"""
    
    def csv_row_to_shopify_product(self, product_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Convert grouped CSV rows to Shopify product format with data cleaning
        """
        if not product_rows:
            return None
        
        # Use first row for product-level data
        main_row = product_rows[0]
        
        print("🔍 Raw CSV data for main row:")
        for key, value in main_row.items():
            if pd.notna(value) and value != '':
                print(f"   {key}: {repr(value)} (type: {type(value).__name__})")
        
        # Basic product data with cleaning
        original_handle = clean_string_value(main_row.get('Handle', ''))
        
        product_data = {
            'handle': original_handle,
            'title': clean_string_value(main_row.get('Title', '')),
            'body_html': clean_string_value(main_row.get('Body (HTML)', '')),
            'vendor': clean_string_value(main_row.get('Vendor', '')),
            'product_type': clean_string_value(main_row.get('Type', '')),
            'tags': clean_string_value(main_row.get('Tags', '')),
            'published': safe_bool_value(main_row.get('Published', 'true')),
        }
        
        # Add SEO fields if present
        seo_title = clean_string_value(main_row.get('SEO Title', ''))
        seo_description = clean_string_value(main_row.get('SEO Description', ''))
        
        if seo_title:
            product_data['seo_title'] = seo_title
        if seo_description:
            product_data['seo_description'] = seo_description
        
        # Process variants with careful numeric handling
        variants = []
        all_options = []  # Collect all option info first
        
        for idx, row in enumerate(product_rows):
            variant_sku = clean_string_value(row.get('Variant SKU', ''))
            if variant_sku:  # Only create variant if SKU exists
                # Clean numeric fields carefully
                price = clean_numeric_value(row.get('Variant Price', '0'))
                compare_price = clean_numeric_value(row.get('Variant Compare At Price'))
                inventory_qty = clean_numeric_value(row.get('Variant Inventory Qty', '0'))
                
                # Convert to int where appropriate
                if inventory_qty is not None:
                    inventory_qty = int(inventory_qty)
                else:
                    inventory_qty = 0
                
                if price is None:
                    price = 0.0
                
                variant = {
                    'sku': variant_sku,
                    'price': str(price),  # Shopify expects price as string
                    'inventory_quantity': inventory_qty,
                    'requires_shipping': safe_bool_value(row.get('Variant Requires Shipping', 'true')),
                    'taxable': safe_bool_value(row.get('Variant Taxable', 'true')),
                    'inventory_management': 'shopify',
                    'inventory_policy': 'deny'
                }
                
                # Add compare_at_price only if it has a valid value
                if compare_price is not None and compare_price > 0:
                    variant['compare_at_price'] = str(compare_price)
                
                # Collect option data for validation
                option1_name = clean_string_value(row.get('Option1 Name', ''))
                option1_value = clean_string_value(row.get('Option1 Value', ''))
                
                all_options.append({
                    'name': option1_name,
                    'value': option1_value,
                    'variant_idx': idx
                })
                
                print(f"\n📦 Variant {idx + 1} data:")
                for key, value in variant.items():
                    print(f"   {key}: {repr(value)} (type: {type(value).__name__})")
                
                # Debug option data
                print(f"   DEBUG - Option1 Name: {repr(option1_name)}")
                print(f"   DEBUG - Option1 Value: {repr(option1_value)}")
                
                variants.append(variant)
        
        # Only add options if ALL variants have matching option values
        valid_options = [opt for opt in all_options if opt['name'] and opt['value']]
        if len(valid_options) == len(variants) and len(valid_options) > 0:
            # All variants have valid options - add them
            option_name = valid_options[0]['name']  # Use first option name
            product_data['options'] = [{'name': option_name}]
            
            for i, variant in enumerate(variants):
                variant['option1'] = valid_options[i]['value']
            
            print(f"\n✅ Added product options: {option_name}")
        else:
            print(f"\n⚠️  Skipping options - not all variants have valid option values")
            print(f"   Valid options: {len(valid_options)}/{len(variants)} variants")
        
        if not variants:
            # Create default variant if none exist
            default_variant = {
                'sku': product_data['handle'] or 'default-sku',
                'price': '0.00',
                'inventory_quantity': 0,
                'requires_shipping': True,
                'taxable': True,
                'inventory_management': 'shopify',
                'inventory_policy': 'deny'
            }
            variants.append(default_variant)
        
        product_data['variants'] = variants
        
        # Process images carefully
        images = []
        for row in product_rows:
            image_src = clean_string_value(row.get('Image Src', ''))
            if image_src and image_src.startswith(('http://', 'https://')):
                image_position = clean_numeric_value(row.get('Image Position', '1'))
                if image_position is None:
                    image_position = 1
                else:
                    image_position = int(image_position)
                
                image = {
                    'src': image_src,
                    'position': image_position,
                    'alt': clean_string_value(row.get('Image Alt Text', ''))
                }
                
                # Avoid duplicate images
                if not any(img['src'] == image['src'] for img in images):
                    images.append(image)
        
        if images:
            product_data['images'] = images
        
        return product_data


def find_test_product() -> Optional[tuple[str, List[Dict[str, Any]]]]:
    """Find a suitable test product from CSV files"""
    print("🔍 Looking for a test product in CSV files...")
    
    csv_files = path_config.get_csv_files()
    
    for csv_file in csv_files[:1]:  # Just check first CSV file
        print(f"   📄 Checking {csv_file.name}...")
        
        try:
            # Read first chunk only
            df = pd.read_csv(csv_file, encoding='utf-8', nrows=1000)
            
            # Group by Handle and find one with data
            handles = df['Handle'].dropna().unique()
            
            for handle in handles[:5]:  # Check first 5 handles
                product_rows = df[df['Handle'] == handle].to_dict('records')
                
                # Look for one with some actual data
                main_row = product_rows[0]
                if (main_row.get('Title') and 
                    main_row.get('Variant SKU') and 
                    pd.notna(main_row.get('Title'))):
                    
                    print(f"   ✅ Found test product: {handle}")
                    print(f"      Title: {main_row.get('Title')}")
                    print(f"      Variants: {len(product_rows)}")
                    return handle, product_rows
            
        except Exception as e:
            print(f"   ❌ Error reading {csv_file.name}: {e}")
            continue
    
    print("   ❌ No suitable test product found")
    return None


def test_json_serialization(product_data: Dict[str, Any]) -> bool:
    """Test if product data can be serialized to JSON"""
    print("\n🧪 Testing JSON serialization...")
    
    try:
        json_string = json.dumps(product_data, indent=2)
        print("   ✅ JSON serialization successful")
        
        # Show first few lines of JSON
        json_lines = json_string.split('\n')
        print("   📄 JSON preview (first 20 lines):")
        for i, line in enumerate(json_lines[:20]):
            print(f"      {line}")
        if len(json_lines) > 20:
            print(f"      ... ({len(json_lines) - 20} more lines)")
        
        return True
        
    except Exception as e:
        print(f"   ❌ JSON serialization failed: {e}")
        
        # Try to find the problematic field
        print("   🔍 Checking individual fields...")
        for key, value in product_data.items():
            try:
                json.dumps({key: value})
                print(f"      ✅ {key}: OK")
            except Exception as field_error:
                print(f"      ❌ {key}: {field_error}")
                print(f"         Value: {repr(value)}")
                print(f"         Type: {type(value)}")
        
        return False


def test_shopify_upload(product_data: Dict[str, Any]) -> bool:
    """Test uploading to Shopify"""
    print("\n🚀 Testing Shopify upload...")
    
    try:
        client = ShopifyClient(shopify_config, use_test_store=True)
        
        if shopify_config.dry_run:
            print("   🔍 DRY RUN mode - simulating upload")
            return True
        
        response = client.create_product(product_data)
        
        if response.get('product', {}).get('id'):
            product_id = response['product']['id']  
            print(f"   ✅ Upload successful! Product ID: {product_id}")
            print(f"      Handle: {response['product'].get('handle')}")
            print(f"      Title: {response['product'].get('title')}")
            return True
        else:
            print(f"   ❌ Upload failed - unexpected response format")
            print(f"      Response: {json.dumps(response, indent=2)}")
            return False
            
    except Exception as e:
        print(f"   ❌ Upload failed with error: {e}")
        
        # The error details should now be included in the exception message
        # from our improved client error handling
        
        return False


def save_test_product_csv(handle: str, product_rows: List[Dict[str, Any]]) -> None:
    """Save the test product's CSV data to a file for examination"""
    csv_path = Path(__file__).parent.parent / "reports" / f"test_product_{handle}_data.csv"
    
    print(f"\n💾 Saving test product CSV data to: {csv_path}")
    
    # Get all possible columns from all rows
    all_columns = set()
    for row in product_rows:
        all_columns.update(row.keys())
    
    # Sort columns for consistent output
    columns = sorted(list(all_columns))
    
    # Write CSV
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        
        for row in product_rows:
            # Clean the row data for CSV output
            clean_row = {}
            for col in columns:
                value = row.get(col, '')
                # Handle pandas NaN values
                if pd.isna(value):
                    clean_row[col] = ''
                else:
                    clean_row[col] = str(value)
            writer.writerow(clean_row)
    
    print(f"   ✅ Saved {len(product_rows)} rows with {len(columns)} columns")
    print(f"   📋 Product: {handle}")
    print(f"   📄 File: {csv_path}")


def main():
    """Main test function"""
    print("=" * 70)
    print("🧪 SINGLE PRODUCT UPLOAD TEST")
    print("=" * 70)
    
    try:
        # Step 1: Find a test product
        test_result = find_test_product()
        if not test_result:
            print("❌ Could not find a suitable test product")
            return 1
        
        handle, product_rows = test_result
        
        # Step 1.5: Save test product CSV data for examination
        save_test_product_csv(handle, product_rows)
        
        # Step 2: Convert to Shopify format
        print(f"\n🔄 Converting product '{handle}' to Shopify format...")
        converter = SafeCSVToShopifyConverter()
        product_data = converter.csv_row_to_shopify_product(product_rows)
        
        if not product_data:
            print("❌ Failed to convert product data")
            return 1
        
        print(f"\n📋 Converted product data summary:")
        print(f"   Handle: {product_data.get('handle')}")
        print(f"   Title: {product_data.get('title')}")
        print(f"   Variants: {len(product_data.get('variants', []))}")
        print(f"   Images: {len(product_data.get('images', []))}")
        
        # Step 3: Test JSON serialization
        if not test_json_serialization(product_data):
            print("❌ Product data is not JSON serializable")
            return 1
        
        # Step 4: Ask if user wants to test upload
        print(f"\n🔍 Data analysis completed. The CSV data has been saved for your examination.")
        print(f"📄 Check: reports/test_product_{handle}_data.csv")
        
        if input("\nDo you want to test the Shopify upload? (y/N): ").lower() == 'y':
            # Test Shopify upload
            if not test_shopify_upload(product_data):
                print("❌ Shopify upload failed")
                return 1
            
            print(f"\n🎉 Test completed successfully!")
            print(f"   ✅ Product data is valid")
            print(f"   ✅ JSON serialization works")
            print(f"   ✅ Shopify upload works")
            print(f"\n💡 You can now run the full import script safely!")
        else:
            print(f"\n✅ Data preparation completed successfully!")
            print(f"   ✅ Test product CSV data saved")
            print(f"   ✅ Product data conversion works")
            print(f"   ✅ JSON serialization works")
            print(f"\n💡 Please examine the CSV data and let me know what needs to be fixed.")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⏹️  Test interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        logger.error(f"Test script failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())