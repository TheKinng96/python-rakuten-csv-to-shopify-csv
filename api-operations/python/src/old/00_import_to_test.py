#!/usr/bin/env python3
"""
Script to convert production CSV data to JSON format for GraphQL import

This script:
1. Reads all 5 CSV files from data/ folder  
2. Converts CSV data to Shopify API format
3. Exports products as JSON for GraphQL processing
4. Provides console feedback on progress (xxx over xxx)
5. Outputs: shared/products_for_import.json
"""
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
import time

import pandas as pd

# Add utils to path
sys.path.append(str(Path(__file__).parent))
from utils.json_output import (
    save_json_report, 
    create_product_import_record,
    log_processing_summary,
    validate_json_structure
)


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
            'handle': self._safe_string(main_row.get('Handle', '')),
            'title': self._safe_string(main_row.get('Title', '')),
            'body_html': self._safe_string(main_row.get('Body (HTML)', '')),
            'vendor': self._safe_string(main_row.get('Vendor', '')),
            'product_type': self._safe_string(main_row.get('Type', '')),
            'tags': self._safe_string(main_row.get('Tags', '')),
            'published': str(main_row.get('Published', 'true')).lower() == 'true',
            'seo_title': self._safe_string(main_row.get('SEO Title', '')),
            'seo_description': self._safe_string(main_row.get('SEO Description', ''))
        }
        
        # Collect all options first - need to inherit option names from first row that has them
        options_map = {}
        option_names = {}  # Track option names by position
        
        # First pass: collect option names
        for row in product_rows:
            # Get option names from any row that has them (usually first row)
            option1_name = row.get('Option1 Name', '')
            if option1_name and not pd.isna(option1_name):
                option_names[1] = str(option1_name).strip()
            
            option2_name = row.get('Option2 Name', '')
            if option2_name and not pd.isna(option2_name):
                option_names[2] = str(option2_name).strip()
            
            option3_name = row.get('Option3 Name', '')
            if option3_name and not pd.isna(option3_name):
                option_names[3] = str(option3_name).strip()
        
        # Second pass: collect option values using the names we found
        for row in product_rows:
            # Process option1
            if 1 in option_names:
                option1_value = row.get('Option1 Value', '')
                if option1_value and not pd.isna(option1_value):
                    option_name = option_names[1]
                    option_value = self._clean_option_value(option1_value)
                    if option_name not in options_map:
                        options_map[option_name] = set()
                    options_map[option_name].add(option_value)
            
            # Process option2
            if 2 in option_names:
                option2_value = row.get('Option2 Value', '')
                if option2_value and not pd.isna(option2_value):
                    option_name = option_names[2]
                    option_value = self._clean_option_value(option2_value)
                    if option_name not in options_map:
                        options_map[option_name] = set()
                    options_map[option_name].add(option_value)
            
            # Process option3
            if 3 in option_names:
                option3_value = row.get('Option3 Value', '')
                if option3_value and not pd.isna(option3_value):
                    option_name = option_names[3]
                    option_value = self._clean_option_value(option3_value)
                    if option_name not in options_map:
                        options_map[option_name] = set()
                    options_map[option_name].add(option_value)
        
        # Convert options_map to proper format
        product_data['options'] = []
        for position, (option_name, values) in enumerate(options_map.items(), 1):
            product_data['options'].append({
                'name': option_name,
                'values': sorted(list(values)),
                'position': position
            })
        
        # Process variants
        variants = []
        for row in product_rows:
            variant_sku = row.get('Variant SKU', '')
            # Only create variant if SKU exists and is not NaN
            if variant_sku and not pd.isna(variant_sku) and str(variant_sku).strip():
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
                
                # Add variant image if it exists and differs from main images
                variant_image = row.get('Variant Image', '')
                if variant_image and not pd.isna(variant_image) and variant_image.strip():
                    variant['image'] = variant_image.strip()
                
                # Build optionValues array for this variant using inherited option names
                option_values = []
                
                # Process option1
                if 1 in option_names:
                    option1_value = row.get('Option1 Value', '')
                    if option1_value and not pd.isna(option1_value):
                        option_values.append({
                            'optionName': option_names[1],
                            'name': self._clean_option_value(option1_value)
                        })
                
                # Process option2
                if 2 in option_names:
                    option2_value = row.get('Option2 Value', '')
                    if option2_value and not pd.isna(option2_value):
                        option_values.append({
                            'optionName': option_names[2],
                            'name': self._clean_option_value(option2_value)
                        })
                
                # Process option3
                if 3 in option_names:
                    option3_value = row.get('Option3 Value', '')
                    if option3_value and not pd.isna(option3_value):
                        option_values.append({
                            'optionName': option_names[3],
                            'name': self._clean_option_value(option3_value)
                        })
                
                # Every variant must have optionValues if there are any options defined
                if option_names and option_values:
                    variant['optionValues'] = option_values
                elif option_names:
                    # If no option values found but option names exist, create default option
                    # This handles cases where variant has no specific option value
                    default_options = []
                    for pos in sorted(option_names.keys()):
                        default_options.append({
                            'optionName': option_names[pos],
                            'name': 'Default'  # Fallback value
                        })
                    variant['optionValues'] = default_options
                
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
                    'alt': self._safe_string(row.get('Image Alt Text', ''))
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
    
    def _safe_string(self, value) -> str:
        """Safely convert value to string, handling NaN values"""
        if pd.isna(value) or value is None:
            return ''
        return str(value)
    
    def _clean_option_value(self, value) -> str:
        """Clean option values - convert floats like 1.0, 2.0 to clean integers like 1, 2"""
        if pd.isna(value) or value is None:
            return ''
        
        str_value = str(value).strip()
        
        # Try to convert to float then int if it's a clean decimal
        try:
            float_val = float(str_value)
            # If it's a whole number (like 1.0, 2.0), convert to integer string
            if float_val.is_integer():
                return str(int(float_val))
            else:
                return str_value
        except (ValueError, TypeError):
            return str_value


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


def convert_products_to_json(grouped_products: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Convert products to JSON format for GraphQL processing
    """
    print(f"🔄 Converting products to JSON format...")
    print(f"   📦 {len(grouped_products):,} products to convert")
    
    # Initialize converter
    converter = CSVToShopifyConverter()
    
    # Track progress
    total_products = len(grouped_products)
    converted_count = 0
    error_count = 0
    start_time = time.time()
    product_records = []
    
    # Process each product
    for current_num, (handle, product_rows) in enumerate(grouped_products.items(), 1):
        try:
            # Convert to Shopify format
            product_data = converter.csv_row_to_shopify_product(product_rows)
            
            if not product_data:
                print(f"   ⚠️  Skipping empty product: {handle}")
                error_count += 1
                continue
            
            # Create JSON record using the utility function
            product_record = create_product_import_record(
                handle=product_data.get('handle', ''),
                title=product_data.get('title', ''),
                body_html=product_data.get('body_html', ''),
                vendor=product_data.get('vendor', ''),
                product_type=product_data.get('product_type', ''),
                tags=product_data.get('tags', ''),
                variants=product_data.get('variants', []),
                images=product_data.get('images', []),
                options=product_data.get('options', []),
                seo_title=product_data.get('seo_title', ''),
                seo_description=product_data.get('seo_description', '')
            )
            
            product_records.append(product_record)
            converted_count += 1
            
            # Progress logging every 50 products or at milestones
            if current_num % 50 == 0 or current_num in [1, 10, 100, 500] or current_num == total_products:
                elapsed = time.time() - start_time
                rate = current_num / elapsed if elapsed > 0 else 0
                eta_seconds = (total_products - current_num) / rate if rate > 0 else 0
                eta_minutes = eta_seconds / 60
                
                print(f"   📈 [CONVERTING] {current_num:,}/{total_products:,} products processed "
                      f"({converted_count:,} success, {error_count:,} errors) "
                      f"| Rate: {rate:.1f}/sec | ETA: {eta_minutes:.1f}min")
        
        except KeyboardInterrupt:
            print(f"\n⏹️  Conversion interrupted by user")
            print(f"   📊 Progress: {current_num:,}/{total_products:,} processed")
            break
        except Exception as e:
            error_count += 1
            print(f"   ❌ Error converting product {handle}: {e}")
            continue
    
    # Final summary
    elapsed_total = time.time() - start_time
    print(f"\n🎉 Conversion completed!")
    print(f"   📊 Total: {total_products:,} products")
    print(f"   ✅ Successful: {converted_count:,}")
    print(f"   ❌ Errors: {error_count:,}")
    print(f"   ⏱️  Time taken: {elapsed_total/60:.1f} minutes")
    
    return product_records


def main():
    """Main execution function"""
    print("=" * 70)
    print("📄 CSV TO JSON CONVERTER (SHOPIFY IMPORT)")
    print("=" * 70)
    
    try:
        # Phase 1: Load CSV data
        grouped_products = load_and_group_csv_data()
        
        if not grouped_products:
            print("❌ No products found in CSV files")
            return 1
        
        # Phase 2: Convert to JSON
        print(f"\n🔄 Converting {len(grouped_products):,} products to JSON format...")
        product_records = convert_products_to_json(grouped_products)
        
        if not product_records:
            print("❌ No products converted successfully")
            return 1
        
        # Phase 3: Validate and save JSON
        print(f"\n💾 Saving JSON data for GraphQL processing...")
        
        required_fields = ['product', 'media']
        if not validate_json_structure(product_records, required_fields):
            print("❌ JSON validation failed")
            return 1
        
        json_path = save_json_report(
            product_records,
            "products_for_import.json",
            f"Shopify products ready for GraphQL import ({len(product_records)} products)"
        )
        
        print(f"\n🎉 Conversion completed successfully!")
        print(f"   📄 JSON data saved to: {json_path}")
        print(f"   🚀 Ready for GraphQL processing")
        print(f"\n💡 Next step: cd node && npm run import-products")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⏹️  Conversion interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Conversion failed with error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())