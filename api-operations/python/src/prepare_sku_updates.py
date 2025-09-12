#!/usr/bin/env python3
"""
Prepare SKU Update Mapping

This script processes Shopify export CSV files and Rakuten tax CSV to create 
a consolidated mapping for updating SKUs from 商品番号 to 商品管理番号.

Usage:
    python prepare_sku_updates.py
"""

import csv
import json
import os
from collections import defaultdict

def main():
    # File paths
    data_dir = '/Users/gen/corekara/rakuten-shopify/api-operations/data'
    rakuten_csv = '/Users/gen/corekara/rakuten-shopify/api-operations/python/output/rakuten-tax.csv'
    output_path = '/Users/gen/corekara/rakuten-shopify/api-operations/shared/sku_update_mapping.json'
    
    print("=" * 70)
    print("SKU UPDATE MAPPING PREPARATION")
    print("=" * 70)
    
    # Step 1: Read Rakuten SKU mappings (商品番号 -> 商品管理番号)
    print("📂 Reading Rakuten tax CSV...")
    rakuten_mappings = {}  # current_sku (商品番号) -> new_sku (商品管理番号)
    different_count = 0
    
    with open(rakuten_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            current_sku = row['商品番号']  # What Shopify currently uses
            new_sku = row['商品管理番号']   # What we want to change to
            
            if current_sku and new_sku:
                rakuten_mappings[current_sku] = new_sku
                if current_sku != new_sku:
                    different_count += 1
    
    print(f"✅ Loaded {len(rakuten_mappings)} Rakuten SKU mappings")
    print(f"📊 Found {different_count} SKUs that need updating")
    
    # Step 2: Read all Shopify export CSVs
    print("\n📂 Reading Shopify export CSVs...")
    shopify_products = defaultdict(list)  # handle -> list of variants
    total_variants = 0
    
    for i in range(1, 6):
        csv_path = os.path.join(data_dir, f'products_export_{i}.csv')
        if not os.path.exists(csv_path):
            print(f"⚠️ Skipping missing file: {csv_path}")
            continue
            
        print(f"   Processing {csv_path}...")
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                handle = row['Handle']
                variant_sku = row['Variant SKU']
                
                if handle and variant_sku:  # Only process rows with both handle and SKU
                    shopify_products[handle].append(variant_sku)
                    total_variants += 1
    
    print(f"✅ Loaded {len(shopify_products)} products with {total_variants} total variants")
    
    # Step 3: Build update mapping
    print("\n🔍 Building SKU update mapping...")
    update_mapping = {}
    total_products_needing_update = 0
    total_variants_needing_update = 0
    
    for handle, variant_skus in shopify_products.items():
        product_updates = []
        
        for current_sku in variant_skus:
            if current_sku in rakuten_mappings:
                new_sku = rakuten_mappings[current_sku]
                
                # Only include if SKU actually changes
                if current_sku != new_sku:
                    product_updates.append({
                        'currentSKU': current_sku,
                        'newSKU': new_sku
                    })
                    total_variants_needing_update += 1
        
        # Only include products that have at least one variant needing update
        if product_updates:
            update_mapping[handle] = {
                'variants': product_updates,
                'totalVariants': len(variant_skus),
                'variantsToUpdate': len(product_updates)
            }
            total_products_needing_update += 1
    
    print(f"✅ Found {total_products_needing_update} products needing SKU updates")
    print(f"✅ Total variants needing update: {total_variants_needing_update}")
    
    # Step 4: Create detailed mapping with metadata
    output_data = {
        'metadata': {
            'generatedAt': '2025-09-12T05:30:00.000Z',
            'description': 'SKU update mapping from 商品番号 to 商品管理番号',
            'totalProducts': len(shopify_products),
            'totalVariants': total_variants,
            'productsNeedingUpdate': total_products_needing_update,
            'variantsNeedingUpdate': total_variants_needing_update,
            'rakutanMappings': len(rakuten_mappings),
            'skuChanges': different_count
        },
        'products': update_mapping
    }
    
    # Step 5: Save to JSON file
    print(f"\n💾 Saving mapping to {output_path}...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Mapping saved successfully!")
    
    # Step 6: Print summary and examples
    print("\n" + "=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)
    print(f"Total Shopify products: {len(shopify_products):,}")
    print(f"Total variants: {total_variants:,}")
    print(f"Products needing SKU updates: {total_products_needing_update:,}")
    print(f"Variants needing SKU updates: {total_variants_needing_update:,}")
    print(f"Percentage of products affected: {(total_products_needing_update/len(shopify_products)*100):.1f}%")
    print(f"Percentage of variants affected: {(total_variants_needing_update/total_variants*100):.1f}%")
    
    # Show examples
    print("\n📝 Example mappings (first 10 products):")
    count = 0
    for handle, product_data in update_mapping.items():
        print(f"\nHandle: {handle}")
        print(f"  Variants to update: {product_data['variantsToUpdate']}/{product_data['totalVariants']}")
        for variant in product_data['variants']:
            print(f"    {variant['currentSKU']} → {variant['newSKU']}")
        
        count += 1
        if count >= 10:
            break
    
    print(f"\n💡 Next steps:")
    print(f"   1. Review the generated mapping file: {output_path}")
    print(f"   2. Run: node src/11_update_product_skus.js --dry-run")
    print(f"   3. Execute: node src/11_update_product_skus.js")

if __name__ == "__main__":
    main()