#!/usr/bin/env python3
"""
Prepare SKU Update Mapping (Version 5)

This script fixes the fundamental flaw by only matching Rakuten entries 
that represent the SAME PRODUCT as what exists in Shopify, not different 
product variants (like free shipping versions that don't exist in Shopify).

Usage:
    python prepare_sku_updates_v5.py
"""

import csv
import json
import os
from collections import defaultdict
import re

def normalize_product_title(title):
    """Normalize product title by removing shipping/packaging differences."""
    if not title:
        return ""
    
    normalized = title.strip()
    
    # Remove shipping indicators
    normalized = re.sub(r'《送料無料》', '', normalized)
    
    # Remove trial/expiring indicators
    normalized = re.sub(r'《賞味期限間近のお試し価格》', '', normalized)
    normalized = re.sub(r'《返品・交換不可》', '', normalized)
    normalized = re.sub(r'《賞味期限\d+年\d+月\d+日》', '', normalized)
    
    # Normalize whitespace
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized

def extract_core_product_info(sku, title=""):
    """Extract core product information for matching."""
    info = {
        'base_sku': sku,
        'quantity': 1,
        'is_set': False,
        'normalized_title': normalize_product_title(title)
    }
    
    # Extract base SKU (remove suffixes except quantity indicators)
    base_sku = re.sub(r'(-f|-t)$', '', sku)  # Remove -f, -t but keep -2s, -3s
    info['base_sku'] = base_sku
    
    # Check for quantity indicators
    quantity_match = re.search(r'-(\d+)s', sku)
    if quantity_match:
        info['quantity'] = int(quantity_match.group(1))
        info['is_set'] = True
    
    # Extract quantity from title if not in SKU
    if not info['is_set']:
        quantity_in_title = re.search(r'(\d+)個|(\d+)袋', title)
        if quantity_in_title:
            qty = int(quantity_in_title.group(1) or quantity_in_title.group(2))
            info['quantity'] = qty
            info['is_set'] = True
    
    return info

def find_exact_rakuten_match(shopify_sku, shopify_title, rakuten_candidates):
    """Find the exact Rakuten match that represents the same product."""
    shopify_info = extract_core_product_info(shopify_sku, shopify_title)
    
    # Look for exact matches first
    for rakuten_item in rakuten_candidates:
        rakuten_info = extract_core_product_info(rakuten_item['currentSKU'], rakuten_item['title'])
        
        # Must match on core product characteristics
        if (shopify_info['base_sku'].lower() == rakuten_info['base_sku'].lower() and
            shopify_info['quantity'] == rakuten_info['quantity'] and
            shopify_info['is_set'] == rakuten_info['is_set']):
            
            # If titles are available, they should be similar after normalization
            if (shopify_info['normalized_title'] and rakuten_info['normalized_title']):
                # Simple similarity check - should be very similar after normalization
                if shopify_info['normalized_title'] in rakuten_info['normalized_title'] or \
                   rakuten_info['normalized_title'] in shopify_info['normalized_title']:
                    return rakuten_item
            else:
                # If no titles, rely on SKU structure match
                return rakuten_item
    
    return None

def main():
    # File paths
    data_dir = '/Users/gen/corekara/rakuten-shopify/api-operations/data'
    rakuten_csv = '/Users/gen/corekara/rakuten-shopify/api-operations/python/output/rakuten-tax.csv'
    output_path = '/Users/gen/corekara/rakuten-shopify/api-operations/shared/sku_update_mapping_v5.json'
    
    print("=" * 70)
    print("SKU UPDATE MAPPING PREPARATION (V5 - EXACT PRODUCT MATCHING)")
    print("=" * 70)
    
    # Step 1: Read Rakuten data
    print("📂 Reading Rakuten tax CSV...")
    rakuten_data = []
    sku_counts = defaultdict(int)
    
    with open(rakuten_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            current_sku = row['商品番号']
            new_sku = row['商品管理番号']
            title = row['商品名']
            
            if current_sku and new_sku and title:
                rakuten_data.append({
                    'currentSKU': current_sku,
                    'newSKU': new_sku,
                    'title': title
                })
                sku_counts[current_sku] += 1
    
    print(f"✅ Loaded {len(rakuten_data)} Rakuten products")
    
    # Step 2: Read Shopify data and build SKU registry
    print(f"\\n📂 Reading Shopify export CSVs...")
    shopify_products = defaultdict(list)
    existing_skus = set()
    total_variants = 0
    
    for i in range(1, 6):
        csv_path = os.path.join(data_dir, f'products_export_{i}.csv')
        if not os.path.exists(csv_path):
            continue
            
        print(f"   Processing {csv_path}...")
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                handle = row['Handle']
                variant_sku = row['Variant SKU']
                title = row['Title'] or ''
                
                if handle and variant_sku:
                    shopify_products[handle].append({
                        'sku': variant_sku,
                        'title': title
                    })
                    existing_skus.add(variant_sku)
                    total_variants += 1
    
    print(f"✅ Loaded {len(shopify_products)} products with {total_variants} total variants")
    
    # Step 3: Build update mapping with exact product matching
    print(f"\\n🔍 Building SKU update mapping with exact product matching...")
    update_mapping = {}
    total_products_needing_update = 0
    total_variants_needing_update = 0
    exact_matches = 0
    no_match_found = 0
    conflicts_prevented = 0
    no_change_needed = 0
    match_details = []
    
    # Find duplicates
    duplicate_skus = {sku: count for sku, count in sku_counts.items() if count > 1}
    
    for handle, shopify_variants in shopify_products.items():
        product_updates = []
        
        for shopify_variant in shopify_variants:
            current_sku = shopify_variant['sku']
            shopify_title = shopify_variant['title']
            
            # Find all Rakuten entries with this SKU
            rakuten_candidates = [item for item in rakuten_data if item['currentSKU'] == current_sku]
            
            if not rakuten_candidates:
                continue
            
            if len(rakuten_candidates) == 1:
                # Simple case - only one option
                rakuten_item = rakuten_candidates[0]
                new_sku = rakuten_item['newSKU']
                
                if current_sku != new_sku:
                    # Check for SKU conflict
                    if new_sku in existing_skus:
                        conflicts_prevented += 1
                        continue
                    
                    product_updates.append({
                        'currentSKU': current_sku,
                        'newSKU': new_sku,
                        'matchType': 'unique',
                        'shopifyTitle': shopify_title,
                        'rakutenTitle': rakuten_item['title']
                    })
                    total_variants_needing_update += 1
                else:
                    no_change_needed += 1
            else:
                # Multiple candidates - find exact product match
                exact_match = find_exact_rakuten_match(
                    current_sku, shopify_title, rakuten_candidates
                )
                
                if exact_match:
                    new_sku = exact_match['newSKU']
                    
                    if current_sku != new_sku:
                        # Check for SKU conflict
                        if new_sku in existing_skus:
                            conflicts_prevented += 1
                            continue
                        
                        product_updates.append({
                            'currentSKU': current_sku,
                            'newSKU': new_sku,
                            'matchType': 'exact_match',
                            'shopifyTitle': shopify_title,
                            'rakutenTitle': exact_match['title']
                        })
                        total_variants_needing_update += 1
                        exact_matches += 1
                        
                        match_details.append({
                            'handle': handle,
                            'currentSKU': current_sku,
                            'newSKU': new_sku,
                            'shopifyInfo': extract_core_product_info(current_sku, shopify_title),
                            'rakutenInfo': extract_core_product_info(exact_match['currentSKU'], exact_match['title']),
                            'shopifyTitle': shopify_title[:80] + '...' if len(shopify_title) > 80 else shopify_title,
                            'rakutenTitle': exact_match['title'][:80] + '...' if len(exact_match['title']) > 80 else exact_match['title']
                        })
                    else:
                        no_change_needed += 1
                else:
                    # No exact match found - this means the Shopify product doesn't 
                    # correspond to any of the Rakuten variants with the same SKU
                    no_match_found += 1
                    print(f"   ℹ️  No exact match found for {current_sku} in {handle} (ignoring mismatched Rakuten variants)")
        
        if product_updates:
            update_mapping[handle] = {
                'variants': product_updates,
                'totalVariants': len(shopify_variants),
                'variantsToUpdate': len(product_updates)
            }
            total_products_needing_update += 1
    
    print(f"✅ Found {total_products_needing_update} products needing SKU updates")
    print(f"✅ Total variants needing update: {total_variants_needing_update}")
    print(f"✅ Exact matches found: {exact_matches}")
    print(f"ℹ️  No change needed: {no_change_needed}")
    print(f"ℹ️  No exact match found: {no_match_found}")
    print(f"🛡️ Conflicts prevented: {conflicts_prevented}")
    
    # Step 4: Create output
    output_data = {
        'metadata': {
            'generatedAt': '2025-09-12T07:30:00.000Z',
            'description': 'SKU update mapping with exact product matching',
            'version': 5,
            'totalProducts': len(shopify_products),
            'totalVariants': total_variants,
            'productsNeedingUpdate': total_products_needing_update,
            'variantsNeedingUpdate': total_variants_needing_update,
            'rakutenProducts': len(rakuten_data),
            'duplicateSKUs': len(duplicate_skus),
            'exactMatches': exact_matches,
            'noChangeNeeded': no_change_needed,
            'noMatchFound': no_match_found,
            'conflictsPrevented': conflicts_prevented
        },
        'matchDetails': match_details,
        'products': update_mapping
    }
    
    # Step 5: Save
    print(f"\\n💾 Saving mapping to {output_path}...")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Mapping saved successfully!")
    
    # Step 6: Summary
    print("\\n" + "=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)
    print(f"Total Shopify products: {len(shopify_products):,}")
    print(f"Total variants: {total_variants:,}")
    print(f"Products needing SKU updates: {total_products_needing_update:,}")
    print(f"Variants needing SKU updates: {total_variants_needing_update:,}")
    print(f"Exact matches: {exact_matches:,}")
    print(f"No change needed: {no_change_needed:,}")
    print(f"No match found (correctly ignored): {no_match_found:,}")
    print(f"Conflicts prevented: {conflicts_prevented:,}")
    
    print(f"\\n💡 Next steps:")
    print(f"   1. Copy: cp {output_path} /Users/gen/corekara/rakuten-shopify/api-operations/node/shared/sku_update_mapping.json")
    print(f"   2. Test: node src/11_update_product_skus.js --dry-run --test-handle mishima-na100-250")
    print(f"   3. Verify that mishima-na100-250 is NOT flagged for updates")

if __name__ == "__main__":
    main()