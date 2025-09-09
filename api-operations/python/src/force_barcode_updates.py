#!/usr/bin/env python3
"""
Generate barcode update JSON with force update capability - shows existing barcodes and forces updates.
"""

import pandas as pd
import json
import os
import glob
from datetime import datetime

def analyze_current_barcodes(data_dir):
    """
    Analyze current barcode status across all products_export files.
    
    Args:
        data_dir: Directory containing products_export_*.csv files
        
    Returns:
        DataFrame with current barcode status
    """
    csv_pattern = os.path.join(data_dir, "products_export_*.csv")
    csv_files = glob.glob(csv_pattern)
    
    all_products = []
    
    for csv_file in sorted(csv_files):
        try:
            df = pd.read_csv(csv_file, encoding='utf-8')
            if len(df) > 0:
                # Select key columns
                subset = df[['Handle', 'Variant SKU', 'Title', 'Variant Barcode']].copy()
                subset['Source_File'] = os.path.basename(csv_file)
                all_products.append(subset)
        except Exception as e:
            print(f"Error reading {csv_file}: {str(e)}")
    
    if all_products:
        combined_df = pd.concat(all_products, ignore_index=True)
        
        # Analyze barcode status
        combined_df['Has_Barcode'] = combined_df['Variant Barcode'].notna() & (combined_df['Variant Barcode'] != '') & (combined_df['Variant Barcode'] != '""')
        
        return combined_df
    else:
        return pd.DataFrame()

def create_force_update_json(update_json_file, current_products_df, output_file=None, force_update=True):
    """
    Create JSON for barcode updates with force update capability.
    
    Args:
        update_json_file: Existing JSON file with catalog updates
        current_products_df: DataFrame with current product barcode status
        output_file: Output JSON file path
        force_update: If True, update even if barcode exists
        
    Returns:
        str: Path to generated JSON file
    """
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_dir = os.path.dirname(update_json_file)
        output_file = os.path.join(base_dir, f"force_barcode_updates_{timestamp}.json")
    
    try:
        # Load the update JSON file
        with open(update_json_file, 'r', encoding='utf-8') as f:
            update_data = json.load(f)
        
        updates = update_data.get('updates', [])
        if not updates:
            print("No updates found in JSON file")
            return None
        
        # Create lookup for current barcode status
        barcode_lookup = {}
        if not current_products_df.empty:
            for _, row in current_products_df.iterrows():
                sku = str(row['Variant SKU']).strip() if pd.notna(row['Variant SKU']) else ""
                if sku:
                    barcode_lookup[sku] = {
                        'current_barcode': str(row['Variant Barcode']) if pd.notna(row['Variant Barcode']) and str(row['Variant Barcode']) != '' else "",
                        'has_barcode': row['Has_Barcode'],
                        'handle': str(row['Handle']) if pd.notna(row['Handle']) else "",
                        'title': str(row['Title']) if pd.notna(row['Title']) else ""
                    }
        
        # Process updates with current barcode information
        processed_updates = []
        skipped_count = 0
        force_updated_count = 0
        new_barcode_count = 0
        
        for update in updates:
            variant_sku = update.get('variant_sku', '')
            new_catalog_id = update.get('catalog_id', '')
            
            current_info = barcode_lookup.get(variant_sku, {})
            current_barcode = current_info.get('current_barcode', '')
            has_barcode = current_info.get('has_barcode', False)
            
            # Create enhanced update entry
            enhanced_update = {
                "handle": update.get('handle', ''),
                "variant_sku": variant_sku,
                "catalog_id": new_catalog_id,
                "current_barcode": current_barcode,
                "has_existing_barcode": has_barcode,
                "force_update": force_update,
                "update_status": ""
            }
            
            # Determine update status
            if not has_barcode:
                enhanced_update["update_status"] = "new_barcode"
                new_barcode_count += 1
            elif has_barcode and force_update:
                enhanced_update["update_status"] = "force_update"
                force_updated_count += 1
            elif has_barcode and not force_update:
                enhanced_update["update_status"] = "skipped_has_barcode"
                skipped_count += 1
            
            # Add to processed updates (include all if force_update, skip existing if not)
            if force_update or not has_barcode:
                processed_updates.append(enhanced_update)
        
        # Create the JSON structure
        json_data = {
            "force_update_enabled": force_update,
            "summary": {
                "total_updates": len(processed_updates),
                "new_barcodes": new_barcode_count,
                "force_updates": force_updated_count,
                "skipped_existing": skipped_count
            },
            "updates": processed_updates
        }
        
        # Save to JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        print(f"Generated force update JSON: {output_file}")
        print(f"Total updates to process: {len(processed_updates)}")
        print(f"New barcodes: {new_barcode_count}")
        print(f"Force updates (existing barcodes): {force_updated_count}")
        print(f"Skipped (has barcode, no force): {skipped_count}")
        
        return output_file
        
    except Exception as e:
        print(f"Error creating force update JSON: {str(e)}")
        return None

def main():
    """Main function to create force update JSON."""
    data_dir = "/Users/gen/corekara/rakuten-shopify/api-operations/data"
    
    print("=== Analyzing Current Barcode Status ===")
    
    # Analyze current products
    current_products_df = analyze_current_barcodes(data_dir)
    
    if current_products_df.empty:
        print("No product data found")
        return
    
    total_products = len(current_products_df)
    with_barcodes = len(current_products_df[current_products_df['Has_Barcode'] == True])
    without_barcodes = total_products - with_barcodes
    
    print(f"Total products: {total_products}")
    print(f"With barcodes: {with_barcodes}")
    print(f"Without barcodes: {without_barcodes}")
    
    # Find latest update JSON files
    update_patterns = [
        "barcode_updates_found_*.json",
        "notfound_updates_*.json"
    ]
    
    for pattern in update_patterns:
        json_pattern = os.path.join(data_dir, pattern)
        json_files = glob.glob(json_pattern)
        
        if json_files:
            latest_json = max(json_files, key=os.path.getmtime)
            print(f"\n=== Processing {os.path.basename(latest_json)} ===")
            
            # Create force update JSON
            force_json = create_force_update_json(
                latest_json, 
                current_products_df, 
                force_update=True
            )
            
            if force_json:
                print(f"Force update JSON ready: {os.path.basename(force_json)}")

if __name__ == "__main__":
    main()