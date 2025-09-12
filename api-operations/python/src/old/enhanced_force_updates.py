#!/usr/bin/env python3
"""
Enhanced barcode update JSON generator with force update capability and existing barcode tracking.
"""

import pandas as pd
import json
import os
import glob
from datetime import datetime

def enhance_update_json_with_force(update_json_file, output_file=None, force_update=True):
    """
    Enhance existing update JSON with force update capability and better tracking.
    
    Args:
        update_json_file: Existing JSON file with catalog updates
        output_file: Output JSON file path
        force_update: If True, forces updates even if barcode exists
        
    Returns:
        str: Path to generated JSON file
    """
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_dir = os.path.dirname(update_json_file)
        base_name = os.path.basename(update_json_file).replace('.json', '')
        output_file = os.path.join(base_dir, f"{base_name}_force_{timestamp}.json")
    
    try:
        # Load the update JSON file
        with open(update_json_file, 'r', encoding='utf-8') as f:
            update_data = json.load(f)
        
        updates = update_data.get('updates', [])
        if not updates:
            print("No updates found in JSON file")
            return None
        
        # Enhanced updates with force capability
        enhanced_updates = []
        
        for update in updates:
            enhanced_update = {
                "handle": update.get('handle', ''),
                "variant_sku": update.get('variant_sku', ''),
                "catalog_id": update.get('catalog_id', ''),
                "current_barcode": update.get('current_barcode', ''),
                "force_update": force_update,
                "update_action": "force_update" if force_update else "update_if_empty",
                "notes": "Will update barcode regardless of existing value" if force_update else "Will only update if no existing barcode"
            }
            enhanced_updates.append(enhanced_update)
        
        # Create enhanced JSON structure
        json_data = {
            "update_mode": "force" if force_update else "conditional",
            "force_update_enabled": force_update,
            "source_file": os.path.basename(update_json_file),
            "total_updates": len(enhanced_updates),
            "description": "Enhanced barcode updates with force capability - will overwrite existing barcodes" if force_update else "Standard barcode updates - only empty barcodes",
            "updates": enhanced_updates
        }
        
        # Save to JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2, ensure_ascii=False)
        
        print(f"Enhanced update JSON created: {output_file}")
        print(f"Update mode: {'FORCE (overwrite existing)' if force_update else 'CONDITIONAL (empty only)'}")
        print(f"Total updates: {len(enhanced_updates)}")
        
        return output_file
        
    except Exception as e:
        print(f"Error creating enhanced update JSON: {str(e)}")
        return None

def show_update_summary(json_file):
    """Show summary of what will be updated."""
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        updates = data.get('updates', [])
        force_mode = data.get('force_update_enabled', False)
        
        print(f"\n=== Update Summary ===")
        print(f"File: {os.path.basename(json_file)}")
        print(f"Mode: {'FORCE UPDATE' if force_mode else 'CONDITIONAL UPDATE'}")
        print(f"Total updates: {len(updates)}")
        
        # Group by handle for summary
        handle_counts = {}
        for update in updates:
            handle = update.get('handle', '')
            if handle in handle_counts:
                handle_counts[handle] += 1
            else:
                handle_counts[handle] = 1
        
        print(f"Unique products: {len(handle_counts)}")
        print(f"Products with most variants:")
        sorted_handles = sorted(handle_counts.items(), key=lambda x: x[1], reverse=True)
        for handle, count in sorted_handles[:5]:
            print(f"  {handle}: {count} variants")
            
    except Exception as e:
        print(f"Error showing summary: {str(e)}")

def main():
    """Main function to create enhanced force update JSONs."""
    data_dir = "/Users/gen/corekara/rakuten-shopify/api-operations/data"
    
    # Find existing catalog update JSON files
    update_files = [
        "catalog_id_updates_found.json",
        "catalog_id_updates.json"
    ]
    
    processed_files = []
    
    for filename in update_files:
        json_file = os.path.join(data_dir, filename)
        
        if os.path.exists(json_file):
            print(f"\n=== Processing {filename} ===")
            
            # Create force update version
            force_json = enhance_update_json_with_force(
                json_file, 
                force_update=True
            )
            
            if force_json:
                processed_files.append(force_json)
                show_update_summary(force_json)
    
    if processed_files:
        print(f"\n=== Force Update Files Created ===")
        for file_path in processed_files:
            print(f"✓ {os.path.basename(file_path)}")
        print("\nThese files will force update barcodes even if they already exist.")
        print("Use these for your Shopify update process to overwrite existing barcodes.")
    else:
        print("No update JSON files found to process")

if __name__ == "__main__":
    main()