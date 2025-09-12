#!/usr/bin/env python3
"""
Script to analyze products with -XXss.jpg images and generate JSON data for GraphQL processing

This script:
1. Scans all CSV files to identify products with -XXss.jpg images
2. Generates JSON data for Node.js GraphQL operations
3. Outputs: shared/ss_images_to_remove.json
"""
import re
import sys
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
from tqdm import tqdm

# Add utils to path
sys.path.append(str(Path(__file__).parent))
from utils.json_output import (
    save_json_report, 
    create_ss_image_removal_record,
    log_processing_summary,
    validate_json_structure
)

# Regex pattern for -XXss.jpg images
SS_IMAGE_PATTERN = re.compile(r'-(\d+)ss\.jpg', re.IGNORECASE)

def get_csv_files() -> List[Path]:
    """Get all CSV files from data directory"""
    data_dir = Path(__file__).parent.parent / "data"
    return list(data_dir.glob("products_export_*.csv"))

def analyze_csv_files_for_ss_images() -> List[Dict[str, Any]]:
    """
    Analyze CSV files to find products with -XXss.jpg images
    Returns list of records for JSON output
    """
    print("🔍 Analyzing CSV files for -XXss images...")
    
    ss_image_records = []
    csv_files = get_csv_files()
    total_rows = 0
    
    for csv_file in csv_files:
        print(f"   📄 Processing {csv_file.name}...")
        
        try:
            # Read CSV in chunks
            chunk_size = 1000
            chunk_iter = pd.read_csv(
                csv_file,
                chunksize=chunk_size,
                encoding='utf-8',
                low_memory=False
            )
            
            file_records = {}  # Group by handle
            
            for chunk in chunk_iter:
                total_rows += len(chunk)
                
                for _, row in chunk.iterrows():
                    handle = row.get('Handle', '')
                    image_src = row.get('Image Src', '')
                    image_position = row.get('Image Position', 0)
                    image_alt = row.get('Image Alt Text', '')
                    
                    if pd.isna(image_src) or not image_src or pd.isna(handle) or not handle:
                        continue
                    
                    # Check for -XXss pattern
                    ss_match = SS_IMAGE_PATTERN.search(str(image_src))
                    if ss_match:
                        if handle not in file_records:
                            file_records[handle] = {
                                'productHandle': handle,
                                'productId': None,  # Will be filled by Node.js
                                'imagesToRemove': []
                            }
                        
                        image_record = {
                            'imageUrl': str(image_src),
                            'imagePosition': int(image_position) if pd.notna(image_position) else 0,
                            'imageAlt': str(image_alt) if pd.notna(image_alt) else '',
                            'ssPattern': ss_match.group(0),
                            'ssNumber': ss_match.group(1),
                            'reason': 'ends_with_ss_pattern'
                        }
                        
                        file_records[handle]['imagesToRemove'].append(image_record)
            
            # Add file records to main list
            for record in file_records.values():
                record['totalImages'] = len(record['imagesToRemove'])
                ss_image_records.append(record)
                
            print(f"      ✅ Found {len(file_records)} products with SS images")
                        
        except Exception as e:
            print(f"      ❌ Error processing {csv_file.name}: {e}")
            continue
    
    print(f"\n📊 Analysis Summary:")
    print(f"   📄 CSV rows processed: {total_rows:,}")
    print(f"   🎯 Products with SS images: {len(ss_image_records)}")
    total_images = sum(len(record['imagesToRemove']) for record in ss_image_records)
    print(f"   🖼️  Total SS images found: {total_images}")
    
    return ss_image_records

def main():
    """Main execution function"""
    print("=" * 70)
    print("🔍 SS IMAGES ANALYSIS (JSON OUTPUT)")
    print("=" * 70)
    
    try:
        # Phase 1: Analyze CSV files
        ss_image_records = analyze_csv_files_for_ss_images()
        
        if not ss_image_records:
            print("\n✅ No products with -XXss images found!")
            # Save empty JSON file for consistency
            save_json_report([], "ss_images_to_remove.json", "No SS images found in CSV data")
            return 0
        
        # Phase 2: Validate and save JSON
        print(f"\n💾 Saving JSON data for GraphQL processing...")
        
        required_fields = ['productHandle', 'imagesToRemove', 'totalImages']
        if not validate_json_structure(ss_image_records, required_fields):
            print("❌ JSON validation failed")
            return 1
        
        json_path = save_json_report(
            ss_image_records,
            "ss_images_to_remove.json",
            f"Products with -XXss images for removal via GraphQL ({len(ss_image_records)} products)"
        )
        
        # Phase 3: Generate summary list
        print(f"\n📋 Generating summary list...")
        
        summary_list = []
        for record in ss_image_records:
            summary_list.append({
                'handle': record['productHandle'],
                'title': 'Title not available in CSV analysis',  # Title would need to be extracted from CSV
                'ss_images_count': record['totalImages'],
                'ss_patterns': list(set(img['ssPattern'] for img in record['imagesToRemove']))
            })
        
        # Print summary list to console  
        print(f"\n📝 Summary List - Products with SS Images:")
        print(f"{'Handle':<30} {'SS Images':<12} {'Patterns':<20}")
        print("-" * 70)
        
        for item in summary_list[:20]:  # Show first 20
            patterns_str = ', '.join(item['ss_patterns'][:3])  # Show first 3 patterns
            if len(item['ss_patterns']) > 3:
                patterns_str += f" (+{len(item['ss_patterns'])-3} more)"
            
            print(f"{item['handle']:<30} {item['ss_images_count']:<12} {patterns_str:<20}")
        
        if len(summary_list) > 20:
            print(f"... and {len(summary_list)-20} more products")
        
        print(f"\n🎉 Analysis completed successfully!")
        print(f"   📄 JSON data saved to: {json_path}")
        print(f"   🚀 Ready for GraphQL processing")
        print(f"\n💡 Next step: cd node && npm run remove-ss")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⏹️  Analysis interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())