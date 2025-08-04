#!/usr/bin/env python3
import os
import pandas as pd
import glob
import json
from datetime import datetime

def check_variant_images():
    """
    Check which Variant SKUs have no Variant Image across all CSV files in the data directory.
    """
    data_dir = os.path.join(os.path.dirname(__file__), "../data")
    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    
    if not csv_files:
        print("No CSV files found in the data directory.")
        return
    
    missing_variant_images = []
    
    for csv_file in csv_files:
        print(f"Processing: {os.path.basename(csv_file)}")
        
        try:
            df = pd.read_csv(csv_file)
            
            # Check if required columns exist
            if 'Variant SKU' not in df.columns or 'Variant Image' not in df.columns:
                print(f"  Warning: Required columns not found in {os.path.basename(csv_file)}")
                continue
            
            # Filter rows that have a Variant SKU but no Variant Image
            missing_images = df[
                (df['Variant SKU'].notna()) & 
                (df['Variant SKU'] != '') & 
                (df['Variant Image'].isna() | (df['Variant Image'] == ''))
            ]
            
            if not missing_images.empty:
                for _, row in missing_images.iterrows():
                    missing_variant_images.append({
                        'File': os.path.basename(csv_file),
                        'Variant SKU': row['Variant SKU'],
                        'Title': row.get('Title', 'N/A'),
                        'Handle': row.get('Handle', 'N/A')
                    })
                
                print(f"  Found {len(missing_images)} variants with missing images")
            else:
                print(f"  All variants have images")
                
        except Exception as e:
            print(f"  Error processing {os.path.basename(csv_file)}: {str(e)}")
    
    # Create JSON report
    report = {
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "total_csv_files_processed": len(csv_files),
            "total_variants_missing_images": len(missing_variant_images),
            "csv_files_processed": [os.path.basename(f) for f in csv_files]
        },
        "missing_variant_images": missing_variant_images
    }
    
    # Save JSON report
    reports_dir = os.path.join(os.path.dirname(__file__), "../reports")
    os.makedirs(reports_dir, exist_ok=True)
    
    report_file = os.path.join(reports_dir, "variant_images_check_report.json")
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"JSON report saved to: {report_file}")
    
    # Summary report
    print(f"\n=== SUMMARY ===")
    print(f"Total CSV files processed: {len(csv_files)}")
    print(f"Total variants missing images: {len(missing_variant_images)}")
    
    if missing_variant_images:
        print(f"\nVariants missing Variant Image:")
        print("-" * 80)
        for item in missing_variant_images:
            print(f"File: {item['File']}")
            print(f"  Variant SKU: {item['Variant SKU']}")
            print(f"  Title: {item['Title']}")
            print(f"  Handle: {item['Handle']}")
            print()
    else:
        print("\nAll variant SKUs have associated variant images!")

if __name__ == "__main__":
    check_variant_images()