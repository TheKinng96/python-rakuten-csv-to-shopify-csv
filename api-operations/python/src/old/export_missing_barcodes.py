#!/usr/bin/env python3
"""
Export functions for products with missing barcodes and retrieve catalog IDs from Rakuten data.
"""

import pandas as pd
import os
import glob
from datetime import datetime

def export_missing_barcode_products(data_dir, output_file=None):
    """
    Export all products with SKU but missing barcode to a single CSV file.
    
    Args:
        data_dir: Directory containing products_export_*.csv files
        output_file: Output file path (optional, defaults to timestamped file)
    
    Returns:
        tuple: (output_file_path, total_count)
    """
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(data_dir, f"missing_barcode_products_{timestamp}.csv")
    
    # Find all products_export_*.csv files
    csv_pattern = os.path.join(data_dir, "products_export_*.csv")
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        print(f"No products_export_*.csv files found in {data_dir}")
        return None, 0
    
    all_products = []
    total_count = 0
    
    for csv_file in sorted(csv_files):
        try:
            df = pd.read_csv(csv_file, encoding='utf-8')
            if len(df) > 0:
                # Add source file column for tracking
                df['Source_File'] = os.path.basename(csv_file)
                all_products.append(df)
                total_count += len(df)
                print(f"Loaded {len(df)} products from {os.path.basename(csv_file)}")
        
        except Exception as e:
            print(f"Error reading {csv_file}: {str(e)}")
    
    if all_products:
        # Combine all products
        combined_df = pd.concat(all_products, ignore_index=True)
        
        # Export to CSV
        combined_df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"\nExported {total_count} products to {output_file}")
        
        return output_file, total_count
    else:
        print("No products found to export")
        return None, 0

def load_catalog_barcode_lookup(catalog_file):
    """
    Load catalog ID (barcode) lookup from Rakuten data.
    
    Args:
        catalog_file: Path to dl-normal-item_no_desc.csv
    
    Returns:
        dict: SKU -> Catalog ID mapping
    """
    try:
        # Try different encodings for the Rakuten file
        encodings = ['cp932', 'shift-jis', 'utf-8', 'latin-1']
        df = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(catalog_file, encoding=encoding)
                print(f"Successfully loaded catalog file with {encoding} encoding")
                break
            except UnicodeDecodeError:
                continue
        
        if df is None:
            raise ValueError("Could not decode file with any supported encoding")
        
        # Key columns:
        # 商品管理番号（商品URL） - column 0 (SKU)
        # カタログID - column 249 (0-indexed, was 250 in 1-indexed)
        
        lookup = {}
        
        for _, row in df.iterrows():
            sku = row.iloc[0]  # 商品管理番号（商品URL）
            catalog_id = row.iloc[249]  # カタログID
            
            # Only add if both SKU and catalog_id exist and are not empty
            if (pd.notna(sku) and pd.notna(catalog_id) and 
                str(sku).strip() != '' and str(catalog_id).strip() != ''):
                # Remove .0 from catalog_id if it's a float
                if isinstance(catalog_id, float):
                    catalog_id = str(int(catalog_id))
                lookup[str(sku).strip()] = str(catalog_id).strip()
        
        print(f"Loaded {len(lookup)} SKU -> Catalog ID mappings from {os.path.basename(catalog_file)}")
        return lookup
    
    except Exception as e:
        print(f"Error loading catalog file {catalog_file}: {str(e)}")
        return {}

def match_products_with_barcodes(products_file, catalog_lookup):
    """
    Match products with their catalog IDs (barcodes) from Rakuten data.
    
    Args:
        products_file: Path to exported products CSV
        catalog_lookup: Dict mapping SKU -> Catalog ID
    
    Returns:
        tuple: (updated_df, match_count, total_count)
    """
    try:
        df = pd.read_csv(products_file, encoding='utf-8')
        
        # Add new columns for matching results
        df['Rakuten_Catalog_ID'] = ''
        df['Barcode_Match_Status'] = ''
        
        match_count = 0
        
        for idx, row in df.iterrows():
            variant_sku = row.get('Variant SKU', '')
            
            if pd.notna(variant_sku) and str(variant_sku).strip() != '':
                sku = str(variant_sku).strip()
                
                if sku in catalog_lookup:
                    df.at[idx, 'Rakuten_Catalog_ID'] = catalog_lookup[sku]
                    df.at[idx, 'Barcode_Match_Status'] = 'Found'
                    match_count += 1
                else:
                    df.at[idx, 'Barcode_Match_Status'] = 'Not Found'
        
        return df, match_count, len(df)
    
    except Exception as e:
        print(f"Error matching products with barcodes: {str(e)}")
        return None, 0, 0

def update_shopify_barcodes(products_df, output_file):
    """
    Update Shopify products with matched catalog IDs as barcodes.
    
    Args:
        products_df: DataFrame with matched catalog IDs
        output_file: Output file for updated products
    
    Returns:
        int: Number of products updated with barcodes
    """
    updated_count = 0
    
    for idx, row in products_df.iterrows():
        catalog_id = row.get('Rakuten_Catalog_ID', '')
        if catalog_id and catalog_id.strip() != '':
            # Update the Variant Barcode column
            products_df.at[idx, 'Variant Barcode'] = catalog_id.strip()
            updated_count += 1
    
    # Save updated file
    products_df.to_csv(output_file, index=False, encoding='utf-8')
    
    return updated_count

def main():
    """Main function to execute the complete workflow."""
    data_dir = "/Users/gen/corekara/rakuten-shopify/api-operations/data"
    catalog_file = "/Users/gen/corekara/rakuten-shopify/manual/sample/dl-normal-item_no_desc.csv"
    
    print("=== Export Missing Barcode Products ===")
    
    # Step 1: Export all products with missing barcodes
    exported_file, total_products = export_missing_barcode_products(data_dir)
    
    if not exported_file or total_products == 0:
        print("No products to process")
        return
    
    print(f"\n=== Loading Catalog Lookup ===")
    
    # Step 2: Load catalog barcode lookup
    catalog_lookup = load_catalog_barcode_lookup(catalog_file)
    
    if not catalog_lookup:
        print("No catalog lookup data available")
        return
    
    print(f"\n=== Matching Products with Barcodes ===")
    
    # Step 3: Match products with catalog IDs
    updated_df, match_count, total_count = match_products_with_barcodes(exported_file, catalog_lookup)
    
    if updated_df is None:
        print("Error matching products")
        return
    
    print(f"Matched {match_count} out of {total_count} products with catalog IDs")
    
    # Step 4: Create updated file with barcodes
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    updated_file = os.path.join(data_dir, f"products_with_barcodes_updated_{timestamp}.csv")
    
    barcode_updated_count = update_shopify_barcodes(updated_df, updated_file)
    
    print(f"\n=== Results ===")
    print(f"Total products exported: {total_products}")
    print(f"Catalog ID matches found: {match_count}")
    print(f"Barcodes updated: {barcode_updated_count}")
    print(f"Updated file saved: {updated_file}")
    
    # Step 5: Save detailed report
    report_file = os.path.join(data_dir, f"barcode_matching_report_{timestamp}.csv")
    updated_df.to_csv(report_file, index=False, encoding='utf-8')
    print(f"Detailed report saved: {report_file}")

if __name__ == "__main__":
    main()