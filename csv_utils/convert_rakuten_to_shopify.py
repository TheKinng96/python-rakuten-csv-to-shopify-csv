#!/usr/bin/env python3
"""
Script to convert Rakuten product data to Shopify format.
The script will:
1. Read the merged Rakuten products file
2. Process and combine duplicate SKUs
3. Sort products by 商品管理番号（商品URL）
4. Save the processed data in Shopify format
"""

import pandas as pd
import glob
import os
import re
import math
from datetime import datetime, timedelta
import traceback
from pathlib import Path

def find_common_substring(strings: list[str]) -> str:
    """Find the longest common substring among a list of strings."""
    if not strings:
        return ""
    
    # If there's only one string, return it
    if len(strings) == 1:
        return strings[0]
    
    # Find the shortest string to use as reference
    # For example, given:
    #     strings = ["flower", "flow", "flight"]
    # min(strings, key=len) returns "flow" (length 4), 
    # because len("flow") == 4 is the smallest among [6, 4, 6].
    shortest = min(strings, key=len)
    
    # Try different lengths of substrings from the shortest string
    for length in range(len(shortest), 0, -1):
        for start in range(len(shortest) - length + 1):
            substring = shortest[start:start + length]
            # Check if this substring appears in all strings
            if all(substring in s for s in strings):
                return substring
    
    return ""

def clean_product_name(name: str) -> str:
    """Clean product name by removing common prefixes/suffixes."""
    # Remove common prefixes/suffixes that might differ between variants
    patterns = [
        r'^【.*?】',  # Remove text in Japanese brackets at the start
        r'【.*?】$',  # Remove text in Japanese brackets at the end
        r'^\[.*?\]',  # Remove text in square brackets at the start
        r'\[.*?\]$',  # Remove text in square brackets at the end
        r'^\d+個セット',  # Remove "X個セット" at the start
        r'^\d+本セット',  # Remove "X本セット" at the start
        r'^\d+パック',   # Remove "Xパック" at the start
        r'^\d+個入り',   # Remove "X個入り" at the start
        r'^\d+本入り',   # Remove "X本入り" at the start
        r'^\d+枚入り',   # Remove "X枚入り" at the start
        r'^\d+個',      # Remove "X個" at the start
        r'^\d+本',      # Remove "X本" at the start
        r'^\d+枚',      # Remove "X枚" at the start
        r'^\d+セット',  # Remove "Xセット" at the start
    ]
    
    cleaned_name = name
    for pattern in patterns:
        cleaned_name = re.sub(pattern, '', cleaned_name)
    
    return cleaned_name.strip()

# Return the base SKU
def extract_base_sku(sku: str) -> str:
    """Extract the base SKU by removing variant suffixes like -01, -02, etc."""
    if pd.isna(sku) or sku == '':
        return ''
    
    # Match patterns like xxxx-01, xxxx-02, etc.
    match = re.match(r'^(.*?)(?:-\d+)?$', str(sku))
    if match:
        return match.group(1)
    return str(sku)

def split_dataframe(df: pd.DataFrame, chunk_size_mb: int = 10) -> list[pd.DataFrame]:
    """Split a DataFrame into smaller chunks based on file size."""
    # Estimate the size of the DataFrame in MB
    df_size_mb = len(df) * len(df.columns) * 100 / (1024 * 1024)  # Rough estimate
    
    # Calculate number of chunks needed
    num_chunks = math.ceil(df_size_mb / chunk_size_mb)
    
    if num_chunks <= 1:
        return [df]
    
    # Calculate rows per chunk
    rows_per_chunk = math.ceil(len(df) / num_chunks)
    
    # Split the DataFrame
    chunks = []
    for i in range(0, len(df), rows_per_chunk):
        chunk = df.iloc[i:i+rows_per_chunk] # select rows from i to i+rows_per_chunk
        chunks.append(chunk)
    
    return chunks

def convert_rakuten_to_shopify() -> None:
    """Convert Rakuten product data to Shopify format."""
    start_time = datetime.now()
    stats = {
        'total_products': 0,
        'unique_base_skus': set(),
        'max_variants': 0,
        'processing_time': None,
        'output_files': [],
        'processed_files': []
    }

    # Read the merged file
    input_file = 'output/merged_products.csv'
    print(f"Reading merged file: {input_file}")
    
    # Try different encodings
    encodings = ['shift-jis', 'utf-8', 'cp932']
    merged_df = None
    
    for encoding in encodings:
        try:
            merged_df = pd.read_csv(input_file, encoding=encoding, dtype=str)
            print(f"Successfully read {input_file} with {encoding} encoding")
            print(f"Columns found: {', '.join(merged_df.columns)}")
            print(f"Sample of first row: {merged_df.iloc[0].to_dict()}")
            break
        except Exception as e:
            print(f"Failed to read {input_file} with {encoding} encoding: {str(e)}")
            continue
    
    if merged_df is None or merged_df.empty:
        print("Error: Could not read the merged file")
        return
    
    stats['processed_files'].append(input_file)
    print(f"Total rows in merged file: {len(merged_df)}")
    
    # Group products by base SKU
    base_sku_groups = {}
    sku_column = None
    
    # Find the correct SKU column
    possible_sku_columns = ['商品管理番号（商品URL）', '商品管理番号', 'SKU']
    for col in possible_sku_columns:
        if col in merged_df.columns:
            sku_column = col
            print(f"Using column '{col}' as SKU identifier")
            break
    
    if not sku_column:
        print("Error: Could not find SKU column")
        return
    
    # Find the correct product name column
    name_column = None
    possible_name_columns = ['商品名', '商品名（商品URL）', '商品タイトル', 'Title']
    for col in possible_name_columns:
        if col in merged_df.columns:
            name_column = col
            print(f"Using column '{col}' as product name")
            break
    
    if not name_column:
        print("Error: Could not find product name column")
        return
    
    # First, identify and combine duplicate SKUs
    print("Identifying and combining duplicate SKUs...")
    sku_groups = {}
    for _, row in merged_df.iterrows():
        sku = row.get(sku_column, '')
        if pd.isna(sku) or not sku:
            continue
            
        if sku not in sku_groups:
            sku_groups[sku] = []
        sku_groups[sku].append(row)
    
    # Combine duplicate SKUs
    combined_rows = []
    for sku, rows in sku_groups.items():
        if len(rows) > 1:
            print(f"Found {len(rows)} rows for SKU: {sku}")
            # Create a combined row with data from all rows
            combined_row = rows[0].copy()  # Start with the first row
            
            # Merge data from additional rows
            for i in range(1, len(rows)):
                for col in merged_df.columns:
                    # If the column in the first row is empty but has data in subsequent rows, use that data
                    if pd.isna(combined_row[col]) and pd.notna(rows[i][col]):
                        combined_row[col] = rows[i][col]
            
            combined_rows.append(combined_row)
        else:
            combined_rows.append(rows[0])
    
    # Create a new DataFrame with combined rows
    combined_df = pd.DataFrame(combined_rows)
    print(f"Combined {len(merged_df)} rows into {len(combined_df)} rows")
    
    # Now group by base SKU
    for _, row in combined_df.iterrows():
        sku = row.get(sku_column, '')
        if pd.isna(sku) or not sku:
            continue
            
        base_sku = sku.split('-')[0] if '-' in sku else sku
        if base_sku not in base_sku_groups:
            base_sku_groups[base_sku] = []
        base_sku_groups[base_sku].append(row)
    
    print(f"Found {len(base_sku_groups)} unique base SKUs")
    
    # Process each base SKU group
    processed_products = []
    for base_sku, variants in base_sku_groups.items():
        stats['unique_base_skus'].add(base_sku)
        stats['max_variants'] = max(stats['max_variants'], len(variants))
        
        # Find product name from variants
        product_name = None
        for variant in variants:
            if name_column in variant and pd.notna(variant[name_column]):
                product_name = variant[name_column]
                break
        
        if not product_name:
            print(f"Warning: No product name found for base SKU: {base_sku}")
            product_name = base_sku
        
        # Add all variants to processed products
        for variant in variants:
            processed_products.append(variant)
            stats['total_products'] += 1
    
    if not processed_products:
        print("No products were processed")
        return
    
    print(f"Processed {len(processed_products)} total products")
    
    # Create final DataFrame and sort by 商品管理番号（商品URL）
    final_df = pd.DataFrame(processed_products)
    final_df = final_df.sort_values(by=sku_column, ascending=True)
    
    # Convert to Shopify format
    print("Converting to Shopify format...")
    shopify_df = create_shopify_format(final_df)
    
    # Ensure output directory exists
    output_dir = Path('output')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save the final DataFrame
    output_file = output_dir / 'shopify_products.csv'
    shopify_df.to_csv(output_file, index=False, encoding='utf-8')
    stats['output_files'].append(str(output_file))
    print(f"Saved processed products to {output_file}")
    
    # Calculate processing time
    end_time = datetime.now()
    stats['processing_time'] = end_time - start_time
    
    # Generate summary report
    generate_summary_report(stats)
    
    print("\nProcessing complete!")
    print(f"Total time: {stats['processing_time']}")
    print(f"Total products processed: {stats['total_products']}")
    print(f"Unique base SKUs: {len(stats['unique_base_skus'])}")
    print(f"Maximum variants per product: {stats['max_variants']}")
    print(f"Output file: {output_file}")

def generate_summary_report(stats):
    """Generate a markdown summary report of the conversion process."""
    # Format processing time
    processing_time = stats['processing_time']
    if isinstance(processing_time, timedelta):
        total_seconds = processing_time.total_seconds()
        hours = int(total_seconds // 3600)
        minutes = int((total_seconds % 3600) // 60)
        seconds = total_seconds % 60
        time_str = f"{hours:02d}:{minutes:02d}:{seconds:06.3f}"
    else:
        time_str = str(processing_time)

    report = f"""# Rakuten to Shopify Product Conversion Summary

## Processing Statistics

- **Total Products Processed**: {stats['total_products']}
- **Unique Base SKUs**: {len(stats['unique_base_skus'])}
- **Maximum Variants per Product**: {stats['max_variants']}
- **Total Processing Time**: {time_str}

## Input File

- **Source**: {os.path.basename(stats['processed_files'][0])}
  - Located in the output directory
  - Contains merged Rakuten product data

## Output File

- **Destination**: shopify_products.csv
  - Located in the output directory
  - Contains products formatted for Shopify
  - Sorted by 商品管理番号（商品URL）
  - Duplicate SKUs have been combined
  - Products are grouped by base SKU

## Processing Details

1. **Input Processing**
   - Read merged Rakuten products file
   - Identified and combined duplicate SKUs
   - Grouped products by base SKU

2. **Data Organization**
   - Products are sorted by 商品管理番号（商品URL）
   - Each row represents a unique variant
   - Common product information is shared across variants
   - Variant-specific information is preserved

3. **Output Format**
   - CSV format with UTF-8 encoding
   - All Japanese text is properly encoded
   - Maintains original Rakuten data structure
   - Ready for Shopify import

## Notes

- The script processes the merged Rakuten products file
- Products are properly grouped by their base SKU
- Variant information is preserved
- All Japanese text is properly encoded using UTF-8
"""
    
    # Ensure output directory exists
    output_dir = Path('output')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save the report to a markdown file in the output directory
    report_file = output_dir / 'conversion_summary.md'
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"Summary report saved to {report_file}")

def create_shopify_format(df):
    """Convert the merged DataFrame to Shopify format."""
    # Define Shopify columns based on migration planning
    shopify_columns = [
        'Handle', 'Title', 'Body (HTML)', 'Vendor', 'Product Category', 'Type', 
        'Published', 'Option1 Name', 'Option1 Value', 'Option1 Linked To',
        'Option2 Name', 'Option2 Value', 'Option2 Linked To',
        'Option3 Name', 'Option3 Value', 'Option3 Linked To',
        'Variant SKU', 'Variant Grams', 'Variant Inventory Qty', 
        'Variant Inventory Tracker', 'Variant Inventory Policy', 
        'Variant Fulfillment Service', 'Variant Price', 'Variant Compare At Price',
        'Variant Requires Shipping', 'Variant Taxable', 'Variant Barcode',
        'Image Src', 'Image Position', 'Image Alt Text', 'Gift Card',
        'SEO Title', 'SEO Description', 'Google Shopping / Google Product Category',
        'Google Shopping / Gender', 'Google Shopping / Age Group',
        'Google Shopping / MPN', 'Google Shopping / AdWords Grouping',
        'Google Shopping / AdWords Labels', 'Google Shopping / Condition',
        'Google Shopping / Custom Product', 'Google Shopping / Custom Label 0',
        'Google Shopping / Custom Label 1', 'Google Shopping / Custom Label 2',
        'Google Shopping / Custom Label 3', 'Google Shopping / Custom Label 4',
        'Google Shopping / Product Type', 'Google Shopping / Bid',
        'Google Shopping / Product ID', 'Google Shopping / Price',
        'Google Shopping / Brand', 'Google Shopping / GTIN',
        'Google Shopping / UPC', 'Google Shopping / EAN',
        'Google Shopping / ISBN', 'Variant Image', 'Variant Weight Unit',
        'Variant Tax Code', 'Cost per item', 'Status', 'Collection',
        'Tags', 'Variant Price', 'Variant Compare At Price', 'Variant Inventory Qty',
        'Variant Inventory Policy', 'Variant Fulfillment Service', 'Variant Requires Shipping',
        'Variant Taxable', 'Variant Barcode', 'Variant Weight Unit', 'Variant Tax Code',
        'Cost per item', 'Status'
    ]
    
    # Get all Rakuten columns that are not in Shopify columns
    rakuten_columns = [col for col in df.columns if col not in shopify_columns]
    
    # Create a list to store all rows
    all_rows = []
    
    # Group products by base SKU
    base_sku_groups = {}
    for _, row in df.iterrows():
        sku = row.get('商品管理番号（商品URL）', '')
        if pd.isna(sku) or not sku:
            continue
            
        # Check if this SKU is a variant of any existing base SKU
        is_variant = False
        for base_sku in list(base_sku_groups.keys()):
            if sku.startswith(base_sku + '-'):
                # This is a variant of an existing base SKU
                base_sku_groups[base_sku].append(row)
                is_variant = True
                break
        
        if not is_variant:
            # This is either a new base SKU or a custom version of an existing base SKU
            # Check if we already have this SKU as a base
            if sku in base_sku_groups:
                # This is a custom version of an existing base SKU
                base_sku_groups[sku].append(row)
            else:
                # This is a new base SKU
                base_sku_groups[sku] = [row]
    
    # Process each base SKU group
    for base_sku, variants in base_sku_groups.items():
        # Find product name from variants
        product_name = None
        for variant in variants:
            if '商品名' in variant and pd.notna(variant['商品名']):
                product_name = variant['商品名']
                break
            elif '商品名（商品URL）' in variant and pd.notna(variant['商品名（商品URL）']):
                product_name = variant['商品名（商品URL）']
                break
        
        if not product_name:
            print(f"Warning: No product name found for base SKU: {base_sku}")
            product_name = base_sku
        
        # Create a row for each variant
        for variant in variants:
            shopify_row = {}
            
            # Initialize all Shopify columns with None
            for col in shopify_columns:
                shopify_row[col] = None
                
            # Initialize all Rakuten columns with None
            for col in rakuten_columns:
                shopify_row[col] = None
            
            # Copy all Rakuten data from the original variant
            for col in rakuten_columns:
                if col in variant and pd.notna(variant[col]):
                    shopify_row[col] = variant[col]
            
            # Set Handle to base SKU
            shopify_row['Handle'] = base_sku
            
            # Set Title to product name
            shopify_row['Title'] = product_name
            
            # Map Body (HTML) (商品説明文)
            body_html = []
            if 'PC用商品説明文' in variant and pd.notna(variant['PC用商品説明文']):
                body_html.append(variant['PC用商品説明文'])
            if 'スマートフォン用商品説明文' in variant and pd.notna(variant['スマートフォン用商品説明文']):
                body_html.append(variant['スマートフォン用商品説明文'])
            if 'PC用販売説明文' in variant and pd.notna(variant['PC用販売説明文']):
                body_html.append(variant['PC用販売説明文'])
            shopify_row['Body (HTML)'] = ' | '.join(body_html) if body_html else ''
            
            # Map Vendor (ブランド名)
            if 'にっぽん津々浦々' in variant:
                shopify_row['Vendor'] = 'にっぽん津々浦々'
            
            # Map Type (ジャンルID)
            if 'ジャンルID' in variant:
                shopify_row['Type'] = variant['ジャンルID']
            
            # Set Published to true
            shopify_row['Published'] = 'true'
            
            # Map Option1 Name (バリエーション項目名定義)
            if 'バリエーション項目名定義' in variant:
                shopify_row['Option1 Name'] = variant['バリエーション項目名定義']
            
            # Map Option1 Value (バリエーション項目キー定義)
            if 'バリエーション項目キー定義' in variant:
                shopify_row['Option1 Value'] = variant['バリエーション項目キー定義']
            
            # Map Variant SKU (商品管理番号)
            if '商品管理番号（商品URL）' in variant:
                shopify_row['Variant SKU'] = variant['商品管理番号（商品URL）']
            
            # Map Variant Grams (単品重量)
            if '商品属性（単品重量）' in variant:
                shopify_row['Variant Grams'] = variant['商品属性（単品重量）']
            
            # Map Variant Inventory Qty (在庫数)
            if '在庫数' in variant:
                shopify_row['Variant Inventory Qty'] = variant['在庫数']
            
            # Set Variant Inventory Tracker to shopify
            shopify_row['Variant Inventory Tracker'] = 'shopify'
            
            # Set Variant Inventory Policy to deny
            shopify_row['Variant Inventory Policy'] = 'deny'
            
            # Set Variant Fulfillment Service to manual
            shopify_row['Variant Fulfillment Service'] = 'manual'
            
            # Map Variant Price (販売価格)
            if '販売価格' in variant:
                shopify_row['Variant Price'] = variant['販売価格']
            
            # Map Variant Compare At Price (表示価格)
            if '表示価格' in variant:
                shopify_row['Variant Compare At Price'] = variant['表示価格']
            
            # Set Variant Requires Shipping to true
            shopify_row['Variant Requires Shipping'] = 'true'
            
            # Set Variant Taxable to true
            shopify_row['Variant Taxable'] = 'true'
            
            # Set Variant Barcode to empty
            shopify_row['Variant Barcode'] = ''
            
            # Map Image Src (商品画像)
            image_rows = []
            has_multiple_images = False
            
            # First, check if there are multiple images
            for i in range(2, 21):  # Check images 2 through 20
                type_col = f'商品画像タイプ{i}'
                path_col = f'商品画像パス{i}'
                if type_col in variant and path_col in variant:
                    if pd.notna(variant[type_col]) and pd.notna(variant[path_col]):
                        has_multiple_images = True
                        break
            
            # Process the first image
            type_col = '商品画像タイプ1'
            path_col = '商品画像パス1'
            alt_col = '商品画像名(ALT) 1'
            
            if type_col in variant and path_col in variant:
                image_type = variant[type_col]
                image_path = variant[path_col]
                if pd.notna(image_type) and pd.notna(image_path):
                    image_type = str(image_type).lower()
                    image_path = str(image_path).lower()
                    image_url = f"https://tshop.r10s.jp/tsutsu-uraura/{image_type}{image_path}"
                    
                    # If there's only one image, include it in the main row
                    if not has_multiple_images:
                        shopify_row['Image Src'] = image_url
                        shopify_row['Image Position'] = '1'
                        if alt_col in variant and pd.notna(variant[alt_col]):
                            shopify_row['Image Alt Text'] = variant[alt_col]
                    else:
                        # If there are multiple images, create a separate row for the first image
                        image_row = {}
                        # Initialize all columns as None
                        for col in shopify_columns + rakuten_columns:
                            image_row[col] = None
                        
                        # Copy all Rakuten data from the original variant
                        for col in rakuten_columns:
                            if col in variant and pd.notna(variant[col]):
                                image_row[col] = variant[col]
                        
                        # Set only the required fields
                        image_row['Handle'] = base_sku
                        image_row['Image Src'] = image_url
                        image_row['Image Position'] = '1'
                        if alt_col in variant and pd.notna(variant[alt_col]):
                            image_row['Image Alt Text'] = variant[alt_col]
                        
                        image_rows.append(image_row)
            
            # Process additional images (2-20)
            for i in range(2, 21):
                type_col = f'商品画像タイプ{i}'
                path_col = f'商品画像パス{i}'
                alt_col = f'商品画像名(ALT) {i}'
                if type_col in variant and path_col in variant:
                    image_type = variant[type_col]
                    image_path = variant[path_col]
                    if pd.notna(image_type) and pd.notna(image_path):
                        image_type = str(image_type).lower()
                        image_path = str(image_path).lower()
                        image_url = f"https://tshop.r10s.jp/tsutsu-uraura/{image_type}{image_path}"
                        
                        # Create a new row for each additional image
                        image_row = {}
                        # Initialize all columns as None
                        for col in shopify_columns + rakuten_columns:
                            image_row[col] = None
                        
                        # Copy all Rakuten data from the original variant
                        for col in rakuten_columns:
                            if col in variant and pd.notna(variant[col]):
                                image_row[col] = variant[col]
                        
                        # Set only the required fields
                        image_row['Handle'] = base_sku
                        image_row['Image Src'] = image_url
                        image_row['Image Position'] = str(i)
                        if alt_col in variant and pd.notna(variant[alt_col]):
                            image_row['Image Alt Text'] = variant[alt_col]
                        
                        image_rows.append(image_row)
            
            # Add the main row with all product information
            all_rows.append(shopify_row)
            
            # Add additional rows for each image (only if there are multiple images)
            if has_multiple_images:
                all_rows.extend(image_rows)
    
    # Create DataFrame from all rows at once
    shopify_df = pd.DataFrame(all_rows)
    
    # Reorder columns to ensure Shopify columns come first
    final_columns = shopify_columns + rakuten_columns
    shopify_df = shopify_df[final_columns]
    
    return shopify_df

if __name__ == '__main__':
    convert_rakuten_to_shopify() 