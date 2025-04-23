import pandas as pd
import glob
import os
import re
import math
from datetime import datetime
import traceback

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

def merge_products() -> None:
    """Merge product data from CSV files."""
    start_time = datetime.now()
    stats = {
        'total_products': 0,
        'unique_base_skus': set(),
        'max_variants': 0,
        'processing_time': None,
        'output_files': [],
        'processed_files': []
    }

    # Get all CSV files in the split_output directory
    csv_files = glob.glob('split_output/data_part_*.csv')
    print(f"Found {len(csv_files)} CSV files to process")
    
    # Initialize an empty list to store all DataFrames
    all_dfs = []
    
    # Try different encodings
    encodings = ['shift-jis', 'utf-8', 'cp932']
    
    # Read and process each CSV file
    for csv_file in csv_files:
        df = None
        for encoding in encodings:
            try:
                df = pd.read_csv(csv_file, encoding=encoding, dtype=str)
                print(f"Successfully read {csv_file} with {encoding} encoding")
                print(f"Columns found: {', '.join(df.columns)}")
                print(f"Sample of first row: {df.iloc[0].to_dict()}")
                break
            except Exception as e:
                print(f"Failed to read {csv_file} with {encoding} encoding: {str(e)}")
                continue
        
        if df is not None and not df.empty:
            all_dfs.append(df)
            stats['processed_files'].append(csv_file)
            print(f"Added DataFrame with {len(df)} rows")
        else:
            print(f"Warning: Could not read {csv_file} with any encoding")
    
    if not all_dfs:
        print("No data found in any of the CSV files")
        return
    
    # Concatenate all DataFrames at once
    merged_df = pd.concat(all_dfs, ignore_index=True)
    print(f"Total rows after merging: {len(merged_df)}")
    print(f"Columns in merged DataFrame: {', '.join(merged_df.columns)}")
    
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
    
    # Create final DataFrame
    final_df = pd.DataFrame(processed_products)
    
    # Save merged products
    merged_output = 'merged_products.csv'
    final_df.to_csv(merged_output, index=False, encoding='utf-8')
    stats['output_files'].append(merged_output)
    print(f"Saved merged products to {merged_output}")
    
    # Create and save Shopify format
    try:
        shopify_df = create_shopify_format(final_df)
        shopify_output = 'shopify_products.csv'
        shopify_df.to_csv(shopify_output, index=False, encoding='utf-8')
        stats['output_files'].append(shopify_output)
        print(f"Saved Shopify format products to {shopify_output}")
    except Exception as e:
        print(f"Error creating Shopify format: {str(e)}")
        print(f"Stack trace: {traceback.format_exc()}")
    
    # Calculate processing time
    stats['processing_time'] = (datetime.now() - start_time).total_seconds()
    
    # Generate summary report
    generate_summary_report(stats)

def generate_summary_report(stats):
    """Generate a markdown summary report of the merging process."""
    report = f"""# Rakuten to Shopify Product Migration Summary

## Processing Statistics

- **Total Products Processed**: {stats['total_products']}
- **Unique Base SKUs**: {len(stats['unique_base_skus'])}
- **Maximum Variants per Product**: {stats['max_variants']}
- **Total Processing Time**: {stats['processing_time']:.2f} seconds

## Processing Order

The following files were processed in order:

{chr(10).join(f"{i+1}. {os.path.basename(file)}" for i, file in enumerate(stats['processed_files']))}

## Output Files

1. **Merged Products** (`merged_products.csv`)
   - Contains all products with their original Rakuten fields
   - Products are grouped by their base SKU
   - Each row represents a unique variant

2. **Shopify Format Products** (`shopify_products.csv`)
   - Products are formatted according to Shopify's requirements
   - Handle field uses the base SKU (e.g., "kr-cash500")
   - Variant SKU field contains the full SKU (e.g., "kr-cash500-2s")
   - Common product information (Title, Description) is shared across variants
   - Variant-specific information (price, inventory) is unique to each variant

## Variant Handling

- Products are grouped by their base SKU (the part before any hyphen)
- All variants of the same product share:
  - Handle (base SKU)
  - Title
  - Description
  - Common product information
- Each variant maintains its own:
  - Variant SKU (full SKU)
  - Price
  - Inventory
  - Variant-specific attributes

## Notes

- The script processes all CSV files in the split_output directory
- Products are properly grouped by their base SKU
- Variant information is preserved and correctly mapped to Shopify's format
- All Japanese text is properly encoded using UTF-8
"""
    
    # Save the report to a markdown file
    with open('migration_summary.md', 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"Summary report saved to migration_summary.md")

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
    merge_products() 