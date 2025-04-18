import pandas as pd
import glob
import os
import re
from collections import defaultdict
from difflib import SequenceMatcher

def find_common_substring(strings):
    """Find the longest common substring among a list of strings."""
    if not strings:
        return ""
    
    # If there's only one string, return it
    if len(strings) == 1:
        return strings[0]
    
    # Find the shortest string to use as reference
    shortest = min(strings, key=len)
    
    # Try different lengths of substrings from the shortest string
    for length in range(len(shortest), 0, -1):
        for start in range(len(shortest) - length + 1):
            substring = shortest[start:start + length]
            # Check if this substring appears in all strings
            if all(substring in s for s in strings):
                return substring
    
    return ""

def clean_product_name(name):
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

def extract_base_sku(sku):
    """Extract the base SKU by removing variant suffixes like -01, -02, etc."""
    if pd.isna(sku) or sku == '':
        return ''
    
    # Match patterns like xxxx-01, xxxx-02, etc.
    match = re.match(r'^(.*?)(?:-\d+)?$', str(sku))
    if match:
        return match.group(1)
    return str(sku)

def merge_products():
    # Get all CSV files in the split_output directory
    csv_files = glob.glob('split_output/data_part_*.csv')
    
    # Dictionary to store products by base SKU
    products_by_base_sku = defaultdict(list)
    
    # Process each CSV file
    for file in csv_files:
        print(f"Processing {file}...")
        
        # Read CSV with Shift-JIS encoding
        df = pd.read_csv(file, encoding='shift-jis', low_memory=False)
        
        # Check if SKU管理番号 column exists
        if 'SKU管理番号' not in df.columns:
            print(f"Warning: SKU管理番号 column not found in {file}")
            continue
        
        # Group by base SKU
        for _, row in df.iterrows():
            sku = row['SKU管理番号']
            if pd.isna(sku) or sku == '':
                continue
                
            base_sku = extract_base_sku(sku)
            products_by_base_sku[base_sku].append(row)
    
    # Create a new DataFrame for merged products
    merged_products = []
    
    # Process each base SKU group
    for base_sku, products in products_by_base_sku.items():
        if not products:
            continue
            
        print(f"\nProcessing base SKU: {base_sku}")
        print(f"Number of variants: {len(products)}")
        
        # Extract product names
        product_names = [str(p['商品名']) for p in products if '商品名' in p and pd.notna(p['商品名'])]
        
        if not product_names:
            print(f"Warning: No product names found for base SKU {base_sku}")
            continue
            
        # Clean product names
        cleaned_names = [clean_product_name(name) for name in product_names]
        
        # Find common part of product names
        common_name = find_common_substring(cleaned_names)
        
        if not common_name:
            print(f"Warning: No common name found for base SKU {base_sku}")
            # Use the first product name as fallback
            common_name = product_names[0]
        
        print(f"Original names: {product_names}")
        print(f"Cleaned names: {cleaned_names}")
        print(f"Common name: {common_name}")
        
        # Create a base product with common information
        base_product = products[0].copy()
        
        # Update the product name with the common part
        if '商品名' in base_product:
            base_product['商品名'] = common_name
        
        # Add all variants to the merged products
        for product in products:
            # Create a copy of the base product
            merged_product = base_product.copy()
            
            # Update with variant-specific information
            for key, value in product.items():
                if pd.notna(value) and value != '':
                    merged_product[key] = value
            
            # Add to merged products
            merged_products.append(merged_product)
    
    # Convert to DataFrame
    merged_df = pd.DataFrame(merged_products)
    
    # Save to CSV with all Rakuten fields
    output_file = 'merged_products.csv'
    merged_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\nMerged products saved to {output_file}")
    print(f"Total products: {len(merged_df)}")
    
    # Create Shopify format CSV
    shopify_df = create_shopify_format(merged_df)
    
    # Save Shopify format CSV
    shopify_output_file = 'shopify_products.csv'
    shopify_df.to_csv(shopify_output_file, index=False, encoding='utf-8-sig')
    print(f"Shopify format products saved to {shopify_output_file}")
    
    return merged_df, shopify_df

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
    
    # Create a new DataFrame with Shopify columns
    shopify_df = pd.DataFrame(columns=shopify_columns)
    
    # Map Rakuten fields to Shopify fields
    for _, row in df.iterrows():
        shopify_row = {}
        
        # Map Handle (商品管理番号)
        if '商品管理番号（商品URL）' in row:
            shopify_row['Handle'] = row['商品管理番号（商品URL）']
        
        # Map Title (商品名)
        if '商品名' in row:
            shopify_row['Title'] = row['商品名']
        
        # Map Body (HTML) (商品説明文)
        body_html = []
        if 'PC用商品説明文' in row and pd.notna(row['PC用商品説明文']):
            body_html.append(row['PC用商品説明文'])
        if 'スマートフォン用商品説明文' in row and pd.notna(row['スマートフォン用商品説明文']):
            body_html.append(row['スマートフォン用商品説明文'])
        if 'PC用販売説明文' in row and pd.notna(row['PC用販売説明文']):
            body_html.append(row['PC用販売説明文'])
        shopify_row['Body (HTML)'] = ' | '.join(body_html) if body_html else ''
        
        # Map Vendor (ブランド名)
        if 'にっぽん津々浦々' in row:
            shopify_row['Vendor'] = 'にっぽん津々浦々'
        
        # Map Type (ジャンルID)
        if 'ジャンルID' in row:
            shopify_row['Type'] = row['ジャンルID']
        
        # Set Published to true
        shopify_row['Published'] = 'true'
        
        # Map Option1 Name (バリエーション項目名定義)
        if 'バリエーション項目名定義' in row:
            shopify_row['Option1 Name'] = row['バリエーション項目名定義']
        
        # Map Option1 Value (バリエーション項目キー定義)
        if 'バリエーション項目キー定義' in row:
            shopify_row['Option1 Value'] = row['バリエーション項目キー定義']
        
        # Map Variant SKU (SKU管理番号)
        if 'SKU管理番号' in row:
            shopify_row['Variant SKU'] = row['SKU管理番号']
        
        # Map Variant Grams (単品重量)
        if '商品属性（単品重量）' in row:
            shopify_row['Variant Grams'] = row['商品属性（単品重量）']
        
        # Map Variant Inventory Qty (在庫数)
        if '在庫数' in row:
            shopify_row['Variant Inventory Qty'] = row['在庫数']
        
        # Set Variant Inventory Tracker to shopify
        shopify_row['Variant Inventory Tracker'] = 'shopify'
        
        # Set Variant Inventory Policy to deny
        shopify_row['Variant Inventory Policy'] = 'deny'
        
        # Set Variant Fulfillment Service to manual
        shopify_row['Variant Fulfillment Service'] = 'manual'
        
        # Map Variant Price (販売価格)
        if '販売価格' in row:
            shopify_row['Variant Price'] = row['販売価格']
        
        # Map Variant Compare At Price (表示価格)
        if '表示価格' in row:
            shopify_row['Variant Compare At Price'] = row['表示価格']
        
        # Set Variant Requires Shipping to true
        shopify_row['Variant Requires Shipping'] = 'true'
        
        # Set Variant Taxable to true
        shopify_row['Variant Taxable'] = 'true'
        
        # Set Variant Barcode to empty
        shopify_row['Variant Barcode'] = ''
        
        # Map Image Src (商品画像)
        if '商品画像タイプ1' in row and '商品画像パス1' in row:
            shopify_row['Image Src'] = f"https://tshop.r10s.jp/tsutsu-uraura/{row['商品画像タイプ1']}/{row['商品画像パス1']}"
        
        # Set Image Position to 1
        shopify_row['Image Position'] = '1'
        
        # Map Image Alt Text (商品画像名(ALT))
        if '商品画像名(ALT) 1' in row:
            shopify_row['Image Alt Text'] = row['商品画像名(ALT) 1']
        
        # Set Gift Card to false
        shopify_row['Gift Card'] = 'false'
        
        # Map SEO Title (Title)
        if '商品名' in row:
            shopify_row['SEO Title'] = row['商品名']
        
        # Map SEO Description (キャッチコピー)
        if 'キャッチコピー' in row:
            shopify_row['SEO Description'] = row['キャッチコピー']
        
        # Set Variant Weight Unit to g
        shopify_row['Variant Weight Unit'] = 'g'
        
        # Set Status to active
        shopify_row['Status'] = 'active'
        
        # Map Collection (最長のカテゴリパス)
        if '表示先カテゴリ' in row:
            categories = row['表示先カテゴリ'].split('\\')
            if categories:
                shopify_row['Collection'] = categories[-1]
        
        # Map Tags (カテゴリキーワード)
        if '表示先カテゴリ' in row:
            categories = row['表示先カテゴリ'].split('\\')
            tags = [f"Tag {i+1}" for i, _ in enumerate(categories)]
            shopify_row['Tags'] = ', '.join(tags)
        
        # Add all Rakuten fields after Shopify fields
        for col in df.columns:
            if col not in shopify_columns:
                shopify_row[col] = row[col]
        
        # Add the row to the Shopify DataFrame
        shopify_df = pd.concat([shopify_df, pd.DataFrame([shopify_row])], ignore_index=True)
    
    return shopify_df

if __name__ == '__main__':
    merge_products() 