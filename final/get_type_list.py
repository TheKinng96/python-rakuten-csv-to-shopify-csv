import csv
from pathlib import Path

# --- Configuration ---

# 1. Define file paths
input_file_path = Path("data/rakuten_collection.csv")
output_file_path = Path("output/product_types_for_shopify.csv")

# 2. Define the new, detailed matching rules.
#    The script will check these in order. The first match for a handler wins.
#    - 'exact_segments': Checks if any category segment is an exact match.
#    - 'first_segment_exact': Checks only the first category segment.
#    - 'name_contains': Checks if the product name contains any of these strings.
MATCHING_RULES = {
    # Rule for: 調味料 (Seasonings)
    "調味料": {
        "first_segment_exact": ["調味料"]
    },
    # Rule for: お酒・ワイン (Alcohol/Wine)
    "お酒・ワイン": {
        "exact_segments": ["お酒", "ワイン"],
        "name_contains": ["お酒", "ワイン"]
    },
    # Rule for: 食品 (Food)
    "食品": {
        "exact_segments": ["食品"]
    },
    # Rule for: 飲料・ドリンク (Beverages/Drinks)
    "飲料・ドリンク": {
        "exact_segments": ["ドリンク"]
    },
    # Rule for: ヘルス・ビューティー (Health & Beauty)
    "ヘルス・ビューティー": {
        "exact_segments": ["ヘルス＆ビューティー", "ビューティ"]
    },
    # Rule for: サプリメント・ダイエット・健康 (Supplements/Diet/Health)
    "サプリメント・ダイエット・健康": {
        "exact_segments": ["サプリメント＆ダイエット＆健康"]
    },
    # Rule for: ホーム・キッチン (Home & Kitchen)
    "ホーム・キッチン": {
        "exact_segments": ["ホーム＆キッチン"]
    },
    # Rule for: ペットフード・ペット用品 (Pet Food & Supplies)
    "ペットフード・ペット用品": {
        "exact_segments": ["ペットフード＆ペット用品", "ペットフード", "ペット用品"]
    }
}

# 3. Define headers for the output CSV.
output_headers = ['handler', 'product_type']

# --- Main Logic ---

# This dictionary will store the final result: {handler: category}
handler_product_type_map = {}

# --- Step 1: Read and Process the Input CSV ---
try:
    with input_file_path.open(mode='r', encoding='shift_jis', newline='') as infile:
        reader = csv.DictReader(infile)
        
        print(f"Reading and processing '{input_file_path}'...")
        
        for i, row in enumerate(reader, 1):
            handler = row.get('商品管理番号（商品URL）')
            full_category_path = row.get('表示先カテゴリ', '') # Default to empty string if missing
            product_name = row.get('商品名', '') # Default to empty string

            # Basic data validation for the row
            if not handler:
                print(f"  - Warning: Skipping row {i} due to missing handler.")
                continue

            # If we already found a definitive type for this handler, skip further checks.
            if handler in handler_product_type_map:
                continue

            # Prepare category segments for checking by splitting on both types of slashes.
            category_segments = [s.strip() for s in full_category_path.replace('／', '\\').split('\\')]
            
            # --- Apply Rules ---
            matched_type = None
            for product_type, rules in MATCHING_RULES.items():
                # Check 1: First segment is an exact match
                if 'first_segment_exact' in rules and category_segments:
                    if category_segments[0] in rules['first_segment_exact']:
                        matched_type = product_type
                        break # Found a match, stop checking other rules for this row

                # Check 2: Any segment is an exact match
                if 'exact_segments' in rules:
                    if any(segment in rules['exact_segments'] for segment in category_segments):
                        matched_type = product_type
                        break

                # Check 3: Product name contains a keyword
                if 'name_contains' in rules:
                    if any(keyword in product_name for keyword in rules['name_contains']):
                        matched_type = product_type
                        break
            
            # If a match was found for this row, store it and move to the next row.
            if matched_type:
                print(f"  ✓ Match found: Handler '{handler}' -> Type '{matched_type}' (from row {i})")
                handler_product_type_map[handler] = matched_type

except FileNotFoundError:
    print(f"❌ Error: The input file '{input_file_path}' was not found.")
    exit()
except Exception as e:
    print(f"❌ An unexpected error occurred: {e}")
    exit()

# --- Step 2: Write the Results to the Output CSV ---
if not handler_product_type_map:
    print("\nNo matching products found. The output file will not be created.")
else:
    print(f"\nFound {len(handler_product_type_map)} unique products. Writing to '{output_file_path}'...")
    try:
        with output_file_path.open(mode='w', encoding='utf-8-sig', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=output_headers)
            writer.writeheader()
            for handler, product_type in handler_product_type_map.items():
                writer.writerow({'handler': handler, 'product_type': product_type})
        
        print(f"✅ Success! Results have been saved to '{output_file_path}'.")

    except Exception as e:
        print(f"❌ An error occurred while writing the output file: {e}")