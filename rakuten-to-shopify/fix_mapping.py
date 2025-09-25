#!/usr/bin/env python3

import json
from pathlib import Path

# Load the mapping file
mapping_file = Path('data/mapping_meta.json')
with open(mapping_file, 'r', encoding='utf-8') as f:
    mapping = json.load(f)

print(f"Loaded {len(mapping)} mapping entries")

# Define the column name corrections needed
corrections = {
    # These need [絞込み] prefix added
    "ご当地 (product.metafields.custom.area)": "[絞込み]ご当地 (product.metafields.custom.area)",
    "ブランド・メーカー (product.metafields.custom.brand)": "[絞込み]ブランド・メーカー (product.metafields.custom.brand)",
    "こだわり・認証 (product.metafields.custom.commitment)": "[絞込み]こだわり・認証 (product.metafields.custom.commitment)",
    "商品カテゴリー (product.metafields.custom.attributes)": "[絞込み]商品カテゴリー (product.metafields.custom.attributes)",
    "成分・特性 (product.metafields.custom.component)": "[絞込み]成分・特性 (product.metafields.custom.component)",
    "迷ったら (product.metafields.custom.doubt)": "[絞込み]迷ったら (product.metafields.custom.doubt)",
    "季節イベント (product.metafields.custom.event)": "[絞込み]季節イベント (product.metafields.custom.event)",
    "味・香り・フレーバー (product.metafields.custom.flavor)": "[絞込み]味・香り・フレーバー (product.metafields.custom.flavor)",
    "お酒の分類 (product.metafields.custom.osake)": "[絞込み]お酒の分類 (product.metafields.custom.osake)",
    "ペットフード・用品分類 (product.metafields.custom.petfood)": "[絞込み]ペットフード・用品分類 (product.metafields.custom.petfood)",
    "容量・サイズ(product.metafields.custom.size)": "[絞込み]容量・サイズ (product.metafields.custom.search_size)",
    # Note: size field also needs to be corrected to search_size
}

# Apply corrections
corrected_count = 0
for attr, target_column in mapping.items():
    if target_column in corrections:
        old_column = target_column
        new_column = corrections[target_column]
        mapping[attr] = new_column
        corrected_count += 1
        print(f"Fixed: {attr} -> {new_column}")

print(f"\nCorrected {corrected_count} mappings")

# Save the corrected mapping file
with open(mapping_file, 'w', encoding='utf-8') as f:
    json.dump(mapping, f, indent=2, ensure_ascii=False)

print(f"Saved corrected mapping to {mapping_file}")

# Verify some key mappings
key_attrs = ["原産国／製造国", "コーヒー・茶の原料原産地", "ワインの産地", "採水国"]
for attr in key_attrs:
    if attr in mapping:
        print(f"✓ {attr} -> {mapping[attr]}")
    else:
        print(f"✗ {attr} not found in mapping")

print("\nMapping corrections completed!")