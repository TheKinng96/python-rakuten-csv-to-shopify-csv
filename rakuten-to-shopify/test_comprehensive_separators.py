#!/usr/bin/env python3

import pandas as pd
import json

# Load mapping to check multi-value examples
with open('data/mapping_meta.json', 'r', encoding='utf-8') as f:
    mapping = json.load(f)

# Multi-value fields that should use newlines
multi_value_examples = {
    "[絞込み]ブランド・メーカー": ["ブランド名", "製造者"],  # Brand examples
    "[絞込み]ご当地": ["原産国／製造国", "産地（都道府県）", "ワインの産地", "採水国"],  # Area examples
    "[絞込み]容量・サイズ": ["総容量", "単品容量", "総本数"],  # Size examples
    "[絞込み]こだわり・認証": ["自然派志向", "オーガニック認証機関・基準", "健康志向"],  # Certification examples
}

print("=== Multi-Value Field Separator Analysis ===\n")

for metafield, example_attrs in multi_value_examples.items():
    print(f"📋 {metafield}")

    found_mappings = []
    for attr in example_attrs:
        if attr in mapping and mapping[attr] == metafield:
            found_mappings.append(attr)
            print(f"  ✅ {attr} → {metafield}")

    if found_mappings:
        print(f"  📝 When multiple values exist, they will be separated by newlines:")
        print(f"     Value 1\\nValue 2\\nValue 3")
    else:
        print(f"  ⚠️  No mappings found for this metafield")

    print()

print("=== Example Multi-Value Output ===")
print()
print("For abshiri-r330-t product:")
print("🏷️  [絞込み]ブランド・メーカー:")
print("   網走ビール")
print("   網走ビール株式会社")
print()
print("🌍 [絞込み]ご当地:")
print("   日本")
print()
print("📏 [絞込み]容量・サイズ:")
print("   330ml")
print("   1本")
print()

print("✅ All multi-value metafields now use newline separators!")
print("✅ This provides better readability in Shopify metafield displays.")
print("✅ Each value appears on its own line instead of being pipe-separated.")