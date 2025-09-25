#!/usr/bin/env python3

import pandas as pd
import json
import sys
sys.path.append('src')

# Test the size filtering logic
def process_size_value(attr_name: str, value: str, unit: str) -> str:
    """
    Process size values for [絞込み]容量・サイズ metafield
    """
    if not value or pd.isna(value):
        return ''

    value_str = str(value).strip()
    unit_str = str(unit).strip() if unit and not pd.isna(unit) else ''

    # Volume-based attributes: convert to size categories
    volume_attrs = ["単品容量", "総容量", "内容量", "容量"]
    if attr_name in volume_attrs and unit_str.lower() in ['ml', 'l', 'ミリリットル', 'リットル', 'liter', 'litre', 'milliliter', 'millilitre']:
        return categorize_volume_size(value_str, unit_str)

    # Clothing/standard size attributes: keep if valid size
    size_attrs = ["サイズ（S/M/L）", "サイズ（大/中/小）", "ペットグッズのサイズ"]
    if attr_name in size_attrs:
        # Check if it's a valid clothing size
        valid_sizes = ['XXS', 'XS', 'S', 'M', 'L', 'XL', 'XXL', '大', '中', '小']
        if value_str.upper() in valid_sizes or any(size in value_str.upper() for size in valid_sizes):
            return value_str

    # Count/number attributes: filter out pure numbers
    count_attrs = ["総本数", "単品（個装）本数", "本数", "総個数", "個数", "総枚数", "枚数", "総入数", "入数"]
    if attr_name in count_attrs:
        # Skip pure numbers for count attributes
        try:
            float(value_str)
            return ''  # Pure number - skip it
        except (ValueError, TypeError):
            return value_str  # Non-numeric value - keep it

    # For other attributes, return empty to avoid clutter
    return ''

def categorize_volume_size(value: str, unit: str) -> str:
    """Categorize volume into size categories"""
    try:
        # Convert value to number
        volume_num = float(value)

        # Convert to ml if necessary
        if unit.lower() in ['l', 'リットル', 'リットル', 'liter', 'litre']:
            volume_ml = volume_num * 1000
        elif unit.lower() in ['ml', 'ミリリットル', 'milliliter', 'millilitre']:
            volume_ml = volume_num
        else:
            # If unit is unclear, assume ml
            volume_ml = volume_num

        # Categorize based on volume in ml
        if volume_ml <= 100:
            return "SS（〜100ml）"
        elif volume_ml <= 250:
            return "S（〜250ml）"
        elif volume_ml <= 500:
            return "M（〜500ml）"
        elif volume_ml <= 1000:
            return "L（〜1L）"
        else:
            return "LL（1L以上）"

    except (ValueError, TypeError):
        # If we can't parse the number, return original value
        return f"{value}{unit}" if unit else value

# Test with abshiri attributes
print("=== Size Filtering Test for abshiri-r330-t ===\n")

test_attributes = [
    ("総本数", "1", ""),  # Should be filtered out (pure number)
    ("単品容量", "330", "ml"),  # Should become M（〜500ml）
    ("総容量", "330", "ml"),  # Should become M（〜500ml）
    ("サイズ（S/M/L）", "M", ""),  # Should be kept as M
]

size_values = []

for attr_name, value, unit in test_attributes:
    result = process_size_value(attr_name, value, unit)

    print(f"Attribute: {attr_name}")
    print(f"  Input: {value} {unit}".strip())
    print(f"  Output: '{result}'")

    if result:
        if result not in size_values:  # Simulate first-valid-only logic
            size_values.append(result)
            print(f"  ✅ Accepted (first valid size)")
        else:
            print(f"  ⏭️  Skipped (duplicate size)")
    else:
        print(f"  ❌ Filtered out")
    print()

print(f"Final [絞込み]容量・サイズ value: '{size_values[0] if size_values else ''}'")
print()
print("✅ Expected result: 'M（〜500ml）' (volume-based size)")
print("✅ Pure number '1' from 総本数 should be filtered out")
print("✅ Only the first valid size should be kept")