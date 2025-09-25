#!/usr/bin/env python3

import pandas as pd
import json
from pathlib import Path

# Load the step 04 CSV output
df = pd.read_csv('step_output/output_04.csv')

# Load the mapping configuration
with open('data/mapping_meta.json', 'r', encoding='utf-8') as f:
    mapping = json.load(f)

# Check if 原産国／製造国 is in the mapping
attr_name = "原産国／製造国"
if attr_name in mapping:
    print(f"✓ Found {attr_name} in mapping -> {mapping[attr_name]}")
else:
    print(f"✗ {attr_name} NOT found in mapping")

# Find abshiri product
abshiri_row = df[df['Handle'] == 'abshiri-r330-t']
if abshiri_row.empty:
    print("✗ abshiri-r330-t not found")
    exit(1)

abshiri_idx = abshiri_row.index[0]
print(f"✓ Found abshiri-r330-t at index {abshiri_idx}")

# Check the dynamic mapping creation logic
print("\n=== Dynamic Mapping Test ===")
for i in range(1, 25):  # Check first 24 slots
    item_col = f'商品属性（項目）{i}'
    value_col = f'商品属性（値）{i}'

    if item_col in df.columns and value_col in df.columns:
        item_val = df.loc[abshiri_idx, item_col]
        value_val = df.loc[abshiri_idx, value_col]

        if pd.notna(item_val) and str(item_val).strip():
            print(f"Slot {i}: {item_val} = {value_val}")

            # Check if this matches our target attribute
            if str(item_val).strip() == attr_name:
                print(f"  ★ MATCH! This should map to: {mapping[attr_name]}")

                # Test the mask conditions
                item_mask = df[item_col] == attr_name
                value_mask = df[value_col].notna() & (df[value_col] != '') & (df[value_col] != '　')
                combined_mask = item_mask & value_mask

                print(f"  item_mask matches {item_mask.sum()} rows")
                print(f"  value_mask matches {value_mask.sum()} rows")
                print(f"  combined_mask matches {combined_mask.sum()} rows")

                # Check if abshiri matches the combined mask
                if combined_mask[abshiri_idx]:
                    print(f"  ✓ abshiri-r330-t matches the combined mask")
                else:
                    print(f"  ✗ abshiri-r330-t does NOT match the combined mask")
                    print(f"    item_mask[{abshiri_idx}]: {item_mask[abshiri_idx]}")
                    print(f"    value_mask[{abshiri_idx}]: {value_mask[abshiri_idx]}")

print(f"\n=== Summary ===")
print(f"The attribute {attr_name} should be found and mapped.")
print(f"If the debug logging isn't showing, there might be an issue with the mask logic.")