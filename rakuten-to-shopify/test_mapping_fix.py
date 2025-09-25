#!/usr/bin/env python3

import pandas as pd
import json
import sys
sys.path.append('src')
from pathlib import Path

# Load the step 04 CSV (after image processing)
df = pd.read_csv('step_output/output_04.csv')

# Load the corrected mapping
with open('data/mapping_meta.json', 'r', encoding='utf-8') as f:
    mapping = json.load(f)

print(f"Loaded DataFrame with {len(df)} rows")
print(f"Loaded {len(mapping)} mappings")

# Test the dynamic mapping logic
attr_name = "原産国／製造国"
target_column = mapping[attr_name]
print(f"\nTesting mapping: {attr_name} -> {target_column}")

# Check if abshiri has this attribute
abshiri_row = df[df['Handle'] == 'abshiri-r330-t']
if abshiri_row.empty:
    print("❌ abshiri product not found")
    exit(1)

abshiri_idx = abshiri_row.index[0]

# Initialize the target column if it doesn't exist
if target_column not in df.columns:
    df[target_column] = ''
    print(f"✅ Initialized column: {target_column}")

# Find the attribute in the 商品属性 structure
found_attr = False
for i in range(1, 101):
    item_col = f'商品属性（項目）{i}'
    value_col = f'商品属性（値）{i}'

    if item_col in df.columns and value_col in df.columns:
        item_val = df.loc[abshiri_idx, item_col]
        value_val = df.loc[abshiri_idx, value_col]

        if pd.notna(item_val) and str(item_val).strip() == attr_name:
            print(f"✅ Found attribute in slot {i}: {item_val} = {value_val}")

            # Test the mask logic
            item_mask = df[item_col] == attr_name
            value_mask = df[value_col].notna() & (df[value_col] != '') & (df[value_col] != '　')
            combined_mask = item_mask & value_mask

            print(f"  Mask results: item_mask={item_mask.sum()}, value_mask={value_mask.sum()}, combined={combined_mask.sum()}")

            if combined_mask[abshiri_idx]:
                print("  ✅ abshiri matches the combined mask")

                # Apply the mapping
                df.loc[abshiri_idx, target_column] = str(value_val).strip()

                new_value = df.loc[abshiri_idx, target_column]
                print(f"  ✅ Applied mapping: {target_column} = '{new_value}'")
                found_attr = True
                break
            else:
                print("  ❌ abshiri does NOT match the combined mask")
                print(f"    item_mask[{abshiri_idx}]: {item_mask[abshiri_idx]}")
                print(f"    value_mask[{abshiri_idx}]: {value_mask[abshiri_idx]}")

if not found_attr:
    print("❌ Attribute not found or not mappable")
else:
    # Save the result
    df.to_csv('step_output/test_mapping_output.csv', index=False)
    print(f"\n✅ Saved test output to step_output/test_mapping_output.csv")

    # Verify the result
    final_value = df.loc[abshiri_idx, target_column]
    print(f"Final {target_column} value: '{final_value}'")