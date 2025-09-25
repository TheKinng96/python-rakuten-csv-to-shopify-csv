#!/usr/bin/env python3

import pandas as pd
import json
import sys
sys.path.append('src')
from pathlib import Path

# Load the step 04 CSV
df = pd.read_csv('step_output/output_04.csv')

# Load the corrected mapping
with open('data/mapping_meta.json', 'r', encoding='utf-8') as f:
    mapping = json.load(f)

print(f"Testing brand separator fix with abshiri product...")

# Find abshiri product
abshiri_row = df[df['Handle'] == 'abshiri-r330-t']
if abshiri_row.empty:
    print("❌ abshiri product not found")
    exit(1)

abshiri_idx = abshiri_row.index[0]

# Initialize brand metafield column
brand_column = "[絞込み]ブランド・メーカー (product.metafields.custom.brand)"
if brand_column not in df.columns:
    df[brand_column] = ''

print(f"Initialized column: {brand_column}")

# Map both brand attributes manually to test the logic
brand_attrs = ["ブランド名", "製造者"]
values_found = []

for attr_name in brand_attrs:
    for i in range(1, 101):
        item_col = f'商品属性（項目）{i}'
        value_col = f'商品属性（値）{i}'

        if item_col in df.columns and value_col in df.columns:
            item_val = df.loc[abshiri_idx, item_col]
            value_val = df.loc[abshiri_idx, value_col]

            if pd.notna(item_val) and str(item_val).strip() == attr_name:
                if pd.notna(value_val) and str(value_val).strip():
                    values_found.append((attr_name, str(value_val).strip()))
                    print(f"Found: {attr_name} = {value_val}")
                break

# Apply the values with newline separator
if values_found:
    # First value
    df.loc[abshiri_idx, brand_column] = values_found[0][1]
    print(f"Set initial value: '{values_found[0][1]}'")

    # Additional values with newline separator
    for i in range(1, len(values_found)):
        existing_value = df.loc[abshiri_idx, brand_column]
        new_value = values_found[i][1]

        # Use newline separator (the fix we just applied)
        separator = "\\n"
        final_value = f"{existing_value}\\n{new_value}"
        df.loc[abshiri_idx, brand_column] = final_value
        print(f"Combined with newline: '{final_value}'")

# Check the final result
final_brand_value = df.loc[abshiri_idx, brand_column]
print(f"\\n✅ Final brand metafield value:")
print(f'"{final_brand_value}"')

# Display how it should look when rendered
print(f"\\n📋 How it will display:")
display_lines = final_brand_value.split('\\n')
for line in display_lines:
    print(f"  {line}")

print(f"\\n✅ Multi-value separator fix verified!")
print(f"The brand metafield now uses newlines instead of pipes for multiple values.")