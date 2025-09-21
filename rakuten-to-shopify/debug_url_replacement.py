#!/usr/bin/env python3
"""
Debug URL replacement in Step 4
"""
import pandas as pd
from bs4 import BeautifulSoup

# Read a small sample to debug
df = pd.read_csv("step_output/output_04.csv", nrows=10)

print("Checking URL replacement...")

for i, row in df.iterrows():
    html = row['Body (HTML)']

    if pd.notna(html) and 'image.rakuten.co.jp' in str(html):
        print(f"\nRow {i}:")
        print("HTML length:", len(str(html)))

        # Check if HTML contains img tags
        soup = BeautifulSoup(str(html), 'html.parser')
        img_tags = soup.find_all('img')

        print(f"Found {len(img_tags)} img tags")

        for j, img in enumerate(img_tags[:3]):  # Show first 3
            src = img.get('src')
            if src and 'image.rakuten.co.jp' in src:
                print(f"  IMG {j}: {src}")

        break