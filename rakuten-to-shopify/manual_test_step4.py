#!/usr/bin/env python3
"""Manually test Step 4 URL replacement"""
import pickle
import pandas as pd
from src.rakuten_to_shopify.pipeline.steps import step_04_image_processing

# Load Step 3 output
with open('step_output/step_03_output.pkl', 'rb') as f:
    data = pickle.load(f)

print("Before Step 4:")
df_before = data['html_processed_df']
sample_html = df_before['Body (HTML)'].iloc[0]
rakuten_count_before = sample_html.count('image.rakuten.co.jp')
print(f"Sample HTML has {rakuten_count_before} Rakuten URLs")

# Run Step 4
print("\nRunning Step 4...")
result = step_04_image_processing.execute(data)

print("After Step 4:")
df_after = result['html_processed_df']
sample_html_after = df_after['Body (HTML)'].iloc[0]
rakuten_count_after = sample_html_after.count('image.rakuten.co.jp')
shopify_count_after = sample_html_after.count('cdn.shopify.com')

print(f"Sample HTML has {rakuten_count_after} Rakuten URLs")
print(f"Sample HTML has {shopify_count_after} Shopify URLs")

if shopify_count_after > 0:
    print("✅ URL replacement worked!")
else:
    print("❌ URL replacement failed!")

# Show a sample replacement
if shopify_count_after > 0:
    print("\nSample Shopify URL found:")
    start = sample_html_after.find('cdn.shopify.com')
    if start >= 0:
        end = sample_html_after.find('"', start)
        print(sample_html_after[start-20:end+1])