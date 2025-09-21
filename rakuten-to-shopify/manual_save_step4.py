#!/usr/bin/env python3
"""Manually save Step 4 output to verify it works"""
import pickle
from src.rakuten_to_shopify.pipeline.steps import step_04_image_processing

# Load Step 3 output
with open('step_output/step_03_output.pkl', 'rb') as f:
    data = pickle.load(f)

print("Running Step 4 and saving correct output...")

# Run Step 4
result = step_04_image_processing.execute(data)

# Save the corrected CSV
df_corrected = result['html_processed_df']
df_corrected.to_csv('step_output/output_04_corrected.csv', index=False, encoding='utf-8')

print(f"✅ Saved corrected output: step_output/output_04_corrected.csv")

# Verify the replacement worked
import pandas as pd
sample_check = pd.read_csv('step_output/output_04_corrected.csv', nrows=5)
rakuten_count = str(sample_check.to_string()).count('image.rakuten.co.jp')
shopify_count = str(sample_check.to_string()).count('cdn.shopify.com')

print(f"Verification - Rakuten URLs: {rakuten_count}, Shopify URLs: {shopify_count}")