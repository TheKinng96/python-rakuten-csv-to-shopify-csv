#!/usr/bin/env python3
"""
Create the final corrected output by running Step 3 → Step 4 manually
"""
import pickle
from src.rakuten_to_shopify.pipeline.steps import step_03_html_processing, step_04_image_processing

print("🔄 Creating final corrected output...")

# Start with Step 2 output
print("📂 Loading Step 2 output...")
with open('step_output/step_02_output.pkl', 'rb') as f:
    data = pickle.load(f)

print(f"   Loaded {len(data['shopify_df'])} rows from Step 2")

# Run Step 3 (HTML cleaning with fixed EC-UP removal)
print("🧹 Running Step 3 (HTML cleaning)...")
result_03 = step_03_html_processing.execute(data)

print(f"   ✅ Step 3 complete:")
print(f"      - EC-UP blocks removed: {result_03['html_stats']['ec_up_blocks_removed']}")
print(f"      - Marketing content removed: {result_03['html_stats']['marketing_content_removed']}")
print(f"      - Tables made responsive: {result_03['html_stats']['tables_made_responsive']}")
print(f"      - Images found: {result_03['html_stats']['images_found']}")
print(f"      - Unique image URLs collected: {len(result_03['collected_image_urls'])}")

# Run Step 4 (Image URL replacement with failed image removal)
print("🖼️  Running Step 4 (Image URL replacement)...")
result_04 = step_04_image_processing.execute(result_03)

print(f"   ✅ Step 4 complete:")
print(f"      - HTML descriptions with images: {result_04['image_stats']['html_descriptions_with_images']}")
print(f"      - Rakuten URLs found: {result_04['image_stats']['html_rakuten_urls_found']}")
print(f"      - URLs replaced: {result_04['image_stats']['html_urls_replaced']}")

# Save the final corrected output
final_df = result_04['image_processed_df']
final_df.to_csv('step_output/final_corrected_output.csv', index=False, encoding='utf-8')

print(f"💾 Saved final corrected output: step_output/final_corrected_output.csv")

# Verify the results
import pandas as pd
sample_check = pd.read_csv('step_output/final_corrected_output.csv', nrows=5)
ec_up_count = str(sample_check.to_string()).count('EC-UP')
rakuten_count = str(sample_check.to_string()).count('image.rakuten.co.jp')
shopify_count = str(sample_check.to_string()).count('cdn.shopify.com')

print(f"\n📊 Verification (first 5 rows):")
print(f"   EC-UP blocks: {ec_up_count}")
print(f"   Rakuten URLs: {rakuten_count}")
print(f"   Shopify URLs: {shopify_count}")

# Check the full file
print(f"\n🔍 Full file verification:")
with open('step_output/final_corrected_output.csv', 'r', encoding='utf-8') as f:
    content = f.read()

full_ec_up = content.count('EC-UP')
full_rakuten = content.count('image.rakuten.co.jp')
full_shopify = content.count('cdn.shopify.com')

print(f"   Total EC-UP blocks: {full_ec_up}")
print(f"   Total Rakuten URLs: {full_rakuten}")
print(f"   Total Shopify URLs: {full_shopify}")

if full_ec_up == 0:
    print(f"   🎉 SUCCESS: All EC-UP blocks removed!")
else:
    print(f"   ⚠️  WARNING: {full_ec_up} EC-UP blocks still remain")

print(f"\n✅ Final corrected output created successfully!")