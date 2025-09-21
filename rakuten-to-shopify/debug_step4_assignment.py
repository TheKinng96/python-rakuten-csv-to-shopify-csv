#!/usr/bin/env python3
"""Debug Step 4 DataFrame assignment"""
import pandas as pd
from bs4 import BeautifulSoup

# Create test data
test_html = '''<img src="https://image.rakuten.co.jp/tsutsu-uraura/cabinet/test.jpg" alt="test">'''

df = pd.DataFrame({
    'Body (HTML)': [test_html, 'no images', test_html]
})

print("Before replacement:")
print(df['Body (HTML)'].iloc[0][:100])

def replace_html_image_urls(html_content, shopify_cdn_base, stats):
    if pd.isna(html_content) or not html_content.strip():
        return html_content

    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        img_tags = soup.find_all('img')

        replacements_made = 0
        for img in img_tags:
            url = img.get('src')
            if url and 'image.rakuten.co.jp' in url and 'tsutsu-uraura' in url:
                filename = url.split('/')[-1]
                shopify_url = f"{shopify_cdn_base}{filename}?v=1758179452"
                img['src'] = shopify_url
                replacements_made += 1
                print(f"  Replaced: {url} → {shopify_url}")

        result = str(soup)
        print(f"  Function returning: {result[:100]}")
        return result

    except Exception as e:
        print(f"Error: {e}")
        return str(html_content)

# Test the replacement
shopify_cdn_base = "https://cdn.shopify.com/s/files/1/0637/6059/7127/files/"
stats = {}

print("\nTesting replacement function:")
new_html = replace_html_image_urls(test_html, shopify_cdn_base, stats)

print("\nTesting DataFrame assignment:")
df['Body (HTML)'] = df['Body (HTML)'].apply(
    lambda html: replace_html_image_urls(html, shopify_cdn_base, stats)
)

print("\nAfter replacement:")
print(df['Body (HTML)'].iloc[0][:100])