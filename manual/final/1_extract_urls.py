"""
Step 1: Extracts Rakuten image URLs from the 'Body (HTML)' of a Shopify CSV.
-----------------------------------------------------------------------------
Usage: python 1_extract_urls.py
"""
import re
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup

# --- Configuration ---
OUT_DIR = Path("output")
SHOPIFY_INPUT_FILE = OUT_DIR / "shopify_products.csv"
URL_MAP_OUTPUT_FILE = OUT_DIR / "body_html_image_urls.csv"
RAKUTEN_DOMAIN_TO_FIND = "https://image.rakuten.co.jp/tsutsu-uraura/"

def main():
    """Main execution function."""
    print(f"--- Running: Step 1: Extract URLs from '{SHOPIFY_INPUT_FILE.name}' ---")
    OUT_DIR.mkdir(exist_ok=True)

    if not SHOPIFY_INPUT_FILE.is_file():
        print(f"  [ERROR] Input file not found: '{SHOPIFY_INPUT_FILE}'")
        print("          Please run the main conversion script first.")
        return

    found_items = set()
    try:
        df = pd.read_csv(SHOPIFY_INPUT_FILE, dtype=str, keep_default_na=False)
        if "Handle" not in df.columns or "Body (HTML)" not in df.columns:
            print("  [ERROR] Input CSV is missing 'Handle' and/or 'Body (HTML)' columns.")
            return

        print("  - Searching for image URLs...")
        for _, row in df.iterrows():
            handle, html_content = row["Handle"], row["Body (HTML)"]
            if not handle.strip() or not html_content.strip():
                continue
            
            soup = BeautifulSoup(html_content, 'lxml')
            for img_tag in soup.find_all('img', src=re.compile(f"^{re.escape(RAKUTEN_DOMAIN_TO_FIND)}")):
                src = img_tag.get('src', '').strip()
                if src:
                    found_items.add((handle.strip(), src))

        if not found_items:
            print("  - No image URLs matching the specified domain were found.")
            return
        
        print(f"  - Found {len(found_items)} unique handle-image pairs.")
        url_df = pd.DataFrame(sorted(list(found_items)), columns=['handle', 'url'])
        url_df.to_csv(URL_MAP_OUTPUT_FILE, index=False, header=True)
        print(f"  - Successfully created URL map: '{URL_MAP_OUTPUT_FILE.name}'")

    except Exception as e:
        print(f"  [ERROR] An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
    print("--- Step 1 Finished ---\n")