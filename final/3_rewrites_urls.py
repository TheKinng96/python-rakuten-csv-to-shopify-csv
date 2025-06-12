"""
Step 3: Rewrites image URLs in the Shopify CSV to the new format.
------------------------------------------------------------------
Usage: python 3_rewrite_urls.py
"""
from pathlib import Path
import pandas as pd

# --- Configuration ---
OUT_DIR = Path("output")
SHOPIFY_INPUT_FILE = OUT_DIR / "shopify_products.csv"
URL_MAP_INPUT_FILE = OUT_DIR / "body_html_image_urls.csv"
FINAL_SHOPIFY_OUTPUT_FILE = OUT_DIR / "final_shopify_products_ready_for_upload.csv"

# The base URL for your new image paths.
# OPTION 1: Leave empty ("") to use the recommended Shopify Liquid format.
# OPTION 2: Use a hardcoded CDN path (e.g., "https://cdn.shopify.com/s/files/1/YOUR_STORE_ID/files/").
SHOPIFY_FILES_BASE_URL = ""

def main():
    """Main execution function."""
    print(f"--- Running: Step 3: Rewrite Body (HTML) URLs ---")

    if not SHOPIFY_INPUT_FILE.is_file():
        print(f"  [ERROR] Input file not found: '{SHOPIFY_INPUT_FILE}'")
        return
    if not URL_MAP_INPUT_FILE.is_file():
        print(f"  [ERROR] URL map file not found: '{URL_MAP_INPUT_FILE}'")
        print("          Please run '1_extract_urls.py' first.")
        return

    try:
        url_map_df = pd.read_csv(URL_MAP_INPUT_FILE, dtype=str, keep_default_na=False)
        replacement_map = {}
        for _, row in url_map_df.iterrows():
            handle, old_url = row['handle'], row['url']
            original_filename = Path(old_url.split("?")[0]).name
            new_filename = f"{handle}_{original_filename}"
            
            if SHOPIFY_FILES_BASE_URL:
                replacement_map[old_url] = f"{SHOPIFY_FILES_BASE_URL}{new_filename}"
            else:
                replacement_map[old_url] = f"{{{{ '{new_filename}' | file_url }}}}"
        
        if not replacement_map:
            print("  - No URL mappings found. Copying original file.")
            SHOPIFY_INPUT_FILE.replace(FINAL_SHOPIFY_OUTPUT_FILE)
            return

        print(f"  - Reading '{SHOPIFY_INPUT_FILE.name}' and applying {len(replacement_map)} URL replacements...")
        with open(SHOPIFY_INPUT_FILE, 'r', encoding='utf-8', newline='') as f:
            content = f.read()
        
        original_content = content
        for old, new in replacement_map.items():
            content = content.replace(old, new)
        
        if content == original_content:
            print("  - No matching URLs were found in the file to replace.")
        
        with open(FINAL_SHOPIFY_OUTPUT_FILE, 'w', encoding='utf-8', newline='') as f:
            f.write(content)
        print(f"  - Successfully created final file: '{FINAL_SHOPIFY_OUTPUT_FILE.name}'")

    except Exception as e:
        print(f"  [ERROR] An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
    print("--- Step 3 Finished ---\n")