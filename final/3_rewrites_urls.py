"""
Step 3: Rewrites image URLs in the Shopify CSV to the new format.
------------------------------------------------------------------
Usage: python 3_rewrite_urls.py
"""
from pathlib import Path
import pandas as pd

# --- Configuration ---
OUT_DIR = Path("output")
SHOPIFY_INPUT_FILE = OUT_DIR / "shopify_products_preview.csv"
URL_MAP_INPUT_FILE = OUT_DIR / "body_html_image_urls.csv"
FINAL_SHOPIFY_OUTPUT_FILE = OUT_DIR / "final_shopify_products_ready_for_upload.csv"

# ==============================================================================
# CORRECTED CONFIGURATION:
# Set this to your Shopify Files CDN path.
# IMPORTANT: Make sure it ends with a forward slash /
# ==============================================================================
SHOPIFY_FILES_BASE_URL = "https://cdn.shopify.com/s/files/1/0637/6059/7127/files/"


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
            # This is the filename created by the download script (e.g., xl-ekjd-8f7a_maruta1.jpg)
            new_filename = f"{handle}_{original_filename}"
            
            # Since SHOPIFY_FILES_BASE_URL is now set, this block will build the full URL
            if SHOPIFY_FILES_BASE_URL:
                replacement_map[old_url] = f"{SHOPIFY_FILES_BASE_URL}{new_filename}"
            else:
                # This block is for the Liquid format, which will now be skipped
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