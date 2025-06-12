"""
Step 2: Downloads all images listed in the URL map file.
---------------------------------------------------------
Usage: python 2_download_images.py
"""
from pathlib import Path
import pandas as pd
import requests

# --- Configuration ---
OUT_DIR = Path("output")
URL_MAP_INPUT_FILE = OUT_DIR / "body_html_image_urls.csv"
DOWNLOADED_IMAGES_DIR = OUT_DIR / "downloaded_body_html_images"
DOWNLOAD_ERROR_LOG = OUT_DIR / "download_errors.log.csv"
IMAGES_PER_FOLDER = 200

def main():
    """Main execution function."""
    print(f"--- Running: Step 2: Download Images ---")
    DOWNLOADED_IMAGES_DIR.mkdir(exist_ok=True)

    if not URL_MAP_INPUT_FILE.is_file():
        print(f"  [ERROR] URL map file not found: '{URL_MAP_INPUT_FILE}'")
        print("          Please run '1_extract_urls.py' first.")
        return

    try:
        items_to_download = pd.read_csv(URL_MAP_INPUT_FILE, dtype=str, keep_default_na=False).to_dict('records')
    except Exception as e:
        print(f"  [ERROR] Could not read or process the URL map file: {e}")
        return

    if not items_to_download:
        print("  - No URLs found in the file to download.")
        return

    print(f"  - Preparing to download {len(items_to_download)} images...")
    failed_downloads = []
    for i, item in enumerate(items_to_download, 1):
        handle, url = item['handle'], item['url']
        try:
            group_index = (i - 1) // IMAGES_PER_FOLDER
            folder_name = f"{group_index * IMAGES_PER_FOLDER + 1}-{ (group_index + 1) * IMAGES_PER_FOLDER}"
            current_download_dir = DOWNLOADED_IMAGES_DIR / folder_name
            current_download_dir.mkdir(exist_ok=True)

            original_filename = Path(url.split("?")[0]).name
            new_filename = f"{handle}_{original_filename}"
            destination_path = current_download_dir / new_filename

            if destination_path.exists():
                continue

            response = requests.get(url, timeout=20)
            response.raise_for_status()

            with open(destination_path, 'wb') as f:
                f.write(response.content)
        except Exception as e:
            failed_downloads.append({'handle': handle, 'url': url, 'error': str(e)})
            
    total, failed = len(items_to_download), len(failed_downloads)
    print(f"\n  - Download summary: {total - failed} successful, {failed} failed.")
    if failed_downloads:
        print(f"  - Writing {failed} download failure(s) to '{DOWNLOAD_ERROR_LOG.name}'...")
        pd.DataFrame(failed_downloads).to_csv(DOWNLOAD_ERROR_LOG, index=False)

if __name__ == "__main__":
    main()
    print("--- Step 2 Finished ---\n")