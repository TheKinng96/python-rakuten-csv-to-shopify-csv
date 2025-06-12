import pandas as pd
from pathlib import Path
import requests

def download_images_from_csv(csv_path: Path = Path("output/shopify_image_urls.csv"), download_dir: Path = Path("output/images2"), error_log_path: Path = Path("output/download_errors.log.csv")) -> None:
    """
    Reads a CSV with 'handle' and 'url', downloads each image, renames it,
    organizes it into subfolders, and logs any failures to a separate file.

    Args:
        csv_path (Path): Path to the input CSV file containing handles and URLs.
        download_dir (Path): The base directory where image subfolders will be created.
        error_log_path (Path): Path to save the CSV log of any download failures.
    """
    print(f"\n[+] Starting image download process...")
    IMAGES_PER_FOLDER = 200

    if not csv_path.is_file():
        print(f"  - URL list file not found at '{csv_path}'. Skipping download.")
        return

    download_dir.mkdir(exist_ok=True)
    print(f"  - Images will be saved into subfolders inside: {download_dir.resolve()}")

    try:
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        if 'handle' not in df.columns or 'url' not in df.columns:
            print("  - Error: Input CSV is missing 'handle' and/or 'url' column. Aborting.")
            return
        df.dropna(subset=['handle', 'url'], inplace=True)
        items_to_download = df.to_dict('records')
    except Exception as e:
        print(f"  - Error: Could not read or process the CSV file: {e}")
        return

    if not items_to_download:
        print("  - No items found in the file to download.")
        return

    total_items = len(items_to_download)
    downloaded_count = 0
    skipped_count = 0
    
    # --- NEW: A list to hold only the details of failed downloads ---
    failed_downloads = []

    for i, item in enumerate(items_to_download, 1):
        handle = item['handle']
        url = item['url']

        try:
            group_index = (i - 1) // IMAGES_PER_FOLDER
            start_num = group_index * IMAGES_PER_FOLDER + 1
            end_num = start_num + IMAGES_PER_FOLDER - 1
            folder_name = f"{start_num}-{end_num}"
            current_download_dir = download_dir / folder_name
            current_download_dir.mkdir(exist_ok=True)

            original_filename = Path(url.split("?")[0]).name
            if not original_filename:
                error_message = "Could not determine filename from URL"
                print(f"  - [{i}/{total_items}] FAILED: {error_message} for URL: {url}")
                # --- NEW: Add failure details to our list ---
                failed_downloads.append({'handle': handle, 'url': url, 'error': error_message})
                continue

            new_filename = f"{handle}_{original_filename}"
            destination_path = current_download_dir / new_filename

            if destination_path.exists():
                # This is a successful skip, not an error, so no change here
                print(f"  - [{i}/{total_items}] SKIPPING: '{new_filename}' in '{folder_name}' already exists.")
                skipped_count += 1
                continue

            print(f"  - [{i}/{total_items}] DOWNLOADING: '{new_filename}' to '{folder_name}'...", end="", flush=True)
            response = requests.get(url, timeout=20)
            response.raise_for_status()

            with open(destination_path, 'wb') as f:
                f.write(response.content)
            
            print(" Done.")
            downloaded_count += 1

        except requests.exceptions.RequestException as e:
            error_message = str(e)
            print(f" FAILED. (Error: {error_message})")
            # --- NEW: Add failure details to our list ---
            failed_downloads.append({'handle': handle, 'url': url, 'error': error_message})
        except Exception as e:
            error_message = f"An unexpected error occurred: {e}"
            print(f" FAILED. ({error_message})")
            # --- NEW: Add failure details to our list ---
            failed_downloads.append({'handle': handle, 'url': url, 'error': error_message})

    # --- NEW: After the loop, write the failure log if there were any errors ---
    if failed_downloads:
        print(f"\n  - Writing {len(failed_downloads)} download failure(s) to '{error_log_path}'...")
        error_df = pd.DataFrame(failed_downloads)
        error_df.to_csv(error_log_path, index=False, columns=['handle', 'url', 'error'])
    else:
        print("\n  - No download failures to log.")

    # --- MODIFIED: Update the summary to use the length of the new list ---
    print("\n  --- Download Summary ---")
    print(f"  Total Items in list:     {total_items}")
    print(f"  Successfully downloaded: {downloaded_count}")
    print(f"  Skipped (already exist): {skipped_count}")
    print(f"  Failed downloads:        {len(failed_downloads)}")
    print("  ------------------------")

if __name__ == "__main__":
    download_images_from_csv()