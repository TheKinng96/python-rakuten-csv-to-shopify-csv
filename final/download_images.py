import pandas as pd
from pathlib import Path
import requests

def download_images_from_csv(csv_path: Path = Path("output/shopify_image_urls.csv"), download_dir: Path = Path("output/images")) -> None:
    """
    Reads a CSV file with a 'url' column and downloads each image to a
    specified directory. Skips images that already exist.

    Args:
        csv_path (Path): Path to the input CSV file containing image URLs.
        download_dir (Path): The directory where images will be saved.
    """
    print(f"\n[+] Starting image download process...")

    if not csv_path.is_file():
        print(f"  - URL list file not found at '{csv_path}'. Skipping download.")
        return

    # Create the destination directory if it doesn't exist
    download_dir.mkdir(exist_ok=True)
    print(f"  - Images will be saved to: {download_dir.resolve()}")

    try:
        df = pd.read_csv(csv_path)
        if 'url' not in df.columns:
            print("  - Error: Input CSV is missing the required 'url' column. Aborting.")
            return
        # Get a list of unique, non-empty URLs
        urls = df['url'].dropna().unique().tolist()
    except Exception as e:
        print(f"  - Error: Could not read or process the CSV file: {e}")
        return

    if not urls:
        print("  - No URLs found in the file to download.")
        return

    # Initialize counters for a final report
    total_urls = len(urls)
    downloaded_count = 0
    skipped_count = 0
    failed_count = 0

    for i, url in enumerate(urls, 1):
        try:
            # Derive a filename from the URL (e.g., 'image.jpg')
            filename = Path(url.split("?")[0]).name
            if not filename:
                print(f"  - [{i}/{total_urls}] FAILED: Could not determine filename for URL: {url}")
                failed_count += 1
                continue

            destination_path = download_dir / filename

            # --- KEY FEATURE: Skip download if file already exists ---
            if destination_path.exists():
                print(f"  - [{i}/{total_urls}] SKIPPING: '{filename}' already exists.")
                skipped_count += 1
                continue

            # Make the web request to get the image
            print(f"  - [{i}/{total_urls}] DOWNLOADING: '{filename}'...", end="", flush=True)
            response = requests.get(url, timeout=20) # 20-second timeout for the request
            response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)

            # Save the image content to a file in binary write mode
            with open(destination_path, 'wb') as f:
                f.write(response.content)
            
            print(" Done.")
            downloaded_count += 1

        except requests.exceptions.RequestException as e:
            # Handle network-related errors (timeout, connection error, bad status code)
            print(f" FAILED. (Error: {e})")
            failed_count += 1
        except Exception as e:
            # Handle other potential errors (e.g., file system permissions)
            print(f" FAILED. (An unexpected error occurred: {e})")
            failed_count += 1

    # Print a final summary of the operation
    print("\n  --- Download Summary ---")
    print(f"  Total URLs in list:      {total_urls}")
    print(f"  Successfully downloaded: {downloaded_count}")
    print(f"  Skipped (already exist): {skipped_count}")
    print(f"  Failed downloads:        {failed_count}")
    print("  ------------------------")

if __name__ == "__main__":
    download_images_from_csv()