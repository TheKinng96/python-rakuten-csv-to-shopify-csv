#!/usr/bin/env python3
"""
Download Images from HTML Descriptions
Downloads all unique images extracted from HTML descriptions in Step 03
"""
import json
import requests
from pathlib import Path
import time
from urllib.parse import urlparse
import hashlib
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

class HTMLImageDownloader:
    def __init__(self,
                 json_file: Path = Path("step_output/step_03_cleaned_image_urls.json"),
                 download_dir: Path = Path("step_output/cleaned_html_images"),
                 images_per_folder: int = 200,
                 max_workers: int = 8,
                 timeout: int = 30):

        self.json_file = json_file
        self.download_dir = download_dir
        self.images_per_folder = images_per_folder
        self.max_workers = max_workers
        self.timeout = timeout

        # Thread-safe counters
        self.lock = threading.Lock()
        self.downloaded_count = 0
        self.skipped_count = 0
        self.failed_count = 0
        self.total_count = 0

        # Error tracking
        self.failed_downloads = []

        print(f"🖼️  HTML Image Downloader")
        print(f"   📁 Download directory: {download_dir}")
        print(f"   🔢 Images per folder: {images_per_folder}")
        print(f"   🧵 Max workers: {max_workers}")
        print(f"   ⏱️  Timeout: {timeout}s")

    def load_image_urls(self):
        """Load image URLs from JSON file"""
        if not self.json_file.exists():
            raise FileNotFoundError(f"Image URLs file not found: {self.json_file}")

        with open(self.json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        return data.get('image_urls', [])

    def get_filename_from_url(self, url: str, index: int) -> str:
        """Generate filename from URL with fallback to index"""
        try:
            parsed = urlparse(url)
            path = parsed.path

            # Extract filename from path
            if path:
                filename = Path(path).name
                if filename and '.' in filename:
                    return filename

            # Fallback: create filename from URL hash
            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            extension = '.jpg'  # Default extension for Rakuten images

            # Try to detect extension from URL
            if '.png' in url.lower():
                extension = '.png'
            elif '.gif' in url.lower():
                extension = '.gif'
            elif '.webp' in url.lower():
                extension = '.webp'

            return f"image_{index:05d}_{url_hash}{extension}"

        except Exception:
            return f"image_{index:05d}.jpg"

    def get_subfolder_path(self, index: int) -> Path:
        """Get subfolder path based on image index"""
        folder_num = (index - 1) // self.images_per_folder + 1
        start_idx = (folder_num - 1) * self.images_per_folder + 1
        end_idx = folder_num * self.images_per_folder

        folder_name = f"{start_idx:05d}-{end_idx:05d}"
        return self.download_dir / folder_name

    def download_single_image(self, url: str, index: int) -> dict:
        """Download a single image and return result"""
        result = {
            'index': index,
            'url': url,
            'status': 'unknown',
            'filepath': None,
            'error': None,
            'size_bytes': 0
        }

        try:
            # Generate filename and folder path
            filename = self.get_filename_from_url(url, index)
            subfolder = self.get_subfolder_path(index)
            subfolder.mkdir(parents=True, exist_ok=True)
            filepath = subfolder / filename

            # Check if file already exists
            if filepath.exists():
                with self.lock:
                    self.skipped_count += 1
                result.update({
                    'status': 'skipped',
                    'filepath': str(filepath),
                    'size_bytes': filepath.stat().st_size
                })
                return result

            # Download the image
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            response = requests.get(url, headers=headers, timeout=self.timeout, stream=True)
            response.raise_for_status()

            # Save the image
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            file_size = filepath.stat().st_size

            with self.lock:
                self.downloaded_count += 1

            result.update({
                'status': 'success',
                'filepath': str(filepath),
                'size_bytes': file_size
            })

        except requests.exceptions.RequestException as e:
            with self.lock:
                self.failed_count += 1
                self.failed_downloads.append({
                    'index': index,
                    'url': url,
                    'error': str(e),
                    'error_type': 'request_error'
                })
            result.update({
                'status': 'failed',
                'error': str(e)
            })

        except Exception as e:
            with self.lock:
                self.failed_count += 1
                self.failed_downloads.append({
                    'index': index,
                    'url': url,
                    'error': str(e),
                    'error_type': 'general_error'
                })
            result.update({
                'status': 'failed',
                'error': str(e)
            })

        return result

    def download_all_images(self):
        """Download all images using thread pool"""
        print(f"\n📥 Loading image URLs...")

        try:
            image_urls = self.load_image_urls()
            self.total_count = len(image_urls)

            if not image_urls:
                print("❌ No image URLs found!")
                return

            print(f"   Found {self.total_count} unique image URLs")

        except Exception as e:
            print(f"❌ Error loading image URLs: {e}")
            return

        # Create download directory
        self.download_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n🚀 Starting download with {self.max_workers} threads...")
        start_time = time.time()

        # Track progress
        progress_interval = max(1, self.total_count // 20)  # Update every 5%

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all download tasks
            future_to_url = {
                executor.submit(self.download_single_image, url, idx): (url, idx)
                for idx, url in enumerate(image_urls, 1)
            }

            # Process completed downloads
            for completed, future in enumerate(as_completed(future_to_url), 1):
                result = future.result()

                # Progress reporting
                if completed % progress_interval == 0 or completed == self.total_count:
                    progress = (completed / self.total_count) * 100
                    print(f"   Progress: {completed}/{self.total_count} ({progress:.1f}%) - "
                          f"✅ {self.downloaded_count} downloaded, "
                          f"⏭️ {self.skipped_count} skipped, "
                          f"❌ {self.failed_count} failed")

        elapsed = time.time() - start_time
        self.print_summary(elapsed)
        self.save_error_log()

    def print_summary(self, elapsed_time: float):
        """Print download summary"""
        print(f"\n📊 Download Summary:")
        print(f"   ⏱️  Total time: {elapsed_time:.1f}s")
        print(f"   📁 Total images: {self.total_count}")
        print(f"   ✅ Downloaded: {self.downloaded_count}")
        print(f"   ⏭️  Skipped (existed): {self.skipped_count}")
        print(f"   ❌ Failed: {self.failed_count}")

        if elapsed_time > 0:
            rate = self.total_count / elapsed_time
            print(f"   🚀 Average rate: {rate:.1f} images/second")

        if self.downloaded_count > 0:
            print(f"   💾 Images saved to: {self.download_dir}")

    def save_error_log(self):
        """Save failed downloads to CSV log"""
        if not self.failed_downloads:
            print(f"   🎉 No download errors!")
            return

        error_file = self.download_dir / "download_errors.csv"

        try:
            df = pd.DataFrame(self.failed_downloads)
            df.to_csv(error_file, index=False, encoding='utf-8')
            print(f"   📝 Error log saved: {error_file}")
        except Exception as e:
            print(f"   ⚠️  Could not save error log: {e}")


def main():
    """Main execution function"""
    print("=" * 70)
    print("🖼️  HTML IMAGE DOWNLOADER")
    print("   Downloads all images extracted from HTML descriptions")
    print("=" * 70)

    try:
        # Initialize downloader
        downloader = HTMLImageDownloader(
            json_file=Path("step_output/step_03_cleaned_image_urls.json"),
            download_dir=Path("step_output/cleaned_html_images"),
            images_per_folder=200,
            max_workers=8,
            timeout=30
        )

        # Start download process
        downloader.download_all_images()

        print(f"\n🎉 Download process completed!")
        print(f"💡 Next step: Upload images to Shopify CDN and update HTML URLs")

    except KeyboardInterrupt:
        print(f"\n⚠️  Download interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()