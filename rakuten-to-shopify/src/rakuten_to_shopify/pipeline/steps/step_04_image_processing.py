"""
Step 04: Image Processing and URL Management

Processes product images from Rakuten and generates proper Shopify image columns.
Handles up to 20 images per product with position management and URL fixes.
Also replaces HTML description image URLs with Shopify CDN URLs.
"""

import logging
import pandas as pd
import re
from typing import Dict, Any, List
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process product images and create Shopify image columns

    Args:
        data: Pipeline context containing html_processed_df and config

    Returns:
        Dict containing dataframe with processed images
    """
    logger.info("Processing product images...")

    # Get the most recent processed dataframe with all columns preserved
    df = data.get('html_processed_df', data['shopify_df']).copy()
    config = data['config']

    # Track image processing statistics
    image_stats = {
        'products_with_images': 0,
        'total_images_processed': 0,
        'gold_urls_fixed': 0,
        'cabinet_urls_processed': 0,
        'empty_image_fields': 0,
        'html_descriptions_with_images': 0,
        'html_rakuten_urls_found': 0,
        'html_urls_replaced': 0
    }

    # Shopify CDN base URL for HTML image replacement
    shopify_cdn_base = "https://cdn.shopify.com/s/files/1/0637/6059/7127/files/"

    # Load failed downloads to skip replacement for missing images
    failed_urls = load_failed_download_urls()

    # Create image columns (Image Src, Image Position, Image Alt Text)
    image_columns = []
    for i in range(1, config.max_images_per_product + 1):
        image_columns.extend([
            f'Image Src {i}' if i > 1 else 'Image Src',
            f'Image Position {i}' if i > 1 else 'Image Position',
            f'Image Alt Text {i}' if i > 1 else 'Image Alt Text'
        ])

    # Initialize image columns (preserve existing values)
    for col in image_columns:
        if col not in df.columns:
            df[col] = ''

    # Process images for each row
    df_processed = df.apply(
        lambda row: process_product_images(row, config, image_stats),
        axis=1
    )

    # Update dataframe with processed image data
    for idx, row_data in df_processed.items():
        for col, value in row_data.items():
            if col.startswith('Image'):
                df.at[idx, col] = value

    # Replace Rakuten URLs with Shopify CDN URLs using proven working logic
    logger.info("Replacing Rakuten image URLs with Shopify CDN URLs in HTML descriptions...")

    # Use the proven working approach from clean pipeline
    def clean_and_replace_html(html_content):
        """Clean EC-UP blocks and replace Rakuten URLs with Shopify CDN URLs"""
        if pd.isna(html_content) or not html_content:
            return html_content

        import re
        from pathlib import Path
        from urllib.parse import urlparse

        html_str = str(html_content)

        # Step 1: Remove EC-UP blocks (proven working patterns)
        ec_up_patterns = [
            r'<!--EC-UP_[^>]*?START-->(.*?)<!--EC-UP_[^>]*?END-->',
            r'<!--EC-UP_[^>]*?-->',
            r'<style[^>]*?ecup[^>]*?>(.*?)</style>',
            r'<div[^>]*?class="[^"]*ecup[^"]*"[^>]*?>(.*?)</div>',
        ]

        for pattern in ec_up_patterns:
            html_str = re.sub(pattern, '', html_str, flags=re.DOTALL | re.IGNORECASE)

        # Step 2: Replace Rakuten image URLs with Shopify CDN URLs
        try:
            soup = BeautifulSoup(html_str, 'html.parser')

            for img in soup.find_all('img'):
                for attr in ['src', 'data-original-src']:
                    url = img.get(attr)
                    if url and 'image.rakuten.co.jp' in url and 'tsutsu-uraura' in url:
                        # Skip failed downloads - remove the img tag entirely
                        if failed_urls and url in failed_urls:
                            img.decompose()
                            break

                        # Extract filename and replace with Shopify CDN
                        try:
                            parsed = urlparse(url)
                            filename = Path(parsed.path).name.split('?')[0]
                            if filename and '.' in filename:
                                shopify_url = f"{shopify_cdn_base}{filename}?v=1758179452"
                                img[attr] = shopify_url
                        except:
                            pass

            return str(soup)

        except Exception as e:
            logger.warning(f"Error in HTML processing: {e}")
            return html_str

    # Apply the combined cleaning and replacement
    df['Body (HTML)'] = df['Body (HTML)'].apply(clean_and_replace_html)

    # CRITICAL: Update the original data dict with our modified DataFrame
    # This ensures the step runner saves the correct DataFrame to CSV
    data['html_processed_df'] = df

    # Debug: Verify DataFrame changes before returning
    sample_html = str(df['Body (HTML)'].head(2).to_string())
    ec_up_count = sample_html.count('EC-UP')
    shopify_count = sample_html.count('cdn.shopify.com')
    logger.info(f"DEBUG: DataFrame before return has {ec_up_count} EC-UP, {shopify_count} Shopify CDN in first 2 rows")

    # Log image processing results
    logger.info(f"Image processing and URL replacement completed")
    for key, value in image_stats.items():
        logger.info(f"Image stats - {key}: {value}")

    return {
        'html_processed_df': df,  # Keep same key for step chaining
        'image_processed_df': df,  # Also provide for backwards compatibility
        'image_stats': image_stats
    }


def process_product_images(row: pd.Series, config, stats: Dict[str, Any]) -> Dict[str, str]:
    """
    Process images for a single product

    Args:
        row: Product row from dataframe
        config: Pipeline configuration
        stats: Statistics tracking dictionary

    Returns:
        Dict with image column data
    """
    image_data = {}
    product_name = row.get('商品名', '')

    # Extract images from existing Image Src columns and Rakuten fields
    image_urls = []

    # Collect existing complete Image Src URLs (already constructed in step 02)
    for i in range(1, config.max_images_per_product + 1):
        src_col = 'Image Src' if i == 1 else f'Image Src {i}'

        if src_col in row and row[src_col] and str(row[src_col]).strip():
            complete_url = str(row[src_col]).strip()
            if complete_url not in image_urls:
                image_urls.append(complete_url)

    # Then, extract from any remaining Rakuten fields (fallback)
    rakuten_urls = extract_image_urls(row, config)
    for url in rakuten_urls:
        if url not in image_urls:
            image_urls.append(url)

    if image_urls:
        stats['products_with_images'] += 1

        # Process up to max_images_per_product
        for i, url in enumerate(image_urls[:config.max_images_per_product], 1):
            stats['total_images_processed'] += 1

            # Set image data - URLs and alt text are already correct from step 02
            src_col = 'Image Src' if i == 1 else f'Image Src {i}'
            pos_col = 'Image Position' if i == 1 else f'Image Position {i}'
            alt_col = 'Image Alt Text' if i == 1 else f'Image Alt Text {i}'

            # Preserve the already-correct URL and alt text from step 02
            image_data[src_col] = url
            image_data[pos_col] = str(i)

            # Keep original alt text from step 02 (商品画像名)
            if alt_col in row and row[alt_col] and str(row[alt_col]).strip():
                image_data[alt_col] = str(row[alt_col]).strip()
            else:
                # Fallback only if no alt text was set in step 02
                image_data[alt_col] = f"{product_name} - 画像{i}"

    else:
        stats['empty_image_fields'] += 1

    return image_data


def extract_image_urls(row: pd.Series, config) -> List[str]:
    """
    Extract image URLs from Rakuten product data

    Args:
        row: Product row from dataframe
        config: Pipeline configuration

    Returns:
        List of image URLs
    """
    image_urls = []

    # Image fields to check (in order of priority)
    image_fields = [
        '商品画像パス1',
        '商品画像パス2',
        '商品画像パス3',
        '商品画像パス4',
        '商品画像パス5',
        '商品画像パス6',
        '商品画像パス7',
        '商品画像パス8',
        '商品画像パス9',
        '商品画像パス10',
        '商品画像パス11',
        '商品画像パス12',
        '商品画像パス13',
        '商品画像パス14',
        '商品画像パス15',
        '商品画像パス16',
        '商品画像パス17',
        '商品画像パス18',
        '商品画像パス19',
        '商品画像パス20'
    ]

    # Extract URLs from available fields
    for field in image_fields:
        if field in row and pd.notna(row[field]) and row[field].strip():
            url = str(row[field]).strip()
            if url and url not in image_urls:
                image_urls.append(url)

    return image_urls


def fix_image_url(url: str, config, stats: Dict[str, Any]) -> str:
    """
    Fix image URL patterns and convert to absolute URLs

    Args:
        url: Original image URL
        config: Pipeline configuration
        stats: Statistics tracking dictionary

    Returns:
        Fixed absolute URL
    """
    if not url:
        return ''

    original_url = url

    # Fix gold URL pattern
    if 'tsutsu-uraura/gold/' in url:
        url = config.fix_gold_url(url)
        stats['gold_urls_fixed'] += 1
    elif 'cabinet' in url:
        stats['cabinet_urls_processed'] += 1

    # Convert to absolute URL
    absolute_url = config.to_absolute_url(url)

    return absolute_url


def validate_image_url(url: str) -> bool:
    """
    Validate if URL looks like a valid image URL

    Args:
        url: URL to validate

    Returns:
        bool: True if URL appears valid
    """
    if not url:
        return False

    # Check for valid image extensions
    image_extensions = r'\.(jpg|jpeg|png|gif|webp)(\?.*)?$'
    if re.search(image_extensions, url, re.IGNORECASE):
        return True

    # Check for valid URL pattern
    url_pattern = r'^https?://[^\s<>"{}|\\^`\[\]]+$'
    if re.match(url_pattern, url):
        return True

    return False


def create_variant_image_mapping(df: pd.DataFrame) -> Dict[str, str]:
    """
    Create mapping of variant SKUs to their specific images

    Args:
        df: Dataframe with processed products

    Returns:
        Dict mapping variant SKU to image URL
    """
    variant_images = {}

    for _, row in df.iterrows():
        sku = row.get('Variant SKU', '')
        main_image = row.get('Image Src', '')

        if sku and main_image:
            variant_images[sku] = main_image

    return variant_images


def replace_html_image_urls(html_content: str, shopify_cdn_base: str, stats: Dict[str, Any], failed_urls: set = None) -> str:
    """Replace Rakuten image URLs with Shopify CDN URLs using proven working logic"""
    if pd.isna(html_content) or not html_content:
        return html_content

    try:
        soup = BeautifulSoup(str(html_content), 'html.parser')

        if soup.find_all('img'):
            stats['html_descriptions_with_images'] += 1

        replacements = 0

        for img in soup.find_all('img'):
            for attr in ['src', 'data-original-src']:
                url = img.get(attr)
                if url and 'image.rakuten.co.jp' in url and 'tsutsu-uraura' in url:
                    stats['html_rakuten_urls_found'] += 1

                    # Skip failed downloads - remove the img tag entirely
                    if failed_urls and url in failed_urls:
                        img.decompose()
                        replacements += 1
                        break

                    # Extract filename and replace with Shopify CDN
                    try:
                        parsed = urlparse(url)
                        filename = Path(parsed.path).name.split('?')[0]
                        if filename and '.' in filename:
                            shopify_url = f"{shopify_cdn_base}{filename}?v=1758179452"
                            img[attr] = shopify_url
                            replacements += 1
                    except:
                        pass

        stats['html_urls_replaced'] += replacements
        return str(soup)

    except Exception as e:
        logger.warning(f"Error replacing HTML image URLs: {e}")
        return str(html_content)


def is_rakuten_url(url: str) -> bool:
    """Check if URL is a Rakuten image URL"""
    if not url:
        return False

    return 'image.rakuten.co.jp' in url and 'tsutsu-uraura' in url


def extract_filename_from_rakuten_url(url: str) -> str:
    """
    Extract filename from Rakuten URL

    Examples:
    https://image.rakuten.co.jp/tsutsu-uraura/cabinet/productpic/yufu.jpg → yufu.jpg
    https://image.rakuten.co.jp/tsutsu-uraura/cabinet/gift_info/tanzaku/ochugen_seal010r.jpg → ochugen_seal010r.jpg
    """
    try:
        parsed = urlparse(url)
        path = parsed.path

        if path:
            # Extract filename from path
            filename = Path(path).name

            # Remove query parameters from filename if they got included
            filename = filename.split('?')[0]

            if filename and '.' in filename:
                return filename

        return None

    except Exception as e:
        logger.warning(f"Error extracting filename from URL {url}: {e}")
        return None


def load_failed_download_urls() -> set:
    """
    Load URLs that failed to download from the error log

    Returns:
        Set of URLs that failed to download
    """
    failed_urls = set()
    error_file = Path("step_output/cleaned_html_images/download_errors.csv")

    if error_file.exists():
        try:
            import pandas as pd
            df_errors = pd.read_csv(error_file)
            if 'url' in df_errors.columns:
                failed_urls = set(df_errors['url'].dropna().tolist())
                logger.info(f"Loaded {len(failed_urls)} failed download URLs to skip")
        except Exception as e:
            logger.warning(f"Error loading failed download URLs: {e}")

    return failed_urls