"""
Step 04: Image Processing and URL Management

Processes product images from Rakuten and generates proper Shopify image columns.
Handles up to 20 images per product with position management and URL fixes.
"""

import logging
import pandas as pd
import re
from typing import Dict, Any, List

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

    df = data['html_processed_df'].copy()
    config = data['config']

    # Track image processing statistics
    image_stats = {
        'products_with_images': 0,
        'total_images_processed': 0,
        'gold_urls_fixed': 0,
        'cabinet_urls_processed': 0,
        'empty_image_fields': 0
    }

    # Create image columns (Image Src, Image Position, Image Alt Text)
    image_columns = []
    for i in range(1, config.max_images_per_product + 1):
        image_columns.extend([
            f'Image Src {i}' if i > 1 else 'Image Src',
            f'Image Position {i}' if i > 1 else 'Image Position',
            f'Image Alt Text {i}' if i > 1 else 'Image Alt Text'
        ])

    # Initialize image columns
    for col in image_columns:
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

    # Log image processing results
    logger.info(f"Image processing completed")
    for key, value in image_stats.items():
        logger.info(f"Image stats - {key}: {value}")

    return {
        'image_processed_df': df,
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

    # Extract images from various Rakuten fields
    image_urls = extract_image_urls(row, config)

    if image_urls:
        stats['products_with_images'] += 1

        # Process up to max_images_per_product
        for i, url in enumerate(image_urls[:config.max_images_per_product], 1):
            stats['total_images_processed'] += 1

            # Fix image URL
            fixed_url = fix_image_url(url, config, stats)

            # Set image data
            src_col = 'Image Src' if i == 1 else f'Image Src {i}'
            pos_col = 'Image Position' if i == 1 else f'Image Position {i}'
            alt_col = 'Image Alt Text' if i == 1 else f'Image Alt Text {i}'

            image_data[src_col] = fixed_url
            image_data[pos_col] = str(i)
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
        '商品画像URL',
        '商品画像URL2',
        '商品画像URL3',
        '商品画像URL4',
        '商品画像URL5',
        '商品画像URL6',
        '商品画像URL7',
        '商品画像URL8',
        '商品画像URL9',
        '商品画像URL10',
        '商品画像URL11',
        '商品画像URL12',
        '商品画像URL13',
        '商品画像URL14',
        '商品画像URL15',
        '商品画像URL16',
        '商品画像URL17',
        '商品画像URL18',
        '商品画像URL19',
        '商品画像URL20'
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