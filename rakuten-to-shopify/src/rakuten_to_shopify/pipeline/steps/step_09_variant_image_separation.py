"""
Step 09: Variant Image Separation

Fixes the variant image issue where all variants share the same Image Src.
Each variant should have its own Image Src showing the correct packaging (1-pack, 2-pack, 5-pack, etc.).
The variant-specific images are currently stored in the Variant Image column but need to be moved to Image Src.
"""

import logging
import pandas as pd
from typing import Dict, Any

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fix variant image separation by moving variant-specific images to Image Src column

    Args:
        data: Pipeline context containing cleaned_df

    Returns:
        Dict containing dataframe with corrected variant images
    """
    logger.info("Starting variant image separation...")

    df = data['cleaned_df'].copy()

    # Debug: Check what DataFrame we received
    logger.info(f"DEBUG: Received DataFrame shape: {df.shape}")
    logger.info(f"DEBUG: DataFrame columns include Image Src: {'Image Src' in df.columns}")

    # Debug: Check alps-c-appple-1000 before processing
    test_data = df[df['Handle'] == 'alps-c-appple-1000']
    if len(test_data) > 0:
        logger.info("DEBUG: alps-c-appple-1000 BEFORE processing:")
        for idx, row in test_data.iterrows():
            if pd.notna(row.get('Variant SKU')):
                sku = row['Variant SKU']
                img_src = row['Image Src'].split('/')[-1] if pd.notna(row['Image Src']) else 'None'
                var_img = row['Variant Image'].split('/')[-1] if pd.notna(row['Variant Image']) else 'None'
                logger.info(f"  DEBUG: {sku} - Image Src: {img_src}, Variant Image: {var_img}")

    # Track statistics
    products_with_variants = 0
    variants_with_different_images = 0
    images_corrected = 0

    # Group by Handle to process each product separately
    for handle, group in df.groupby('Handle'):
        if len(group) > 1:  # Only process products with multiple variants
            products_with_variants += 1

            # Check if variants have different Variant Image values
            variant_images = group['Variant Image'].dropna().unique()

            if len(variant_images) > 1:
                variants_with_different_images += 1

            logger.info(f"Processing {handle}: {len(group)} variants with {len(variant_images)} different images")

            # For each variant in this product group (process ALL variants, not just those with different images)
            for idx, row in group.iterrows():
                variant_image = row['Variant Image']
                current_image_src = row['Image Src']

                # If variant has a specific image and it's different from current Image Src
                if pd.notna(variant_image) and variant_image.strip() and variant_image != current_image_src:
                    # Move the variant-specific image to Image Src
                    df.at[idx, 'Image Src'] = variant_image
                    images_corrected += 1

                    logger.info(f"  {handle} variant {row.get('Variant SKU', 'unknown')}: Image Src changed from '{current_image_src.split('/')[-1]}' to '{variant_image.split('/')[-1]}'")

                    # Special debug for alps-c-appple-1000
                    if handle == 'alps-c-appple-1000':
                        logger.info(f"  DEBUG: alps-c-appple-1000 idx {idx} updated successfully")

    # Additional fix: For products where the first variant has empty Image Src but has Variant Image,
    # also copy Variant Image to Image Src (this handles the main product row issue)
    for handle, group in df.groupby('Handle'):
        first_variant_idx = group.index[0]
        first_variant = group.iloc[0]

        if (pd.isna(first_variant['Image Src']) or not first_variant['Image Src'].strip()) and \
           pd.notna(first_variant['Variant Image']) and first_variant['Variant Image'].strip():
            df.at[first_variant_idx, 'Image Src'] = first_variant['Variant Image']
            images_corrected += 1
            logger.debug(f"  {handle} main variant: Set empty Image Src to '{first_variant['Variant Image']}'")

    # Log statistics
    logger.info(f"Variant image separation completed:")
    logger.info(f"  Products with multiple variants: {products_with_variants}")
    logger.info(f"  Products with different variant images: {variants_with_different_images}")
    logger.info(f"  Total image corrections made: {images_corrected}")

    # Debug: Check alps-c-appple-1000 AFTER processing
    test_data_after = df[df['Handle'] == 'alps-c-appple-1000']
    if len(test_data_after) > 0:
        logger.info("DEBUG: alps-c-appple-1000 AFTER processing:")
        for idx, row in test_data_after.iterrows():
            if pd.notna(row.get('Variant SKU')):
                sku = row['Variant SKU']
                img_src = row['Image Src'].split('/')[-1] if pd.notna(row['Image Src']) else 'None'
                var_img = row['Variant Image'].split('/')[-1] if pd.notna(row['Variant Image']) else 'None'
                match = '✓' if img_src == var_img else '✗'
                logger.info(f"  DEBUG: {sku} - Image Src: {img_src}, Variant Image: {var_img} {match}")

    # Verify the fix by checking a few sample products
    sample_handles = ['ajinomoto-canola-1000', 'ajinomoto-goma-160', 'ajinomoto-kome-900']
    for handle in sample_handles:
        if handle in df['Handle'].values:
            handle_data = df[df['Handle'] == handle]
            logger.info(f"Verification - {handle}:")
            for _, row in handle_data.iterrows():
                logger.info(f"  SKU: {row.get('Variant SKU', 'unknown')}, Image Src: {row['Image Src']}, Variant Image: {row['Variant Image']}")

    return {
        'cleaned_df': df,
        'variant_image_stats': {
            'products_with_variants': products_with_variants,
            'variants_with_different_images': variants_with_different_images,
            'images_corrected': images_corrected
        }
    }