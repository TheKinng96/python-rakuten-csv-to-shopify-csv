"""
Step 06: Image Restructuring

Converts the incorrect "Image Src 2-20" column format to proper Shopify row-based image structure.
Ensures proper image positioning and variant image assignments.
"""

import logging
import pandas as pd
import json
from pathlib import Path
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Restructure images from column-based to row-based format and integrate cleaned image URLs

    Args:
        data: Pipeline context containing metafield_mapped_df and config

    Returns:
        Dict containing dataframe with properly structured images
    """
    logger.info("Restructuring images from column-based to row-based format...")

    df = data['metafield_mapped_df'].copy()
    config = data['config']

    # Track image restructuring statistics
    image_stats = {
        'products_processed': 0,
        'additional_rows_created': 0,
        'images_restructured': 0,
        'columns_removed': 0,
        'cleaned_urls_integrated': 0
    }

    # Load and integrate cleaned image URLs from step 3
    df = integrate_cleaned_image_urls(df, config, image_stats)

    # Note: Keep Image Src 2-20 columns for restructuring (these are now properly populated from step 02)
    # Only remove any truly incorrect/duplicate columns if they exist
    image_stats['columns_removed'] = 0

    # Restructure images to proper row-based format
    restructured_df = restructure_images_to_rows(df, image_stats)

    # Remove extra image columns (Image Src 2-20, Image Alt Text 2-20, Image Position 2-20)
    restructured_df = remove_extra_image_columns(restructured_df, image_stats)

    # Log image restructuring results
    logger.info(f"Image restructuring completed")
    for key, value in image_stats.items():
        logger.info(f"Image stats - {key}: {value}")

    return {
        'image_restructured_df': restructured_df,
        'image_stats': image_stats
    }


def remove_extra_image_columns(df: pd.DataFrame, stats: Dict[str, Any]) -> pd.DataFrame:
    """
    Remove extra image columns (Image Src 2-20, Image Alt Text 2-20, Image Position 2-20, SKU画像タイプ, SKU画像パス, SKU画像名（ALT）)

    Args:
        df: Dataframe to clean up
        stats: Statistics tracking dictionary

    Returns:
        Dataframe with extra image columns removed
    """
    logger.info("Removing extra image columns (Image Src 2-20, Image Alt Text 2-20, Image Position 2-20, SKU画像タイプ, SKU画像パス, SKU画像名（ALT）)...")

    columns_to_remove = []

    # Find Image Src 2-20 columns
    for i in range(2, 21):
        img_src_col = f'Image Src {i}'
        img_alt_col = f'Image Alt Text {i}'
        img_pos_col = f'Image Position {i}'

        if img_src_col in df.columns:
            columns_to_remove.append(img_src_col)
        if img_alt_col in df.columns:
            columns_to_remove.append(img_alt_col)
        if img_pos_col in df.columns:
            columns_to_remove.append(img_pos_col)

    # Find SKU image columns
    sku_image_columns = ['SKU画像タイプ', 'SKU画像パス', 'SKU画像名（ALT）']
    for col in sku_image_columns:
        if col in df.columns:
            columns_to_remove.append(col)

    if columns_to_remove:
        logger.info(f"Removing {len(columns_to_remove)} extra image columns: {columns_to_remove[:5]}{'...' if len(columns_to_remove) > 5 else ''}")
        df = df.drop(columns=columns_to_remove)
        stats['columns_removed'] = len(columns_to_remove)
    else:
        logger.info("No extra image columns found to remove")
        stats['columns_removed'] = 0

    return df


def restructure_images_to_rows(df: pd.DataFrame, stats: Dict[str, Any]) -> pd.DataFrame:
    """
    Restructure images from columns to proper Shopify row format

    Args:
        df: Dataframe to restructure
        stats: Statistics tracking dictionary

    Returns:
        Restructured dataframe
    """
    logger.info("Converting image structure to Shopify row-based format...")

    # Group by Handle to process each product separately
    grouped = df.groupby('Handle')
    all_rows = []

    for handle, group in grouped:
        if pd.isna(handle) or handle == '':
            # Skip rows without handles
            all_rows.extend(group.to_dict('records'))
            continue

        stats['products_processed'] += 1
        product_rows = restructure_product_images(group, handle, stats)
        all_rows.extend(product_rows)

    # Create new dataframe from restructured rows
    if all_rows:
        restructured_df = pd.DataFrame(all_rows)
        # Reorder columns to match expected Shopify format
        restructured_df = reorder_columns_for_shopify(restructured_df)
    else:
        restructured_df = df

    return restructured_df


def restructure_product_images(group: pd.DataFrame, handle: str, stats: Dict[str, Any]) -> List[Dict]:
    """
    Restructure images for a single product handle

    Args:
        group: Dataframe group for this handle
        handle: Product handle
        stats: Statistics tracking dictionary

    Returns:
        List of row dictionaries for this product
    """
    rows = group.to_dict('records')
    product_rows = []

    # Collect all images from the group
    all_images = []

    for row in rows:
        # Check main Image Src
        if row.get('Image Src') and str(row['Image Src']).strip():
            # Ensure position is an integer
            position = row.get('Image Position', 1)
            try:
                position = int(position) if position else 1
            except (ValueError, TypeError):
                position = 1

            image_info = {
                'src': row['Image Src'],
                'position': position,
                'alt': row.get('Image Alt Text', ''),
                'is_main': True
            }
            if image_info not in all_images:
                all_images.append(image_info)

        # Look for additional images in any remaining image columns
        for col in row.keys():
            if col.startswith('Image Src ') and col != 'Image Src':
                if row.get(col) and str(row[col]).strip():
                    # Extract position from column name
                    try:
                        position = int(col.split()[-1])
                    except (ValueError, IndexError):
                        position = len(all_images) + 1

                    alt_col = f"Image Alt Text {col.split()[-1]}"
                    image_info = {
                        'src': row[col],
                        'position': position,
                        'alt': row.get(alt_col, ''),
                        'is_main': False
                    }
                    if image_info not in all_images:
                        all_images.append(image_info)

    # Sort images by position
    all_images.sort(key=lambda x: x['position'])

    # Update positions to be sequential
    for i, img in enumerate(all_images, 1):
        img['position'] = i

    # Create product rows with proper image structure
    if not all_images:
        # No images, just return original rows
        product_rows.extend(rows)
    else:
        # Process rows: main product rows + variant rows + additional image rows
        main_rows = []

        # Identify main product row (first row with Title or SKU data)
        for i, row in enumerate(rows):
            if (row.get('Title') and str(row['Title']).strip()) or \
               (row.get('Variant SKU') and str(row['Variant SKU']).strip()):

                # This is a product/variant row, assign first available image
                if all_images:
                    img = all_images[0]
                    row['Image Src'] = img['src']
                    row['Image Position'] = img['position']
                    row['Image Alt Text'] = img['alt']

                    # If this is a variant row, check if it should have a variant image
                    if row.get('Variant SKU') and str(row['Variant SKU']).strip():
                        # Keep existing Variant Image if present
                        if not row.get('Variant Image') or not str(row['Variant Image']).strip():
                            row['Variant Image'] = img['src']

                main_rows.append(row)

        # If no main rows found, treat first row as main
        if not main_rows and rows:
            row = rows[0]
            if all_images:
                img = all_images[0]
                row['Image Src'] = img['src']
                row['Image Position'] = img['position']
                row['Image Alt Text'] = img['alt']
            main_rows.append(row)

        product_rows.extend(main_rows)

        # Add additional image rows (starting from second image)
        for img in all_images[len(main_rows):]:
            image_row = create_image_only_row(handle, img)
            product_rows.append(image_row)
            stats['additional_rows_created'] += 1

        stats['images_restructured'] += len(all_images)

    return product_rows


def create_image_only_row(handle: str, image_info: Dict) -> Dict:
    """
    Create an image-only row for additional images

    Args:
        handle: Product handle
        image_info: Image information dictionary

    Returns:
        Row dictionary with only image data
    """
    return {
        'Handle': handle,
        'Title': '',
        'Body (HTML)': '',
        'Vendor': '',
        'Product Category': '',
        'Type': '',
        'Tags': '',
        'Published': '',
        'Option1 Name': '',
        'Option1 Value': '',
        'Option1 Linked To': '',
        'Option2 Name': '',
        'Option2 Value': '',
        'Option2 Linked To': '',
        'Option3 Name': '',
        'Option3 Value': '',
        'Option3 Linked To': '',
        'Variant SKU': '',
        'Variant Grams': '',
        'Variant Inventory Tracker': '',
        'Variant Inventory Qty': '',
        'Variant Inventory Policy': '',
        'Variant Fulfillment Service': '',
        'Variant Price': '',
        'Variant Compare At Price': '',
        'Variant Requires Shipping': '',
        'Variant Taxable': '',
        'Variant Barcode': '',
        'Image Src': image_info['src'],
        'Image Position': image_info['position'],
        'Image Alt Text': image_info['alt'],
        'Gift Card': '',
        'SEO Title': '',
        'SEO Description': '',
        'Variant Image': '',
        'Variant Weight Unit': '',
        'Variant Tax Code': '',
        'Cost per item': '',
        'Status': ''
    }


def reorder_columns_for_shopify(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reorder columns to match expected Shopify CSV format

    Args:
        df: Dataframe to reorder

    Returns:
        Dataframe with properly ordered columns
    """
    # Define the expected column order based on Shopify standard + metafields
    standard_columns = [
        "Handle", "Title", "Body (HTML)", "Vendor", "Product Category", "Type", "Tags", "Published",
        "Option1 Name", "Option1 Value", "Option1 Linked To", "Option2 Name", "Option2 Value",
        "Option2 Linked To", "Option3 Name", "Option3 Value", "Option3 Linked To", "Variant SKU",
        "Variant Grams", "Variant Inventory Tracker", "Variant Inventory Qty", "Variant Inventory Policy",
        "Variant Fulfillment Service", "Variant Price", "Variant Compare At Price", "Variant Requires Shipping",
        "Variant Taxable", "Variant Barcode", "Image Src", "Image Position", "Image Alt Text", "Gift Card",
        "SEO Title", "SEO Description"
    ]

    # Get all metafield columns (containing 'metafields')
    metafield_columns = [col for col in df.columns if 'metafields' in col]
    metafield_columns.sort()  # Sort metafield columns alphabetically

    # Get remaining columns
    other_columns = [col for col in df.columns
                    if col not in standard_columns and col not in metafield_columns]
    other_columns.sort()

    # Final column order
    final_columns = []
    for col in standard_columns:
        if col in df.columns:
            final_columns.append(col)

    final_columns.extend(metafield_columns)
    final_columns.extend(other_columns)

    # Reorder dataframe
    return df[final_columns]


def create_image_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Create a summary of image restructuring results

    Args:
        df: Restructured dataframe

    Returns:
        Summary dictionary
    """
    summary = {
        'total_products': df['Handle'].nunique(),
        'total_rows': len(df),
        'rows_with_images': (df['Image Src'].notna() & (df['Image Src'] != '')).sum(),
        'rows_with_variant_images': (df['Variant Image'].notna() & (df['Variant Image'] != '')).sum(),
        'unique_image_positions': df[df['Image Position'].notna()]['Image Position'].nunique(),
        'max_image_position': df[df['Image Position'].notna()]['Image Position'].max() if any(df['Image Position'].notna()) else 0
    }

    return summary


def integrate_cleaned_image_urls(df: pd.DataFrame, config: Any, stats: Dict[str, Any]) -> pd.DataFrame:
    """
    Integrate cleaned image URLs from step 3 into the main dataframe

    Args:
        df: Dataframe to integrate images into
        config: Pipeline configuration
        stats: Statistics tracking dictionary

    Returns:
        Dataframe with integrated image URLs
    """
    logger.info("Loading cleaned image URLs from step 3...")

    # Load cleaned image URLs from step 3 output
    output_dir = Path("step_output")
    image_urls_file = output_dir / "step_03_cleaned_image_urls.json"

    if not image_urls_file.exists():
        logger.warning(f"Cleaned image URLs file not found: {image_urls_file}")
        return df

    try:
        with open(image_urls_file, 'r', encoding='utf-8') as f:
            image_data = json.load(f)

        # Extract image URLs - they are stored as a list
        image_urls = image_data.get('image_urls', [])
        logger.info(f"Loaded {len(image_urls)} cleaned image URLs from step 3")

        if not image_urls:
            logger.warning("No image URLs found in step 3 cleaned data")
            return df

        # Create a mapping of handles to their first available image URL
        # Group by handle and assign first available image to each handle
        handle_to_image = {}
        handles = df['Handle'].unique()

        # Distribute image URLs to handles
        # For now, assign images sequentially to handles that don't have Image Src
        available_images = image_urls.copy()

        for handle in handles:
            if pd.isna(handle) or handle == '':
                continue

            # Get rows for this handle
            handle_mask = df['Handle'] == handle
            handle_rows = df.loc[handle_mask]

            # Check if this handle already has any images
            has_image = False
            for idx, row in handle_rows.iterrows():
                # Check main Image Src and any additional Image Src columns
                if row.get('Image Src') and str(row['Image Src']).strip():
                    has_image = True
                    break
                # Also check Image Src 2-20 columns
                for i in range(2, 21):
                    img_col = f'Image Src {i}'
                    if row.get(img_col) and str(row[img_col]).strip():
                        has_image = True
                        break
                if has_image:
                    break

            # If no image, assign one from available images
            if not has_image and available_images:
                image_url = available_images.pop(0)
                handle_to_image[handle] = image_url

        # Apply image URLs to dataframe
        integrated_count = 0
        for handle, image_url in handle_to_image.items():
            handle_mask = df['Handle'] == handle
            handle_rows_indices = df.index[handle_mask].tolist()

            if handle_rows_indices:
                # Assign image to the first row of this handle (main product row)
                first_row_idx = handle_rows_indices[0]
                df.loc[first_row_idx, 'Image Src'] = image_url
                df.loc[first_row_idx, 'Image Position'] = 1
                df.loc[first_row_idx, 'Image Alt Text'] = ''  # Could be improved with actual alt text
                integrated_count += 1

                # Also set as variant image if this row has a variant SKU
                if df.loc[first_row_idx, 'Variant SKU'] and str(df.loc[first_row_idx, 'Variant SKU']).strip():
                    if not df.loc[first_row_idx, 'Variant Image'] or not str(df.loc[first_row_idx, 'Variant Image']).strip():
                        df.loc[first_row_idx, 'Variant Image'] = image_url

        stats['cleaned_urls_integrated'] = integrated_count
        logger.info(f"Integrated {integrated_count} cleaned image URLs into products")

        return df

    except Exception as e:
        logger.error(f"Failed to load cleaned image URLs: {e}")
        return df