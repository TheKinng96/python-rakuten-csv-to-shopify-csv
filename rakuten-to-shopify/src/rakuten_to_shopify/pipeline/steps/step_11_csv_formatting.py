"""
Step 11: CSV Formatting and Field Standardization

Formats all fields according to Shopify CSV requirements and applies
proper data types, default values, and field-specific formatting rules.
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, Any

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format CSV fields according to Shopify requirements

    Args:
        data: Pipeline context containing description_finalized_df and config

    Returns:
        Dict containing dataframe with formatted fields
    """
    logger.info("Formatting CSV fields for Shopify compliance...")

    df = data['description_finalized_df'].copy()
    config = data['config']

    # Track formatting statistics
    formatting_stats = {
        'total_rows': len(df),
        'fields_formatted': 0,
        'price_fields_formatted': 0,
        'boolean_fields_set': 0,
        'inventory_fields_set': 0,
        'required_fields_validated': 0
    }

    # Apply standard Shopify field formatting
    format_standard_fields(df, config, formatting_stats)

    # Format variant-specific fields
    format_variant_fields(df, config, formatting_stats)

    # Format price and inventory fields
    format_price_and_inventory_fields(df, config, formatting_stats)

    # Format boolean and required fields
    format_boolean_and_required_fields(df, config, formatting_stats)

    # Apply special CSV formatting
    apply_csv_special_formatting(df, config, formatting_stats)

    # Log formatting results
    logger.info(f"CSV formatting completed")
    for key, value in formatting_stats.items():
        logger.info(f"Formatting stats - {key}: {value}")

    return {
        'csv_formatted_df': df,
        'formatting_stats': formatting_stats
    }


def format_standard_fields(df: pd.DataFrame, config, stats: Dict[str, Any]):
    """
    Format standard Shopify CSV fields

    Args:
        df: Dataframe to format
        config: Pipeline configuration
        stats: Statistics tracking dictionary
    """
    # Vendor - set default if empty
    if 'Vendor' not in df.columns:
        df['Vendor'] = ''
    df['Vendor'] = df['Vendor'].fillna('').astype(str)

    # Product Category - ensure not empty for main products
    if 'Product Category' not in df.columns:
        df['Product Category'] = ''

    # Option fields formatting
    format_option_fields(df, stats)

    # Image fields formatting
    format_image_fields(df, stats)

    stats['fields_formatted'] += 6  # Count of field groups formatted


def format_option_fields(df: pd.DataFrame, stats: Dict[str, Any]):
    """
    Format option fields (Option1, Option2, Option3)

    Args:
        df: Dataframe to format
        stats: Statistics tracking dictionary
    """
    # Initialize option fields if not present
    option_fields = [
        'Option1 Name', 'Option1 Value', 'Option1 Linked To',
        'Option2 Name', 'Option2 Value', 'Option2 Linked To',
        'Option3 Name', 'Option3 Value', 'Option3 Linked To'
    ]

    for field in option_fields:
        if field not in df.columns:
            df[field] = ''
        df[field] = df[field].fillna('').astype(str)

    # Ensure single variant products have "Default Title"
    single_variant_mask = df.groupby('Handle')['Handle'].transform('count') == 1
    df.loc[single_variant_mask, 'Option1 Value'] = 'Default Title'
    df.loc[single_variant_mask, 'Option1 Name'] = ''


def format_image_fields(df: pd.DataFrame, stats: Dict[str, Any]):
    """
    Format image-related fields

    Args:
        df: Dataframe to format
        stats: Statistics tracking dictionary
    """
    # Ensure all image fields exist and are properly formatted
    image_fields = ['Image Src', 'Image Position', 'Image Alt Text', 'Variant Image']

    for field in image_fields:
        if field not in df.columns:
            df[field] = ''
        df[field] = df[field].fillna('').astype(str)

    # Set variant images to main image for all variants
    main_products = df[df['Published'] == 'TRUE']
    for _, main_product in main_products.iterrows():
        handle = main_product['Handle']
        main_image = main_product.get('Image Src', '')

        if main_image:
            df.loc[df['Handle'] == handle, 'Variant Image'] = main_image


def format_variant_fields(df: pd.DataFrame, config, stats: Dict[str, Any]):
    """
    Format variant-specific fields

    Args:
        df: Dataframe to format
        config: Pipeline configuration
        stats: Statistics tracking dictionary
    """
    # Variant SKU
    if 'Variant SKU' not in df.columns:
        df['Variant SKU'] = df['管理番号']
    df['Variant SKU'] = df['Variant SKU'].fillna('').astype(str)

    # Variant Weight Unit
    if 'Variant Weight Unit' not in df.columns:
        df['Variant Weight Unit'] = 'kg'

    # Variant Tax Code
    if 'Variant Tax Code' not in df.columns:
        df['Variant Tax Code'] = ''

    stats['fields_formatted'] += 3


def format_price_and_inventory_fields(df: pd.DataFrame, config, stats: Dict[str, Any]):
    """
    Format price and inventory fields

    Args:
        df: Dataframe to format
        config: Pipeline configuration
        stats: Statistics tracking dictionary
    """
    # Variant Price - convert from 販売価格
    if 'Variant Price' not in df.columns:
        df['Variant Price'] = ''

    # Format prices from Rakuten data
    if '販売価格' in df.columns:
        df['Variant Price'] = df['販売価格'].apply(format_price_value)
        stats['price_fields_formatted'] += 1

    # Variant Compare At Price
    if 'Variant Compare At Price' not in df.columns:
        df['Variant Compare At Price'] = ''

    # Cost per item
    if 'Cost per item' not in df.columns:
        df['Cost per item'] = ''

    # Variant Grams - set default weight
    if 'Variant Grams' not in df.columns:
        df['Variant Grams'] = '100'  # Default 100g

    # Inventory fields
    inventory_fields = {
        'Variant Inventory Tracker': 'shopify',
        'Variant Inventory Qty': '0',
        'Variant Inventory Policy': 'deny',
        'Variant Fulfillment Service': 'manual'
    }

    for field, default_value in inventory_fields.items():
        if field not in df.columns:
            df[field] = default_value
        df[field] = df[field].fillna(default_value).astype(str)

    stats['inventory_fields_set'] += len(inventory_fields)


def format_boolean_and_required_fields(df: pd.DataFrame, config, stats: Dict[str, Any]):
    """
    Format boolean and required fields

    Args:
        df: Dataframe to format
        config: Pipeline configuration
        stats: Statistics tracking dictionary
    """
    # Boolean fields with proper TRUE/FALSE values
    boolean_fields = {
        'Published': lambda row: 'TRUE' if row.get('Published') == 'TRUE' else 'FALSE',
        'Variant Requires Shipping': 'TRUE',
        'Variant Taxable': 'TRUE',
        'Gift Card': 'FALSE'
    }

    for field, value_or_func in boolean_fields.items():
        if field not in df.columns:
            df[field] = ''

        if callable(value_or_func):
            df[field] = df.apply(value_or_func, axis=1)
        else:
            df[field] = df[field].fillna(value_or_func).apply(
                lambda x: value_or_func if not x or str(x).strip() == '' else str(x)
            )

    stats['boolean_fields_set'] += len(boolean_fields)

    # Status field
    if 'Status' not in df.columns:
        df['Status'] = 'active'

    stats['required_fields_validated'] += 1


def apply_csv_special_formatting(df: pd.DataFrame, config, stats: Dict[str, Any]):
    """
    Apply special CSV formatting rules

    Args:
        df: Dataframe to format
        config: Pipeline configuration
        stats: Statistics tracking dictionary
    """
    # Apply special quoted empty fields formatting
    for column in df.columns:
        if column in config.special_quoted_empty_fields:
            df[column] = df[column].apply(
                lambda x: '""' if pd.isna(x) or str(x).strip() == '' else str(x)
            )

    # Ensure all metafield columns exist with empty values
    all_metafields = config.custom_metafields + config.shopify_metafields
    for metafield in all_metafields:
        if metafield not in df.columns:
            df[metafield] = ''
        df[metafield] = df[metafield].fillna('').astype(str)

    stats['fields_formatted'] += len(all_metafields)


def format_price_value(price_str) -> str:
    """
    Format price value for Shopify

    Args:
        price_str: Raw price string from Rakuten

    Returns:
        Formatted price string
    """
    if pd.isna(price_str) or not price_str:
        return '0'

    try:
        # Clean price string
        clean_price = str(price_str).replace(',', '').replace('¥', '').replace('円', '').strip()

        # Convert to float and back to string to normalize
        price_float = float(clean_price)
        return str(int(price_float)) if price_float.is_integer() else str(price_float)

    except (ValueError, TypeError):
        logger.warning(f"Invalid price value: {price_str}")
        return '0'


def validate_csv_format(df: pd.DataFrame, config) -> Dict[str, Any]:
    """
    Validate CSV format compliance

    Args:
        df: Formatted dataframe
        config: Pipeline configuration

    Returns:
        Validation results
    """
    validation = {
        'total_rows': len(df),
        'required_columns_present': True,
        'missing_required_columns': [],
        'data_type_issues': [],
        'value_issues': []
    }

    # Check required columns
    required_columns = config.complete_header
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        validation['required_columns_present'] = False
        validation['missing_required_columns'] = missing_columns

    # Check for data type issues
    for column in df.columns:
        if column in df.columns:
            # Check for null values in required fields
            if column in ['Handle', 'Title', 'Variant SKU']:
                null_count = df[column].isna().sum()
                if null_count > 0:
                    validation['value_issues'].append({
                        'column': column,
                        'issue': f'{null_count} null values in required field'
                    })

    # Check price format
    if 'Variant Price' in df.columns:
        invalid_prices = df[~df['Variant Price'].str.match(r'^\d+(\.\d+)?$', na=False)]
        if len(invalid_prices) > 0:
            validation['value_issues'].append({
                'column': 'Variant Price',
                'issue': f'{len(invalid_prices)} invalid price formats'
            })

    return validation


def reorder_columns(df: pd.DataFrame, config) -> pd.DataFrame:
    """
    Reorder columns to match Shopify standard header

    Args:
        df: Dataframe to reorder
        config: Pipeline configuration

    Returns:
        Dataframe with reordered columns
    """
    # Get available columns in the correct order
    ordered_columns = []
    for col in config.complete_header:
        if col in df.columns:
            ordered_columns.append(col)

    # Add any remaining columns that aren't in the standard header
    remaining_columns = [col for col in df.columns if col not in ordered_columns]
    ordered_columns.extend(remaining_columns)

    return df[ordered_columns]