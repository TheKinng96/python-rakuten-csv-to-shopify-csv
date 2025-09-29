"""
Step 07: Tax Rate Mapping

Maps 消費税率 (consumption tax rate) field to Shopify metafield format.
Converts decimal values (0.1, 0.08) to percentage strings (10%, 8%).
"""

import logging
import pandas as pd
from typing import Dict, Any

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map tax rates from 消費税率 field to Shopify metafield format

    Args:
        data: Pipeline context containing image_restructured_df and config

    Returns:
        Dict containing dataframe with mapped tax rates
    """
    logger.info("Mapping tax rates to Shopify metafield format...")

    df = data['image_restructured_df'].copy()
    config = data['config']

    # Track tax mapping statistics
    tax_stats = {
        'products_processed': 0,
        'tax_rates_mapped': 0,
        'rate_0_1_count': 0,  # 10%
        'rate_0_08_count': 0,  # 8%
        'other_rates_count': 0,
        'missing_rates_count': 0
    }

    # Apply tax rate mapping
    df_with_tax = map_tax_rates(df, tax_stats)

    # Remove original tax columns after mapping
    df_with_tax = remove_original_tax_columns(df_with_tax, tax_stats)

    # Log tax mapping results
    logger.info(f"Tax rate mapping completed")
    for key, value in tax_stats.items():
        logger.info(f"Tax stats - {key}: {value}")

    return {
        'tax_mapped_df': df_with_tax,
        'tax_stats': tax_stats
    }


def map_tax_rates(df: pd.DataFrame, stats: Dict[str, Any]) -> pd.DataFrame:
    """
    Map tax rates from 消費税率 field to 消費税率 (product.metafields.custom.tax) metafield

    Args:
        df: Dataframe to process
        stats: Statistics tracking dictionary

    Returns:
        Dataframe with mapped tax rates
    """
    logger.info("Processing tax rate mapping...")

    # Check if 消費税率 column exists
    if '消費税率' not in df.columns:
        logger.warning("消費税率 column not found in dataframe, skipping tax mapping")
        return df

    # Create the metafield column
    metafield_column = '消費税率 (product.metafields.custom.tax)'
    df[metafield_column] = ''

    # Track unique handles to count products processed
    processed_handles = set()

    # Process each row
    for idx, row in df.iterrows():
        handle = row.get('Handle')
        if handle and handle not in processed_handles:
            stats['products_processed'] += 1
            processed_handles.add(handle)

        # Get tax rate value
        tax_rate = row.get('消費税率')

        if pd.isna(tax_rate) or tax_rate == '' or tax_rate == 0:
            stats['missing_rates_count'] += 1
            continue

        try:
            # Convert to float for processing
            rate_value = float(tax_rate)

            # Map rate values to percentage strings
            if rate_value == 0.1:
                df.at[idx, metafield_column] = '10%'
                stats['rate_0_1_count'] += 1
                stats['tax_rates_mapped'] += 1
            elif rate_value == 0.08:
                df.at[idx, metafield_column] = '8%'
                stats['rate_0_08_count'] += 1
                stats['tax_rates_mapped'] += 1
            else:
                # Handle other rates by converting decimal to percentage
                percentage = int(rate_value * 100)
                df.at[idx, metafield_column] = f'{percentage}%'
                stats['other_rates_count'] += 1
                stats['tax_rates_mapped'] += 1
                logger.info(f"Mapped unusual tax rate {rate_value} to {percentage}% for handle {handle}")

        except (ValueError, TypeError) as e:
            logger.warning(f"Could not process tax rate '{tax_rate}' for handle {handle}: {e}")
            stats['missing_rates_count'] += 1
            continue

    return df


def remove_original_tax_columns(df: pd.DataFrame, stats: Dict[str, Any]) -> pd.DataFrame:
    """
    Remove original tax columns (消費税, 消費税率) after mapping to metafield format

    Args:
        df: Dataframe to clean up
        stats: Statistics tracking dictionary

    Returns:
        Dataframe with original tax columns removed
    """
    logger.info("Removing original tax columns (消費税, 消費税率)...")

    columns_to_remove = []
    original_tax_columns = ['消費税', '消費税率']

    for col in original_tax_columns:
        if col in df.columns:
            columns_to_remove.append(col)

    if columns_to_remove:
        logger.info(f"Removing {len(columns_to_remove)} original tax columns: {columns_to_remove}")
        df = df.drop(columns=columns_to_remove)
        stats['original_tax_columns_removed'] = len(columns_to_remove)
    else:
        logger.info("No original tax columns found to remove")
        stats['original_tax_columns_removed'] = 0

    return df


def create_tax_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Create a summary of tax mapping results

    Args:
        df: Processed dataframe

    Returns:
        Summary dictionary
    """
    metafield_column = '消費税率 (product.metafields.custom.tax)'

    if metafield_column not in df.columns:
        return {'error': 'Tax metafield column not found'}

    # Count tax rate distributions
    tax_counts = df[df[metafield_column] != ''][metafield_column].value_counts()

    summary = {
        'total_products': df['Handle'].nunique(),
        'total_rows': len(df),
        'rows_with_tax_rates': (df[metafield_column] != '').sum(),
        'tax_rate_distribution': tax_counts.to_dict(),
        'unique_tax_rates': len(tax_counts)
    }

    return summary