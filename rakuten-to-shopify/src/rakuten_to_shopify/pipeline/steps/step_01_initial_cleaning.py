"""
Step 01: Initial Data Cleaning

Performs initial data cleaning including null handling, whitespace trimming,
and basic data type conversions. Prepares data for downstream processing.
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, Any

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Clean and prepare the raw dataframe for processing

    Args:
        data: Pipeline context containing raw_df

    Returns:
        Dict containing cleaned dataframe
    """
    logger.info("Performing initial data cleaning...")

    df = data['raw_df'].copy()
    config = data['config']

    # Track cleaning statistics
    initial_rows = len(df)
    cleaning_stats = {}

    # 1. Handle whitespace in all string columns
    string_columns = df.select_dtypes(include=['object']).columns
    for col in string_columns:
        df[col] = df[col].astype(str).str.strip()

    # 2. Replace various null representations with actual NaN
    null_values = ['', 'NULL', 'null', 'NaN', 'nan', 'None', 'none', '　']  # Including full-width space
    df = df.replace(null_values, np.nan)

    # 3. Clean SKU (SKU管理番号) and product name (商品名) - require at least one
    df['SKU管理番号'] = df['SKU管理番号'].str.strip()
    df['商品名'] = df['商品名'].str.strip()

    # Count individual null values for reporting
    sku_nulls = df['SKU管理番号'].isna().sum()
    name_nulls = df['商品名'].isna().sum()

    # Create masks for filtering logic
    sku_valid = df['SKU管理番号'].notna()
    name_valid = df['商品名'].notna()
    either_valid = sku_valid | name_valid
    both_null = (~sku_valid) & (~name_valid)

    # Log detailed statistics
    logger.info(f"SKU analysis: {sku_nulls} null, {sku_valid.sum()} valid")
    logger.info(f"Name analysis: {name_nulls} null, {name_valid.sum()} valid")
    logger.info(f"Rows with either SKU OR Name valid: {either_valid.sum()}")
    logger.info(f"Rows with both SKU AND Name null: {both_null.sum()}")

    # Filter out rows where BOTH SKU and product name are null
    rows_to_drop = both_null.sum()
    if rows_to_drop > 0:
        logger.warning(f"Dropping {rows_to_drop} rows with both SKU and product name null")
        df = df[either_valid]

    cleaning_stats.update({
        'sku_nulls': sku_nulls,
        'name_nulls': name_nulls,
        'rows_dropped_both_null': rows_to_drop,
        'rows_with_either_valid': either_valid.sum()
    })

    # 4. Clean price field (通常購入販売価格)
    df['通常購入販売価格'] = df['通常購入販売価格'].astype(str).str.replace(',', '').str.replace('¥', '').str.strip()

    # Convert to numeric, keeping as string for later processing
    price_errors = 0
    for idx, price in df['通常購入販売価格'].items():
        if pd.notna(price):
            try:
                float(price)
            except (ValueError, TypeError):
                df.at[idx, '通常購入販売価格'] = np.nan
                price_errors += 1

    if price_errors > 0:
        logger.warning(f"Found {price_errors} invalid price values, set to null")

    cleaning_stats['invalid_prices_cleaned'] = price_errors

    # 5. Clean category exclusions based on config (DISABLED - no collection CSV)
    # category_col = 'カテゴリ'
    # if category_col in df.columns:
    #     excluded_count = 0
    #     for category in config.category_exclusion_list:
    #         mask = df[category_col].str.contains(category, na=False, case=False)
    #         excluded_count += mask.sum()
    #         df = df[~mask]
    #     cleaning_stats['rows_excluded_by_category'] = excluded_count
    #     if excluded_count > 0:
    #         logger.info(f"Excluded {excluded_count} rows based on category filters")

    # 6. Filter Gojuon character rows (DISABLED - no collection CSV)
    # if '商品名' in df.columns:
    #     gojuon_mask = (
    #         df['商品名'].str.match(config.gojuon_row_pattern, na=False) |
    #         df['商品名'].str.match(config.gojuon_sono_pattern, na=False)
    #     )
    #     gojuon_count = gojuon_mask.sum()
    #     if gojuon_count > 0:
    #         df = df[~gojuon_mask]
    #         logger.info(f"Filtered {gojuon_count} Gojuon character rows")
    #     cleaning_stats['gojuon_rows_filtered'] = gojuon_count

    # 7. Create clean product code if available
    if '商品コード' in df.columns:
        df['商品コード'] = df['商品コード'].str.strip()

    # 8. Convert NaN back to empty strings for CSV output
    df = df.fillna('')

    # 9. Reset index after filtering
    df = df.reset_index(drop=True)

    final_rows = len(df)
    cleaning_stats.update({
        'initial_rows': initial_rows,
        'final_rows': final_rows,
        'total_rows_removed': initial_rows - final_rows,
        'removal_percentage': round((initial_rows - final_rows) / initial_rows * 100, 2)
    })

    # Log cleaning results
    logger.info(f"Cleaning completed: {initial_rows} → {final_rows} rows")
    logger.info(f"Retention rate: {100 - cleaning_stats['removal_percentage']:.1f}%")

    # Log breakdown of remaining data
    final_sku_valid = df['SKU管理番号'].notna().sum()
    final_name_valid = df['商品名'].notna().sum()
    final_both_valid = (df['SKU管理番号'].notna() & df['商品名'].notna()).sum()

    logger.info(f"Final data composition:")
    logger.info(f"  - Rows with valid SKU: {final_sku_valid}")
    logger.info(f"  - Rows with valid Name: {final_name_valid}")
    logger.info(f"  - Rows with both valid: {final_both_valid}")

    for key, value in cleaning_stats.items():
        logger.info(f"Cleaning stats - {key}: {value}")

    return {
        'cleaned_df': df,
        'cleaning_stats': cleaning_stats
    }