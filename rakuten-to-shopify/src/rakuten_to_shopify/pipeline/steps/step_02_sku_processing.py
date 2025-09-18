"""
Step 02: SKU Processing and Handle Generation

Processes Rakuten SKUs to generate Shopify handles and variant information.
Handles variant suffixes (-3s, -6s, -t) and creates proper grouping.
"""

import logging
import pandas as pd
import re
from typing import Dict, Any

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process SKUs and generate Shopify handles

    Args:
        data: Pipeline context containing cleaned_df and config

    Returns:
        Dict containing dataframe with handle and variant information
    """
    logger.info("Processing SKUs and generating handles...")

    df = data['cleaned_df'].copy()
    config = data['config']

    # Process SKUs and generate handles
    df['Handle'] = df['管理番号'].apply(config.derive_handle)
    df['Variant SKU'] = df['管理番号']

    # Extract set count for variants
    df['Set Count'] = df['管理番号'].apply(config.get_set_count)

    # Identify variant types based on SKU suffix
    def identify_variant_type(sku: str) -> str:
        """Identify the type of variant based on SKU suffix"""
        if not isinstance(sku, str):
            return 'main'

        if sku.endswith('-ss'):
            return 'ss_variant'
        elif re.search(r'-\d+s$', sku):
            return 'set_variant'
        elif sku.endswith('-t'):
            return 'trial_variant'
        else:
            return 'main'

    df['Variant Type'] = df['管理番号'].apply(identify_variant_type)

    # Create variant position for sorting
    def get_variant_position(sku: str, variant_type: str) -> int:
        """Get sorting position for variants within a product group"""
        if variant_type == 'main':
            return 0
        elif variant_type == 'trial_variant':
            return 1
        elif variant_type == 'set_variant':
            # Extract set count for ordering
            match = re.search(r'-(\d+)s$', sku)
            return int(match.group(1)) if match else 999
        elif variant_type == 'ss_variant':
            return 1000  # Put SS variants last
        else:
            return 999

    df['Variant Position'] = df.apply(
        lambda row: get_variant_position(row['管理番号'], row['Variant Type']),
        axis=1
    )

    # Sort by handle and variant position to group products properly
    df = df.sort_values(['Handle', 'Variant Position']).reset_index(drop=True)

    # Generate option1 values based on variant type and set count
    def generate_option1_value(variant_type: str, set_count: str, sku: str) -> str:
        """Generate Option1 Value for Shopify variants"""
        if variant_type == 'main':
            return 'Default Title'
        elif variant_type == 'trial_variant':
            return 'トライアル'
        elif variant_type == 'set_variant':
            return f'{set_count}個セット'
        elif variant_type == 'ss_variant':
            return 'スーパーセール'
        else:
            return 'Default Title'

    df['Option1 Value'] = df.apply(
        lambda row: generate_option1_value(
            row['Variant Type'],
            row['Set Count'],
            row['管理番号']
        ),
        axis=1
    )

    # Set Option1 Name for products with variants
    df['Option1 Name'] = ''

    # Group by handle to identify which products have multiple variants
    handle_counts = df.groupby('Handle').size()
    multi_variant_handles = handle_counts[handle_counts > 1].index

    # Set Option1 Name for multi-variant products
    df.loc[df['Handle'].isin(multi_variant_handles), 'Option1 Name'] = 'バリエーション'

    # Statistics
    total_products = len(df['Handle'].unique())
    total_variants = len(df)
    main_products = len(df[df['Variant Type'] == 'main'])
    set_variants = len(df[df['Variant Type'] == 'set_variant'])
    trial_variants = len(df[df['Variant Type'] == 'trial_variant'])
    ss_variants = len(df[df['Variant Type'] == 'ss_variant'])

    sku_stats = {
        'total_products': total_products,
        'total_variants': total_variants,
        'main_products': main_products,
        'set_variants': set_variants,
        'trial_variants': trial_variants,
        'ss_variants': ss_variants,
        'multi_variant_products': len(multi_variant_handles)
    }

    # Log SKU processing results
    logger.info(f"SKU processing completed: {total_variants} variants across {total_products} products")
    for key, value in sku_stats.items():
        logger.info(f"SKU stats - {key}: {value}")

    return {
        'sku_processed_df': df,
        'sku_stats': sku_stats
    }