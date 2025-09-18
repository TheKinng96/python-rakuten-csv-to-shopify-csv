"""
Step 08: Variant Grouping and Product Consolidation

Groups product variants by handle and consolidates product information.
Ensures proper Shopify product structure with main product and variant data.
"""

import logging
import pandas as pd
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Group variants and consolidate product information

    Args:
        data: Pipeline context containing type_assigned_df and config

    Returns:
        Dict containing dataframe with grouped variants
    """
    logger.info("Grouping variants and consolidating products...")

    df = data['type_assigned_df'].copy()
    config = data['config']

    # Track grouping statistics
    grouping_stats = {
        'total_input_rows': len(df),
        'unique_handles': 0,
        'single_variant_products': 0,
        'multi_variant_products': 0,
        'variants_grouped': 0,
        'names_merged': 0
    }

    # Sort by handle and variant position for proper grouping
    df = df.sort_values(['Handle', 'Variant Position']).reset_index(drop=True)

    # Group by handle and process
    grouped_data = []
    handle_groups = df.groupby('Handle')

    for handle, group_df in handle_groups:
        grouped_product = process_variant_group(group_df, config, grouping_stats)
        grouped_data.extend(grouped_product)

    # Create final dataframe
    final_df = pd.DataFrame(grouped_data)

    # Calculate final statistics
    grouping_stats['unique_handles'] = len(final_df['Handle'].unique())
    grouping_stats['total_output_rows'] = len(final_df)

    # Log grouping results
    logger.info(f"Variant grouping completed")
    for key, value in grouping_stats.items():
        logger.info(f"Grouping stats - {key}: {value}")

    return {
        'variant_grouped_df': final_df,
        'grouping_stats': grouping_stats
    }


def process_variant_group(group_df: pd.DataFrame, config, stats: Dict[str, Any]) -> List[Dict]:
    """
    Process a group of variants for a single handle

    Args:
        group_df: Dataframe containing all variants for one handle
        config: Pipeline configuration
        stats: Statistics tracking dictionary

    Returns:
        List of dictionaries representing processed variants
    """
    group_data = []
    group_size = len(group_df)

    if group_size == 1:
        stats['single_variant_products'] += 1
    else:
        stats['multi_variant_products'] += 1
        stats['variants_grouped'] += group_size

    # Get the main product (first row after sorting)
    main_product = group_df.iloc[0].copy()

    # Consolidate product-level information from all variants
    consolidated_info = consolidate_product_info(group_df, config, stats)

    # Apply consolidated info to all variants
    for idx, variant_row in group_df.iterrows():
        variant_data = variant_row.to_dict()

        # Apply consolidated product information
        for key, value in consolidated_info.items():
            variant_data[key] = value

        # Set product/variant specific fields
        if idx == group_df.index[0]:  # First variant (main product)
            variant_data['Published'] = 'TRUE'
        else:  # Additional variants
            # Clear product-level fields for non-main variants
            product_level_fields = [
                'Title', 'Body (HTML)', 'Vendor', 'Product Category', 'Type', 'Tags',
                'SEO Title', 'SEO Description', 'Gift Card'
            ]
            for field in product_level_fields:
                variant_data[field] = ''

            variant_data['Published'] = ''

        group_data.append(variant_data)

    return group_data


def consolidate_product_info(group_df: pd.DataFrame, config, stats: Dict[str, Any]) -> Dict[str, Any]:
    """
    Consolidate product information across variants

    Args:
        group_df: Dataframe containing variants for one product
        config: Pipeline configuration
        stats: Statistics tracking dictionary

    Returns:
        Dict with consolidated product information
    """
    consolidated = {}

    # Product name consolidation
    names = group_df['商品名'].dropna().unique()
    if len(names) > 1:
        # Merge similar names by finding common parts
        merged_name = merge_product_names(names)
        stats['names_merged'] += 1
    else:
        merged_name = names[0] if len(names) > 0 else ''

    consolidated['Title'] = merged_name

    # Use main product's information for most fields
    main_product = group_df.iloc[0]

    consolidated.update({
        'Body (HTML)': main_product.get('Body (HTML)', ''),
        'Vendor': main_product.get('Vendor', ''),
        'Product Category': main_product.get('Product Category', ''),
        'Type': main_product.get('Type', ''),
        'SEO Title': main_product.get('SEO Title', ''),
        'SEO Description': main_product.get('SEO Description', ''),
        'Gift Card': 'FALSE'
    })

    # Consolidate tags from all variants
    all_tags = []
    for _, variant in group_df.iterrows():
        tags = variant.get('Tags', '')
        if tags:
            variant_tags = [tag.strip() for tag in str(tags).split(',')]
            all_tags.extend(variant_tags)

    # Remove duplicates and empty tags
    unique_tags = list(dict.fromkeys([tag for tag in all_tags if tag.strip()]))
    consolidated['Tags'] = ', '.join(unique_tags) if unique_tags else ''

    # Consolidate metafields (use first non-empty value)
    metafield_columns = config.custom_metafields + config.shopify_metafields
    for metafield in metafield_columns:
        if metafield in group_df.columns:
            non_empty_values = group_df[metafield].dropna()
            non_empty_values = non_empty_values[non_empty_values != '']
            if len(non_empty_values) > 0:
                consolidated[metafield] = non_empty_values.iloc[0]
            else:
                consolidated[metafield] = ''

    return consolidated


def merge_product_names(names: List[str]) -> str:
    """
    Merge multiple product names by finding common parts

    Args:
        names: List of product names to merge

    Returns:
        Merged product name
    """
    if len(names) <= 1:
        return names[0] if names else ''

    # Find the longest common subsequence approach
    # For simplicity, use the first name and append unique parts from others
    base_name = names[0]

    # Extract unique parts from other names
    unique_parts = []
    for name in names[1:]:
        # Find parts in this name that aren't in the base name
        name_words = set(name.split())
        base_words = set(base_name.split())
        unique_words = name_words - base_words

        if unique_words:
            unique_parts.extend(unique_words)

    # Combine base name with unique parts
    if unique_parts:
        # Remove duplicates while preserving order
        seen = set()
        filtered_parts = []
        for part in unique_parts:
            if part not in seen:
                seen.add(part)
                filtered_parts.append(part)

        merged_name = f"{base_name} ({', '.join(filtered_parts)})"
    else:
        merged_name = base_name

    return merged_name


def validate_variant_structure(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate the variant structure after grouping

    Args:
        df: Dataframe with grouped variants

    Returns:
        Dict with validation results
    """
    validation = {
        'total_products': len(df['Handle'].unique()),
        'total_variants': len(df),
        'products_with_multiple_variants': 0,
        'variants_missing_options': 0,
        'published_variants': 0,
        'issues': []
    }

    # Check variant structure
    handle_groups = df.groupby('Handle')

    for handle, group in handle_groups:
        group_size = len(group)

        if group_size > 1:
            validation['products_with_multiple_variants'] += 1

            # Check if all variants have proper option values
            for _, variant in group.iterrows():
                option1_value = variant.get('Option1 Value', '')
                if not option1_value or option1_value == 'Default Title':
                    if group_size > 1:  # Only flag if there are multiple variants
                        validation['variants_missing_options'] += 1
                        validation['issues'].append({
                            'handle': handle,
                            'sku': variant.get('Variant SKU', ''),
                            'issue': 'Missing option value for multi-variant product'
                        })

        # Check published status
        published_count = (group['Published'] == 'TRUE').sum()
        validation['published_variants'] += published_count

        if published_count != 1:
            validation['issues'].append({
                'handle': handle,
                'issue': f'Should have exactly 1 published variant, found {published_count}'
            })

    return validation


def create_variant_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a summary of variant distribution

    Args:
        df: Dataframe with grouped variants

    Returns:
        Summary dataframe
    """
    summary_data = []

    handle_groups = df.groupby('Handle')
    for handle, group in handle_groups:
        variant_count = len(group)
        variant_types = group['Variant Type'].unique().tolist()
        option_values = group['Option1 Value'].unique().tolist()

        summary_data.append({
            'handle': handle,
            'variant_count': variant_count,
            'variant_types': ', '.join(variant_types),
            'option_values': ', '.join([str(v) for v in option_values if v]),
            'main_sku': group.iloc[0]['Variant SKU'],
            'title': group.iloc[0]['Title']
        })

    return pd.DataFrame(summary_data)