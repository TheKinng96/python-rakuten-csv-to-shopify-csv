"""
Step 05: Metafield Mapping

Maps Rakuten product attributes to Shopify metafield columns using the mapping_meta.json
configuration. Handles both custom and Shopify metafields with proper value formatting.
"""

import logging
import pandas as pd
import json
from pathlib import Path
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map Rakuten attributes to Shopify metafields

    Args:
        data: Pipeline context containing image_processed_df and config

    Returns:
        Dict containing dataframe with mapped metafields
    """
    logger.info("Mapping Rakuten attributes to Shopify metafields...")

    df = data['image_processed_df'].copy()
    config = data['config']

    # Load metafield mapping configuration
    mapping_file = Path(data['output_dir'].parent) / 'data' / 'mapping_meta.json'
    metafield_mapping = load_metafield_mapping(mapping_file)

    # Track mapping statistics
    mapping_stats = {
        'total_mappings_available': len(metafield_mapping),
        'mappings_applied': 0,
        'custom_metafields_mapped': 0,
        'shopify_metafields_mapped': 0,
        'empty_values_skipped': 0
    }

    # Initialize all metafield columns
    initialize_metafield_columns(df, config)

    # Apply metafield mappings
    apply_metafield_mappings(df, metafield_mapping, config, mapping_stats)

    # Handle special tag mappings
    apply_special_tag_mappings(df, config, mapping_stats)

    # Log mapping results
    logger.info(f"Metafield mapping completed")
    for key, value in mapping_stats.items():
        logger.info(f"Mapping stats - {key}: {value}")

    return {
        'metafield_mapped_df': df,
        'mapping_stats': mapping_stats,
        'metafield_mapping': metafield_mapping
    }


def load_metafield_mapping(mapping_file: Path) -> Dict[str, str]:
    """
    Load metafield mapping configuration from JSON file

    Args:
        mapping_file: Path to mapping_meta.json

    Returns:
        Dict mapping Rakuten keys to Shopify metafield columns
    """
    try:
        if mapping_file.exists():
            with open(mapping_file, 'r', encoding='utf-8') as f:
                mapping_data = json.load(f)
                logger.info(f"Loaded {len(mapping_data)} metafield mappings from {mapping_file}")
                return mapping_data
        else:
            logger.warning(f"Mapping file not found: {mapping_file}")
            return {}
    except Exception as e:
        logger.error(f"Error loading metafield mapping: {e}")
        return {}


def initialize_metafield_columns(df: pd.DataFrame, config):
    """
    Initialize all metafield columns with empty values

    Args:
        df: Dataframe to initialize
        config: Pipeline configuration
    """
    # Initialize custom metafields
    for metafield in config.custom_metafields:
        df[metafield] = ''

    # Initialize Shopify metafields
    for metafield in config.shopify_metafields:
        df[metafield] = ''


def apply_metafield_mappings(df: pd.DataFrame, mapping: Dict[str, str], config, stats: Dict[str, Any]):
    """
    Apply metafield mappings to dataframe

    Args:
        df: Dataframe to process
        mapping: Metafield mapping dictionary
        config: Pipeline configuration
        stats: Statistics tracking dictionary
    """
    for rakuten_key, shopify_column in mapping.items():
        if rakuten_key in df.columns and shopify_column in df.columns:
            # Copy values from Rakuten column to Shopify metafield column
            non_empty_mask = df[rakuten_key].notna() & (df[rakuten_key] != '') & (df[rakuten_key] != '　')

            if non_empty_mask.any():
                df.loc[non_empty_mask, shopify_column] = df.loc[non_empty_mask, rakuten_key]
                stats['mappings_applied'] += 1

                # Track type of metafield mapped
                if shopify_column in config.custom_metafields:
                    stats['custom_metafields_mapped'] += 1
                elif shopify_column in config.shopify_metafields:
                    stats['shopify_metafields_mapped'] += 1

                mapped_count = non_empty_mask.sum()
                logger.debug(f"Mapped {mapped_count} values: {rakuten_key} → {shopify_column}")
            else:
                stats['empty_values_skipped'] += 1


def apply_special_tag_mappings(df: pd.DataFrame, config, stats: Dict[str, Any]):
    """
    Apply special tag mappings for specific Rakuten attributes

    Args:
        df: Dataframe to process
        config: Pipeline configuration
        stats: Statistics tracking dictionary
    """
    # Process special tags (parallel imports, defective items)
    for rakuten_value, shopify_tag in config.special_tags.items():
        # Find rows that contain this special value
        for col in df.columns:
            if col.startswith('[絞込み]') or col in config.free_tag_keys:
                mask = df[col].str.contains(rakuten_value, na=False, case=False)
                if mask.any():
                    # Add to Tags column
                    existing_tags = df.loc[mask, 'Tags'].fillna('')
                    new_tags = existing_tags.apply(lambda x: add_tag_if_not_exists(x, shopify_tag))
                    df.loc[mask, 'Tags'] = new_tags

                    logger.debug(f"Applied special tag mapping: {rakuten_value} → {shopify_tag}")


def add_tag_if_not_exists(existing_tags: str, new_tag: str) -> str:
    """
    Add a tag to existing tags if it doesn't already exist

    Args:
        existing_tags: Current tags string
        new_tag: Tag to add

    Returns:
        Updated tags string
    """
    if not existing_tags:
        return new_tag

    tags_list = [tag.strip() for tag in existing_tags.split(',')]
    if new_tag not in tags_list:
        tags_list.append(new_tag)

    return ', '.join(tags_list)


def validate_metafield_values(df: pd.DataFrame, config) -> Dict[str, Any]:
    """
    Validate metafield values and provide statistics

    Args:
        df: Dataframe with mapped metafields
        config: Pipeline configuration

    Returns:
        Dict with validation statistics
    """
    validation_stats = {}

    # Check custom metafields
    for metafield in config.custom_metafields:
        if metafield in df.columns:
            non_empty_count = df[metafield].notna().sum()
            validation_stats[f'custom_{metafield.split("(")[0].strip()}_count'] = non_empty_count

    # Check Shopify metafields
    for metafield in config.shopify_metafields:
        if metafield in df.columns:
            non_empty_count = df[metafield].notna().sum()
            validation_stats[f'shopify_{metafield.split("(")[0].strip()}_count'] = non_empty_count

    return validation_stats


def create_metafield_summary(df: pd.DataFrame, config) -> pd.DataFrame:
    """
    Create a summary of metafield usage

    Args:
        df: Dataframe with mapped metafields
        config: Pipeline configuration

    Returns:
        Summary dataframe
    """
    summary_data = []

    # Analyze custom metafields
    for metafield in config.custom_metafields:
        if metafield in df.columns:
            non_empty_count = df[metafield].notna().sum()
            fill_rate = (non_empty_count / len(df)) * 100 if len(df) > 0 else 0

            summary_data.append({
                'metafield': metafield,
                'type': 'custom',
                'populated_count': non_empty_count,
                'fill_rate_percent': round(fill_rate, 2)
            })

    # Analyze Shopify metafields
    for metafield in config.shopify_metafields:
        if metafield in df.columns:
            non_empty_count = df[metafield].notna().sum()
            fill_rate = (non_empty_count / len(df)) * 100 if len(df) > 0 else 0

            summary_data.append({
                'metafield': metafield,
                'type': 'shopify',
                'populated_count': non_empty_count,
                'fill_rate_percent': round(fill_rate, 2)
            })

    return pd.DataFrame(summary_data)