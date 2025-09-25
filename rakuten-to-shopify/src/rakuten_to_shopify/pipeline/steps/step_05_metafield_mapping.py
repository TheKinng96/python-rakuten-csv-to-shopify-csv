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

    # Use the most recent dataframe with image processing
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

    # Remove Rakuten attribute columns after mapping is complete - they're not needed for Shopify
    attribute_columns_to_remove = [col for col in df.columns if '商品属性' in col]
    if attribute_columns_to_remove:
        logger.info(f"Removing {len(attribute_columns_to_remove)} Rakuten attribute columns from final output")
        df = df.drop(columns=attribute_columns_to_remove)

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
        if metafield not in df.columns:
            df[metafield] = ''

    # Initialize Shopify metafields
    for metafield in config.shopify_metafields:
        if metafield not in df.columns:
            df[metafield] = ''

    # Initialize missing standard columns
    missing_standard_columns = [
        "Product Category",
        "Type",
        "Option1 Linked To",
        "Option2 Linked To",
        "Option3 Linked To"
    ]

    for column in missing_standard_columns:
        if column not in df.columns:
            df[column] = ''


def apply_metafield_mappings(df: pd.DataFrame, mapping: Dict[str, str], config, stats: Dict[str, Any]):
    """
    Apply metafield mappings to dataframe

    Args:
        df: Dataframe to process
        mapping: Metafield mapping dictionary
        config: Pipeline configuration
        stats: Statistics tracking dictionary
    """
    # Multi-value metafields that should use newline separation
    multi_value_fields = {
        "[絞込み]ご当地 (product.metafields.custom.area)",
        "[絞込み]商品カテゴリー (product.metafields.custom.attributes)",
        "[絞込み]ブランド・メーカー (product.metafields.custom.brand)",
        "ブランド・メーカー (product.metafields.custom.brand)",
        "[絞込み]こだわり・認証 (product.metafields.custom.commitment)",
        "こだわり・認証 (product.metafields.custom.commitment)",
        "食品の状態 (product.metafields.custom.condition)",
        "[絞込み]容量・サイズ (product.metafields.custom.search_size)",
        "その他 (product.metafields.custom.etc)",
        "[絞込み]季節イベント (product.metafields.custom.event)",
        "[絞込み]味・香り・フレーバー (product.metafields.custom.flavor)",
        "原材料名 (product.metafields.custom.ingredients)",
        "[絞込み]お酒の分類 (product.metafields.custom.osake)",
        "[絞込み]ペットフード・用品分類 (product.metafields.custom.petfood)",
        "シリーズ名 (product.metafields.custom.series)",
        "肌質 (product.metafields.custom.skin)",
        "対象害虫 (product.metafields.custom.target)"
    }

    # Apply dynamic attribute mapping for 商品属性 structure
    dynamic_mapping = create_dynamic_attribute_mapping(df, mapping)
    logger.info(f"Created dynamic mapping for {len(dynamic_mapping)} attribute values")

    # First apply the dynamic attribute mappings
    for key, (shopify_column, attr_name, item_col, value_col) in dynamic_mapping.items():
        if value_col in df.columns and shopify_column in df.columns:
            # Find rows where this specific attribute exists
            item_mask = df[item_col] == attr_name
            value_mask = df[value_col].notna() & (df[value_col] != '') & (df[value_col] != '　')
            combined_mask = item_mask & value_mask

            if combined_mask.any():
                # Get values for this specific attribute
                values = df.loc[combined_mask, value_col]
                units = None

                # Get corresponding units if available
                unit_col = value_col.replace('（値）', '（単位）')
                if unit_col in df.columns:
                    units = df.loc[combined_mask, unit_col]

                # Apply special processing based on attribute type
                processed_values = values.copy()
                for idx in values.index:
                    raw_value = values[idx]
                    unit_value = units[idx] if units is not None else None
                    processed_value = process_attribute_value(
                        attr_name, raw_value, unit_value, shopify_column
                    )
                    processed_values[idx] = processed_value

                    # Debug logging for abshiri product
                    if 'abshiri' in str(df.loc[idx, 'Handle']).lower():
                        logger.info(f"DEBUG - Processing {attr_name} for abshiri: raw='{raw_value}', unit='{unit_value}', processed='{processed_value}', target='{shopify_column}'")

                # Process multi-value fields
                if shopify_column in multi_value_fields:
                    processed_values = processed_values.apply(lambda x: process_multi_value_field(x))
                    # Combine with existing values in the metafield
                    for idx in values.index:
                        existing_value = df.loc[idx, shopify_column]
                        new_value = str(processed_values.loc[idx]).strip()

                        # Debug logging for abshiri product
                        if 'abshiri' in str(df.loc[idx, 'Handle']).lower():
                            logger.info(f"DEBUG - Assignment for {attr_name} (abshiri): existing='{existing_value}', new='{new_value}', target='{shopify_column}'")

                        # Only proceed if we have a valid new value
                        if new_value:
                            # Special handling for size metafield: keep only the first valid value
                            if shopify_column == "[絞込み]容量・サイズ (product.metafields.custom.search_size)":
                                if not existing_value or not str(existing_value).strip():
                                    df.loc[idx, shopify_column] = new_value
                                    if 'abshiri' in str(df.loc[idx, 'Handle']).lower():
                                        logger.info(f"DEBUG - Size assignment (abshiri): '{new_value}' (first valid)")
                                # If we already have a size, skip additional values
                                elif 'abshiri' in str(df.loc[idx, 'Handle']).lower():
                                    logger.info(f"DEBUG - Size skipped (abshiri): '{new_value}' (already has '{existing_value}')")
                            elif existing_value and str(existing_value).strip():
                                # Use newline separator for other multi-value fields
                                separator = "\n"
                                final_value = f"{str(existing_value).strip()}{separator}{new_value}"
                                df.loc[idx, shopify_column] = final_value
                                if 'abshiri' in str(df.loc[idx, 'Handle']).lower():
                                    logger.info(f"DEBUG - Combined assignment (abshiri): '{final_value}'")
                            else:
                                df.loc[idx, shopify_column] = new_value
                                if 'abshiri' in str(df.loc[idx, 'Handle']).lower():
                                    logger.info(f"DEBUG - Direct assignment (abshiri): '{new_value}'")
                        else:
                            if 'abshiri' in str(df.loc[idx, 'Handle']).lower():
                                logger.info(f"DEBUG - Skipped assignment (abshiri): empty new_value")
                else:
                    df.loc[combined_mask, shopify_column] = processed_values

                stats['mappings_applied'] += 1

                # Track type of metafield mapped
                if shopify_column in config.custom_metafields:
                    stats['custom_metafields_mapped'] += 1
                elif shopify_column in config.shopify_metafields:
                    stats['shopify_metafields_mapped'] += 1

                mapped_count = combined_mask.sum()
                logger.info(f"Mapped {mapped_count} values: {attr_name} ({value_col}) → {shopify_column}")
            else:
                stats['empty_values_skipped'] += 1

    # Then apply traditional direct column mappings (for backward compatibility)
    for rakuten_key, shopify_column in mapping.items():
        if rakuten_key in df.columns and shopify_column in df.columns:
            # Copy values from Rakuten column to Shopify metafield column
            non_empty_mask = df[rakuten_key].notna() & (df[rakuten_key] != '') & (df[rakuten_key] != '　')

            if non_empty_mask.any():
                values = df.loc[non_empty_mask, rakuten_key]

                # Apply special processing for direct mappings too
                processed_values = values.apply(
                    lambda x: process_attribute_value(rakuten_key, x, None, shopify_column)
                )

                # Process multi-value fields to preserve newline formatting
                if shopify_column in multi_value_fields:
                    processed_values = processed_values.apply(
                        lambda x: process_multi_value_field(x)
                    )

                # For multi-value fields, combine with existing values
                if shopify_column in multi_value_fields:
                    for idx in processed_values.index:
                        existing_value = df.loc[idx, shopify_column]
                        new_value = str(processed_values.loc[idx]).strip()

                        # Only proceed if we have a valid new value
                        if new_value:
                            if existing_value and str(existing_value).strip():
                                # Use newline separator for その他 field, pipe separator for others
                                separator = "\n" if shopify_column == "その他 (product.metafields.custom.etc)" else " | "
                                df.loc[idx, shopify_column] = f"{str(existing_value).strip()}{separator}{new_value}"
                            else:
                                df.loc[idx, shopify_column] = new_value
                else:
                    df.loc[non_empty_mask, shopify_column] = processed_values

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

    # Post-process その他 field to ensure all " | " separators are converted to newlines
    etc_column = "その他 (product.metafields.custom.etc)"
    if etc_column in df.columns:
        mask = df[etc_column].notna() & df[etc_column].str.contains(' \| ', na=False)
        if mask.any():
            df.loc[mask, etc_column] = df.loc[mask, etc_column].str.replace(' | ', '\n')
            logger.info(f"Converted pipe separators to newlines for {mask.sum()} rows in {etc_column}")


def create_dynamic_attribute_mapping(df: pd.DataFrame, mapping: Dict[str, str]) -> Dict[str, tuple]:
    """
    Create dynamic mapping for the 商品属性 structure

    Args:
        df: Dataframe with 商品属性 columns
        mapping: Static mapping dictionary

    Returns:
        Dict mapping value columns to (shopify_column, attr_name, item_col) tuples
    """
    dynamic_mapping = {}
    mapping_keys = set(mapping.keys())

    # Check each attribute slot (1-100)
    for i in range(1, 101):
        item_col = f'商品属性（項目）{i}'
        value_col = f'商品属性（値）{i}'

        if item_col in df.columns and value_col in df.columns:
            # Get unique attribute names from this item column
            attributes = df[item_col].dropna().unique()
            for attr in attributes:
                attr_str = str(attr).strip()
                if attr_str and attr_str in mapping_keys:
                    # This attribute matches our mapping
                    shopify_column = mapping[attr_str]
                    # Map the value column to the shopify metafield
                    key = f"{value_col}_{attr_str}"  # Make unique key
                    dynamic_mapping[key] = (shopify_column, attr_str, item_col, value_col)

    return dynamic_mapping


def process_multi_value_field(value: str) -> str:
    """
    Process multi-value fields to ensure proper newline separation

    Args:
        value: The field value to process

    Returns:
        Processed value with proper newline separation
    """
    if not value or pd.isna(value):
        return ''

    value_str = str(value).strip()
    if not value_str:
        return ''

    # If already contains newlines, preserve them
    if '\n' in value_str:
        return value_str

    # For comma-separated values, convert to newline-separated
    if ',' in value_str:
        parts = [part.strip() for part in value_str.split(',') if part.strip()]
        return '\n'.join(parts)

    # Single value, return as-is
    return value_str


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


def process_attribute_value(attr_name: str, value: str, unit: str, shopify_column: str) -> str:
    """
    Process attribute values with special handling for specific attributes

    Args:
        attr_name: The attribute name (e.g., "単品容量", "アルコール度数")
        value: The attribute value
        unit: The unit (e.g., "ml", "%")
        shopify_column: Target Shopify metafield column

    Returns:
        Processed value
    """
    if not value or pd.isna(value):
        return ''

    value_str = str(value).strip()
    unit_str = str(unit).strip() if unit and not pd.isna(unit) else ''

    # Handle size categorization for 容量・サイズ metafield
    if shopify_column == "[絞込み]容量・サイズ (product.metafields.custom.search_size)":
        return process_size_value(attr_name, value_str, unit_str)

    # Handle その他 metafield with key:value+unit format
    if shopify_column == "その他 (product.metafields.custom.etc)":
        if unit_str:
            return f"{attr_name}:{value_str}{unit_str}"
        else:
            return f"{attr_name}:{value_str}"

    # For all other cases, return the original value
    return value_str


def process_size_value(attr_name: str, value: str, unit: str) -> str:
    """
    Process size values for [絞込み]容量・サイズ metafield
    - Volume attributes: Convert to size categories (SS, S, M, L, LL)
    - Clothing size attributes: Keep original values (XS, S, M, L, XL)
    - Count/number attributes: Filter out pure numbers

    Args:
        attr_name: The attribute name
        value: The attribute value
        unit: The unit

    Returns:
        Processed size value or empty string if invalid
    """
    if not value or pd.isna(value):
        return ''

    value_str = str(value).strip()
    unit_str = str(unit).strip() if unit and not pd.isna(unit) else ''

    # Volume-based attributes: convert to size categories
    volume_attrs = ["単品容量", "総容量", "内容量", "容量"]
    if attr_name in volume_attrs and unit_str.lower() in ['ml', 'l', 'ミリリットル', 'リットル', 'liter', 'litre', 'milliliter', 'millilitre']:
        return categorize_volume_size(value_str, unit_str)

    # Clothing/standard size attributes: keep if valid size
    size_attrs = ["サイズ（S/M/L）", "サイズ（大/中/小）", "ペットグッズのサイズ"]
    if attr_name in size_attrs:
        # Check if it's a valid clothing size
        valid_sizes = ['XXS', 'XS', 'S', 'M', 'L', 'XL', 'XXL', '大', '中', '小']
        if value_str.upper() in valid_sizes or any(size in value_str.upper() for size in valid_sizes):
            return value_str

    # Count/number attributes: filter out pure numbers
    count_attrs = ["総本数", "単品（個装）本数", "本数", "総個数", "個数", "総枚数", "枚数", "総入数", "入数"]
    if attr_name in count_attrs:
        # Skip pure numbers for count attributes
        try:
            float(value_str)
            return ''  # Pure number - skip it
        except (ValueError, TypeError):
            return value_str  # Non-numeric value - keep it

    # For other attributes, return empty to avoid clutter
    return ''


def categorize_volume_size(value: str, unit: str) -> str:
    """
    Categorize volume into size categories

    Args:
        value: Volume value
        unit: Volume unit

    Returns:
        Size category: SS（〜100ml）, S（〜250ml）, M（〜500ml）, L（〜1L）, LL（1L以上）
    """
    try:
        # Convert value to number
        volume_num = float(value)

        # Convert to ml if necessary
        if unit.lower() in ['l', 'リットル', 'リットル', 'liter', 'litre']:
            volume_ml = volume_num * 1000
        elif unit.lower() in ['ml', 'ミリリットル', 'milliliter', 'millilitre']:
            volume_ml = volume_num
        else:
            # If unit is unclear, assume ml
            volume_ml = volume_num

        # Categorize based on volume in ml
        if volume_ml <= 100:
            return "SS（〜100ml）"
        elif volume_ml <= 250:
            return "S（〜250ml）"
        elif volume_ml <= 500:
            return "M（〜500ml）"
        elif volume_ml <= 1000:
            return "L（〜1L）"
        else:
            return "LL（1L以上）"

    except (ValueError, TypeError):
        # If we can't parse the number, return original value
        return f"{value}{unit}" if unit else value


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