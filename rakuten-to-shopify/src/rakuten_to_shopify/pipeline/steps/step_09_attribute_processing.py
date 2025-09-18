"""
Step 09: Attribute Processing and Tag Generation

Processes product attributes to generate tags and handle special attribute mappings.
Consolidates free-form tags and applies business logic for tag generation.
"""

import logging
import pandas as pd
from typing import Dict, Any, List, Set

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process attributes and generate tags

    Args:
        data: Pipeline context containing variant_grouped_df and config

    Returns:
        Dict containing dataframe with processed attributes
    """
    logger.info("Processing attributes and generating tags...")

    df = data['variant_grouped_df'].copy()
    config = data['config']

    # Track attribute processing statistics
    attribute_stats = {
        'total_products': len(df),
        'tags_generated': 0,
        'free_tags_processed': 0,
        'special_attributes_mapped': 0,
        'empty_tag_fields_filled': 0
    }

    # Process attributes and tags
    df['Tags'] = df.apply(
        lambda row: process_product_attributes(row, config, attribute_stats),
        axis=1
    )

    # Log attribute processing results
    logger.info(f"Attribute processing completed")
    for key, value in attribute_stats.items():
        logger.info(f"Attribute stats - {key}: {value}")

    return {
        'attribute_processed_df': df,
        'attribute_stats': attribute_stats
    }


def process_product_attributes(row: pd.Series, config, stats: Dict[str, Any]) -> str:
    """
    Process attributes for a single product and generate tags

    Args:
        row: Product row from dataframe
        config: Pipeline configuration
        stats: Statistics tracking dictionary

    Returns:
        Processed tags string
    """
    all_tags = []

    # Start with existing tags
    existing_tags = row.get('Tags', '')
    if existing_tags:
        existing_tag_list = [tag.strip() for tag in str(existing_tags).split(',')]
        all_tags.extend([tag for tag in existing_tag_list if tag])

    # Process free tag keys
    for free_tag_key in config.free_tag_keys:
        if free_tag_key in row and pd.notna(row[free_tag_key]):
            value = str(row[free_tag_key]).strip()
            if value and value not in all_tags:
                all_tags.append(value)
                stats['free_tags_processed'] += 1

    # Process special attribute mappings
    special_tags = extract_special_attribute_tags(row, config, stats)
    for tag in special_tags:
        if tag not in all_tags:
            all_tags.append(tag)

    # Generate tags from product characteristics
    characteristic_tags = generate_characteristic_tags(row, config)
    for tag in characteristic_tags:
        if tag not in all_tags:
            all_tags.append(tag)

    # Generate tags from metafield values
    metafield_tags = extract_metafield_tags(row, config)
    for tag in metafield_tags:
        if tag not in all_tags:
            all_tags.append(tag)

    # Clean and deduplicate tags
    cleaned_tags = clean_and_deduplicate_tags(all_tags)

    if cleaned_tags:
        stats['tags_generated'] += 1
        if not existing_tags:
            stats['empty_tag_fields_filled'] += 1

    return ', '.join(cleaned_tags)


def extract_special_attribute_tags(row: pd.Series, config, stats: Dict[str, Any]) -> List[str]:
    """
    Extract tags from special attribute mappings

    Args:
        row: Product row
        config: Pipeline configuration
        stats: Statistics tracking dictionary

    Returns:
        List of special attribute tags
    """
    special_tags = []

    # Check for special tag mappings
    for attribute_value, tag in config.special_tags.items():
        # Search across relevant columns
        search_columns = [col for col in row.index if '[絞込み]' in str(col) or col in config.free_tag_keys]

        for col in search_columns:
            if pd.notna(row[col]) and attribute_value in str(row[col]):
                special_tags.append(tag)
                stats['special_attributes_mapped'] += 1
                break

    return special_tags


def generate_characteristic_tags(row: pd.Series, config) -> List[str]:
    """
    Generate tags based on product characteristics

    Args:
        row: Product row
        config: Pipeline configuration

    Returns:
        List of characteristic tags
    """
    tags = []

    # Variant type tags
    variant_type = row.get('Variant Type', '')
    if variant_type == 'trial_variant':
        tags.append('トライアル')
    elif variant_type == 'set_variant':
        set_count = row.get('Set Count', '1')
        if set_count != '1':
            tags.append(f'{set_count}個セット')
    elif variant_type == 'ss_variant':
        tags.append('スーパーセール')

    # Tax rate tags
    tax_rate = row.get('Tax Rate', '')
    if tax_rate == config.reduced_tax_rate:
        tags.append('軽減税率対象')

    # Product type tags
    product_type = row.get('Type', '')
    if product_type:
        tags.append(f'カテゴリ_{product_type}')

    return tags


def extract_metafield_tags(row: pd.Series, config) -> List[str]:
    """
    Extract tags from metafield values

    Args:
        row: Product row
        config: Pipeline configuration

    Returns:
        List of tags from metafields
    """
    tags = []

    # Define metafields that should generate tags
    tag_generating_metafields = [
        '[絞込み]ご当地 (product.metafields.custom.area)',
        '[絞込み]ブランド・メーカー (product.metafields.custom.brand)',
        '[絞込み]こだわり・認証 (product.metafields.custom.commitment)',
        '[絞込み]成分・特性 (product.metafields.custom.component)',
        '[絞込み]迷ったら (product.metafields.custom.doubt)',
        '[絞込み]季節イベント (product.metafields.custom.event)',
        '[絞込み]味・香り・フレーバー (product.metafields.custom.flavor)',
        '[絞込み]お酒の分類 (product.metafields.custom.osake)',
        '[絞込み]容量・サイズ (product.metafields.custom.search_size)',
        '[絞込み]ギフト (product.metafields.custom._gift)'
    ]

    for metafield in tag_generating_metafields:
        if metafield in row and pd.notna(row[metafield]):
            value = str(row[metafield]).strip()
            if value:
                # Create tag with prefix
                prefix = metafield.split(']')[0].replace('[絞込み', '').strip()
                if prefix:
                    tag = f'{prefix}_{value}'
                else:
                    tag = value
                tags.append(tag)

    return tags


def clean_and_deduplicate_tags(tags: List[str]) -> List[str]:
    """
    Clean and deduplicate tag list

    Args:
        tags: List of raw tags

    Returns:
        List of cleaned, deduplicated tags
    """
    if not tags:
        return []

    # Clean individual tags
    cleaned = []
    seen = set()

    for tag in tags:
        if not tag:
            continue

        # Clean the tag
        clean_tag = str(tag).strip()

        # Remove empty or very short tags
        if len(clean_tag) < 2:
            continue

        # Remove unwanted characters
        clean_tag = clean_tag.replace('\n', ' ').replace('\r', ' ')
        clean_tag = ' '.join(clean_tag.split())  # Normalize whitespace

        # Deduplicate (case-insensitive)
        lower_tag = clean_tag.lower()
        if lower_tag not in seen:
            seen.add(lower_tag)
            cleaned.append(clean_tag)

    return cleaned


def analyze_tag_distribution(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Analyze tag distribution across products

    Args:
        df: Dataframe with processed tags

    Returns:
        Dict with tag analysis
    """
    analysis = {
        'total_products': len(df),
        'products_with_tags': 0,
        'average_tags_per_product': 0,
        'most_common_tags': {},
        'tag_statistics': {}
    }

    all_tags = []
    products_with_tags = 0

    for _, row in df.iterrows():
        tags = row.get('Tags', '')
        if tags:
            tag_list = [tag.strip() for tag in str(tags).split(',')]
            tag_list = [tag for tag in tag_list if tag]
            if tag_list:
                products_with_tags += 1
                all_tags.extend(tag_list)

    analysis['products_with_tags'] = products_with_tags
    analysis['average_tags_per_product'] = len(all_tags) / len(df) if len(df) > 0 else 0

    # Count tag frequencies
    tag_counts = {}
    for tag in all_tags:
        tag_counts[tag] = tag_counts.get(tag, 0) + 1

    # Get most common tags
    sorted_tags = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)
    analysis['most_common_tags'] = dict(sorted_tags[:20])  # Top 20

    # Tag statistics
    analysis['tag_statistics'] = {
        'total_unique_tags': len(tag_counts),
        'total_tag_instances': len(all_tags),
        'tags_used_once': sum(1 for count in tag_counts.values() if count == 1),
        'max_tags_per_product': max([len([t for t in str(row.get('Tags', '')).split(',') if t.strip()]) for _, row in df.iterrows()]) if len(df) > 0 else 0
    }

    return analysis


def export_tag_analysis(df: pd.DataFrame, output_dir) -> str:
    """
    Export tag analysis to CSV

    Args:
        df: Dataframe with processed tags
        output_dir: Output directory path

    Returns:
        Path to exported analysis file
    """
    analysis_file = output_dir / 'tag_analysis.csv'

    # Create tag analysis dataframe
    tag_data = []
    for _, row in df.iterrows():
        tags = row.get('Tags', '')
        if tags:
            tag_list = [tag.strip() for tag in str(tags).split(',')]
            tag_list = [tag for tag in tag_list if tag]
            tag_data.append({
                'sku': row.get('Variant SKU', ''),
                'handle': row.get('Handle', ''),
                'title': row.get('Title', ''),
                'tag_count': len(tag_list),
                'tags': tags
            })

    if tag_data:
        analysis_df = pd.DataFrame(tag_data)
        analysis_df.to_csv(analysis_file, index=False, encoding='utf-8')
        logger.info(f"Tag analysis exported to {analysis_file}")

    return str(analysis_file)