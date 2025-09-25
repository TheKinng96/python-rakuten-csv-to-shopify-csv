"""
Step 07: Product Type Assignment

Assigns Shopify product types based on Rakuten categories with normalization
and mapping rules. Handles exclusions and applies type mapping configuration.
"""

import logging
import pandas as pd
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Assign product types based on categories

    Args:
        data: Pipeline context containing tax_classified_df and config

    Returns:
        Dict containing dataframe with assigned types
    """
    logger.info("Assigning product types...")

    df = data['image_restructured_df'].copy()
    config = data['config']

    # Track type assignment statistics
    type_stats = {
        'total_products': len(df),
        'types_assigned': 0,
        'types_excluded': 0,
        'mapped_types': 0,
        'default_types': 0,
        'empty_categories': 0
    }

    # Process product types
    df['Type'] = df.apply(
        lambda row: assign_product_type(row, config, type_stats),
        axis=1
    )

    # Log type assignment results
    logger.info(f"Product type assignment completed")
    for key, value in type_stats.items():
        logger.info(f"Type stats - {key}: {value}")

    # Generate type distribution summary
    type_distribution = df['Type'].value_counts().to_dict()
    logger.info("Type distribution:")
    for type_name, count in type_distribution.items():
        logger.info(f"  {type_name}: {count}")

    return {
        'type_assigned_df': df,
        'type_stats': type_stats,
        'type_distribution': type_distribution
    }


def assign_product_type(row: pd.Series, config, stats: Dict[str, Any]) -> str:
    """
    Assign product type for a single product

    Args:
        row: Product row from dataframe
        config: Pipeline configuration
        stats: Statistics tracking dictionary

    Returns:
        Assigned product type string
    """
    # Get category information
    category = row.get('カテゴリ', '')

    if pd.isna(category) or not str(category).strip():
        stats['empty_categories'] += 1
        return ''

    category_str = str(category).strip()

    # Check exclusion list first
    for excluded_category in config.category_exclusion_list:
        if excluded_category in category_str:
            stats['types_excluded'] += 1
            return ''

    # Extract the first meaningful category level
    primary_category = extract_primary_category(category_str)

    # Apply type mapping if available
    if primary_category in config.type_mapping:
        mapped_type = config.type_mapping[primary_category]
        stats['mapped_types'] += 1
        stats['types_assigned'] += 1
        return mapped_type

    # Use primary category as type if it passes validation
    if is_valid_type(primary_category):
        stats['types_assigned'] += 1
        return primary_category

    # Default case
    stats['default_types'] += 1
    return ''


def extract_primary_category(category_str: str) -> str:
    """
    Extract the primary category from Rakuten category string

    Args:
        category_str: Raw category string from Rakuten

    Returns:
        Cleaned primary category
    """
    # Handle different category formats
    # Format 1: "カテゴリ > サブカテゴリ > 詳細カテゴリ"
    if '>' in category_str:
        parts = [part.strip() for part in category_str.split('>')]
        # Use the most specific non-empty category
        for part in reversed(parts):
            if part and len(part) > 1:
                return part
        return parts[0] if parts else category_str

    # Format 2: "カテゴリ/サブカテゴリ/詳細カテゴリ"
    if '/' in category_str:
        parts = [part.strip() for part in category_str.split('/')]
        # Use the most specific non-empty category
        for part in reversed(parts):
            if part and len(part) > 1:
                return part
        return parts[0] if parts else category_str

    # Format 3: Single category
    return category_str.strip()


def is_valid_type(type_candidate: str) -> bool:
    """
    Validate if a string is suitable as a product type

    Args:
        type_candidate: Candidate type string

    Returns:
        bool: True if valid type
    """
    if not type_candidate or len(type_candidate.strip()) < 2:
        return False

    # Exclude overly generic terms
    generic_terms = {
        'その他', '雑貨', '商品', 'アイテム', '製品', '用品',
        'グッズ', '関連', '他', '等', 'など'
    }

    if type_candidate.strip() in generic_terms:
        return False

    # Exclude single characters or very short strings
    if len(type_candidate.strip()) < 2:
        return False

    return True


def normalize_type_name(type_name: str) -> str:
    """
    Normalize type name for consistency

    Args:
        type_name: Raw type name

    Returns:
        Normalized type name
    """
    if not type_name:
        return ''

    # Basic normalization
    normalized = type_name.strip()

    # Remove common suffixes
    suffixes_to_remove = ['用品', '関連', '・他', '等', 'など', '類']
    for suffix in suffixes_to_remove:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)].strip()

    # Common replacements
    replacements = {
        'ドリンク': '飲料・ドリンク',
        '酒類': 'お酒・ワイン',
        'コスメ': '化粧品・コスメ',
        'サプリ': 'サプリメント・ダイエット・健康'
    }

    for old, new in replacements.items():
        if old in normalized:
            normalized = new
            break

    return normalized


def create_type_mapping_suggestions(df: pd.DataFrame, config) -> Dict[str, List[str]]:
    """
    Create suggestions for type mappings based on data analysis

    Args:
        df: Dataframe with assigned types
        config: Pipeline configuration

    Returns:
        Dict with mapping suggestions
    """
    suggestions = {
        'new_mappings': [],
        'consolidation_opportunities': [],
        'excluded_categories_found': []
    }

    # Analyze categories that weren't mapped
    unmapped_categories = set()
    for _, row in df.iterrows():
        category = row.get('カテゴリ', '')
        assigned_type = row.get('Type', '')

        if category and not assigned_type:
            primary_cat = extract_primary_category(str(category))
            if primary_cat not in config.type_mapping:
                unmapped_categories.add(primary_cat)

    suggestions['new_mappings'] = list(unmapped_categories)

    # Look for consolidation opportunities
    type_counts = df['Type'].value_counts()
    similar_types = {}

    for type_name in type_counts.index:
        if type_name:
            # Group similar types
            base_name = type_name.split('・')[0].split('/')[0]
            if base_name not in similar_types:
                similar_types[base_name] = []
            similar_types[base_name].append(type_name)

    for base_name, type_list in similar_types.items():
        if len(type_list) > 1:
            suggestions['consolidation_opportunities'].append({
                'base_name': base_name,
                'variants': type_list,
                'total_count': sum(type_counts.get(t, 0) for t in type_list)
            })

    return suggestions


def export_type_analysis(df: pd.DataFrame, config, output_dir) -> str:
    """
    Export detailed type analysis to CSV

    Args:
        df: Dataframe with type assignments
        config: Pipeline configuration
        output_dir: Output directory path

    Returns:
        Path to exported analysis file
    """
    analysis_file = output_dir / 'type_assignment_analysis.csv'

    # Create analysis dataframe
    analysis_data = []

    type_counts = df['Type'].value_counts()
    for type_name, count in type_counts.items():
        # Get sample products for this type
        sample_products = df[df['Type'] == type_name][['管理番号', '商品名', 'カテゴリ']].head(5)

        analysis_data.append({
            'type': type_name,
            'product_count': count,
            'percentage': round((count / len(df)) * 100, 2),
            'sample_skus': ', '.join(sample_products['管理番号'].tolist()),
            'sample_names': ' | '.join(sample_products['商品名'].tolist()[:2])
        })

    analysis_df = pd.DataFrame(analysis_data)
    analysis_df = analysis_df.sort_values('product_count', ascending=False)

    analysis_df.to_csv(analysis_file, index=False, encoding='utf-8')
    logger.info(f"Type analysis exported to {analysis_file}")

    return str(analysis_file)