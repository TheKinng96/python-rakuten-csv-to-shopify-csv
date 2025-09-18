"""
Step 06: Tax Classification

Classifies products into Japanese tax rates (8% reduced rate for food/beverages vs 10% standard rate)
based on keyword analysis and category information. Sets the tax metafield accordingly.
"""

import logging
import pandas as pd
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Classify products by Japanese tax rates

    Args:
        data: Pipeline context containing metafield_mapped_df and config

    Returns:
        Dict containing dataframe with tax classifications
    """
    logger.info("Classifying products by Japanese tax rates...")

    df = data['metafield_mapped_df'].copy()
    config = data['config']

    # Track tax classification statistics
    tax_stats = {
        'total_products': len(df),
        'tax_8_percent': 0,
        'tax_10_percent': 0,
        'keyword_based_8': 0,
        'keyword_based_10': 0,
        'default_assignments': 0,
        'category_based_assignments': 0
    }

    # Apply tax classification
    df['Tax Rate'] = df.apply(
        lambda row: classify_tax_rate(row, config, tax_stats),
        axis=1
    )

    # Set the tax metafield
    tax_metafield = '消費税率 (product.metafields.custom.tax)'
    if tax_metafield in df.columns:
        df[tax_metafield] = df['Tax Rate']

    # Calculate final statistics
    tax_stats['tax_8_percent'] = (df['Tax Rate'] == config.reduced_tax_rate).sum()
    tax_stats['tax_10_percent'] = (df['Tax Rate'] == config.default_tax_rate).sum()

    # Log tax classification results
    logger.info(f"Tax classification completed")
    for key, value in tax_stats.items():
        logger.info(f"Tax stats - {key}: {value}")

    return {
        'tax_classified_df': df,
        'tax_stats': tax_stats
    }


def classify_tax_rate(row: pd.Series, config, stats: Dict[str, Any]) -> str:
    """
    Classify a single product's tax rate

    Args:
        row: Product row from dataframe
        config: Pipeline configuration
        stats: Statistics tracking dictionary

    Returns:
        Tax rate string ('8%' or '10%')
    """
    # Fields to check for tax classification
    classification_fields = [
        '商品名',
        'カテゴリ',
        'Type',
        'PC用商品説明文',
        '商品説明文（スマートフォン用）',
        'PC用キャッチコピー'
    ]

    # Check for 8% reduced rate keywords (food, beverages)
    for field in classification_fields:
        if field in row and pd.notna(row[field]):
            field_value = str(row[field]).lower()

            # Check for 8% keywords
            for keyword in config.tax_8_keywords:
                if keyword in field_value:
                    stats['keyword_based_8'] += 1
                    return config.reduced_tax_rate

    # Check for 10% standard rate keywords (general merchandise, alcohol)
    for field in classification_fields:
        if field in row and pd.notna(row[field]):
            field_value = str(row[field]).lower()

            # Check for 10% keywords
            for keyword in config.tax_10_keywords:
                if keyword in field_value:
                    stats['keyword_based_10'] += 1
                    return config.default_tax_rate

    # Category-based classification
    category = row.get('カテゴリ', '')
    if pd.notna(category):
        category_lower = str(category).lower()

        # Food categories get 8% rate
        food_categories = ['食品', '飲料', 'ドリンク', '調味料', 'お菓子', 'スイーツ']
        if any(cat in category_lower for cat in food_categories):
            stats['category_based_assignments'] += 1
            return config.reduced_tax_rate

        # Alcohol categories get 10% rate (alcohol is not eligible for reduced rate)
        alcohol_categories = ['酒', 'ワイン', 'ビール', '日本酒', '焼酎', 'ウイスキー']
        if any(cat in category_lower for cat in alcohol_categories):
            stats['category_based_assignments'] += 1
            return config.default_tax_rate

    # Default to 10% standard rate
    stats['default_assignments'] += 1
    return config.default_tax_rate


def create_tax_classification_report(df: pd.DataFrame, config) -> Dict[str, Any]:
    """
    Create detailed tax classification report

    Args:
        df: Dataframe with tax classifications
        config: Pipeline configuration

    Returns:
        Dict with detailed tax analysis
    """
    report = {
        'summary': {
            'total_products': len(df),
            'reduced_rate_8_percent': (df['Tax Rate'] == config.reduced_tax_rate).sum(),
            'standard_rate_10_percent': (df['Tax Rate'] == config.default_tax_rate).sum()
        },
        'by_category': {},
        'keyword_analysis': {
            '8_percent_keywords_found': [],
            '10_percent_keywords_found': []
        }
    }

    # Analysis by category
    if 'カテゴリ' in df.columns:
        category_tax = df.groupby(['カテゴリ', 'Tax Rate']).size().unstack(fill_value=0)
        report['by_category'] = category_tax.to_dict()

    # Keyword analysis
    for _, row in df.iterrows():
        tax_rate = row['Tax Rate']
        product_name = str(row.get('商品名', '')).lower()

        if tax_rate == config.reduced_tax_rate:
            for keyword in config.tax_8_keywords:
                if keyword in product_name and keyword not in report['keyword_analysis']['8_percent_keywords_found']:
                    report['keyword_analysis']['8_percent_keywords_found'].append(keyword)

        elif tax_rate == config.default_tax_rate:
            for keyword in config.tax_10_keywords:
                if keyword in product_name and keyword not in report['keyword_analysis']['10_percent_keywords_found']:
                    report['keyword_analysis']['10_percent_keywords_found'].append(keyword)

    return report


def validate_tax_assignments(df: pd.DataFrame, config) -> List[Dict[str, Any]]:
    """
    Validate tax assignments and identify potential issues

    Args:
        df: Dataframe with tax classifications
        config: Pipeline configuration

    Returns:
        List of validation issues
    """
    issues = []

    # Check for alcohol products with 8% rate (should be 10%)
    alcohol_keywords = ['酒', 'ワイン', 'ビール', 'アルコール']
    for _, row in df.iterrows():
        if row['Tax Rate'] == config.reduced_tax_rate:
            product_name = str(row.get('商品名', '')).lower()
            for keyword in alcohol_keywords:
                if keyword in product_name:
                    issues.append({
                        'type': 'alcohol_with_reduced_rate',
                        'sku': row.get('管理番号', ''),
                        'product_name': row.get('商品名', ''),
                        'assigned_rate': row['Tax Rate'],
                        'issue': f'Alcohol product with reduced rate (keyword: {keyword})'
                    })

    # Check for food products with 10% rate that might qualify for 8%
    food_keywords = ['食品', '食材', '米', '肉', '魚', '野菜', '果物']
    for _, row in df.iterrows():
        if row['Tax Rate'] == config.default_tax_rate:
            product_name = str(row.get('商品名', '')).lower()
            for keyword in food_keywords:
                if keyword in product_name:
                    issues.append({
                        'type': 'food_with_standard_rate',
                        'sku': row.get('管理番号', ''),
                        'product_name': row.get('商品名', ''),
                        'assigned_rate': row['Tax Rate'],
                        'suggestion': f'Might qualify for reduced rate (keyword: {keyword})'
                    })

    return issues


def export_tax_classification_summary(df: pd.DataFrame, config, output_dir) -> str:
    """
    Export tax classification summary to CSV

    Args:
        df: Dataframe with tax classifications
        config: Pipeline configuration
        output_dir: Output directory path

    Returns:
        Path to exported summary file
    """
    summary_file = output_dir / 'tax_classification_summary.csv'

    # Create summary by product
    summary_df = df[[
        '管理番号', '商品名', 'カテゴリ', 'Tax Rate'
    ]].copy()

    summary_df.to_csv(summary_file, index=False, encoding='utf-8')
    logger.info(f"Tax classification summary exported to {summary_file}")

    return str(summary_file)