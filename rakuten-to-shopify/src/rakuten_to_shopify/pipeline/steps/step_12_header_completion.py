"""
Step 12: Header Completion and Column Alignment

Ensures all 86 required Shopify CSV columns are present and properly ordered.
Fills missing columns with appropriate default values and validates structure.
"""

import logging
import pandas as pd
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Complete header structure and ensure all columns are present

    Args:
        data: Pipeline context containing csv_formatted_df and config

    Returns:
        Dict containing dataframe with complete header structure
    """
    logger.info("Completing header structure and column alignment...")

    df = data['csv_formatted_df'].copy()
    config = data['config']

    # Track header completion statistics
    header_stats = {
        'initial_columns': len(df.columns),
        'target_columns': len(config.complete_header),
        'columns_added': 0,
        'columns_reordered': 0,
        'missing_columns_filled': 0
    }

    # Add missing columns with defaults
    add_missing_columns(df, config, header_stats)

    # Reorder columns to match Shopify standard
    df_reordered = reorder_columns_to_standard(df, config, header_stats)

    # Validate final structure
    validation_results = validate_final_structure(df_reordered, config)

    # Log header completion results
    logger.info(f"Header completion finished")
    for key, value in header_stats.items():
        logger.info(f"Header stats - {key}: {value}")

    return {
        'header_completed_df': df_reordered,
        'header_stats': header_stats,
        'validation_results': validation_results
    }


def add_missing_columns(df: pd.DataFrame, config, stats: Dict[str, Any]):
    """
    Add missing columns with appropriate default values

    Args:
        df: Dataframe to modify
        config: Pipeline configuration
        stats: Statistics tracking dictionary
    """
    # Default values for different types of columns
    column_defaults = get_column_defaults()

    missing_columns = []
    for col in config.complete_header:
        if col not in df.columns:
            missing_columns.append(col)

    if missing_columns:
        logger.info(f"Adding {len(missing_columns)} missing columns")

        for col in missing_columns:
            default_value = column_defaults.get(col, '')
            df[col] = default_value
            stats['columns_added'] += 1

        stats['missing_columns_filled'] = len(missing_columns)


def get_column_defaults() -> Dict[str, str]:
    """
    Get default values for each column type

    Returns:
        Dict mapping column names to default values
    """
    defaults = {
        # Standard Shopify fields
        'Handle': '',
        'Title': '',
        'Body (HTML)': '',
        'Vendor': '',
        'Product Category': '',
        'Type': '',
        'Tags': '""',  # Special quoted empty
        'Published': 'FALSE',

        # Option fields
        'Option1 Name': '',
        'Option1 Value': 'Default Title',
        'Option1 Linked To': '',
        'Option2 Name': '',
        'Option2 Value': '',
        'Option2 Linked To': '',
        'Option3 Name': '',
        'Option3 Value': '',
        'Option3 Linked To': '',

        # Variant fields
        'Variant SKU': '',
        'Variant Grams': '100',
        'Variant Inventory Tracker': 'shopify',
        'Variant Inventory Qty': '0',
        'Variant Inventory Policy': 'deny',
        'Variant Fulfillment Service': 'manual',
        'Variant Price': '0',
        'Variant Compare At Price': '',
        'Variant Requires Shipping': 'TRUE',
        'Variant Taxable': 'TRUE',
        'Variant Barcode': '""',  # Special quoted empty

        # Image fields
        'Image Src': '',
        'Image Position': '',
        'Image Alt Text': '',

        # Additional fields
        'Gift Card': 'FALSE',
        'SEO Title': '',
        'SEO Description': '',
        'Variant Image': '',
        'Variant Weight Unit': 'kg',
        'Variant Tax Code': '',
        'Cost per item': '',
        'Status': 'active'
    }

    # All metafields default to empty string
    metafield_defaults = {}

    # Custom metafields
    custom_metafields = [
        "アレルギー物質 (product.metafields.custom.allergy)",
        "[絞込み]ご当地 (product.metafields.custom.area)",
        "[絞込み]商品カテゴリー (product.metafields.custom.attributes)",
        "消費期限 (product.metafields.custom.best-before)",
        "[絞込み]ブランド・メーカー (product.metafields.custom.brand)",
        "[絞込み]こだわり・認証 (product.metafields.custom.commitment)",
        "[絞込み]成分・特性 (product.metafields.custom.component)",
        "食品の状態 (product.metafields.custom.condition)",
        "[絞込み]迷ったら (product.metafields.custom.doubt)",
        "その他 (product.metafields.custom.etc)",
        "[絞込み]季節イベント (product.metafields.custom.event)",
        "賞味期限 (product.metafields.custom.expiration-area)",
        "[絞込み]味・香り・フレーバー (product.metafields.custom.flavor)",
        "原材料名 (product.metafields.custom.ingredients)",
        "使用場所 (product.metafields.custom.location)",
        "名称 (product.metafields.custom.name)",
        "栄養成分表示 (product.metafields.custom.ngredient_list)",
        "[絞込み]お酒の分類 (product.metafields.custom.osake)",
        "[絞込み]ペットフード・用品分類 (product.metafields.custom.petfood)",
        "肌の悩み (product.metafields.custom.problem)",
        "[絞込み]容量・サイズ (product.metafields.custom.search_size)",
        "シリーズ名 (product.metafields.custom.series)",
        "肌質 (product.metafields.custom.skin)",
        "保存方法 (product.metafields.custom.storage)",
        "対象害虫 (product.metafields.custom.target)",
        "消費税率 (product.metafields.custom.tax)",
        "内容量 (product.metafields.custom.weight)",
        "[絞込み]ギフト (product.metafields.custom._gift)"
    ]

    # Shopify metafields
    shopify_metafields = [
        "商品評価数 (product.metafields.reviews.rating_count)",
        "年齢層 (product.metafields.shopify.age-group)",
        "アレルゲン情報 (product.metafields.shopify.allergen-information)",
        "色 (product.metafields.shopify.color-pattern)",
        "構成成分 (product.metafields.shopify.constitutive-ingredients)",
        "国 (product.metafields.shopify.country)",
        "食事の好み (product.metafields.shopify.dietary-preferences)",
        "飲料用素材 (product.metafields.shopify.drinkware-material)",
        "辛味レベル (product.metafields.shopify.heat-level)",
        "素材 (product.metafields.shopify.material)",
        "製品形態 (product.metafields.shopify.product-form)",
        "スキンケア効果 (product.metafields.shopify.skin-care-effect)",
        "肌質に適している (product.metafields.shopify.suitable-for-skin-type)",
        "ワインの甘さ (product.metafields.shopify.wine-sweetness)",
        "ワインの種類 (product.metafields.shopify.wine-variety)",
        "付属商品 (product.metafields.shopify--discovery--product_recommendation.complementary_products)",
        "関連商品 (product.metafields.shopify--discovery--product_recommendation.related_products)",
        "関連商品の設定 (product.metafields.shopify--discovery--product_recommendation.related_products_display)",
        "販売促進する商品を検索する (product.metafields.shopify--discovery--product_search_boost.queries)"
    ]

    # Set all metafields to empty string
    for metafield in custom_metafields + shopify_metafields:
        metafield_defaults[metafield] = ''

    # Merge all defaults
    defaults.update(metafield_defaults)

    return defaults


def reorder_columns_to_standard(df: pd.DataFrame, config, stats: Dict[str, Any]) -> pd.DataFrame:
    """
    Reorder columns to match the standard Shopify header order

    Args:
        df: Dataframe to reorder
        config: Pipeline configuration
        stats: Statistics tracking dictionary

    Returns:
        Dataframe with reordered columns
    """
    # Start with the complete header order
    target_order = config.complete_header.copy()

    # Create new dataframe with correct column order
    reordered_columns = []

    for col in target_order:
        if col in df.columns:
            reordered_columns.append(col)
        else:
            logger.warning(f"Expected column not found: {col}")

    # Add any extra columns that aren't in the standard header
    extra_columns = [col for col in df.columns if col not in target_order]
    if extra_columns:
        logger.info(f"Found {len(extra_columns)} extra columns: {extra_columns}")
        reordered_columns.extend(extra_columns)

    # Create reordered dataframe
    df_reordered = df[reordered_columns].copy()

    stats['columns_reordered'] = len(reordered_columns)

    return df_reordered


def validate_final_structure(df: pd.DataFrame, config) -> Dict[str, Any]:
    """
    Validate the final CSV structure

    Args:
        df: Final dataframe
        config: Pipeline configuration

    Returns:
        Validation results
    """
    validation = {
        'total_columns': len(df.columns),
        'expected_columns': len(config.complete_header),
        'structure_valid': True,
        'missing_columns': [],
        'extra_columns': [],
        'column_order_correct': True,
        'data_quality_issues': []
    }

    # Check for missing columns
    missing = [col for col in config.complete_header if col not in df.columns]
    validation['missing_columns'] = missing

    # Check for extra columns
    extra = [col for col in df.columns if col not in config.complete_header]
    validation['extra_columns'] = extra

    # Check column order (first few columns should match)
    first_10_expected = config.complete_header[:10]
    first_10_actual = list(df.columns[:10])

    if first_10_expected != first_10_actual:
        validation['column_order_correct'] = False

    # Overall structure validation
    if missing or len(df.columns) != len(config.complete_header):
        validation['structure_valid'] = False

    # Check data quality
    validation['data_quality_issues'] = check_data_quality(df)

    return validation


def check_data_quality(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Check for data quality issues in the final CSV

    Args:
        df: Final dataframe

    Returns:
        List of data quality issues
    """
    issues = []

    # Check required fields
    required_fields = ['Handle', 'Title', 'Variant SKU']
    for field in required_fields:
        if field in df.columns:
            null_count = df[field].isna().sum()
            empty_count = (df[field] == '').sum()

            if null_count > 0:
                issues.append({
                    'type': 'null_values',
                    'field': field,
                    'count': null_count,
                    'description': f'{null_count} null values in required field'
                })

            if empty_count > 0:
                issues.append({
                    'type': 'empty_values',
                    'field': field,
                    'count': empty_count,
                    'description': f'{empty_count} empty values in required field'
                })

    # Check price format
    if 'Variant Price' in df.columns:
        invalid_prices = df[~df['Variant Price'].astype(str).str.match(r'^\d+(\.\d+)?$', na=False)]
        if len(invalid_prices) > 0:
            issues.append({
                'type': 'invalid_format',
                'field': 'Variant Price',
                'count': len(invalid_prices),
                'description': f'{len(invalid_prices)} rows with invalid price format'
            })

    # Check boolean fields
    boolean_fields = ['Published', 'Variant Requires Shipping', 'Variant Taxable', 'Gift Card']
    for field in boolean_fields:
        if field in df.columns:
            invalid_booleans = df[~df[field].isin(['TRUE', 'FALSE', '', 'true', 'false'])]
            if len(invalid_booleans) > 0:
                issues.append({
                    'type': 'invalid_boolean',
                    'field': field,
                    'count': len(invalid_booleans),
                    'description': f'{len(invalid_booleans)} rows with invalid boolean values'
                })

    return issues


def create_structure_report(df: pd.DataFrame, config, validation_results: Dict, output_dir) -> str:
    """
    Create a detailed structure validation report

    Args:
        df: Final dataframe
        config: Pipeline configuration
        validation_results: Results from structure validation
        output_dir: Output directory

    Returns:
        Path to report file
    """
    report_file = output_dir / 'csv_structure_report.txt'

    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("CSV Structure Validation Report\n")
        f.write("=" * 50 + "\n\n")

        f.write(f"Total Columns: {validation_results['total_columns']}\n")
        f.write(f"Expected Columns: {validation_results['expected_columns']}\n")
        f.write(f"Structure Valid: {validation_results['structure_valid']}\n")
        f.write(f"Column Order Correct: {validation_results['column_order_correct']}\n\n")

        if validation_results['missing_columns']:
            f.write("Missing Columns:\n")
            for col in validation_results['missing_columns']:
                f.write(f"  - {col}\n")
            f.write("\n")

        if validation_results['extra_columns']:
            f.write("Extra Columns:\n")
            for col in validation_results['extra_columns']:
                f.write(f"  - {col}\n")
            f.write("\n")

        if validation_results['data_quality_issues']:
            f.write("Data Quality Issues:\n")
            for issue in validation_results['data_quality_issues']:
                f.write(f"  - {issue['description']}\n")
            f.write("\n")

        f.write("First 10 Columns:\n")
        for i, col in enumerate(df.columns[:10], 1):
            f.write(f"  {i:2d}. {col}\n")

    logger.info(f"Structure report saved to {report_file}")
    return str(report_file)