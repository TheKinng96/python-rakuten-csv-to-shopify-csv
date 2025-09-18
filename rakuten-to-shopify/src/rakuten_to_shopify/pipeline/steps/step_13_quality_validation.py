"""
Step 13: Quality Validation and Data Integrity Checks

Performs comprehensive quality validation on the final CSV data.
Checks for data integrity, business rule compliance, and export readiness.
"""

import logging
import pandas as pd
from typing import Dict, Any, List, Tuple

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Perform comprehensive quality validation

    Args:
        data: Pipeline context containing header_completed_df and config

    Returns:
        Dict containing validation results and quality metrics
    """
    logger.info("Performing comprehensive quality validation...")

    df = data['header_completed_df'].copy()
    config = data['config']

    # Track validation statistics
    validation_stats = {
        'total_products': len(df['Handle'].unique()),
        'total_variants': len(df),
        'validation_checks_performed': 0,
        'critical_issues': 0,
        'warnings': 0,
        'passed_checks': 0
    }

    # Perform validation checks
    validation_results = perform_validation_checks(df, config, validation_stats)

    # Generate quality metrics
    quality_metrics = calculate_quality_metrics(df, config)

    # Create validation summary
    validation_summary = create_validation_summary(validation_results, quality_metrics)

    # Log validation results
    logger.info(f"Quality validation completed")
    for key, value in validation_stats.items():
        logger.info(f"Validation stats - {key}: {value}")

    # Log critical issues
    if validation_stats['critical_issues'] > 0:
        logger.warning(f"Found {validation_stats['critical_issues']} critical issues")

    return {
        'validated_df': df,
        'validation_stats': validation_stats,
        'validation_results': validation_results,
        'quality_metrics': quality_metrics,
        'validation_summary': validation_summary
    }


def perform_validation_checks(df: pd.DataFrame, config, stats: Dict[str, Any]) -> Dict[str, List]:
    """
    Perform all validation checks

    Args:
        df: Dataframe to validate
        config: Pipeline configuration
        stats: Statistics tracking dictionary

    Returns:
        Dict containing validation results by category
    """
    validation_results = {
        'critical_issues': [],
        'warnings': [],
        'passed_checks': [],
        'data_quality': [],
        'business_rules': [],
        'export_readiness': []
    }

    # Critical validation checks
    critical_checks = [
        ('required_fields_present', check_required_fields),
        ('unique_handles_and_skus', check_unique_identifiers),
        ('valid_product_structure', check_product_structure),
        ('price_data_validity', check_price_data)
    ]

    for check_name, check_func in critical_checks:
        stats['validation_checks_performed'] += 1
        result = check_func(df, config)

        if result['status'] == 'critical':
            validation_results['critical_issues'].extend(result['issues'])
            stats['critical_issues'] += len(result['issues'])
        elif result['status'] == 'warning':
            validation_results['warnings'].extend(result['issues'])
            stats['warnings'] += len(result['issues'])
        else:
            validation_results['passed_checks'].append(check_name)
            stats['passed_checks'] += 1

    # Data quality checks
    data_quality_checks = [
        ('html_content_quality', check_html_content),
        ('image_data_quality', check_image_data),
        ('metafield_completeness', check_metafield_completeness),
        ('seo_content_quality', check_seo_content)
    ]

    for check_name, check_func in data_quality_checks:
        stats['validation_checks_performed'] += 1
        result = check_func(df, config)
        validation_results['data_quality'].append({
            'check': check_name,
            'result': result
        })

    # Business rule validation
    business_rule_checks = [
        ('tax_classification_accuracy', check_tax_classifications),
        ('variant_option_consistency', check_variant_options),
        ('category_type_alignment', check_category_type_alignment)
    ]

    for check_name, check_func in business_rule_checks:
        stats['validation_checks_performed'] += 1
        result = check_func(df, config)
        validation_results['business_rules'].append({
            'check': check_name,
            'result': result
        })

    # Export readiness checks
    export_checks = [
        ('csv_format_compliance', check_csv_format),
        ('shopify_import_readiness', check_shopify_compatibility),
        ('data_completeness', check_data_completeness)
    ]

    for check_name, check_func in export_checks:
        stats['validation_checks_performed'] += 1
        result = check_func(df, config)
        validation_results['export_readiness'].append({
            'check': check_name,
            'result': result
        })

    return validation_results


def check_required_fields(df: pd.DataFrame, config) -> Dict[str, Any]:
    """Check that all required fields are present and populated"""
    required_fields = ['Handle', 'Title', 'Variant SKU', 'Variant Price']
    issues = []

    for field in required_fields:
        if field not in df.columns:
            issues.append(f"Missing required column: {field}")
        else:
            null_count = df[field].isna().sum()
            empty_count = (df[field].astype(str) == '').sum()

            if null_count > 0:
                issues.append(f"{field}: {null_count} null values")
            if empty_count > 0:
                issues.append(f"{field}: {empty_count} empty values")

    return {
        'status': 'critical' if issues else 'passed',
        'issues': issues
    }


def check_unique_identifiers(df: pd.DataFrame, config) -> Dict[str, Any]:
    """Check that SKUs are unique and handles are properly grouped"""
    issues = []

    # Check SKU uniqueness
    duplicate_skus = df[df['Variant SKU'].duplicated()]['Variant SKU'].unique()
    if len(duplicate_skus) > 0:
        issues.append(f"Duplicate SKUs found: {len(duplicate_skus)} duplicates")

    # Check handle consistency
    handle_groups = df.groupby('Handle')
    for handle, group in handle_groups:
        if len(group) > 1:
            # Multi-variant product should have proper option structure
            option_values = group['Option1 Value'].unique()
            if len(option_values) <= 1 or 'Default Title' in option_values:
                issues.append(f"Handle {handle}: Improper variant options for multi-variant product")

    return {
        'status': 'critical' if issues else 'passed',
        'issues': issues
    }


def check_product_structure(df: pd.DataFrame, config) -> Dict[str, Any]:
    """Check product and variant structure compliance"""
    issues = []

    # Check published status
    handle_groups = df.groupby('Handle')
    for handle, group in handle_groups:
        published_count = (group['Published'] == 'TRUE').sum()
        if published_count != 1:
            issues.append(f"Handle {handle}: Should have exactly 1 published variant, found {published_count}")

        # Check that main product has all required product-level data
        main_product = group[group['Published'] == 'TRUE']
        if len(main_product) == 1:
            main_row = main_product.iloc[0]
            if not main_row.get('Title', '').strip():
                issues.append(f"Handle {handle}: Main product missing title")

    return {
        'status': 'critical' if issues else 'passed',
        'issues': issues
    }


def check_price_data(df: pd.DataFrame, config) -> Dict[str, Any]:
    """Check price data validity"""
    issues = []

    # Check price format
    if 'Variant Price' in df.columns:
        invalid_prices = df[~df['Variant Price'].astype(str).str.match(r'^\d+(\.\d+)?$', na=False)]
        if len(invalid_prices) > 0:
            issues.append(f"Invalid price format: {len(invalid_prices)} rows")

        # Check for zero prices
        zero_prices = df[df['Variant Price'].astype(str) == '0']
        if len(zero_prices) > 0:
            issues.append(f"Zero prices found: {len(zero_prices)} rows")

    return {
        'status': 'warning' if issues else 'passed',
        'issues': issues
    }


def check_html_content(df: pd.DataFrame, config) -> Dict[str, Any]:
    """Check HTML content quality"""
    issues = []
    stats = {}

    main_products = df[df['Published'] == 'TRUE']

    # Check for empty descriptions
    empty_html = main_products[main_products['Body (HTML)'].astype(str).str.strip() == '']
    stats['empty_descriptions'] = len(empty_html)

    # Check for minimal content
    short_html = main_products[main_products['Body (HTML)'].astype(str).str.len() < 100]
    stats['short_descriptions'] = len(short_html)

    # Check for scope class presence
    missing_scope = main_products[~main_products['Body (HTML)'].astype(str).str.contains(config.scope_class, na=False)]
    stats['missing_scope_class'] = len(missing_scope)

    return {
        'status': 'passed',
        'stats': stats,
        'issues': issues
    }


def check_image_data(df: pd.DataFrame, config) -> Dict[str, Any]:
    """Check image data quality"""
    stats = {}

    main_products = df[df['Published'] == 'TRUE']

    # Check for products with images
    with_images = main_products[main_products['Image Src'].astype(str).str.strip() != '']
    stats['products_with_images'] = len(with_images)
    stats['products_without_images'] = len(main_products) - len(with_images)

    # Check image URL format
    if len(with_images) > 0:
        valid_urls = with_images[with_images['Image Src'].astype(str).str.startswith('http')]
        stats['valid_image_urls'] = len(valid_urls)

    return {
        'status': 'passed',
        'stats': stats,
        'issues': []
    }


def check_metafield_completeness(df: pd.DataFrame, config) -> Dict[str, Any]:
    """Check metafield data completeness"""
    stats = {}

    all_metafields = config.custom_metafields + config.shopify_metafields

    for metafield in all_metafields:
        if metafield in df.columns:
            populated_count = df[df[metafield].astype(str).str.strip() != ''].shape[0]
            stats[f"{metafield}_populated"] = populated_count

    return {
        'status': 'passed',
        'stats': stats,
        'issues': []
    }


def check_seo_content(df: pd.DataFrame, config) -> Dict[str, Any]:
    """Check SEO content quality"""
    issues = []
    stats = {}

    main_products = df[df['Published'] == 'TRUE']

    # Check SEO titles
    with_seo_title = main_products[main_products['SEO Title'].astype(str).str.strip() != '']
    stats['products_with_seo_title'] = len(with_seo_title)

    # Check SEO descriptions
    with_seo_desc = main_products[main_products['SEO Description'].astype(str).str.strip() != '']
    stats['products_with_seo_description'] = len(with_seo_desc)

    return {
        'status': 'passed',
        'stats': stats,
        'issues': issues
    }


def check_tax_classifications(df: pd.DataFrame, config) -> Dict[str, Any]:
    """Check tax classification accuracy"""
    issues = []
    stats = {}

    if 'Tax Rate' in df.columns:
        tax_8_count = (df['Tax Rate'] == config.reduced_tax_rate).sum()
        tax_10_count = (df['Tax Rate'] == config.default_tax_rate).sum()

        stats['tax_8_percent_products'] = tax_8_count
        stats['tax_10_percent_products'] = tax_10_count

    return {
        'status': 'passed',
        'stats': stats,
        'issues': issues
    }


def check_variant_options(df: pd.DataFrame, config) -> Dict[str, Any]:
    """Check variant option consistency"""
    issues = []

    handle_groups = df.groupby('Handle')
    for handle, group in handle_groups:
        if len(group) > 1:
            # Multi-variant product
            option1_name = group['Option1 Name'].iloc[0]
            if not option1_name:
                issues.append(f"Handle {handle}: Multi-variant product missing Option1 Name")

    return {
        'status': 'warning' if issues else 'passed',
        'issues': issues
    }


def check_category_type_alignment(df: pd.DataFrame, config) -> Dict[str, Any]:
    """Check category and type alignment"""
    stats = {}

    # Count products by type
    type_counts = df['Type'].value_counts().to_dict()
    stats['type_distribution'] = type_counts

    return {
        'status': 'passed',
        'stats': stats,
        'issues': []
    }


def check_csv_format(df: pd.DataFrame, config) -> Dict[str, Any]:
    """Check CSV format compliance"""
    issues = []

    # Check column count
    if len(df.columns) != len(config.complete_header):
        issues.append(f"Column count mismatch: expected {len(config.complete_header)}, got {len(df.columns)}")

    # Check for special quoted fields
    for field in config.special_quoted_empty_fields:
        if field in df.columns:
            empty_values = df[df[field].astype(str).str.strip() == '']
            quoted_empty = df[df[field] == '""']
            if len(empty_values) > len(quoted_empty):
                issues.append(f"Field {field}: Some empty values not properly quoted")

    return {
        'status': 'warning' if issues else 'passed',
        'issues': issues
    }


def check_shopify_compatibility(df: pd.DataFrame, config) -> Dict[str, Any]:
    """Check Shopify import compatibility"""
    issues = []

    # Check boolean field formats
    boolean_fields = ['Published', 'Variant Requires Shipping', 'Variant Taxable', 'Gift Card']
    for field in boolean_fields:
        if field in df.columns:
            invalid_values = df[~df[field].isin(['TRUE', 'FALSE', ''])]
            if len(invalid_values) > 0:
                issues.append(f"Field {field}: {len(invalid_values)} invalid boolean values")

    return {
        'status': 'critical' if issues else 'passed',
        'issues': issues
    }


def check_data_completeness(df: pd.DataFrame, config) -> Dict[str, Any]:
    """Check overall data completeness"""
    stats = {}

    # Calculate completion rates for key fields
    key_fields = ['Title', 'Body (HTML)', 'Type', 'Variant Price']
    for field in key_fields:
        if field in df.columns:
            populated = df[df[field].astype(str).str.strip() != ''].shape[0]
            completion_rate = (populated / len(df)) * 100
            stats[f"{field}_completion_rate"] = round(completion_rate, 2)

    return {
        'status': 'passed',
        'stats': stats,
        'issues': []
    }


def calculate_quality_metrics(df: pd.DataFrame, config) -> Dict[str, Any]:
    """Calculate overall quality metrics"""
    metrics = {
        'total_products': len(df['Handle'].unique()),
        'total_variants': len(df),
        'data_completeness_score': 0,
        'seo_completeness_score': 0,
        'metafield_utilization_score': 0
    }

    # Calculate data completeness score
    key_fields = ['Title', 'Body (HTML)', 'Type', 'Variant Price', 'Image Src']
    completion_scores = []

    for field in key_fields:
        if field in df.columns:
            populated = df[df[field].astype(str).str.strip() != ''].shape[0]
            score = (populated / len(df)) * 100
            completion_scores.append(score)

    metrics['data_completeness_score'] = round(sum(completion_scores) / len(completion_scores), 2)

    # Calculate SEO completeness score
    main_products = df[df['Published'] == 'TRUE']
    if len(main_products) > 0:
        seo_fields = ['SEO Title', 'SEO Description']
        seo_scores = []

        for field in seo_fields:
            if field in df.columns:
                populated = main_products[main_products[field].astype(str).str.strip() != ''].shape[0]
                score = (populated / len(main_products)) * 100
                seo_scores.append(score)

        metrics['seo_completeness_score'] = round(sum(seo_scores) / len(seo_scores), 2)

    # Calculate metafield utilization score
    all_metafields = config.custom_metafields + config.shopify_metafields
    metafield_scores = []

    for metafield in all_metafields:
        if metafield in df.columns:
            populated = df[df[metafield].astype(str).str.strip() != ''].shape[0]
            score = (populated / len(df)) * 100
            metafield_scores.append(score)

    if metafield_scores:
        metrics['metafield_utilization_score'] = round(sum(metafield_scores) / len(metafield_scores), 2)

    return metrics


def create_validation_summary(validation_results: Dict, quality_metrics: Dict) -> Dict[str, Any]:
    """Create validation summary"""
    summary = {
        'overall_status': 'passed',
        'critical_issues_count': len(validation_results['critical_issues']),
        'warnings_count': len(validation_results['warnings']),
        'passed_checks_count': len(validation_results['passed_checks']),
        'quality_score': 0,
        'recommendations': []
    }

    # Determine overall status
    if summary['critical_issues_count'] > 0:
        summary['overall_status'] = 'failed'
    elif summary['warnings_count'] > 0:
        summary['overall_status'] = 'passed_with_warnings'

    # Calculate quality score (0-100)
    quality_score = (
        quality_metrics.get('data_completeness_score', 0) * 0.4 +
        quality_metrics.get('seo_completeness_score', 0) * 0.3 +
        quality_metrics.get('metafield_utilization_score', 0) * 0.3
    )
    summary['quality_score'] = round(quality_score, 2)

    # Generate recommendations
    if quality_metrics.get('data_completeness_score', 0) < 80:
        summary['recommendations'].append("Improve data completeness for key fields")

    if quality_metrics.get('seo_completeness_score', 0) < 70:
        summary['recommendations'].append("Add SEO titles and descriptions for better search visibility")

    if quality_metrics.get('metafield_utilization_score', 0) < 50:
        summary['recommendations'].append("Populate more metafields for better product categorization")

    return summary