"""
Step 14: Export Generation and Final Output

Generates the final Shopify CSV files with proper formatting and creates
comprehensive reports and logs for the conversion process.
"""

import logging
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate final exports and reports

    Args:
        data: Pipeline context containing validated_df and all stats

    Returns:
        Dict containing export results and file paths
    """
    logger.info("Generating final exports and reports...")

    df = data['validated_df'].copy()
    config = data['config']
    output_dir = data['output_dir']

    # Track export statistics
    export_stats = {
        'total_products_exported': len(df['Handle'].unique()),
        'total_variants_exported': len(df),
        'files_generated': 0,
        'reports_created': 0,
        'export_size_mb': 0
    }

    # Generate main CSV export
    main_csv_path = generate_main_csv(df, config, output_dir, export_stats)

    # Generate additional exports
    additional_exports = generate_additional_exports(df, config, output_dir, export_stats)

    # Generate comprehensive reports
    reports = generate_reports(data, output_dir, export_stats)

    # Create export summary
    export_summary = create_export_summary(data, export_stats, additional_exports, reports)

    # Log export results
    logger.info(f"Export generation completed")
    logger.info(f"Main CSV: {main_csv_path}")
    logger.info(f"Generated {export_stats['files_generated']} files")
    for key, value in export_stats.items():
        logger.info(f"Export stats - {key}: {value}")

    return {
        'export_completed': True,
        'main_csv_path': main_csv_path,
        'additional_exports': additional_exports,
        'reports': reports,
        'export_stats': export_stats,
        'export_summary': export_summary
    }


def generate_main_csv(df: pd.DataFrame, config, output_dir: Path, stats: Dict[str, Any]) -> str:
    """
    Generate the main Shopify CSV file

    Args:
        df: Final processed dataframe
        config: Pipeline configuration
        output_dir: Output directory
        stats: Statistics tracking dictionary

    Returns:
        Path to generated CSV file
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"shopify_products_{timestamp}.csv"
    csv_path = output_dir / csv_filename

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Apply final CSV formatting
    formatted_df = apply_final_csv_formatting(df, config)

    # Export to CSV with proper encoding and formatting
    formatted_df.to_csv(
        csv_path,
        index=False,
        encoding=config.output_encoding,
        quoting=1,  # Quote all fields
        escapechar='\\',
        lineterminator='\n'
    )

    # Calculate file size
    file_size_mb = csv_path.stat().st_size / (1024 * 1024)
    stats['export_size_mb'] = round(file_size_mb, 2)
    stats['files_generated'] += 1

    logger.info(f"Main CSV exported: {csv_path} ({file_size_mb:.2f} MB)")

    return str(csv_path)


def apply_final_csv_formatting(df: pd.DataFrame, config) -> pd.DataFrame:
    """
    Apply final CSV formatting rules

    Args:
        df: Dataframe to format
        config: Pipeline configuration

    Returns:
        Formatted dataframe
    """
    formatted_df = df.copy()

    # Apply special formatting for specific fields
    for column in formatted_df.columns:
        formatted_df[column] = formatted_df[column].apply(
            lambda x: config.format_csv_value(x, column)
        )

    return formatted_df


def generate_additional_exports(df: pd.DataFrame, config, output_dir: Path, stats: Dict[str, Any]) -> Dict[str, str]:
    """
    Generate additional export files

    Args:
        df: Final processed dataframe
        config: Pipeline configuration
        output_dir: Output directory
        stats: Statistics tracking dictionary

    Returns:
        Dict of additional export file paths
    """
    exports = {}
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1. Products summary (main products only)
    main_products_df = df[df['Published'] == 'TRUE'].copy()
    products_summary_path = output_dir / f"products_summary_{timestamp}.csv"

    summary_columns = [
        'Handle', 'Title', 'Type', 'Variant SKU', 'Variant Price',
        'Tags', 'SEO Title', 'SEO Description'
    ]

    available_columns = [col for col in summary_columns if col in main_products_df.columns]
    main_products_df[available_columns].to_csv(
        products_summary_path, index=False, encoding=config.output_encoding
    )

    exports['products_summary'] = str(products_summary_path)
    stats['files_generated'] += 1

    # 2. Variants export (all variants)
    variants_export_path = output_dir / f"all_variants_{timestamp}.csv"

    variant_columns = [
        'Handle', 'Variant SKU', 'Option1 Name', 'Option1 Value',
        'Variant Price', 'Variant Inventory Qty', 'Published'
    ]

    available_variant_columns = [col for col in variant_columns if col in df.columns]
    df[available_variant_columns].to_csv(
        variants_export_path, index=False, encoding=config.output_encoding
    )

    exports['variants_export'] = str(variants_export_path)
    stats['files_generated'] += 1

    # 3. Metafields export (non-empty metafields only)
    metafields_export_path = output_dir / f"metafields_data_{timestamp}.csv"

    all_metafields = config.custom_metafields + config.shopify_metafields
    metafield_columns = ['Handle', 'Variant SKU'] + [col for col in all_metafields if col in df.columns]

    # Filter to rows with at least one non-empty metafield
    metafield_df = df[metafield_columns].copy()
    metafield_mask = False

    for metafield in all_metafields:
        if metafield in metafield_df.columns:
            metafield_mask |= (metafield_df[metafield].astype(str).str.strip() != '')

    if metafield_mask.any():
        metafield_df[metafield_mask].to_csv(
            metafields_export_path, index=False, encoding=config.output_encoding
        )
        exports['metafields_export'] = str(metafields_export_path)
        stats['files_generated'] += 1

    # 4. Images export (products with images)
    images_export_path = output_dir / f"product_images_{timestamp}.csv"

    image_columns = [
        'Handle', 'Variant SKU', 'Image Src', 'Image Position', 'Image Alt Text'
    ]

    available_image_columns = [col for col in image_columns if col in df.columns]
    images_df = df[available_image_columns].copy()

    # Filter to products with images
    has_images = images_df['Image Src'].astype(str).str.strip() != ''
    if has_images.any():
        images_df[has_images].to_csv(
            images_export_path, index=False, encoding=config.output_encoding
        )
        exports['images_export'] = str(images_export_path)
        stats['files_generated'] += 1

    return exports


def generate_reports(data: Dict[str, Any], output_dir: Path, stats: Dict[str, Any]) -> Dict[str, str]:
    """
    Generate comprehensive reports

    Args:
        data: Complete pipeline data
        output_dir: Output directory
        stats: Statistics tracking dictionary

    Returns:
        Dict of report file paths
    """
    reports = {}
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1. Pipeline execution report
    pipeline_report_path = generate_pipeline_report(data, output_dir, timestamp)
    reports['pipeline_report'] = pipeline_report_path
    stats['reports_created'] += 1

    # 2. Data transformation summary
    transformation_report_path = generate_transformation_report(data, output_dir, timestamp)
    reports['transformation_report'] = transformation_report_path
    stats['reports_created'] += 1

    # 3. Quality validation report
    validation_report_path = generate_validation_report(data, output_dir, timestamp)
    reports['validation_report'] = validation_report_path
    stats['reports_created'] += 1

    # 4. Metafield usage report
    metafield_report_path = generate_metafield_report(data, output_dir, timestamp)
    reports['metafield_report'] = metafield_report_path
    stats['reports_created'] += 1

    return reports


def generate_pipeline_report(data: Dict[str, Any], output_dir: Path, timestamp: str) -> str:
    """Generate comprehensive pipeline execution report"""
    report_path = output_dir / f"pipeline_execution_report_{timestamp}.json"

    # Collect all statistics from pipeline steps
    pipeline_report = {
        'execution_timestamp': timestamp,
        'input_file': str(data.get('input_file', '')),
        'total_execution_time': data.get('stats', {}).get('start_time', 0),
        'pipeline_steps': {},
        'final_statistics': {}
    }

    # Collect statistics from each step
    step_stats = [
        'validation_stats', 'cleaning_stats', 'sku_stats', 'html_stats',
        'image_stats', 'mapping_stats', 'tax_stats', 'type_stats',
        'grouping_stats', 'attribute_stats', 'description_stats',
        'formatting_stats', 'header_stats', 'validation_stats'
    ]

    for stat_key in step_stats:
        if stat_key in data:
            pipeline_report['pipeline_steps'][stat_key] = data[stat_key]

    # Final statistics
    if 'validated_df' in data:
        df = data['validated_df']
        pipeline_report['final_statistics'] = {
            'total_products': len(df['Handle'].unique()),
            'total_variants': len(df),
            'published_products': len(df[df['Published'] == 'TRUE']),
            'products_with_images': len(df[df['Image Src'].astype(str).str.strip() != '']),
            'products_with_descriptions': len(df[df['Body (HTML)'].astype(str).str.strip() != ''])
        }

    # Save report
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(pipeline_report, f, indent=2, ensure_ascii=False, default=str)

    return str(report_path)


def generate_transformation_report(data: Dict[str, Any], output_dir: Path, timestamp: str) -> str:
    """Generate data transformation summary report"""
    report_path = output_dir / f"transformation_summary_{timestamp}.txt"

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("Rakuten to Shopify Transformation Summary\n")
        f.write("=" * 50 + "\n\n")

        # Input data summary
        if 'validation_stats' in data:
            stats = data['validation_stats']
            f.write(f"Input Data:\n")
            f.write(f"  Total rows: {stats.get('total_rows', 'N/A')}\n")
            f.write(f"  Unique SKUs: {stats.get('unique_skus', 'N/A')}\n")
            f.write(f"  Columns: {stats.get('columns_count', 'N/A')}\n\n")

        # Cleaning summary
        if 'cleaning_stats' in data:
            stats = data['cleaning_stats']
            f.write(f"Data Cleaning:\n")
            f.write(f"  Rows removed: {stats.get('total_rows_removed', 'N/A')}\n")
            f.write(f"  Removal percentage: {stats.get('removal_percentage', 'N/A')}%\n\n")

        # Final output summary
        if 'export_stats' in data:
            stats = data['export_stats']
            f.write(f"Final Output:\n")
            f.write(f"  Products exported: {stats.get('total_products_exported', 'N/A')}\n")
            f.write(f"  Variants exported: {stats.get('total_variants_exported', 'N/A')}\n")
            f.write(f"  Export size: {stats.get('export_size_mb', 'N/A')} MB\n")

    return str(report_path)


def generate_validation_report(data: Dict[str, Any], output_dir: Path, timestamp: str) -> str:
    """Generate quality validation report"""
    report_path = output_dir / f"quality_validation_report_{timestamp}.json"

    validation_report = {
        'timestamp': timestamp,
        'validation_summary': data.get('validation_summary', {}),
        'quality_metrics': data.get('quality_metrics', {}),
        'validation_results': data.get('validation_results', {})
    }

    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(validation_report, f, indent=2, ensure_ascii=False, default=str)

    return str(report_path)


def generate_metafield_report(data: Dict[str, Any], output_dir: Path, timestamp: str) -> str:
    """Generate metafield usage report"""
    report_path = output_dir / f"metafield_usage_report_{timestamp}.csv"

    if 'validated_df' in data and 'config' in data:
        df = data['validated_df']
        config = data['config']

        # Create metafield usage summary
        metafield_data = []
        all_metafields = config.custom_metafields + config.shopify_metafields

        for metafield in all_metafields:
            if metafield in df.columns:
                populated_count = len(df[df[metafield].astype(str).str.strip() != ''])
                usage_rate = (populated_count / len(df)) * 100

                metafield_data.append({
                    'metafield': metafield,
                    'type': 'custom' if metafield in config.custom_metafields else 'shopify',
                    'populated_count': populated_count,
                    'total_variants': len(df),
                    'usage_rate_percent': round(usage_rate, 2)
                })

        if metafield_data:
            metafield_df = pd.DataFrame(metafield_data)
            metafield_df = metafield_df.sort_values('usage_rate_percent', ascending=False)
            metafield_df.to_csv(report_path, index=False, encoding='utf-8')

    return str(report_path)


def create_export_summary(data: Dict[str, Any], export_stats: Dict[str, Any],
                         additional_exports: Dict[str, str], reports: Dict[str, str]) -> Dict[str, Any]:
    """Create comprehensive export summary"""
    summary = {
        'export_timestamp': datetime.now().isoformat(),
        'success': True,
        'statistics': export_stats,
        'files_generated': {
            'main_csv': True,
            'additional_exports': len(additional_exports),
            'reports': len(reports)
        },
        'data_summary': {},
        'recommendations': []
    }

    # Add data summary
    if 'validated_df' in data:
        df = data['validated_df']
        summary['data_summary'] = {
            'total_products': len(df['Handle'].unique()),
            'total_variants': len(df),
            'average_variants_per_product': round(len(df) / len(df['Handle'].unique()), 2)
        }

    # Add recommendations based on validation results
    if 'validation_summary' in data:
        validation = data['validation_summary']
        quality_score = validation.get('quality_score', 0)

        if quality_score < 80:
            summary['recommendations'].append("Consider improving data quality before import")

        if validation.get('critical_issues_count', 0) > 0:
            summary['recommendations'].append("Resolve critical issues before proceeding with import")

        if validation.get('warnings_count', 0) > 0:
            summary['recommendations'].append("Review and address validation warnings")

    return summary