"""
Step 10: Description Finalization

Finalizes product descriptions by creating SEO titles and descriptions,
and applying final formatting to the HTML body content.
"""

import logging
import pandas as pd
import re
from typing import Dict, Any

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Finalize product descriptions and SEO content

    Args:
        data: Pipeline context containing attribute_processed_df and config

    Returns:
        Dict containing dataframe with finalized descriptions
    """
    logger.info("Finalizing product descriptions and SEO content...")

    df = data['attribute_processed_df'].copy()
    config = data['config']

    # Track description finalization statistics
    description_stats = {
        'total_products': len(df),
        'seo_titles_generated': 0,
        'seo_descriptions_generated': 0,
        'html_descriptions_finalized': 0,
        'empty_descriptions': 0
    }

    # Process descriptions for main products only (Published = TRUE)
    main_products_mask = df['Published'] == 'TRUE'
    main_products = df[main_products_mask]

    # Generate SEO titles
    df.loc[main_products_mask, 'SEO Title'] = main_products.apply(
        lambda row: generate_seo_title(row, config, description_stats),
        axis=1
    )

    # Generate SEO descriptions
    df.loc[main_products_mask, 'SEO Description'] = main_products.apply(
        lambda row: generate_seo_description(row, config, description_stats),
        axis=1
    )

    # Finalize HTML body content
    df['Body (HTML)'] = df.apply(
        lambda row: finalize_html_body(row, config, description_stats),
        axis=1
    )

    # Log description finalization results
    logger.info(f"Description finalization completed")
    for key, value in description_stats.items():
        logger.info(f"Description stats - {key}: {value}")

    return {
        'description_finalized_df': df,
        'description_stats': description_stats
    }


def generate_seo_title(row: pd.Series, config, stats: Dict[str, Any]) -> str:
    """
    Generate SEO title for a product

    Args:
        row: Product row
        config: Pipeline configuration
        stats: Statistics tracking dictionary

    Returns:
        Generated SEO title
    """
    title = row.get('Title', '')
    if not title:
        return ''

    # Basic SEO title (60 character limit)
    base_title = str(title).strip()

    # Add category if available and space permits
    product_type = row.get('Type', '')
    if product_type and len(base_title) < 40:
        seo_title = f"{base_title} | {product_type}"
    else:
        seo_title = base_title

    # Truncate if too long
    if len(seo_title) > 60:
        seo_title = seo_title[:57] + '...'

    stats['seo_titles_generated'] += 1
    return seo_title


def generate_seo_description(row: pd.Series, config, stats: Dict[str, Any]) -> str:
    """
    Generate SEO description for a product

    Args:
        row: Product row
        config: Pipeline configuration
        stats: Statistics tracking dictionary

    Returns:
        Generated SEO description
    """
    # Components for SEO description
    title = row.get('Title', '')
    product_type = row.get('Type', '')
    catch_copy = row.get('PC用キャッチコピー', '')

    # Extract key features from metafields
    key_features = extract_key_features(row, config)

    # Build description (160 character limit)
    description_parts = []

    if title:
        description_parts.append(str(title).strip())

    if catch_copy:
        clean_catch = clean_text_for_seo(str(catch_copy))
        if clean_catch and len(clean_catch) < 100:
            description_parts.append(clean_catch)

    if key_features:
        features_text = '、'.join(key_features[:3])  # Max 3 features
        description_parts.append(features_text)

    if product_type:
        description_parts.append(f"{product_type}をお探しの方におすすめ")

    # Combine and truncate
    seo_description = '。'.join(description_parts)

    if len(seo_description) > 160:
        seo_description = seo_description[:157] + '...'

    stats['seo_descriptions_generated'] += 1
    return seo_description


def extract_key_features(row: pd.Series, config) -> list:
    """
    Extract key features from metafields for SEO

    Args:
        row: Product row
        config: Pipeline configuration

    Returns:
        List of key features
    """
    features = []

    # Priority metafields for SEO
    priority_metafields = [
        '[絞込み]ブランド・メーカー (product.metafields.custom.brand)',
        '[絞込み]こだわり・認証 (product.metafields.custom.commitment)',
        '[絞込み]味・香り・フレーバー (product.metafields.custom.flavor)',
        '[絞込み]容量・サイズ (product.metafields.custom.search_size)'
    ]

    for metafield in priority_metafields:
        if metafield in row and pd.notna(row[metafield]):
            value = str(row[metafield]).strip()
            if value and len(value) < 20:  # Keep features short
                features.append(value)

    return features


def finalize_html_body(row: pd.Series, config, stats: Dict[str, Any]) -> str:
    """
    Finalize HTML body content

    Args:
        row: Product row
        config: Pipeline configuration
        stats: Statistics tracking dictionary

    Returns:
        Finalized HTML body
    """
    html_body = row.get('Body (HTML)', '')

    if not html_body or pd.isna(html_body):
        stats['empty_descriptions'] += 1
        return ''

    # Apply final HTML formatting
    finalized_html = apply_final_html_formatting(str(html_body), config)

    stats['html_descriptions_finalized'] += 1
    return finalized_html


def apply_final_html_formatting(html_content: str, config) -> str:
    """
    Apply final formatting to HTML content

    Args:
        html_content: HTML content to format
        config: Pipeline configuration

    Returns:
        Formatted HTML content
    """
    if not html_content:
        return ''

    # Ensure proper scope class wrapping
    if config.scope_class not in html_content:
        html_content = f'<div class="{config.scope_class}">{html_content}</div>'

    # Final cleanup patterns
    html_content = final_html_cleanup(html_content)

    return html_content


def final_html_cleanup(html_content: str) -> str:
    """
    Apply final HTML cleanup patterns

    Args:
        html_content: HTML content to clean

    Returns:
        Cleaned HTML content
    """
    # Remove excessive whitespace
    html_content = re.sub(r'\s+', ' ', html_content)

    # Remove empty tags
    html_content = re.sub(r'<([^>]+)>\s*</\1>', '', html_content)

    # Normalize line breaks
    html_content = re.sub(r'(<br[^>]*>\s*){3,}', '<br><br>', html_content)

    # Remove script tags if any remain
    html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)

    # Remove style tags
    html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)

    return html_content.strip()


def clean_text_for_seo(text: str) -> str:
    """
    Clean text for SEO usage

    Args:
        text: Raw text to clean

    Returns:
        Cleaned text suitable for SEO
    """
    if not text:
        return ''

    # Remove HTML tags
    clean_text = re.sub(r'<[^>]+>', '', text)

    # Remove excessive whitespace
    clean_text = re.sub(r'\s+', ' ', clean_text)

    # Remove special characters that don't work well in SEO
    clean_text = re.sub(r'[^\w\s\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF、。！？]', '', clean_text)

    return clean_text.strip()


def validate_seo_content(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate SEO content quality

    Args:
        df: Dataframe with SEO content

    Returns:
        Validation results
    """
    validation = {
        'total_products': len(df[df['Published'] == 'TRUE']),
        'seo_title_issues': [],
        'seo_description_issues': [],
        'html_body_issues': []
    }

    main_products = df[df['Published'] == 'TRUE']

    for _, row in main_products.iterrows():
        handle = row.get('Handle', '')
        seo_title = row.get('SEO Title', '')
        seo_description = row.get('SEO Description', '')
        html_body = row.get('Body (HTML)', '')

        # Check SEO title
        if not seo_title:
            validation['seo_title_issues'].append({'handle': handle, 'issue': 'Missing SEO title'})
        elif len(seo_title) > 60:
            validation['seo_title_issues'].append({'handle': handle, 'issue': 'SEO title too long'})

        # Check SEO description
        if not seo_description:
            validation['seo_description_issues'].append({'handle': handle, 'issue': 'Missing SEO description'})
        elif len(seo_description) > 160:
            validation['seo_description_issues'].append({'handle': handle, 'issue': 'SEO description too long'})

        # Check HTML body
        if not html_body:
            validation['html_body_issues'].append({'handle': handle, 'issue': 'Missing HTML body'})

    return validation


def create_description_report(df: pd.DataFrame, output_dir) -> str:
    """
    Create description quality report

    Args:
        df: Dataframe with descriptions
        output_dir: Output directory path

    Returns:
        Path to report file
    """
    report_file = output_dir / 'description_quality_report.csv'

    # Create report data
    report_data = []
    main_products = df[df['Published'] == 'TRUE']

    for _, row in main_products.iterrows():
        seo_title = row.get('SEO Title', '')
        seo_description = row.get('SEO Description', '')
        html_body = row.get('Body (HTML)', '')

        report_data.append({
            'handle': row.get('Handle', ''),
            'title': row.get('Title', ''),
            'seo_title_length': len(seo_title),
            'seo_description_length': len(seo_description),
            'has_html_body': bool(html_body),
            'html_body_length': len(html_body),
            'seo_title_ok': len(seo_title) <= 60,
            'seo_description_ok': len(seo_description) <= 160
        })

    if report_data:
        report_df = pd.DataFrame(report_data)
        report_df.to_csv(report_file, index=False, encoding='utf-8')
        logger.info(f"Description quality report exported to {report_file}")

    return str(report_file)