"""
Step 03: HTML Processing and Cleaning

Processes and cleans HTML descriptions from Rakuten PC商品説明文.
Applies responsive table fixes, removes marketing content, and normalizes formatting.
"""

import logging
import pandas as pd
import re
from bs4 import BeautifulSoup, NavigableString
from typing import Dict, Any, List, Tuple

logger = logging.getLogger(__name__)


def execute(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process and clean HTML descriptions

    Args:
        data: Pipeline context containing sku_processed_df and config

    Returns:
        Dict containing dataframe with cleaned HTML
    """
    logger.info("Processing and cleaning HTML descriptions...")

    df = data['sku_processed_df'].copy()
    config = data['config']

    # Track HTML processing statistics
    html_stats = {
        'total_descriptions': 0,
        'empty_descriptions': 0,
        'ec_up_blocks_removed': 0,
        'marketing_content_removed': 0,
        'tables_made_responsive': 0,
        'images_processed': 0
    }

    # Process each description
    df['Body (HTML)'] = df['PC用商品説明文'].apply(
        lambda x: process_html_description(x, config, html_stats)
    )

    # Log HTML processing results
    logger.info(f"HTML processing completed")
    for key, value in html_stats.items():
        logger.info(f"HTML stats - {key}: {value}")

    return {
        'html_processed_df': df,
        'html_stats': html_stats
    }


def process_html_description(html_content: str, config, stats: Dict[str, Any]) -> str:
    """
    Process a single HTML description with all cleaning rules

    Args:
        html_content: Raw HTML from Rakuten
        config: Pipeline configuration
        stats: Statistics tracking dictionary

    Returns:
        Cleaned HTML string
    """
    stats['total_descriptions'] += 1

    if pd.isna(html_content) or not html_content.strip():
        stats['empty_descriptions'] += 1
        return ''

    try:
        # Parse HTML
        soup = BeautifulSoup(html_content, 'html.parser')

        # 1. Remove EC-UP blocks
        ec_up_removed = remove_ec_up_blocks(soup, config)
        stats['ec_up_blocks_removed'] += ec_up_removed

        # 2. Remove marketing content
        marketing_removed = remove_marketing_content(soup, config)
        stats['marketing_content_removed'] += marketing_removed

        # 3. Make tables responsive
        tables_fixed = make_tables_responsive(soup)
        stats['tables_made_responsive'] += tables_fixed

        # 4. Process images
        images_processed = process_images(soup, config)
        stats['images_processed'] += images_processed

        # 5. Apply scope class
        apply_scope_class(soup, config.scope_class)

        # 6. Clean up formatting
        clean_formatting(soup)

        # 7. Remove unwanted links
        remove_unwanted_links(soup, config)

        # Convert back to HTML string
        cleaned_html = str(soup)

        # Final cleanup with regex
        cleaned_html = final_regex_cleanup(cleaned_html)

        return cleaned_html

    except Exception as e:
        logger.warning(f"Error processing HTML: {e}")
        return str(html_content)  # Return original if processing fails


def remove_ec_up_blocks(soup: BeautifulSoup, config) -> int:
    """Remove EC-UP advertising blocks"""
    removed_count = 0

    # Find and remove EC-UP comment blocks
    for comment in soup.find_all(string=lambda text: isinstance(text, NavigableString) and 'EC-UP_' in str(text)):
        if comment.parent:
            comment.parent.decompose()
            removed_count += 1

    # Remove with regex pattern
    html_str = str(soup)
    original_len = len(html_str)
    html_str = config.ec_up_pattern.sub('', html_str)

    if len(html_str) < original_len:
        removed_count += 1
        soup.clear()
        soup.append(BeautifulSoup(html_str, 'html.parser'))

    return removed_count


def remove_marketing_content(soup: BeautifulSoup, config) -> int:
    """Remove marketing phrases and links"""
    removed_count = 0

    # Remove elements containing marketing phrases
    for pattern in config.marketing_patterns:
        for element in soup.find_all(string=lambda text: pattern.search(str(text)) if text else False):
            if element.parent:
                element.parent.decompose()
                removed_count += 1

    return removed_count


def make_tables_responsive(soup: BeautifulSoup) -> int:
    """Make tables responsive with overflow wrapper"""
    tables_fixed = 0

    for table in soup.find_all('table'):
        # Create wrapper div
        wrapper = soup.new_tag('div')
        wrapper['style'] = 'overflow-x: auto; margin: 1em 0;'

        # Wrap the table
        table.wrap(wrapper)

        # Add responsive attributes to table
        if not table.get('style'):
            table['style'] = ''

        table['style'] += 'min-width: 100%; border-collapse: collapse;'

        tables_fixed += 1

    return tables_fixed


def process_images(soup: BeautifulSoup, config) -> int:
    """Process and fix image URLs"""
    images_processed = 0

    for img in soup.find_all('img'):
        src = img.get('src')
        if src:
            # Fix gold URL pattern
            fixed_src = config.fix_gold_url(src)

            # Convert to absolute URL
            absolute_src = config.to_absolute_url(fixed_src)

            img['src'] = absolute_src

            # Add responsive attributes
            img['style'] = 'max-width: 100%; height: auto;'

            # Add alt text if missing
            if not img.get('alt'):
                img['alt'] = '商品画像'

            images_processed += 1

    return images_processed


def apply_scope_class(soup: BeautifulSoup, scope_class: str):
    """Apply CSS scope class to main content"""
    # Find the body or create a wrapper div
    body_content = soup.find('body')
    if not body_content:
        # Wrap all content in a div with scope class
        wrapper = soup.new_tag('div', **{'class': scope_class})

        # Move all content into wrapper
        contents = list(soup.children)
        for content in contents:
            if hasattr(content, 'extract'):
                content.extract()
                wrapper.append(content)

        soup.append(wrapper)
    else:
        # Add class to existing body
        existing_classes = body_content.get('class', [])
        if isinstance(existing_classes, str):
            existing_classes = [existing_classes]
        existing_classes.append(scope_class)
        body_content['class'] = existing_classes


def clean_formatting(soup: BeautifulSoup):
    """Clean up formatting and normalize fonts"""
    # Remove font tags and replace with spans
    for font_tag in soup.find_all('font'):
        font_tag.name = 'span'

        # Convert font attributes to style
        size = font_tag.get('size')
        color = font_tag.get('color')
        face = font_tag.get('face')

        style_parts = []
        if size:
            style_parts.append(f'font-size: {size}')
        if color:
            style_parts.append(f'color: {color}')
        if face:
            style_parts.append(f'font-family: {face}')

        if style_parts:
            existing_style = font_tag.get('style', '')
            new_style = '; '.join(style_parts)
            font_tag['style'] = f"{existing_style}; {new_style}".strip('; ')

        # Remove font attributes
        for attr in ['size', 'color', 'face']:
            if font_tag.get(attr):
                del font_tag[attr]

    # Normalize line breaks
    for br in soup.find_all('br'):
        # Ensure br tags are self-closing
        br.string = None


def remove_unwanted_links(soup: BeautifulSoup, config):
    """Remove unwanted links (Rakuten internal links)"""
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']

        # Check if link matches unwanted patterns
        should_remove = any(pattern.search(href) for pattern in config.link_patterns)

        if should_remove:
            # Replace link with its text content
            a_tag.replace_with(a_tag.get_text())


def final_regex_cleanup(html_str: str) -> str:
    """Final cleanup with regex patterns"""
    # Remove empty paragraphs
    html_str = re.sub(r'<p[^>]*>\s*</p>', '', html_str)

    # Remove excessive whitespace
    html_str = re.sub(r'\s+', ' ', html_str)

    # Clean up multiple line breaks
    html_str = re.sub(r'(<br[^>]*>\s*){3,}', '<br><br>', html_str)

    return html_str.strip()