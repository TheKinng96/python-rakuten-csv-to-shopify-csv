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

    df = data['shopify_df'].copy()
    config = data['config']

    # Track HTML processing statistics
    html_stats = {
        'total_descriptions': 0,
        'empty_descriptions': 0,
        'ec_up_blocks_removed': 0,
        'marketing_content_removed': 0,
        'tables_made_responsive': 0,
        'images_found': 0
    }

    # Collect all image URLs for later Shopify CDN processing
    image_urls = []

    # Process each description
    def process_with_image_collection(html_content):
        result = process_html_description(html_content, config, html_stats, image_urls)
        return result

    df['Body (HTML)'] = df['Body (HTML)'].apply(process_with_image_collection)

    # Log HTML processing results
    logger.info(f"HTML processing completed")
    for key, value in html_stats.items():
        logger.info(f"HTML stats - {key}: {value}")

    # Log image collection results
    logger.info(f"Collected {len(image_urls)} unique image URLs from HTML descriptions")

    # Export image URLs to JSON file for easy access
    import json
    from pathlib import Path

    output_dir = Path("step_output")
    output_dir.mkdir(exist_ok=True)

    image_urls_data = {
        'timestamp': '2025-09-18',
        'source': 'HTML descriptions after EC-UP removal and cleaning (Step 03)',
        'note': 'These URLs are extracted from cleaned HTML - EC-UP promotional blocks removed',
        'stats': {
            'total_descriptions': html_stats['total_descriptions'],
            'descriptions_with_images': len([url for url in image_urls if url]),
            'unique_image_urls': len(image_urls)
        },
        'image_urls': image_urls
    }

    json_file = output_dir / "step_03_cleaned_image_urls.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(image_urls_data, f, indent=2, ensure_ascii=False)

    logger.info(f"Exported cleaned image URLs to: {json_file}")

    return {
        'html_processed_df': df,
        'html_stats': html_stats,
        'collected_image_urls': image_urls
    }


def process_html_description(html_content: str, config, stats: Dict[str, Any], image_urls: List[str] = None) -> str:
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
        images_found = process_images(soup, config, image_urls)
        stats['images_found'] += images_found

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

    # Convert to string for regex processing
    html_str = str(soup)
    original_html = html_str

    # Define EC-UP removal patterns
    ec_up_patterns = [
        # Remove complete EC-UP blocks with content
        r'<!--EC-UP_[^>]*?START-->(.*?)<!--EC-UP_[^>]*?END-->',
        # Remove any remaining EC-UP comments
        r'<!--EC-UP_[^>]*?-->',
        # Remove EC-UP style blocks
        r'<style[^>]*?ecup[^>]*?>(.*?)</style>',
        # Remove EC-UP div containers
        r'<div[^>]*?class="[^"]*ecup[^"]*"[^>]*?>(.*?)</div>',
    ]

    # Apply each pattern
    for pattern in ec_up_patterns:
        matches_before = len(re.findall(pattern, html_str, re.DOTALL | re.IGNORECASE))
        html_str = re.sub(pattern, '', html_str, flags=re.DOTALL | re.IGNORECASE)
        if matches_before > 0:
            removed_count += matches_before
            logger.debug(f"Removed {matches_before} EC-UP blocks with pattern: {pattern[:50]}...")

    # Clean up extra whitespace left by removals
    html_str = re.sub(r'\n\s*\n\s*\n', '\n\n', html_str)
    html_str = re.sub(r'<br[^>]*>\s*<br[^>]*>\s*<br[^>]*>', '<br><br>', html_str)

    # Update soup with cleaned HTML if changes were made
    if html_str != original_html:
        soup.clear()
        new_soup = BeautifulSoup(html_str, 'html.parser')
        for element in new_soup.contents:
            soup.append(element)
        logger.debug(f"EC-UP removal: {len(original_html)} → {len(html_str)} chars")

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
    """Make tables responsive with proper mobile CSS wrapper"""
    tables_fixed = 0

    for table in soup.find_all('table'):
        # Create responsive wrapper div with comprehensive mobile styles
        wrapper = soup.new_tag('div')
        wrapper['style'] = (
            'overflow-x: auto; '
            'margin: 1em 0; '
            '-webkit-overflow-scrolling: touch; '
            'border: 1px solid #ddd; '
            'border-radius: 4px;'
        )

        # Wrap the table
        table.wrap(wrapper)

        # Add comprehensive responsive attributes to table
        table_style = (
            'min-width: 100%; '
            'border-collapse: collapse; '
            'font-size: 14px; '
            'line-height: 1.4;'
        )

        if table.get('style'):
            table['style'] = f"{table['style']}; {table_style}"
        else:
            table['style'] = table_style

        # Add responsive styling to table cells
        for cell in table.find_all(['td', 'th']):
            cell_style = (
                'padding: 8px 12px; '
                'border: 1px solid #ddd; '
                'word-wrap: break-word; '
                'max-width: 200px;'
            )

            if cell.get('style'):
                cell['style'] = f"{cell['style']}; {cell_style}"
            else:
                cell['style'] = cell_style

        tables_fixed += 1

    return tables_fixed


def process_images(soup: BeautifulSoup, config, image_urls: List[str] = None) -> int:
    """Collect image URLs for later Shopify CDN processing"""
    images_found = 0

    for img in soup.find_all('img'):
        src = img.get('src')
        if src:
            # Fix gold URL pattern
            fixed_src = config.fix_gold_url(src)

            # Convert to absolute URL
            absolute_src = config.to_absolute_url(fixed_src)

            # Store original URL for later CDN migration
            img['data-original-src'] = absolute_src
            # Keep current src for now
            img['src'] = absolute_src

            # Collect unique image URLs for later processing
            if image_urls is not None and absolute_src not in image_urls:
                image_urls.append(absolute_src)

            # Add responsive attributes
            img['style'] = 'max-width: 100%; height: auto;'

            # Add alt text if missing
            if not img.get('alt'):
                img['alt'] = '商品画像'

            images_found += 1

    return images_found


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