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

    # Work directly on the existing DataFrame to ensure changes persist
    df = data['shopify_df']
    config = data['config']

    # Track HTML processing statistics
    html_stats = {
        'total_descriptions': 0,
        'empty_descriptions': 0,
        'ec_up_blocks_removed': 0,
        'marketing_content_removed': 0,
        'tables_made_responsive': 0,
        'images_found': 0,
        'rakuten_links_removed': 0,
        'empty_containers_removed': 0
    }

    # Collect all image URLs for later Shopify CDN processing
    image_urls = []


    # First pass: Remove EC-UP blocks and search.rakuten.co.jp links from all descriptions
    def clean_ec_up_and_search_links(html_content):
        """Apply EC-UP removal and search.rakuten.co.jp link removal"""
        if html_content is None or pd.isna(html_content) or not str(html_content).strip():
            return ''

        html_str = str(html_content)

        # Remove EC-UP blocks
        cleaned_html, ec_up_removed = remove_ec_up_blocks_from_string(html_str, config)
        html_stats['ec_up_blocks_removed'] += ec_up_removed

        # Remove rakuten links (search.rakuten.co.jp and item.rakuten.co.jp)
        search_links_removed = remove_search_rakuten_links(cleaned_html)
        html_stats['rakuten_links_removed'] += search_links_removed
        cleaned_html = remove_search_rakuten_links_regex(cleaned_html)

        # Clean up empty containers after link removal
        containers_removed = remove_empty_containers_regex(cleaned_html)
        html_stats['empty_containers_removed'] += containers_removed
        cleaned_html = remove_empty_containers_regex_cleanup(cleaned_html)

        return cleaned_html

    df['Body (HTML)'] = df['Body (HTML)'].apply(clean_ec_up_and_search_links)

    # Second pass: Apply other processing only to non-empty descriptions
    def process_with_image_collection(html_content):
        if html_content is None or pd.isna(html_content) or not str(html_content).strip():
            html_stats['empty_descriptions'] += 1
            return ''

        html_stats['total_descriptions'] += 1
        html_content = str(html_content)

        # Skip processing if content is just 'None' string
        if html_content.strip().lower() == 'none':
            return ''

        try:
            # Parse HTML (EC-UP already removed)
            soup = BeautifulSoup(html_content, 'html.parser')

            # Remove marketing content
            marketing_removed = remove_marketing_content(soup, config)
            html_stats['marketing_content_removed'] += marketing_removed

            # Make tables responsive
            tables_fixed = make_tables_responsive(soup)
            html_stats['tables_made_responsive'] += tables_fixed

            # Process images
            images_found = process_images(soup, config, image_urls)
            html_stats['images_found'] += images_found

            # Add mobile responsiveness
            make_mobile_responsive(soup)

            # Apply scope class
            apply_scope_class(soup, 'shopify-product-description')

            # Clean up formatting
            clean_formatting(soup)

            # Remove remaining unwanted links (non-search.rakuten.co.jp) and empty containers
            links_removed, containers_removed = remove_unwanted_links(soup, config)
            # Note: search.rakuten.co.jp links already removed in first pass

            # Convert back to HTML string
            try:
                # First extract style tags as they should be at the beginning
                style_tags = soup.find_all('style') if soup else []
                style_content = ''
                if style_tags:
                    for style_tag in style_tags:
                        if style_tag:
                            style_content += str(style_tag)
                            style_tag.extract()  # Remove from soup temporarily

                # Get the remaining content
                if soup and soup.find():  # Check soup is valid
                    content_html = str(soup)
                else:
                    content_html = html_content if isinstance(html_content, str) else ''

                # Wrap content in responsive div
                wrapped_content = f'<div style="width: 100%; overflow-x: auto; -webkit-overflow-scrolling: touch;">\n{content_html}\n</div>'

                # Combine style tags at beginning with wrapped content
                cleaned_html = style_content + wrapped_content

                # Final cleanup with regex
                cleaned_html = final_regex_cleanup(cleaned_html)

                return cleaned_html

            except Exception as conv_err:
                logger.warning(f"Error converting HTML back to string: {conv_err}")
                # Return whatever we can salvage
                if soup:
                    try:
                        return str(soup)
                    except:
                        pass
                return str(html_content) if html_content else ''

        except Exception as e:
            logger.warning(f"Error processing HTML: {e}")
            return str(html_content) if html_content else ''  # Return EC-UP cleaned content

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

    # Debug: Verify DataFrame changes before returning
    sample_html = str(df['Body (HTML)'].head(10).to_string())
    ec_up_count = sample_html.count('EC-UP')
    total_ec_up = str(df['Body (HTML)'].to_string()).count('EC-UP')
    logger.info(f"DEBUG: DataFrame before return has {ec_up_count} EC-UP in first 10 rows, {total_ec_up} total")

    # CRITICAL: Also update the input data dict to ensure step runner gets changes
    data['html_processed_df'] = df

    return {
        'html_processed_df': df,
        'html_stats': html_stats,
        'collected_image_urls': image_urls
    }




def remove_ec_up_blocks_from_string(html_str: str, config) -> tuple[str, int]:
    """
    Remove EC-UP promotional blocks from HTML string

    Args:
        html_str: HTML content as string
        config: Pipeline configuration

    Returns:
        Tuple of (cleaned_html_string, removed_count)
    """
    if not html_str:
        return html_str, 0

    original_html = html_str
    removed_count = 0

    # EC-UP block patterns - fixed to capture entire blocks including tags
    ec_up_patterns = [
        # Complete EC-UP blocks from START to END (including the comment tags)
        r'<!--EC-UP_[^>]*?START-->.*?<!--EC-UP_[^>]*?END-->',
        # Standalone EC-UP comments
        r'<!--EC-UP_[^>]*?-->',
        # EC-UP style blocks (complete tags)
        r'<style[^>]*?ecup[^>]*?>.*?</style>',
        # EC-UP div containers (complete tags)
        r'<div[^>]*?class="[^"]*ecup[^"]*"[^>]*?>.*?</div>',
    ]

    # Apply each pattern
    for pattern in ec_up_patterns:
        matches_before = len(re.findall(pattern, html_str, re.DOTALL | re.IGNORECASE))
        html_str = re.sub(pattern, '', html_str, flags=re.DOTALL | re.IGNORECASE)
        if matches_before > 0:
            removed_count += matches_before

    # Clean up extra whitespace left by removals
    html_str = re.sub(r'\n\s*\n\s*\n', '\n\n', html_str)
    html_str = re.sub(r'<br[^>]*>\s*<br[^>]*>\s*<br[^>]*>', '<br><br>', html_str)

    return html_str, removed_count


def remove_search_rakuten_links(html_str: str) -> int:
    """Count all rakuten domain links for statistics"""
    if not html_str:
        return 0

    # Count all rakuten domain links (including commented-out ones)
    search_patterns = [
        # Regular anchor tags - search.rakuten.co.jp
        r'<a[^>]*href=["\']?[^"\']*search\.rakuten\.co\.jp[^"\']*["\']?[^>]*>.*?</a>',
        r'<a[^>]*href=""[^"]*search\.rakuten\.co\.jp[^"]*""[^>]*>.*?</a>',
        # Regular anchor tags - item.rakuten.co.jp
        r'<a[^>]*href=["\']?[^"\']*item\.rakuten\.co\.jp[^"\']*["\']?[^>]*>.*?</a>',
        r'<a[^>]*href=""[^"]*item\.rakuten\.co\.jp[^"]*""[^>]*>.*?</a>',
        # Regular anchor tags - www.rakuten.ne.jp
        r'<a[^>]*href=["\']?[^"\']*www\.rakuten\.ne\.jp[^"\']*["\']?[^>]*>.*?</a>',
        r'<a[^>]*href=""[^"]*www\.rakuten\.ne\.jp[^"]*""[^>]*>.*?</a>',
        # Commented-out links - search.rakuten.co.jp
        r'<!--a[^>]*?search\.rakuten\.co\.jp.*?</a-->',
        r'<!--a.*?search\.rakuten\.co\.jp.*?a-->',
        # Commented-out links - item.rakuten.co.jp
        r'<!--a[^>]*?item\.rakuten\.co\.jp.*?</a-->',
        r'<!--a.*?item\.rakuten\.co\.jp.*?a-->',
        # Commented-out links - www.rakuten.ne.jp
        r'<!--a[^>]*?www\.rakuten\.ne\.jp.*?</a-->',
        r'<!--a.*?www\.rakuten\.ne\.jp.*?a-->',
        # Broader patterns for any remaining rakuten domains (excluding image.rakuten.co.jp)
        r'<a[^>]*href=["\']?[^"\']*(?:item|search|www)\.rakuten\.[^"\']*["\']?[^>]*>.*?</a>',
        r'<!--a[^>]*?(?:item|search|www)\.rakuten\..*?</a-->',
        # Image tags with rakuten domains (excluding image.rakuten.co.jp)
        r'<img[^>]*src=["\']?[^"\']*www\.rakuten\.ne\.jp[^"\']*["\']?[^>]*>',
    ]

    count = 0
    for pattern in search_patterns:
        matches = len(re.findall(pattern, html_str, re.DOTALL | re.IGNORECASE))
        count += matches

    return count


def remove_search_rakuten_links_regex(html_str: str) -> str:
    """Remove all rakuten domain links using regex"""
    if not html_str:
        return html_str

    # Pattern to match all rakuten domain links (excluding image.rakuten.co.jp)
    search_patterns = [
        # Regular anchor tags - search.rakuten.co.jp
        r'<a[^>]*href=["\']?[^"\']*search\.rakuten\.co\.jp[^"\']*["\']?[^>]*>.*?</a>',
        r'<a[^>]*href=""[^"]*search\.rakuten\.co\.jp[^"]*""[^>]*>.*?</a>',
        # Regular anchor tags - item.rakuten.co.jp
        r'<a[^>]*href=["\']?[^"\']*item\.rakuten\.co\.jp[^"\']*["\']?[^>]*>.*?</a>',
        r'<a[^>]*href=""[^"]*item\.rakuten\.co\.jp[^"]*""[^>]*>.*?</a>',
        # Regular anchor tags - www.rakuten.ne.jp
        r'<a[^>]*href=["\']?[^"\']*www\.rakuten\.ne\.jp[^"\']*["\']?[^>]*>.*?</a>',
        r'<a[^>]*href=""[^"]*www\.rakuten\.ne\.jp[^"]*""[^>]*>.*?</a>',
        # Commented-out links - search.rakuten.co.jp
        r'<!--a[^>]*?search\.rakuten\.co\.jp.*?</a-->',
        r'<!--a.*?search\.rakuten\.co\.jp.*?a-->',
        # Commented-out links - item.rakuten.co.jp
        r'<!--a[^>]*?item\.rakuten\.co\.jp.*?</a-->',
        r'<!--a.*?item\.rakuten\.co\.jp.*?a-->',
        # Commented-out links - www.rakuten.ne.jp
        r'<!--a[^>]*?www\.rakuten\.ne\.jp.*?</a-->',
        r'<!--a.*?www\.rakuten\.ne\.jp.*?a-->',
        # Broader patterns for any remaining rakuten domains (excluding image.rakuten.co.jp)
        r'<a[^>]*href=["\']?[^"\']*(?:item|search|www)\.rakuten\.[^"\']*["\']?[^>]*>.*?</a>',
        r'<!--a[^>]*?(?:item|search|www)\.rakuten\..*?</a-->',
        # Image tags with rakuten domains (excluding image.rakuten.co.jp)
        r'<img[^>]*src=["\']?[^"\']*www\.rakuten\.ne\.jp[^"\']*["\']?[^>]*>',
    ]

    for pattern in search_patterns:
        html_str = re.sub(pattern, '', html_str, flags=re.DOTALL | re.IGNORECASE)

    return html_str


def remove_empty_containers_regex(html_str: str) -> int:
    """Count empty containers for statistics"""
    if not html_str:
        return 0

    # Count empty div, span, p tags
    empty_patterns = [
        r'<div[^>]*>\s*</div>',
        r'<span[^>]*>\s*</span>',
        r'<p[^>]*>\s*</p>',
        r'<section[^>]*>\s*</section>',
        r'<article[^>]*>\s*</article>',
    ]

    count = 0
    for pattern in empty_patterns:
        matches = len(re.findall(pattern, html_str, re.DOTALL | re.IGNORECASE))
        count += matches

    return count


def remove_empty_containers_regex_cleanup(html_str: str) -> str:
    """Remove empty containers using regex (after link removal)"""
    if not html_str:
        return html_str

    # Patterns for empty containers (multiple passes for nested containers)
    empty_patterns = [
        r'<div[^>]*>\s*</div>',
        r'<span[^>]*>\s*</span>',
        r'<p[^>]*>\s*</p>',
        r'<section[^>]*>\s*</section>',
        r'<article[^>]*>\s*</article>',
    ]

    # Multiple passes to handle nested empty containers
    for _ in range(3):  # Max 3 passes should be enough for most nesting
        original_html = html_str
        for pattern in empty_patterns:
            html_str = re.sub(pattern, '', html_str, flags=re.DOTALL | re.IGNORECASE)

        # If no changes made, we're done
        if html_str == original_html:
            break

    return html_str


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

        # Move all content into wrapper, except style tags
        contents = list(soup.children)
        for content in contents:
            if hasattr(content, 'extract'):
                # Keep style tags at document level - don't move them into wrapper
                if hasattr(content, 'name') and content.name == 'style':
                    continue
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
        # Ensure br tags are self-closing by clearing any content
        if br.string:
            br.string.extract()


def remove_unwanted_links(soup: BeautifulSoup, config):
    """Remove unwanted links (Rakuten internal links) and clean up empty containers"""
    links_removed = 0

    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']

        # Check if link matches unwanted patterns (including search.rakuten.co.jp)
        should_remove = any(pattern.search(href) for pattern in config.link_patterns)

        # Also explicitly check for search.rakuten.co.jp
        if 'search.rakuten.co.jp' in href:
            should_remove = True

        if should_remove:
            # Replace link with its text content
            text_content = a_tag.get_text().strip()
            if text_content:
                a_tag.replace_with(text_content)
            else:
                # If link has no text, remove it entirely
                a_tag.decompose()
            links_removed += 1

    # Clean up empty divs after link removal
    empty_containers_removed = remove_empty_containers(soup)

    return links_removed, empty_containers_removed


def remove_empty_containers(soup: BeautifulSoup):
    """Remove empty div, span, and other container elements after link removal"""
    # Tags to check for emptiness
    container_tags = ['div', 'span', 'p', 'section', 'article']

    removed_count = 0
    # Repeat until no more empty containers are found (nested empty containers)
    while True:
        found_empty = False

        for tag_name in container_tags:
            for tag in soup.find_all(tag_name):
                # Check if tag is empty (no text content and no non-whitespace children)
                text_content = tag.get_text().strip()
                has_meaningful_children = any(
                    child.name in ['img', 'br', 'hr', 'input', 'iframe', 'video', 'audio']
                    for child in tag.find_all()
                    if hasattr(child, 'name')
                )

                if not text_content and not has_meaningful_children:
                    tag.decompose()
                    removed_count += 1
                    found_empty = True

        # If no empty containers found in this pass, we're done
        if not found_empty:
            break

    return removed_count


def final_regex_cleanup(html_str: str) -> str:
    """Final cleanup with regex patterns"""
    # Remove empty paragraphs
    html_str = re.sub(r'<p[^>]*>\s*</p>', '', html_str)

    # Remove excessive whitespace
    html_str = re.sub(r'\s+', ' ', html_str)

    # Clean up multiple line breaks
    html_str = re.sub(r'(<br[^>]*>\s*){3,}', '<br><br>', html_str)

    return html_str.strip()


def make_mobile_responsive(soup):
    """Add mobile responsiveness CSS and classes to HTML content"""
    if soup is None or not soup.find():
        return

    # Enhanced Mobile CSS - scoped to .shopify-product-description
    mobile_css = """.shopify-product-description hr {
  width: 100% !important;
  max-width: 100% !important;
  margin: 7rem 0;
}


@media (max-width: 768px) {.shopify-product-description .mobile-responsive-img, .shopify-product-description img {
    max-width: 100% !important;
    width: auto !important;
    height: auto !important;
    display: block !important;
    margin: 0 auto !important;
  }.shopify-product-description .mobile-responsive-text, .shopify-product-description [style*="white-space:nowrap"], .shopify-product-description [style*="white-space: nowrap"] {
    white-space: normal !important;
    font-size: 16px !important;
    word-wrap: break-word !important;
    overflow-wrap: break-word !important;
  }.shopify-product-description .mobile-responsive-hr, .shopify-product-description hr {
    width: 100% !important;
    max-width: 100% !important;
    box-sizing: border-box !important;
    margin: 2rem 0 !important;
  }.shopify-product-description .mobile-responsive-table {
    display: block !important;
    width: 100% !important;
    max-width: 100% !important;
    border-collapse: separate !important;
    border-spacing: 0 !important;
    overflow: visible !important;
    margin: 16px 0 !important;
  }.shopify-product-description .mobile-responsive-table tbody, .shopify-product-description .mobile-responsive-table thead, .shopify-product-description .mobile-responsive-table tfoot {
    display: block !important;
    width: 100% !important;
  }.shopify-product-description .mobile-responsive-table tr {
    display: block !important;
    width: 100% !important;
    margin: 0 !important;
    padding: 0 !important;
    border-left: 1px solid #e0e0e0 !important;
    border-right: 1px solid #e0e0e0 !important;
    border-top: none !important;
    border-bottom: 1px solid #e0e0e0 !important;
    background: #fff !important;
    border-radius: 0 !important;
    margin-bottom: 0 !important;
  }.shopify-product-description .mobile-responsive-table tbody tr:first-child, .shopify-product-description .mobile-responsive-table thead tr:first-child {
    border-top: 1px solid #e0e0e0 !important;
    overflow: hidden !important;
  }.shopify-product-description .mobile-responsive-table tbody tr:last-child, .shopify-product-description .mobile-responsive-table tfoot tr:last-child {
    overflow: hidden !important;
    margin-bottom: 0px !important;
  }.shopify-product-description .mobile-responsive-table tbody tr:only-child {
    border: 1px solid #e0e0e0 !important;
    border-radius: 12px !important;
    overflow: hidden !important;
  }.shopify-product-description .mobile-responsive-table tbody tr:not(:first-child):not(:last-child) {
    border-top: none !important;
  }.shopify-product-description .mobile-responsive-table tbody tr {
    margin-bottom: 0 !important;
  }.shopify-product-description .mobile-responsive-table tbody tr td, .shopify-product-description .mobile-responsive-table th {
    display: block !important;
    width: 100% !important;
    max-width: 100% !important;
    box-sizing: border-box !important;
    padding: 16px !important;
    text-align: left !important;
    border: none !important;
    margin: 0 !important;
    word-wrap: break-word !important;
    overflow-wrap: break-word !important;
    border-radius: 0 !important;
  }.shopify-product-description .mobile-responsive-table td.no-padding, .shopify-product-description .mobile-responsive-table th.no-padding {
    padding: 0 !important;
  }.shopify-product-description .mobile-responsive-table.complex-layout tr:not(.colspan-row) {
    display: grid !important;
    grid-template-columns: 1fr !important;
    gap: 0 !important;
    margin-bottom: 0 !important;
  }.shopify-product-description .mobile-responsive-table.complex-layout .image-cell {
    grid-row: 1 !important;
    display: block !important;
    width: 100% !important;
    text-align: center !important;
    padding: 16px !important;
    background: #f8f9fa !important;
    margin: 0 !important;
    border-radius: 0 !important;
  }.shopify-product-description .mobile-responsive-table.complex-layout .text-cell {
    grid-row: 2 !important;
    display: block !important;
    width: 100% !important;
    padding: 16px !important;
    margin: 0 !important;
    border-radius: 0 !important;
  }.shopify-product-description .mobile-responsive-table.info-table {
    border: none !important;
    background: transparent !important;
  }.shopify-product-description .mobile-responsive-table.info-table tr {
    border: 1px solid #ddd !important;
    margin: 0 !important;
    background: #fff !important;
    border-bottom: none !important;
  }.shopify-product-description .mobile-responsive-table.info-table td:first-child {
    background-color: #f8f9fa !important;
    font-weight: bold !important;
    border-bottom: 1px solid #eee !important;
  }.shopify-product-description .mobile-responsive-table.info-table td:last-child {
    border-bottom: none !important;
  }.shopify-product-description .mobile-responsive-div {
    max-width: 100% !important;
    overflow-x: auto !important;
    -webkit-overflow-scrolling: touch;
    box-sizing: border-box !important;
  }.shopify-product-description .mobile-responsive-flex, .shopify-product-description div[style*="display: flex"], .shopify-product-description div[style*="display:flex"] {
    flex-direction: column !important;
    align-items: stretch !important;
    gap: 1rem !important;
  }.shopify-product-description .mobile-responsive-flex > *, .shopify-product-description div[style*="display: flex"] > *, .shopify-product-description div[style*="display:flex"] > * {
    width: 100% !important;
    max-width: 100% !important;
    flex: none !important;
    margin-right: 0 !important;
    margin-left: 0 !important;
    display: block !important;
  }.shopify-product-description .mobile-responsive-flex > div, .shopify-product-description div[style*="display: flex"] > div, .shopify-product-description div[style*="display:flex"] > div {
    flex-basis: auto !important;
    flex-grow: 0 !important;
    flex-shrink: 0 !important;
  }


  *[width] {
    max-width: 100% !important;
    width: auto !important;
  }
}


@media (max-width: 400px) {.shopify-product-description .mobile-responsive-table {
    font-size: 14px !important;
  }.shopify-product-description .mobile-responsive-table td, .shopify-product-description .mobile-responsive-table th {
    padding: 12px !important;
  }


  hr {
    margin: 1.5rem 0 !important;
  }
}"""

    # Add CSS at the beginning
    style_tag = soup.new_tag('style')
    style_tag.string = mobile_css
    soup.insert(0, style_tag)

    # Removed button styles as they're not in the expected output

    # Remove white-space:nowrap from all elements and add mobile-responsive-text class
    for element in soup.find_all(attrs={'style': True}):
        original_style = element.get('style', '')
        if 'white-space' in original_style.lower():
            new_style = re.sub(r'white-space\s*:\s*[^;]+;?', '', original_style, flags=re.IGNORECASE)
            new_style = new_style.strip('; ').strip()

            if new_style:
                element['style'] = new_style
            else:
                del element['style']

            classes = element.get('class', [])
            if isinstance(classes, str):
                classes = classes.split()
            if 'mobile-responsive-text' not in classes:
                classes.append('mobile-responsive-text')
            element['class'] = classes

    # Process images - add mobile-responsive-img class and display:block
    for img in soup.find_all('img'):
        # Remove fixed width/height from style attribute but add display:block
        if img.has_attr('style'):
            style = img['style']
            style = re.sub(r'width\s*:\s*[^;]+;?', '', style, flags=re.IGNORECASE)
            style = re.sub(r'height\s*:\s*[^;]+;?', '', style, flags=re.IGNORECASE)
            style = style.strip('; ').strip()

            # Always ensure display: block is present
            if style and 'display' not in style.lower():
                img['style'] = f"{style}; display: block"
            elif style:
                img['style'] = style
            else:
                img['style'] = 'display: block'
        else:
            # Add display: block if no style attribute
            img['style'] = 'display: block'

        # Remove width/height attributes
        if img.has_attr('width'):
            del img['width']
        if img.has_attr('height'):
            del img['height']

        # Add mobile-responsive-img class
        classes = img.get('class', [])
        if isinstance(classes, str):
            classes = classes.split()
        if 'mobile-responsive-img' not in classes:
            classes.append('mobile-responsive-img')
        img['class'] = classes

    # Process HR elements - add mobile-responsive-hr class for reduced margin
    for hr in soup.find_all('hr'):
        if hr.has_attr('width'):
            del hr['width']

        classes = hr.get('class', [])
        if isinstance(classes, str):
            classes = classes.split()
        if 'mobile-responsive-hr' not in classes:
            classes.append('mobile-responsive-hr')
        hr['class'] = classes

    # Process tables - add mobile-responsive-table class
    for table in soup.find_all('table'):
        classes = table.get('class', [])
        if isinstance(classes, str):
            classes = classes.split()

        if 'mobile-responsive-table' not in classes:
            classes.append('mobile-responsive-table')

        # Detect table type
        is_complex = _is_complex_table(table)
        is_info = _is_info_table(table)

        if is_complex:
            classes.append('complex-layout')
            _process_complex_table(table)
        elif is_info:
            classes.append('info-table')

        table['class'] = classes

        # Add inline styles to tables based on type
        existing_style = table.get('style', '')
        if is_info:
            table_inline_styles = 'border-collapse: collapse; margin: 0; border: 1px solid #bfbfbf; width: 100%; max-width: 100%; table-layout: auto; border-collapse: collapse; margin: 0; width: 100%; font-weight: normal;'
        else:
            table_inline_styles = 'border-collapse: collapse; margin: 0; border: 0px solid; width: auto; max-width: 100%; table-layout: auto; border-collapse: collapse; margin: 0; width: auto; font-weight: normal;'

        # Apply the styles
        if existing_style:
            table['style'] = f"{existing_style}; {table_inline_styles}"
        else:
            table['style'] = table_inline_styles

        # Check for cells with padded children - remove cell padding and add inline styles
        for cell in table.find_all(['td', 'th']):
            # Add inline styles to cells
            cell_style = cell.get('style', '')
            if cell_style:
                # Preserve existing style and add new properties
                cell['style'] = f"{cell_style}; padding: 8px; word-wrap: break-word; overflow-wrap: break-word;"
            else:
                # Set default cell styles
                cell['style'] = 'padding: 8px; word-wrap: break-word; overflow-wrap: break-word;'

            # Check for specific cell requirements like width
            if 'width:' not in cell_style.lower():
                # Add width based on cell content
                if cell.find('img'):
                    cell['style'] += ' width: auto; min-width: 180px;'
                else:
                    cell['style'] += ' width: auto; min-width: 230px;'

            if _has_padded_child(cell):
                cell_classes = cell.get('class', [])
                if isinstance(cell_classes, str):
                    cell_classes = cell_classes.split()
                if 'no-padding' not in cell_classes:
                    cell_classes.append('no-padding')
                cell['class'] = cell_classes

    # Process DIVs with flex layouts and fixed widths
    for div in soup.find_all('div'):
        if div.has_attr('style'):
            style = div['style']
            div_classes = div.get('class', [])
            if isinstance(div_classes, str):
                div_classes = div_classes.split()

            # Handle fixed widths
            if re.search(r'width\s*:\s*\d+px', style, re.IGNORECASE):
                style = re.sub(r'width\s*:\s*\d+px', 'max-width: 100%', style, flags=re.IGNORECASE)
                div['style'] = style

                if 'mobile-responsive-div' not in div_classes:
                    div_classes.append('mobile-responsive-div')

            # Handle flex layouts
            if 'display' in style.lower() and 'flex' in style.lower():
                if 'mobile-responsive-flex' not in div_classes:
                    div_classes.append('mobile-responsive-flex')

            if div_classes:
                div['class'] = div_classes


def _is_complex_table(table) -> bool:
    """Check if table has complex layout (image + text in separate cells)"""
    for row in table.find_all('tr'):
        cells = row.find_all(['td', 'th'])
        if len(cells) >= 2:
            has_img = any(cell.find('img') for cell in cells)
            has_text = any(len(cell.get_text(strip=True)) > 50 for cell in cells)
            if has_img and has_text:
                return True
    return False


def _is_info_table(table) -> bool:
    """Check if table is info/spec table (label-value pairs)"""
    rows = table.find_all('tr')
    if len(rows) < 3:
        return False

    label_value_count = 0
    for row in rows:
        cells = row.find_all(['td', 'th'])
        if len(cells) >= 2 and not any(cell.get('colspan') for cell in cells):
            left_text = cells[0].get_text(strip=True)
            if len(left_text) < 50 and ('■' in left_text or ':' in left_text):
                label_value_count += 1

    return label_value_count >= len(rows) * 0.5


def _process_complex_table(table):
    """Add classes to complex table cells"""
    for row in table.find_all('tr'):
        if any(cell.get('colspan') for cell in row.find_all(['td', 'th'])):
            classes = row.get('class', [])
            if isinstance(classes, str):
                classes = classes.split()
            if 'colspan-row' not in classes:
                classes.append('colspan-row')
            row['class'] = classes
        else:
            for cell in row.find_all(['td', 'th']):
                classes = cell.get('class', [])
                if isinstance(classes, str):
                    classes = classes.split()

                if cell.find('img'):
                    if 'image-cell' not in classes:
                        classes.append('image-cell')
                elif len(cell.get_text(strip=True)) > 20:
                    if 'text-cell' not in classes:
                        classes.append('text-cell')

                if classes:
                    cell['class'] = classes


def _has_padded_child(cell) -> bool:
    """Check if cell's only direct child has padding > 0"""
    # Get only element nodes (not text nodes)
    direct_element_children = [child for child in cell.children
                             if hasattr(child, 'name') and child.name]

    # Must have exactly one direct element child
    if len(direct_element_children) != 1:
        return False

    child = direct_element_children[0]

    # Check if child has padding in style attribute
    if child.has_attr('style'):
        style = child['style'].lower()
        if 'padding' in style:
            # Look for padding values > 0
            padding_pattern = r'padding[^:]*:\s*([^;]+)'
            matches = re.findall(padding_pattern, style)
            for match in matches:
                # Check if any padding value is > 0
                if re.search(r'[1-9]\d*px|[1-9]\d*em|[1-9]\d*rem|[1-9]\d*%', match):
                    return True

    return False