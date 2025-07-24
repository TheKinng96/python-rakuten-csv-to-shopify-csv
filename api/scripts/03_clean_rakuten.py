#!/usr/bin/env python3
"""
Script to discover and clean Rakuten EC-UP leftover content from product descriptions

This script:
1. Scans all CSV files to discover ALL EC-UP patterns (not just known ones)
2. Analyzes associated content and styling for each pattern
3. Generates comprehensive pattern discovery report (03_rakuten_patterns_found.csv)
4. Connects to Shopify API to remove EC-UP content blocks
5. Generates processing report (03_rakuten_content_removed.csv)
"""
import csv
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Set, Tuple

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from shopify_manager.client import ShopifyClient
from shopify_manager.config import shopify_config, path_config
from shopify_manager.logger import get_script_logger
from shopify_manager.models import ShopifyProduct, ECUpRecord

logger = get_script_logger("03_clean_rakuten")


class ECUpPatternDiscovery:
    """Discovers and analyzes all EC-UP patterns in content"""
    
    def __init__(self):
        # Dynamic pattern discovery - will find ALL EC-UP patterns
        self.ec_up_pattern = re.compile(
            r'<!--EC-UP_([^_]+)_(\d+)_START-->(.*?)<!--EC-UP_\1_\2_END-->',
            re.IGNORECASE | re.DOTALL
        )
        
        # Known patterns for categorization
        self.known_patterns = {
            'Rakuichi': 'rakuichi',
            'Favorite': 'favorite', 
            'Similar': 'similar',
            'SameTime': 'sametime',
            'Resale': 'resale'
        }
        
        # Style detection patterns
        self.style_patterns = {
            'css_class': re.compile(r'class\s*=\s*["\']([^"\']*ecup[^"\']*)["\']', re.IGNORECASE),
            'inline_style': re.compile(r'style\s*=\s*["\']([^"\']*)["\']', re.IGNORECASE),
            'style_tag': re.compile(r'<style[^>]*>(.*?)</style>', re.IGNORECASE | re.DOTALL)
        }
    
    def discover_all_patterns(self, html_content: str) -> List[Dict[str, Any]]:
        """
        Discover all EC-UP patterns in HTML content
        Returns list of pattern discoveries with analysis
        """
        if not html_content or pd.isna(html_content):
            return []
        
        discoveries = []
        
        # Find all EC-UP patterns
        matches = self.ec_up_pattern.finditer(str(html_content))
        
        for match in matches:
            pattern_name = match.group(1)
            pattern_number = match.group(2)  
            content_block = match.group(3)
            full_pattern = match.group(0)
            
            # Analyze the content block
            analysis = self._analyze_content_block(content_block)
            
            discovery = {
                'ec_up_pattern': f"EC-UP_{pattern_name}_{pattern_number}",
                'pattern_type': self.known_patterns.get(pattern_name, 'unknown'),
                'pattern_name': pattern_name,
                'pattern_number': pattern_number,
                'full_match': full_pattern,
                'content_block': content_block,
                'content_length': len(content_block),
                'content_preview': self._create_content_preview(content_block),
                'has_styling': analysis['has_styling'],
                'has_images': analysis['has_images'],
                'has_links': analysis['has_links'],
                'styling_info': analysis['styling_info'],
                'pattern_count': 1  # Will be aggregated later
            }
            
            discoveries.append(discovery)
        
        return discoveries
    
    def _analyze_content_block(self, content_block: str) -> Dict[str, Any]:
        """Analyze a single EC-UP content block"""
        analysis = {
            'has_styling': False,
            'has_images': False,
            'has_links': False,
            'styling_info': []
        }
        
        try:
            soup = BeautifulSoup(content_block, 'lxml')
            
            # Check for images
            images = soup.find_all('img')
            analysis['has_images'] = len(images) > 0
            
            # Check for links
            links = soup.find_all('a')
            analysis['has_links'] = len(links) > 0
            
            # Check for styling
            style_info = []
            
            # CSS classes with ecup
            for element in soup.find_all(class_=True):
                classes = element.get('class', [])
                ecup_classes = [cls for cls in classes if 'ecup' in cls.lower()]
                if ecup_classes:
                    style_info.extend(ecup_classes)
            
            # Inline styles
            for element in soup.find_all(style=True):
                style_info.append(f"inline:{element.get('style')[:50]}...")
            
            # Style tags
            style_tags = soup.find_all('style')
            for style_tag in style_tags:
                style_content = style_tag.get_text()[:100]
                style_info.append(f"style_tag:{style_content}...")
            
            analysis['has_styling'] = len(style_info) > 0
            analysis['styling_info'] = style_info
            
        except Exception as e:
            logger.debug(f"Error analyzing content block: {e}")
        
        return analysis
    
    def _create_content_preview(self, content_block: str, max_length: int = 200) -> str:
        """Create a preview of the content block"""
        try:
            soup = BeautifulSoup(content_block, 'lxml')
            text_content = soup.get_text(strip=True)
            
            if len(text_content) <= max_length:
                return text_content
            
            return text_content[:max_length] + "..."
            
        except Exception:
            # Fallback to raw content preview
            clean_content = re.sub(r'<[^>]+>', ' ', content_block)
            clean_content = ' '.join(clean_content.split())
            
            if len(clean_content) <= max_length:
                return clean_content
            
            return clean_content[:max_length] + "..."
    
    def aggregate_patterns(self, all_discoveries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Aggregate pattern discoveries by pattern type"""
        pattern_counts = {}
        
        for discovery in all_discoveries:
            pattern = discovery['ec_up_pattern']
            
            if pattern not in pattern_counts:
                pattern_counts[pattern] = {
                    'ec_up_pattern': pattern,
                    'pattern_type': discovery['pattern_type'],
                    'pattern_name': discovery['pattern_name'],
                    'pattern_number': discovery['pattern_number'],
                    'pattern_count': 0,
                    'total_content_length': 0,
                    'has_styling_count': 0,
                    'has_images_count': 0,
                    'has_links_count': 0,
                    'example_content_preview': discovery['content_preview'],
                    'styling_examples': set()
                }
            
            pattern_counts[pattern]['pattern_count'] += 1
            pattern_counts[pattern]['total_content_length'] += discovery['content_length']
            
            if discovery['has_styling']:
                pattern_counts[pattern]['has_styling_count'] += 1
                pattern_counts[pattern]['styling_examples'].update(discovery['styling_info'])
            
            if discovery['has_images']:
                pattern_counts[pattern]['has_images_count'] += 1
            
            if discovery['has_links']:
                pattern_counts[pattern]['has_links_count'] += 1
        
        # Convert sets to lists for CSV serialization
        for pattern_data in pattern_counts.values():
            pattern_data['styling_examples'] = list(pattern_data['styling_examples'])
        
        return list(pattern_counts.values())
    
    def clean_ec_up_content(self, html_content: str) -> Tuple[str, Dict[str, Any]]:
        """
        Remove all EC-UP content from HTML
        Returns (cleaned_html, cleanup_metrics)
        """
        if not html_content or pd.isna(html_content):
            return html_content, {'patterns_removed': [], 'content_blocks_removed': 0}
        
        original_content = str(html_content)
        cleaned_content = original_content
        cleanup_metrics = {
            'patterns_removed': [],
            'content_blocks_removed': 0,
            'content_length_before': len(original_content)
        }
        
        # Find and remove all EC-UP patterns
        matches = list(self.ec_up_pattern.finditer(original_content))
        
        # Remove matches in reverse order to maintain string positions
        for match in reversed(matches):
            pattern_name = f"EC-UP_{match.group(1)}_{match.group(2)}"
            cleanup_metrics['patterns_removed'].append(pattern_name)
            cleanup_metrics['content_blocks_removed'] += 1
            
            # Remove the entire match
            start, end = match.span()
            cleaned_content = cleaned_content[:start] + cleaned_content[end:]
        
        cleanup_metrics['content_length_after'] = len(cleaned_content)
        cleanup_metrics['patterns_removed'] = list(set(cleanup_metrics['patterns_removed']))  # Remove duplicates
        
        return cleaned_content, cleanup_metrics


def analyze_csv_for_ec_up_patterns() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Analyze CSV files to discover all EC-UP patterns
    Returns (product_records, pattern_summary)
    """
    logger.info("Starting discovery of EC-UP patterns in CSV files...")
    
    discoverer = ECUpPatternDiscovery()
    product_records = []
    all_discoveries = []
    csv_files = path_config.get_csv_files()
    
    for csv_file in csv_files:
        logger.info(f"Analyzing {csv_file.name}...")
        
        try:
            # Read CSV in chunks to handle large files
            chunk_iter = pd.read_csv(
                csv_file,
                chunksize=shopify_config.chunk_size,
                encoding='utf-8',
                low_memory=False
            )
            
            for chunk_idx, chunk in enumerate(chunk_iter):
                logger.debug(f"Processing chunk {chunk_idx + 1} from {csv_file.name}")
                
                for _, row in chunk.iterrows():
                    handle = row.get('Handle', '')
                    body_html = row.get('Body (HTML)', '')
                    
                    if pd.isna(body_html) or not body_html:
                        continue
                    
                    # Discover EC-UP patterns in this product
                    discoveries = discoverer.discover_all_patterns(str(body_html))
                    
                    if discoveries:
                        # Create product record
                        unique_patterns = list(set(d['ec_up_pattern'] for d in discoveries))
                        content_preview = discoveries[0]['content_preview'] if discoveries else ""
                        
                        product_records.append({
                            'product_handle': handle,
                            'shopify_product_id': None,  # Will be filled when connecting to API
                            'patterns_found': ', '.join(unique_patterns),
                            'pattern_count': len(discoveries),
                            'content_length': len(str(body_html)),
                            'associated_content_preview': content_preview,
                            'has_styling': any(d['has_styling'] for d in discoveries),
                            'csv_source': csv_file.name
                        })
                        
                        # Add to all discoveries for pattern aggregation
                        all_discoveries.extend(discoveries)
                        
        except Exception as e:
            logger.error(f"Error processing {csv_file.name}: {e}")
            continue
    
    # Generate pattern summary
    pattern_summary = discoverer.aggregate_patterns(all_discoveries)
    
    logger.info(f"Found {len(product_records)} products with EC-UP content")
    logger.info(f"Discovered {len(pattern_summary)} unique EC-UP patterns")
    
    return product_records, pattern_summary


def save_pattern_discovery_report(
    product_records: List[Dict[str, Any]], 
    pattern_summary: List[Dict[str, Any]]
) -> None:
    """Save pattern discovery results to CSV reports"""
    
    # Save product-level records
    products_report_path = path_config.get_report_path("03_rakuten_products_with_ec_up.csv")
    logger.info(f"Saving products report to {products_report_path}")
    
    with open(products_report_path, 'w', newline='', encoding='utf-8') as f:
        if product_records:
            fieldnames = product_records[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(product_records)
    
    # Save pattern summary
    patterns_report_path = path_config.get_report_path("03_rakuten_patterns_found.csv")
    logger.info(f"Saving patterns discovery report to {patterns_report_path}")
    
    with open(patterns_report_path, 'w', newline='', encoding='utf-8') as f:
        if pattern_summary:
            fieldnames = [
                'ec_up_pattern', 'pattern_type', 'pattern_name', 'pattern_number',
                'pattern_count', 'total_content_length', 'has_styling_count',
                'has_images_count', 'has_links_count', 'example_content_preview',
                'styling_examples'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)  
            writer.writeheader()
            
            for pattern in pattern_summary:
                # Convert styling_examples list to string for CSV
                pattern_copy = pattern.copy()
                pattern_copy['styling_examples'] = '; '.join(pattern['styling_examples'])
                writer.writerow(pattern_copy)
    
    logger.info(f"Discovery reports saved: {len(product_records)} products, {len(pattern_summary)} patterns")


def process_ec_up_cleanup_via_api(product_records: List[Dict[str, Any]]) -> List[ECUpRecord]:
    """
    Connect to Shopify API and clean EC-UP content
    Returns list of processing records
    """
    logger.info("Starting API processing to clean EC-UP content...")
    
    # Initialize Shopify client and content cleaner
    client = ShopifyClient(shopify_config, use_test_store=True)
    discoverer = ECUpPatternDiscovery()
    processing_records = []
    
    # Group records by product handle for efficient processing
    handle_groups = {}
    for record in product_records:
        handle = record['product_handle']
        if handle not in handle_groups:
            handle_groups[handle] = []
        handle_groups[handle].append(record)
    
    logger.info(f"Processing {len(handle_groups)} unique products")
    
    for handle, handle_records in tqdm(handle_groups.items(), desc="Processing products"):
        try:
            # Get all products to find the one with this handle
            products = client.get_all_products(fields="id,handle,body_html")
            
            shopify_product = None
            for product_data in products:
                if product_data.get('handle') == handle:
                    shopify_product = ShopifyProduct.from_shopify_api(product_data)
                    break
            
            if not shopify_product:
                logger.warning(f"Product with handle '{handle}' not found in Shopify")
                for record in handle_records:
                    processing_records.append(ECUpRecord(
                        product_handle=handle,
                        shopify_product_id=None,
                        timestamp=datetime.now(),
                        status="error",
                        error_message="Product not found in Shopify",
                        ec_up_pattern=record['patterns_found'],
                        pattern_count=record['pattern_count'],
                        associated_content_preview=record['associated_content_preview'],
                        has_styling=record['has_styling'],
                        content_length_before=record['content_length']
                    ))
                continue
            
            # Clean the EC-UP content
            try:
                original_html = shopify_product.body_html
                cleaned_html, cleanup_metrics = discoverer.clean_ec_up_content(original_html)
                
                # Update product if changes were made
                if cleanup_metrics['content_blocks_removed'] > 0:
                    update_data = {'body_html': cleaned_html}
                    client.update_product(shopify_product.id, update_data)
                    
                    processing_records.append(ECUpRecord(
                        product_handle=handle,
                        shopify_product_id=shopify_product.id,
                        timestamp=datetime.now(),
                        status="success",
                        ec_up_pattern=handle_records[0]['patterns_found'],
                        pattern_count=cleanup_metrics['content_blocks_removed'],
                        associated_content_preview=handle_records[0]['associated_content_preview'],
                        has_styling=handle_records[0]['has_styling'],
                        content_length_before=cleanup_metrics['content_length_before'],
                        content_length_after=cleanup_metrics['content_length_after'],
                        patterns_removed=', '.join(cleanup_metrics['patterns_removed'])
                    ))
                    
                    logger.info(f"Cleaned EC-UP content from product {handle} ({cleanup_metrics['content_blocks_removed']} blocks removed)")
                else:
                    processing_records.append(ECUpRecord(
                        product_handle=handle,
                        shopify_product_id=shopify_product.id,
                        timestamp=datetime.now(),
                        status="skipped",
                        error_message="No EC-UP content found to remove",
                        ec_up_pattern=handle_records[0]['patterns_found'],
                        pattern_count=0,
                        associated_content_preview=handle_records[0]['associated_content_preview'],
                        has_styling=handle_records[0]['has_styling'],
                        content_length_before=len(original_html)
                    ))
                    
            except Exception as e:
                logger.error(f"Failed to clean EC-UP content from product {handle}: {e}")
                for record in handle_records:
                    processing_records.append(ECUpRecord(
                        product_handle=handle,
                        shopify_product_id=shopify_product.id,
                        timestamp=datetime.now(),
                        status="error",
                        error_message=str(e),
                        ec_up_pattern=record['patterns_found'],
                        pattern_count=record['pattern_count'],
                        associated_content_preview=record['associated_content_preview'],
                        has_styling=record['has_styling'],
                        content_length_before=record['content_length']
                    ))
                    
        except Exception as e:
            logger.error(f"Error processing product {handle}: {e}")
            for record in handle_records:
                processing_records.append(ECUpRecord(
                    product_handle=handle,
                    shopify_product_id=None,
                    timestamp=datetime.now(),
                    status="error",
                    error_message=str(e),
                    ec_up_pattern=record['patterns_found'],
                    pattern_count=record['pattern_count'],
                    associated_content_preview=record['associated_content_preview'],
                    has_styling=record['has_styling'],
                    content_length_before=record['content_length']
                ))
    
    logger.info(f"API processing completed. {len(processing_records)} records generated")
    return processing_records


def save_processing_report(processing_records: List[ECUpRecord]) -> None:
    """Save processing results to CSV report"""
    report_path = path_config.get_report_path("03_rakuten_content_cleaned.csv")
    
    logger.info(f"Saving processing report to {report_path}")
    
    with open(report_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'product_handle', 'shopify_product_id', 'ec_up_pattern', 'pattern_count',
            'associated_content_preview', 'has_styling', 'content_length_before',
            'content_length_after', 'patterns_removed', 'timestamp', 'status', 'error_message'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for record in processing_records:
            writer.writerow({
                'product_handle': record.product_handle,
                'shopify_product_id': record.shopify_product_id,
                'ec_up_pattern': record.ec_up_pattern,
                'pattern_count': record.pattern_count,
                'associated_content_preview': record.associated_content_preview,
                'has_styling': record.has_styling,
                'content_length_before': record.content_length_before,
                'content_length_after': record.content_length_after,
                'patterns_removed': record.patterns_removed,
                'timestamp': record.timestamp.isoformat(),
                'status': record.status,
                'error_message': record.error_message
            })
    
    # Generate summary
    successful = sum(1 for r in processing_records if r.status == "success")
    total = len(processing_records)
    logger.info(f"Processing report saved: {successful}/{total} products successfully cleaned")


def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("Starting Rakuten EC-UP Content Cleanup Script")
    logger.info("=" * 60)
    
    try:
        # Phase 1: Discover all EC-UP patterns
        logger.info("Phase 1: Discovering EC-UP patterns in CSV files...")
        product_records, pattern_summary = analyze_csv_for_ec_up_patterns()
        save_pattern_discovery_report(product_records, pattern_summary)
        
        if not product_records:
            logger.info("No EC-UP patterns found in CSV files. Exiting.")
            return
        
        # Print pattern summary
        logger.info("=" * 40)
        logger.info("DISCOVERED EC-UP PATTERNS:")
        for pattern in pattern_summary:
            logger.info(f"  {pattern['ec_up_pattern']}: {pattern['pattern_count']} occurrences")
        logger.info("=" * 40)
        
        # Phase 2: Clean via API (only if not dry run or user confirms)
        if shopify_config.dry_run:
            logger.info("DRY RUN mode enabled. Skipping API processing.")
            logger.info(f"Would clean EC-UP content from {len(product_records)} products")
        else:
            logger.info("Phase 2: Cleaning via Shopify API...")
            response = input(f"Clean EC-UP content from {len(product_records)} products via API? (y/N): ")
            if response.lower() == 'y':
                processing_records = process_ec_up_cleanup_via_api(product_records)
                save_processing_report(processing_records)
            else:
                logger.info("API processing skipped by user")
        
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Script failed with error: {e}")
        raise
    finally:
        logger.info("Rakuten EC-UP Content Cleanup Script completed")


if __name__ == "__main__":
    main()