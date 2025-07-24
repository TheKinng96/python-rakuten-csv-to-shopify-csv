#!/usr/bin/env python3
"""
Script to analyze and fix broken HTML table structures in product descriptions

This script:
1. Scans all CSV files to identify products with problematic table structures
2. Analyzes table nesting depth, width conflicts, and malformed structures
3. Generates analysis report (02_html_tables_analysis.csv)
4. Connects to Shopify API to fix table structures
5. Generates processing report (02_html_tables_fixed.csv)
"""
import csv
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional

import pandas as pd
from bs4 import BeautifulSoup, Tag
from tqdm import tqdm

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from shopify_manager.client import ShopifyClient
from shopify_manager.config import shopify_config, path_config
from shopify_manager.logger import get_script_logger
from shopify_manager.models import ShopifyProduct, HTMLTableRecord

logger = get_script_logger("02_fix_html_tables")


class HTMLTableAnalyzer:
    """Analyzes and fixes HTML table structures"""
    
    def __init__(self):
        self.table_patterns = {
            'nested_tables': re.compile(r'<table[^>]*>.*?<table[^>]*>', re.IGNORECASE | re.DOTALL),
            'width_conflicts': re.compile(r'width\s*=\s*["\']?(\d+)["\']?', re.IGNORECASE),
            'malformed_structure': re.compile(r'<table[^>]*>(?!.*</table>)|</table>(?!.*<table)', re.IGNORECASE),
        }
    
    def analyze_html_content(self, html_content: str) -> Dict[str, Any]:
        """
        Analyze HTML content for table issues
        Returns analysis metrics
        """
        if not html_content or pd.isna(html_content):
            return {
                'has_tables': False,
                'table_count': 0,
                'nested_depth': 0,
                'has_width_conflicts': False,
                'has_overlapping_structure': False,
                'table_issue_type': 'none',
                'content_length': 0
            }
        
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            tables = soup.find_all('table')
            
            if not tables:
                return {
                    'has_tables': False,
                    'table_count': 0,
                    'nested_depth': 0,
                    'has_width_conflicts': False,
                    'has_overlapping_structure': False,
                    'table_issue_type': 'none',
                    'content_length': len(html_content)
                }
            
            # Analyze nesting depth
            nested_depth = self._calculate_nesting_depth(soup)
            
            # Check for width conflicts
            width_conflicts = self._check_width_conflicts(tables)
            
            # Check for overlapping structures
            overlapping = self._check_overlapping_structures(html_content)
            
            # Determine primary issue type
            issue_type = self._determine_issue_type(nested_depth, width_conflicts, overlapping)
            
            return {
                'has_tables': True,
                'table_count': len(tables),
                'nested_depth': nested_depth,
                'has_width_conflicts': width_conflicts,
                'has_overlapping_structure': overlapping,
                'table_issue_type': issue_type,
                'content_length': len(html_content)
            }
            
        except Exception as e:
            logger.warning(f"Error analyzing HTML content: {e}")
            return {
                'has_tables': False,
                'table_count': 0,
                'nested_depth': 0,
                'has_width_conflicts': False,
                'has_overlapping_structure': False,
                'table_issue_type': 'parse_error',
                'content_length': len(html_content)
            }
    
    def _calculate_nesting_depth(self, soup: BeautifulSoup) -> int:
        """Calculate maximum table nesting depth"""
        max_depth = 0
        
        def count_depth(element, current_depth=0):
            nonlocal max_depth
            if element.name == 'table':
                current_depth += 1
                max_depth = max(max_depth, current_depth)
            
            if hasattr(element, 'children'):
                for child in element.children:
                    if hasattr(child, 'name'):
                        count_depth(child, current_depth)
        
        count_depth(soup)
        return max_depth
    
    def _check_width_conflicts(self, tables: List[Tag]) -> bool:
        """Check for conflicting width specifications"""
        total_widths = []
        
        for table in tables:
            width_attr = table.get('width')
            if width_attr:
                try:
                    # Extract numeric width value
                    width_match = re.search(r'(\d+)', str(width_attr))
                    if width_match:
                        width = int(width_match.group(1))
                        total_widths.append(width)
                except (ValueError, AttributeError):
                    continue
        
        # Check if any table widths exceed reasonable bounds or conflict
        return any(width > 800 for width in total_widths) or len(set(total_widths)) > len(total_widths) * 0.5
    
    def _check_overlapping_structures(self, html_content: str) -> bool:
        """Check for overlapping table structures using regex"""
        # Look for patterns that indicate tables within tables with potential overlaps
        nested_pattern = re.compile(
            r'<table[^>]*>.*?<table[^>]*>.*?</table>.*?</table>', 
            re.IGNORECASE | re.DOTALL
        )
        return bool(nested_pattern.search(html_content))
    
    def _determine_issue_type(self, nested_depth: int, width_conflicts: bool, overlapping: bool) -> str:
        """Determine the primary issue type"""
        if nested_depth > 2:
            return 'deep_nesting'
        elif overlapping:
            return 'overlapping_structure'
        elif width_conflicts:
            return 'width_conflicts'
        elif nested_depth > 1:
            return 'simple_nesting'
        else:
            return 'none'
    
    def fix_html_tables(self, html_content: str) -> Tuple[str, Dict[str, Any]]:
        """
        Fix HTML table structures
        Returns (fixed_html, fix_metrics)
        """
        if not html_content or pd.isna(html_content):
            return html_content, {'fixes_applied': 0, 'fix_types': []}
        
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            fix_metrics = {'fixes_applied': 0, 'fix_types': []}
            
            # Fix 1: Remove excessive nesting
            fix_metrics['fixes_applied'] += self._fix_nested_tables(soup)
            if fix_metrics['fixes_applied'] > 0:
                fix_metrics['fix_types'].append('nested_tables')
            
            # Fix 2: Normalize widths
            width_fixes = self._fix_width_conflicts(soup)
            fix_metrics['fixes_applied'] += width_fixes
            if width_fixes > 0:
                fix_metrics['fix_types'].append('width_normalization')
            
            # Fix 3: Clean up malformed structures
            structure_fixes = self._fix_malformed_structures(soup)
            fix_metrics['fixes_applied'] += structure_fixes
            if structure_fixes > 0:
                fix_metrics['fix_types'].append('structure_cleanup')
            
            # Extract the body content, avoiding extra html/body tags from lxml
            body = soup.find('body')
            if body:
                fixed_html = ''.join(str(child) for child in body.children)
            else:
                fixed_html = str(soup)
            
            return fixed_html, fix_metrics
            
        except Exception as e:
            logger.warning(f"Error fixing HTML content: {e}")
            return html_content, {'fixes_applied': 0, 'fix_types': ['error'], 'error': str(e)}
    
    def _fix_nested_tables(self, soup: BeautifulSoup) -> int:
        """Fix deeply nested table structures"""
        fixes = 0
        
        # Find tables with nested tables and flatten if possible
        for table in soup.find_all('table'):
            nested_tables = table.find_all('table')
            if len(nested_tables) > 1:  # Has nested tables
                # Strategy: Convert nested tables to div structures where appropriate
                for nested_table in nested_tables[1:]:  # Keep the first, convert others
                    if self._can_convert_table_to_div(nested_table):
                        self._convert_table_to_div(nested_table)
                        fixes += 1
        
        return fixes
    
    def _fix_width_conflicts(self, soup: BeautifulSoup) -> int:
        """Fix conflicting width specifications"""
        fixes = 0
        
        for table in soup.find_all('table'):
            width_attr = table.get('width')
            if width_attr:
                try:
                    width_match = re.search(r'(\d+)', str(width_attr))
                    if width_match:
                        width = int(width_match.group(1))
                        # Normalize excessive widths
                        if width > 800:
                            table['width'] = '100%'
                            fixes += 1
                        elif width < 100:  # Very small widths that might cause issues
                            table['width'] = '100%'
                            fixes += 1
                except (ValueError, AttributeError):
                    # Remove invalid width attributes
                    del table['width']
                    fixes += 1
        
        return fixes
    
    def _fix_malformed_structures(self, soup: BeautifulSoup) -> int:
        """Fix malformed table structures"""
        fixes = 0
        
        # Remove empty tables
        for table in soup.find_all('table'):
            if not table.get_text(strip=True):
                table.decompose()
                fixes += 1
        
        return fixes
    
    def _can_convert_table_to_div(self, table: Tag) -> bool:
        """Check if a table can be safely converted to div structure"""
        # Simple heuristic: if table has only one row or one column, it's likely presentational
        rows = table.find_all('tr')
        if len(rows) <= 1:
            return True
        
        # Check if all rows have the same number of cells (indicating simple structure)
        cell_counts = [len(row.find_all(['td', 'th'])) for row in rows]
        return len(set(cell_counts)) == 1 and cell_counts[0] <= 2
    
    def _convert_table_to_div(self, table: Tag) -> None:
        """Convert simple table structure to div-based layout"""
        # Create new div container
        div_container = soup.new_tag('div', style='margin: 10px 0;')
        
        # Convert table rows to div elements
        for row in table.find_all('tr'):
            row_div = soup.new_tag('div', style='margin: 5px 0;')
            for cell in row.find_all(['td', 'th']):
                cell_div = soup.new_tag('div', style='display: inline-block; margin-right: 10px;')
                cell_div.contents = cell.contents.copy()
                row_div.append(cell_div)
            div_container.append(row_div)
        
        # Replace table with div
        table.replace_with(div_container)


def analyze_csv_for_html_tables() -> List[Dict[str, Any]]:
    """
    Analyze CSV files to find products with problematic HTML table structures
    Returns list of records for analysis report
    """
    logger.info("Starting analysis of CSV files for HTML table issues...")
    
    analyzer = HTMLTableAnalyzer()
    analysis_records = []
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
                    
                    # Analyze HTML content
                    analysis = analyzer.analyze_html_content(str(body_html))
                    
                    # Only record products with actual table issues
                    if analysis['has_tables'] and analysis['table_issue_type'] != 'none':
                        analysis_records.append({
                            'product_handle': handle,
                            'shopify_product_id': None,  # Will be filled when connecting to API
                            'table_issue_type': analysis['table_issue_type'],
                            'nested_depth': analysis['nested_depth'],
                            'table_count': analysis['table_count'],
                            'has_width_conflicts': analysis['has_width_conflicts'],
                            'has_overlapping_structure': analysis['has_overlapping_structure'],
                            'content_length': analysis['content_length'],
                            'csv_source': csv_file.name
                        })
                        
        except Exception as e:
            logger.error(f"Error processing {csv_file.name}: {e}")
            continue
    
    logger.info(f"Found {len(analysis_records)} products with HTML table issues")
    return analysis_records


def save_analysis_report(analysis_records: List[Dict[str, Any]]) -> None:
    """Save analysis results to CSV report"""
    report_path = path_config.get_report_path("02_html_tables_analysis.csv")
    
    logger.info(f"Saving analysis report to {report_path}")
    
    with open(report_path, 'w', newline='', encoding='utf-8') as f:
        if analysis_records:
            fieldnames = analysis_records[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(analysis_records)
        else:
            # Empty file with headers
            writer = csv.writer(f)
            writer.writerow([
                'product_handle', 'shopify_product_id', 'table_issue_type',
                'nested_depth', 'table_count', 'has_width_conflicts',
                'has_overlapping_structure', 'content_length', 'csv_source'
            ])
    
    logger.info(f"Analysis report saved with {len(analysis_records)} records")


def process_html_tables_via_api(analysis_records: List[Dict[str, Any]]) -> List[HTMLTableRecord]:
    """
    Connect to Shopify API and fix HTML table structures
    Returns list of processing records
    """
    logger.info("Starting API processing to fix HTML table structures...")
    
    # Initialize Shopify client and HTML analyzer
    client = ShopifyClient(shopify_config, use_test_store=True)
    analyzer = HTMLTableAnalyzer()
    processing_records = []
    
    # Group records by product handle for efficient processing
    handle_groups = {}
    for record in analysis_records:
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
                    processing_records.append(HTMLTableRecord(
                        product_handle=handle,
                        shopify_product_id=None,
                        timestamp=datetime.now(),
                        status="error",
                        error_message="Product not found in Shopify",
                        table_issue_type=record['table_issue_type'],
                        nested_depth=record['nested_depth'],
                        table_count=record['table_count'],
                        has_width_conflicts=record['has_width_conflicts'],
                        has_overlapping_structure=record['has_overlapping_structure'],
                        html_length_before=record['content_length']
                    ))
                continue
            
            # Fix the HTML content
            try:
                original_html = shopify_product.body_html
                fixed_html, fix_metrics = analyzer.fix_html_tables(original_html)
                
                # Update product if changes were made
                if fix_metrics['fixes_applied'] > 0:
                    update_data = {'body_html': fixed_html}
                    client.update_product(shopify_product.id, update_data)
                    
                    processing_records.append(HTMLTableRecord(
                        product_handle=handle,
                        shopify_product_id=shopify_product.id,
                        timestamp=datetime.now(),
                        status="success",
                        table_issue_type=handle_records[0]['table_issue_type'],
                        nested_depth=handle_records[0]['nested_depth'],
                        table_count=handle_records[0]['table_count'],
                        has_width_conflicts=handle_records[0]['has_width_conflicts'],
                        has_overlapping_structure=handle_records[0]['has_overlapping_structure'],
                        html_length_before=len(original_html),
                        html_length_after=len(fixed_html),
                        tables_restructured_count=fix_metrics['fixes_applied'],
                        fix_applied=', '.join(fix_metrics['fix_types'])
                    ))
                    
                    logger.info(f"Fixed HTML tables for product {handle} ({fix_metrics['fixes_applied']} fixes)")
                else:
                    processing_records.append(HTMLTableRecord(
                        product_handle=handle,
                        shopify_product_id=shopify_product.id,
                        timestamp=datetime.now(),
                        status="skipped",
                        error_message="No fixes needed",
                        table_issue_type=handle_records[0]['table_issue_type'],
                        nested_depth=handle_records[0]['nested_depth'],
                        table_count=handle_records[0]['table_count'],
                        has_width_conflicts=handle_records[0]['has_width_conflicts'],
                        has_overlapping_structure=handle_records[0]['has_overlapping_structure'],
                        html_length_before=len(original_html)
                    ))
                    
            except Exception as e:
                logger.error(f"Failed to fix HTML tables for product {handle}: {e}")
                for record in handle_records:
                    processing_records.append(HTMLTableRecord(
                        product_handle=handle,
                        shopify_product_id=shopify_product.id,
                        timestamp=datetime.now(),
                        status="error",
                        error_message=str(e),
                        table_issue_type=record['table_issue_type'],
                        nested_depth=record['nested_depth'],
                        table_count=record['table_count'],
                        has_width_conflicts=record['has_width_conflicts'],
                        has_overlapping_structure=record['has_overlapping_structure'],
                        html_length_before=record['content_length']
                    ))
                    
        except Exception as e:
            logger.error(f"Error processing product {handle}: {e}")
            for record in handle_records:
                processing_records.append(HTMLTableRecord(
                    product_handle=handle,
                    shopify_product_id=None,
                    timestamp=datetime.now(),
                    status="error",
                    error_message=str(e),
                    table_issue_type=record['table_issue_type'],
                    nested_depth=record['nested_depth'],
                    table_count=record['table_count'],
                    has_width_conflicts=record['has_width_conflicts'],
                    has_overlapping_structure=record['has_overlapping_structure'],
                    html_length_before=record['content_length']
                ))
    
    logger.info(f"API processing completed. {len(processing_records)} records generated")
    return processing_records


def save_processing_report(processing_records: List[HTMLTableRecord]) -> None:
    """Save processing results to CSV report"""
    report_path = path_config.get_report_path("02_html_tables_fixed.csv")
    
    logger.info(f"Saving processing report to {report_path}")
    
    with open(report_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'product_handle', 'shopify_product_id', 'table_issue_type', 'nested_depth',
            'table_count', 'has_width_conflicts', 'has_overlapping_structure',
            'html_length_before', 'html_length_after', 'tables_restructured_count',
            'fix_applied', 'timestamp', 'status', 'error_message'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for record in processing_records:
            writer.writerow({
                'product_handle': record.product_handle,
                'shopify_product_id': record.shopify_product_id,
                'table_issue_type': record.table_issue_type,
                'nested_depth': record.nested_depth,
                'table_count': record.table_count,
                'has_width_conflicts': record.has_width_conflicts,
                'has_overlapping_structure': record.has_overlapping_structure,
                'html_length_before': record.html_length_before,
                'html_length_after': record.html_length_after,
                'tables_restructured_count': record.tables_restructured_count,
                'fix_applied': record.fix_applied,
                'timestamp': record.timestamp.isoformat(),
                'status': record.status,
                'error_message': record.error_message
            })
    
    # Generate summary
    successful = sum(1 for r in processing_records if r.status == "success")
    total = len(processing_records)
    logger.info(f"Processing report saved: {successful}/{total} products successfully fixed")


def main():
    """Main execution function"""
    logger.info("=" * 60)
    logger.info("Starting HTML Tables Fix Script")
    logger.info("=" * 60)
    
    try:
        # Phase 1: Analyze CSV files
        logger.info("Phase 1: Analyzing CSV files...")
        analysis_records = analyze_csv_for_html_tables()
        save_analysis_report(analysis_records)
        
        if not analysis_records:
            logger.info("No HTML table issues found in CSV files. Exiting.")
            return
        
        # Phase 2: Process via API (only if not dry run or user confirms)
        if shopify_config.dry_run:
            logger.info("DRY RUN mode enabled. Skipping API processing.")
            logger.info(f"Would process {len(analysis_records)} products with HTML table issues")
        else:
            logger.info("Phase 2: Processing via Shopify API...")
            response = input(f"Process {len(analysis_records)} products with HTML table issues via API? (y/N): ")
            if response.lower() == 'y':
                processing_records = process_html_tables_via_api(analysis_records)
                save_processing_report(processing_records)
            else:
                logger.info("API processing skipped by user")
        
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Script failed with error: {e}")
        raise
    finally:
        logger.info("HTML Tables Fix Script completed")


if __name__ == "__main__":
    main()