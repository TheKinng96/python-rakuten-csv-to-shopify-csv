#!/usr/bin/env python3
"""
Script to analyze HTML content for table elements that need font-weight: normal

This script:
1. Scans all CSV files to identify products with table elements
2. Analyzes tables that need font-weight: normal added to override bold styling
3. Generates JSON data for Node.js GraphQL operations
4. Outputs: shared/table_font_weight_to_normalize.json
"""
import re
import sys
from pathlib import Path
from typing import List, Dict, Any, Set

import pandas as pd
from bs4 import BeautifulSoup, Tag
from tqdm import tqdm

# Add utils to path
sys.path.append(str(Path(__file__).parent))
from utils.json_output import (
    save_json_report,
    create_html_table_fix_record,
    log_processing_summary,
    validate_json_structure
)


class TableFontNormalAnalyzer:
    """Analyzes HTML content for table elements that need font-weight: normal"""
    
    def __init__(self):
        # Focus on table element itself that should get font-weight: normal
        # We target the <table> tag directly to apply the style that cascades to all cells
        pass
        
    def analyze_html_content(self, html_content: str) -> Dict[str, Any]:
        """
        Analyze HTML content for table elements that need font-weight: normal
        Returns analysis results with tables found
        """
        if not html_content or pd.isna(html_content):
            return {'has_tables': False, 'table_elements_found': [], 'total_elements': 0}
        
        html_str = str(html_content)
        
        # Check if content has tables at all
        has_tables = bool(re.search(r'<table[^>]*>', html_str, re.IGNORECASE))
        if not has_tables:
            return {'has_tables': False, 'table_elements_found': [], 'total_elements': 0}
        
        table_elements_found = []
        
        try:
            soup = BeautifulSoup(html_str, 'html.parser')
            table_elements_found = self._find_table_elements(soup)
        except Exception as e:
            return {
                'has_tables': True,
                'table_elements_found': [],
                'total_elements': 0,
                'parsing_error': f'HTML parsing failed: {str(e)[:100]}'
            }
        
        return {
            'has_tables': True,
            'table_elements_found': table_elements_found,
            'total_elements': len(table_elements_found),
            'table_count': len(soup.find_all('table'))
        }
    
    def _find_table_elements(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Find ALL table elements for font-weight: normal addition"""
        elements_found = []
        
        # Find all table elements
        tables = soup.find_all('table')
        
        for table_idx, table in enumerate(tables):
            style = table.get('style', '')
            
            # Check if table already has font-weight: normal
            has_font_weight_normal = bool(re.search(r'font-weight:\s*normal', style, re.IGNORECASE))
            
            # Add table element to the list, marking if it needs modification
            elements_found.append({
                'table_index': table_idx + 1,
                'element_tag': 'table',
                'current_style': style[:200] if style else '',  # First 200 chars for reference
                'element_text': table.get_text(strip=True)[:100] if table.get_text(strip=True) else '',
                'needs_font_weight_normal': not has_font_weight_normal,  # Mark if it needs the style
                'position_in_table': f'table {table_idx + 1}'
            })
        
        return elements_found
    
    def _get_element_position(self, element) -> str:
        """Get a description of element's position (simplified for table elements)"""
        # Since we're only dealing with table elements now, return simple position
        tables = element.find_parent('body') or element.parent
        if tables:
            table_siblings = tables.find_all('table') if hasattr(tables, 'find_all') else []
            for i, table in enumerate(table_siblings):
                if table == element:
                    return f"table {i + 1}"
        return "table position unknown"
    
    def add_font_weight_normal_to_tables(self, html_content: str) -> Dict[str, Any]:
        """
        Add font-weight: normal to table elements
        Returns modified HTML and change details
        """
        if not html_content or pd.isna(html_content):
            return {
                'modified_html': '',
                'original_html': '',
                'changes_made': [],
                'bytes_added': 0
            }
        
        html_str = str(html_content)
        original_length = len(html_str)
        changes_made = []
        
        try:
            soup = BeautifulSoup(html_str, 'html.parser')
            
            # Find all table elements
            tables = soup.find_all('table')
            
            for table_idx, table in enumerate(tables):
                # Process the table element itself
                style = table.get('style', '')
                
                # Check if table already has font-weight: normal
                has_font_weight_normal = bool(re.search(r'font-weight:\s*normal', style, re.IGNORECASE))
                
                if not has_font_weight_normal:
                    original_style = style
                    
                    # Add font-weight: normal to the table style
                    if style.strip():
                        # Ensure style ends with semicolon before adding
                        if not style.rstrip().endswith(';'):
                            new_style = f"{style}; font-weight: normal;"
                        else:
                            new_style = f"{style} font-weight: normal;"
                    else:
                        new_style = "font-weight: normal;"
                    
                    table['style'] = new_style
                    
                    changes_made.append({
                        'table_index': table_idx + 1,
                        'element_tag': 'table',
                        'original_style': original_style,
                        'new_style': new_style,
                        'position': f'table {table_idx + 1}',
                        'action': 'font_weight_normal_added'
                    })
            
            modified_html = str(soup)
            bytes_added = len(modified_html) - original_length
            
            return {
                'modified_html': modified_html,
                'original_html': html_str,
                'changes_made': changes_made,
                'bytes_added': bytes_added,
                'elements_modified': len(changes_made)
            }
            
        except Exception as e:
            return {
                'modified_html': html_str,  # Return original on error
                'original_html': html_str,
                'changes_made': [],
                'bytes_added': 0,
                'error': f'Modification failed: {str(e)}'
            }


def get_csv_files() -> List[Path]:
    """Get all CSV files from data directory"""
    # Use absolute path resolution
    data_dir = Path(__file__).resolve().parent.parent.parent / "data"
    if not data_dir.exists():
        print(f"⚠️  Data directory not found: {data_dir}")
        return []
    csv_files = list(data_dir.glob("products_export_*.csv"))
    print(f"   📂 Found {len(csv_files)} CSV files in {data_dir}")
    return csv_files


def analyze_csv_files_for_table_font_normal() -> List[Dict[str, Any]]:
    """
    Analyze CSV files to find products with table elements that need font-weight: normal
    Returns list of records for JSON output
    """
    print("🔍 Analyzing CSV files for table elements needing font-weight: normal...")
    
    analyzer = TableFontNormalAnalyzer()
    font_normal_records = []
    csv_files = get_csv_files()
    total_rows = 0
    total_elements = 0
    
    for csv_file in csv_files:
        print(f"   📄 Processing {csv_file.name}...")
        
        try:
            # Read CSV in chunks
            chunk_size = 1000
            chunk_iter = pd.read_csv(
                csv_file,
                chunksize=chunk_size,
                encoding='utf-8',
                low_memory=False
            )
            
            file_records = {}  # Group by handle
            
            for chunk in chunk_iter:
                total_rows += len(chunk)
                
                for _, row in chunk.iterrows():
                    handle = row.get('Handle', '')
                    body_html = row.get('Body (HTML)', '')
                    
                    if pd.isna(body_html) or not body_html or pd.isna(handle) or not handle:
                        continue
                    
                    # Analyze table elements needing font-weight: normal
                    analysis = analyzer.analyze_html_content(str(body_html))
                    
                    # Process ALL products that have tables, regardless of existing font-weight
                    if analysis['has_tables']:
                        total_elements += analysis['total_elements']
                        
                        if handle not in file_records:
                            # Generate modified HTML with font-weight: normal
                            modification_result = analyzer.add_font_weight_normal_to_tables(str(body_html))
                            
                            file_records[handle] = {
                                'productHandle': handle,
                                'currentHtml': str(body_html),
                                'modifiedHtml': modification_result['modified_html'],
                                'originalLength': len(str(body_html)),
                                'modifiedLength': len(modification_result['modified_html']),
                                'bytesAdded': modification_result['bytes_added'],
                                'elementsModified': modification_result['elements_modified'],
                                'changeDetails': {
                                    'total_changes': len(modification_result['changes_made']),
                                    'table_count': analysis.get('table_count', 0),
                                    'elements_needing_normal': analysis['total_elements'],
                                    'changes_by_table': {}
                                },
                                'tableElementDetails': analysis['table_elements_found'],
                                'modificationsApplied': modification_result['changes_made']
                            }
                            
                            # Group changes by table
                            for change in modification_result['changes_made']:
                                table_idx = change['table_index']
                                if table_idx not in file_records[handle]['changeDetails']['changes_by_table']:
                                    file_records[handle]['changeDetails']['changes_by_table'][table_idx] = []
                                file_records[handle]['changeDetails']['changes_by_table'][table_idx].append(change)
            
            # Add file records to main list
            font_normal_records.extend(file_records.values())
            print(f"      ✅ Found {len(file_records)} products with tables needing font-weight: normal")
                        
        except Exception as e:
            print(f"      ❌ Error processing {csv_file.name}: {e}")
            continue
    
    print(f"\n📊 Analysis Summary:")
    print(f"   📄 CSV rows processed: {total_rows:,}")
    print(f"   🎯 Products with tables needing font-weight: normal: {len(font_normal_records)}")
    print(f"   📊 Total table elements to modify: {total_elements}")
    
    # Show size increase potential
    total_bytes_to_add = sum(record['bytesAdded'] for record in font_normal_records)
    total_original_size = sum(record['originalLength'] for record in font_normal_records)
    
    if total_original_size > 0:
        increase_percentage = (total_bytes_to_add / total_original_size) * 100
        print(f"   💾 Size increase: {total_bytes_to_add:,} bytes ({increase_percentage:.2f}%)")
    
    return font_normal_records


def main():
    """Main execution function"""
    print("=" * 70)
    print("🔍 TABLE FONT-WEIGHT NORMAL ANALYSIS (JSON OUTPUT)")
    print("=" * 70)
    
    try:
        # Phase 1: Analyze CSV files
        font_normal_records = analyze_csv_files_for_table_font_normal()
        
        if not font_normal_records:
            print("\n✅ No table elements found that need font-weight: normal!")
            # Save empty JSON file for consistency
            save_json_report([], "table_font_weight_to_normalize.json", "No table elements found needing font-weight: normal")
            return 0
        
        # Phase 2: Validate and save JSON
        print(f"\n💾 Saving JSON data for GraphQL processing...")
        
        required_fields = ['productHandle', 'currentHtml', 'modifiedHtml', 'bytesAdded']
        if not validate_json_structure(font_normal_records, required_fields):
            print("❌ JSON validation failed")
            return 1
        
        json_path = save_json_report(
            font_normal_records,
            "table_font_weight_to_normalize.json",
            f"Products with table elements needing font-weight: normal via GraphQL ({len(font_normal_records)} products)"
        )
        
        # Phase 3: Generate summary statistics
        print(f"\n📋 Summary Statistics:")
        
        total_changes = sum(record['changeDetails']['total_changes'] for record in font_normal_records)
        total_tables = sum(record['changeDetails']['table_count'] for record in font_normal_records)
        total_bytes = sum(record['bytesAdded'] for record in font_normal_records)
        
        print(f"   📊 Products to update: {len(font_normal_records)}")
        print(f"   📊 Total tables to modify: {total_tables}")
        print(f"   📊 Total font-weight: normal additions: {total_changes}")
        print(f"   💾 Total bytes to add: {total_bytes:,}")
        
        # Show top products by changes
        top_products = sorted(font_normal_records, key=lambda x: x['changeDetails']['total_changes'], reverse=True)[:10]
        
        print(f"\n📝 Top Products by Font-Weight Normal Additions:")
        print(f"{'Handle':<30} {'Changes':<8} {'Tables':<8} {'Bytes':<8}")
        print("-" * 60)
        
        for product in top_products:
            handle = product['productHandle'][:29]
            changes = product['changeDetails']['total_changes']
            tables = product['changeDetails']['table_count']
            bytes_added = product['bytesAdded']
            
            print(f"{handle:<30} {changes:<8} {tables:<8} {bytes_added:<8}")
        
        print(f"\n🎉 Analysis completed successfully!")
        print(f"   📄 JSON data saved to: {json_path}")
        print(f"   🚀 Ready for GraphQL processing")
        print(f"\n💡 Next step: cd ../../node && node src/07_add_table_font_normal.js --dry-run")
        
        return 0
        
    except KeyboardInterrupt:
        print("\n⏹️  Analysis interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())